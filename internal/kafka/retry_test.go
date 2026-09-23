package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"finvibe-profit-worker-go/internal/config"
	"finvibe-profit-worker-go/internal/metrics"
	"finvibe-profit-worker-go/internal/redisstore"
	"finvibe-profit-worker-go/internal/service"
	"github.com/alicebob/miniredis/v2"
	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

var errRedisUnavailable = errors.New("redis unavailable")

type fakeConsumer struct {
	mu          sync.Mutex
	assigned    []ckafka.TopicPartition
	pollEvents  []ckafka.Event
	polls       int
	pauses      int
	resumes     int
	seeks       []ckafka.TopicPartition
	stored      []ckafka.Offset
	commits     int
	revokeAfter int // 이 횟수만큼 Poll한 뒤 할당을 모두 잃는다. 0이면 잃지 않는다.
}

func (f *fakeConsumer) Poll(int) ckafka.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.polls++
	if f.revokeAfter > 0 && f.polls >= f.revokeAfter {
		f.assigned = nil
	}
	if len(f.pollEvents) == 0 {
		return nil
	}
	event := f.pollEvents[0]
	f.pollEvents = f.pollEvents[1:]
	return event
}

func (f *fakeConsumer) Assignment() ([]ckafka.TopicPartition, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ckafka.TopicPartition(nil), f.assigned...), nil
}

func (f *fakeConsumer) Pause([]ckafka.TopicPartition) error  { f.pauses++; return nil }
func (f *fakeConsumer) Resume([]ckafka.TopicPartition) error { f.resumes++; return nil }
func (f *fakeConsumer) Seek(tp ckafka.TopicPartition, _ int) error {
	f.seeks = append(f.seeks, tp)
	return nil
}
func (f *fakeConsumer) StoreMessage(m *ckafka.Message) ([]ckafka.TopicPartition, error) {
	f.stored = append(f.stored, m.TopicPartition.Offset)
	return nil, nil
}
func (f *fakeConsumer) Commit() ([]ckafka.TopicPartition, error) { f.commits++; return nil, nil }

type fakeDLT struct {
	published []ckafka.Offset
	causes    []error
}

func (f *fakeDLT) Publish(_ context.Context, msg *ckafka.Message, cause error) error {
	f.published = append(f.published, msg.TopicPartition.Offset)
	f.causes = append(f.causes, cause)
	return nil
}

func testConsumers(dlt DeadLetterPublisher) *Consumers {
	c := New(config.Config{}, nil, nil, metrics.New(prometheus.NewRegistry()), dlt)
	c.retry = retryPolicy{
		priceInterval: time.Millisecond,
		priceRetries:  2,
		recordInitial: time.Millisecond,
		recordMax:     2 * time.Millisecond,
		recordElapsed: 30 * time.Millisecond,
	}
	return c
}

var testTopic = "trade.trade-executed.v1"

func partition(p int32) ckafka.TopicPartition {
	return ckafka.TopicPartition{Topic: &testTopic, Partition: p}
}

func records(offsets ...int64) []message {
	batch := make([]message, 0, len(offsets))
	for _, offset := range offsets {
		tp := partition(0)
		tp.Offset = ckafka.Offset(offset)
		batch = append(batch, message{value: []byte("{}"), raw: &ckafka.Message{TopicPartition: tp}})
	}
	return batch
}

func TestRecordRetriesThenDeadLettersOnlyTheFailingRecord(t *testing.T) {
	dlt := &fakeDLT{}
	c := testConsumers(dlt)
	consumer := &fakeConsumer{assigned: []ckafka.TopicPartition{partition(0)}}
	calls := map[ckafka.Offset]int{}

	c.processRecords(context.Background(), consumer, records(10, 11, 12), metrics.EventTrade, func(_ context.Context, msg message) error {
		calls[msg.raw.TopicPartition.Offset]++
		if msg.raw.TopicPartition.Offset == 11 {
			return errRedisUnavailable
		}
		return nil
	})

	if calls[10] != 1 || calls[12] != 1 || calls[11] < 2 {
		t.Fatalf("handler calls got %v", calls)
	}
	if len(dlt.published) != 1 || dlt.published[0] != 11 {
		t.Fatalf("dlt published %v, want only offset 11", dlt.published)
	}
	if !errors.Is(dlt.causes[0], errRedisUnavailable) {
		t.Fatalf("dlt cause got %v", dlt.causes[0])
	}
	if got := consumer.stored; len(got) != 3 || got[0] != 10 || got[1] != 11 || got[2] != 12 {
		t.Fatalf("stored offsets got %v", got)
	}
	// 재시도 대기 동안 파티션을 멈추고 Poll을 계속 불렀다.
	if consumer.pauses == 0 || consumer.polls == 0 || consumer.resumes == 0 {
		t.Fatalf("pauses=%d polls=%d resumes=%d", consumer.pauses, consumer.polls, consumer.resumes)
	}
}

func TestInvalidRecordGoesToDLTWithoutRetry(t *testing.T) {
	dlt := &fakeDLT{}
	c := testConsumers(dlt)
	consumer := &fakeConsumer{assigned: []ckafka.TopicPartition{partition(0)}}
	calls := 0

	c.processRecords(context.Background(), consumer, records(10), metrics.EventTrade, func(context.Context, message) error {
		calls++
		return invalidPayload("broken")
	})

	if calls != 1 || len(dlt.published) != 1 || consumer.polls != 0 {
		t.Fatalf("calls=%d published=%v polls=%d", calls, dlt.published, consumer.polls)
	}
}

func TestRecordRetrySucceedsAfterTransientFailure(t *testing.T) {
	dlt := &fakeDLT{}
	c := testConsumers(dlt)
	consumer := &fakeConsumer{assigned: []ckafka.TopicPartition{partition(0)}}
	failures := 2

	c.processRecords(context.Background(), consumer, records(10, 11), metrics.EventTrade, func(_ context.Context, msg message) error {
		if msg.raw.TopicPartition.Offset == 11 && failures > 0 {
			failures--
			return errRedisUnavailable
		}
		return nil
	})

	if len(dlt.published) != 0 {
		t.Fatalf("dlt published %v", dlt.published)
	}
	if len(consumer.stored) != 2 {
		t.Fatalf("stored offsets got %v", consumer.stored)
	}
	// 대기에 들어가기 전에 앞 레코드(10)까지 커밋했다.
	if consumer.commits < 2 {
		t.Fatalf("commits got %d", consumer.commits)
	}
}

func TestRecordRetryStopsWhenPartitionIsRevoked(t *testing.T) {
	dlt := &fakeDLT{}
	c := testConsumers(dlt)
	c.retry.recordInitial = 50 * time.Millisecond
	consumer := &fakeConsumer{assigned: []ckafka.TopicPartition{partition(0)}, revokeAfter: 1}

	c.processRecords(context.Background(), consumer, records(10, 11), metrics.EventTrade, func(context.Context, message) error {
		return errRedisUnavailable
	})

	if len(consumer.stored) != 0 || len(dlt.published) != 0 {
		t.Fatalf("stored=%v published=%v", consumer.stored, dlt.published)
	}
	// 처리하지 않은 레코드는 되감아 새 소유자가 다시 읽게 한다.
	if len(consumer.seeks) != 1 || consumer.seeks[0].Offset != 10 {
		t.Fatalf("seeks got %v", consumer.seeks)
	}
}

func TestMessagePolledDuringWaitIsSeekedBack(t *testing.T) {
	c := testConsumers(&fakeDLT{})
	other := partition(1)
	other.Offset = 42
	consumer := &fakeConsumer{
		assigned:   []ckafka.TopicPartition{partition(0), partition(1)},
		pollEvents: []ckafka.Event{&ckafka.Message{TopicPartition: other}},
	}
	failures := 1

	c.processRecords(context.Background(), consumer, records(10), metrics.EventTrade, func(context.Context, message) error {
		if failures > 0 {
			failures--
			return errRedisUnavailable
		}
		return nil
	})

	if len(consumer.seeks) != 1 || consumer.seeks[0].Partition != 1 || consumer.seeks[0].Offset != 42 {
		t.Fatalf("seeks got %v", consumer.seeks)
	}
}

func TestPriceBatchIsDroppedAfterShortRetries(t *testing.T) {
	c := testConsumers(nil)
	consumer := &fakeConsumer{}
	calls := 0

	c.processPriceBatch(context.Background(), consumer, records(10, 11), metrics.EventStockPrice, func(context.Context, []message) error {
		calls++
		return errRedisUnavailable
	})

	if calls != 3 {
		t.Fatalf("handler calls got %d want 3", calls)
	}
	if len(consumer.stored) != 2 || consumer.commits != 1 {
		t.Fatalf("stored=%v commits=%d", consumer.stored, consumer.commits)
	}
}

func TestTradeRecordWithBrokenPayloadIsInvalid(t *testing.T) {
	c := testConsumers(nil)

	err := c.handleTradeRecord(context.Background(), message{value: []byte("not-json")})

	if !errors.Is(err, errInvalidPayload) {
		t.Fatalf("got %v", err)
	}
}

func TestInvalidStockPriceEventDoesNotBlockOthers(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	m := metrics.New(prometheus.NewRegistry())
	store := redisstore.New(rdb, m)
	consumers := New(config.Config{}, service.NewProfitService(store, m, 30*time.Second), nil, m, nil)
	if err := rdb.SAdd(ctx, "stock:10:portfolios", "100").Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, "portfolio:100:stock:10:quantity", "3", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, "portfolio:100:stock:10:current-value", "300", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(ctx, "pf:100", map[string]any{"pv": "240", "cvp": "300", "ac": "1"}).Err(); err != nil {
		t.Fatal(err)
	}

	err := consumers.handleStock(ctx, []message{
		{value: []byte("not-json")},
		{value: []byte(`{"stockId":10,"price":120,"updatedAt":"2026-09-06T09:00:00Z"}`)},
		{value: []byte(`{"stockId":11,"price":120}`)},
	})

	if err != nil {
		t.Fatal(err)
	}
	if got := mr.HGet("pf:100", "cvp"); got != "360" {
		t.Fatalf("portfolio current value got %q", got)
	}
}
