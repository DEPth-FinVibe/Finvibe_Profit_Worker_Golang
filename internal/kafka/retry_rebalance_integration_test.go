package kafka

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"finvibe-profit-worker-go/internal/config"
	"finvibe-profit-worker-go/internal/metrics"
	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"
)

// 긴 재시도 중에 다른 컨슈머가 같은 그룹에 합류하는 상황을 실제 브로커에서 확인한다.
// 재시도 대기는 파티션을 pause하고 Poll을 계속하므로 컨슈머가 그룹에서 빠지지 않아야 하고,
// 파티션을 잃으면 재시도를 멈춰야 한다. 어느 경로든 레코드는 정확히 한 번 반영되고 DLT로 가지 않아야 한다.
func TestLongRetrySurvivesAnotherConsumerJoining(t *testing.T) {
	brokers := brokersOrSkip(t)
	topic := fmt.Sprintf("verify.retry-rebalance.%d", time.Now().UnixNano())
	group := topic + ".group"

	dlt, err := NewDLTProducer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer dlt.Close()
	createTopic(t, dlt, topic, 2)
	if err := dlt.EnsureTopics(context.Background(), []string{topic}); err != nil {
		t.Fatal(err)
	}
	produce(t, brokers, topic, []string{"slow"})

	consumers := New(config.Config{KafkaBrokers: []string{brokers}, MaxPollRecords: 10, BatchMaxWait: 200 * time.Millisecond},
		nil, nil, metrics.New(prometheus.NewRegistry()), dlt)
	// 재시도를 30초까지 이어가며 그 사이에 두 번째 컨슈머를 붙인다.
	consumers.retry = retryPolicy{recordInitial: 500 * time.Millisecond, recordMax: time.Second, recordElapsed: 30 * time.Second}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var (
		applied  atomic.Int64
		attempts atomic.Int64
		failing  atomic.Bool
	)
	failing.Store(true)
	handler := func(_ context.Context, msg message) error {
		attempts.Add(1)
		if failing.Load() {
			return errRedisUnavailable
		}
		applied.Add(1)
		return nil
	}

	var wg sync.WaitGroup
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumers.runGroup(firstCtx, 1, topic, group, "earliest", func(ctx context.Context, consumer kafkaConsumer, batch []message) {
			consumers.processRecords(ctx, consumer, batch, metrics.EventTrade, handler)
		})
		<-firstCtx.Done()
	}()

	// 첫 컨슈머가 재시도에 들어갈 때까지 기다린다.
	waitUntil(ctx, t, "retry started", func() bool { return attempts.Load() >= 2 })

	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumers.runGroup(secondCtx, 1, topic, group, "earliest", func(ctx context.Context, consumer kafkaConsumer, batch []message) {
			consumers.processRecords(ctx, consumer, batch, metrics.EventTrade, handler)
		})
		<-secondCtx.Done()
	}()

	// 합류 뒤에도 재시도가 이어지는지 확인하고, 장애를 해소한다.
	before := attempts.Load()
	time.Sleep(5 * time.Second)
	if attempts.Load() <= before {
		t.Fatalf("retry stopped after the second consumer joined: attempts %d", attempts.Load())
	}
	failing.Store(false)

	waitUntil(ctx, t, "record applied", func() bool { return applied.Load() >= 1 })
	// 재전달로 한 번 더 반영되지 않는지 잠깐 더 본다.
	time.Sleep(5 * time.Second)
	if got := applied.Load(); got != 1 {
		t.Fatalf("applied %d times, want 1", got)
	}

	stopFirst()
	stopSecond()
	wg.Wait()

	if dead := pollOptional(t, brokers, topic+dltSuffix); dead != nil {
		t.Fatalf("unexpected dlt message: %q", dead.Value)
	}
	committed := committedOffset(t, brokers, group, topic, 0)
	if committed != 1 {
		t.Fatalf("committed offset got %v want 1", committed)
	}
}

func waitUntil(ctx context.Context, t *testing.T, what string, done func() bool) {
	t.Helper()
	for !done() {
		if ctx.Err() != nil {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func committedOffset(t *testing.T, brokers, group, topic string, partition int32) ckafka.Offset {
	t.Helper()
	consumer, err := ckafka.NewConsumer(&ckafka.ConfigMap{"bootstrap.servers": brokers, "group.id": group})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	committed, err := consumer.Committed([]ckafka.TopicPartition{{Topic: &topic, Partition: partition}}, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	return committed[0].Offset
}
