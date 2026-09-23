package kafka

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"finvibe-profit-worker-go/internal/config"
	"finvibe-profit-worker-go/internal/metrics"
	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"
)

// 실제 Kafka 브로커에서 DLT 발행, 토픽 생성, 재시도 대기 중 pause·poll·커밋을 확인한다.
// KAFKA_TEST_BROKERS=localhost:9092 처럼 브로커를 주면 실행된다.
func brokersOrSkip(t *testing.T) string {
	t.Helper()
	brokers := os.Getenv("KAFKA_TEST_BROKERS")
	if brokers == "" {
		t.Skip("KAFKA_TEST_BROKERS is not set")
	}
	return brokers
}

func TestEnsureTopicsCreatesDeadLetterTopics(t *testing.T) {
	brokers := brokersOrSkip(t)
	source := fmt.Sprintf("verify.ensure-topics.%d", time.Now().UnixNano())
	producer, err := NewDLTProducer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	if err := producer.EnsureTopics(context.Background(), []string{source}); err != nil {
		t.Fatal(err)
	}
	// 이미 있는 토픽에 다시 호출해도 에러가 아니다.
	if err := producer.EnsureTopics(context.Background(), []string{source}); err != nil {
		t.Fatalf("second EnsureTopics: %v", err)
	}

	metadata, err := producer.producer.GetMetadata(nil, true, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := metadata.Topics[source+dltSuffix]; !ok {
		t.Fatalf("%s was not created", source+dltSuffix)
	}
}

func TestFailingRecordIsDeadLetteredOnRealBroker(t *testing.T) {
	brokers := brokersOrSkip(t)
	topic := fmt.Sprintf("verify.trade.%d", time.Now().UnixNano())
	group := topic + ".group"

	dlt, err := NewDLTProducer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer dlt.Close()
	if err := dlt.EnsureTopics(context.Background(), []string{topic}); err != nil {
		t.Fatal(err)
	}
	produce(t, brokers, topic, []string{"first", "boom", "third"})

	consumers := New(config.Config{KafkaBrokers: []string{brokers}, MaxPollRecords: 10, BatchMaxWait: 500 * time.Millisecond},
		nil, nil, metrics.New(prometheus.NewRegistry()), dlt)
	consumers.retry = retryPolicy{recordInitial: 200 * time.Millisecond, recordMax: 200 * time.Millisecond, recordElapsed: time.Second}
	consumer, err := consumers.newConsumer(group, "earliest")
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	if err := consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	batch := fetchAll(ctx, t, consumers, consumer, 3)

	var handled []string
	consumers.processRecords(ctx, consumer, batch, metrics.EventTrade, func(_ context.Context, msg message) error {
		value := string(msg.value)
		handled = append(handled, value)
		if value == "boom" {
			return errRedisUnavailable
		}
		return nil
	})

	// 실패 레코드는 재시도 뒤 DLT로 가고, 앞뒤 레코드는 처리된다.
	if count := countValue(handled, "first") + countValue(handled, "third"); count != 2 {
		t.Fatalf("handled %v", handled)
	}
	if countValue(handled, "boom") < 2 {
		t.Fatalf("failing record was not retried: %v", handled)
	}

	dead := consumeOne(t, brokers, topic+dltSuffix)
	if string(dead.Value) != "boom" {
		t.Fatalf("dlt value got %q", dead.Value)
	}
	assertHeader(t, dead, "dlt-original-topic", topic)
	assertHeader(t, dead, "dlt-original-partition", "0")

	// 배치의 모든 레코드가 커밋됐다.
	committed, err := consumer.Committed([]ckafka.TopicPartition{{Topic: &topic, Partition: 0}}, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	if committed[0].Offset != 3 {
		t.Fatalf("committed offset got %v want 3", committed[0].Offset)
	}
}

func produce(t *testing.T, brokers, topic string, values []string) {
	t.Helper()
	producer, err := ckafka.NewProducer(&ckafka.ConfigMap{"bootstrap.servers": brokers, "acks": "all"})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	for _, value := range values {
		delivery := make(chan ckafka.Event, 1)
		if err := producer.Produce(&ckafka.Message{
			TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 0},
			Value:          []byte(value),
		}, delivery); err != nil {
			t.Fatal(err)
		}
		if report := (<-delivery).(*ckafka.Message); report.TopicPartition.Error != nil {
			t.Fatal(report.TopicPartition.Error)
		}
	}
}

func fetchAll(ctx context.Context, t *testing.T, consumers *Consumers, consumer *ckafka.Consumer, want int) []message {
	t.Helper()
	var batch []message
	for len(batch) < want {
		if ctx.Err() != nil {
			t.Fatalf("fetched %d records, want %d", len(batch), want)
		}
		fetched, err := consumers.fetchBatch(ctx, consumer)
		if err != nil {
			t.Fatal(err)
		}
		batch = append(batch, fetched...)
	}
	return batch
}

func consumeOne(t *testing.T, brokers, topic string) *ckafka.Message {
	t.Helper()
	consumer, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          topic + ".reader",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	if err := consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if msg, ok := consumer.Poll(500).(*ckafka.Message); ok {
			return msg
		}
	}
	t.Fatalf("no message on %s", topic)
	return nil
}

func assertHeader(t *testing.T, msg *ckafka.Message, key, want string) {
	t.Helper()
	for _, header := range msg.Headers {
		if header.Key == key {
			if string(header.Value) != want {
				t.Fatalf("header %s got %q want %q", key, header.Value, want)
			}
			return
		}
	}
	t.Fatalf("header %s is missing", key)
}

func countValue(values []string, want string) int {
	count := 0
	for _, value := range values {
		if value == want {
			count++
		}
	}
	return count
}
