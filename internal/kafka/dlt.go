package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const dltSuffix = ".DLT"

// DeadLetterPublisher는 재시도를 포기한 레코드를 <원본 토픽>.DLT로 보낸다.
type DeadLetterPublisher interface {
	Publish(ctx context.Context, msg *ckafka.Message, cause error) error
}

type DLTProducer struct {
	producer *ckafka.Producer
	timeout  time.Duration
}

func NewDLTProducer(brokers string) (*DLTProducer, error) {
	producer, err := ckafka.NewProducer(&ckafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"acks":               "all",
		"enable.idempotence": true,
	})
	if err != nil {
		return nil, err
	}
	go func() {
		// 메시지별 delivery 채널을 쓰므로 여기에는 클라이언트 수준 에러만 온다.
		for event := range producer.Events() {
			if kafkaErr, ok := event.(ckafka.Error); ok {
				slog.Error("dlt producer", "err", kafkaErr)
			}
		}
	}()
	return &DLTProducer{producer: producer, timeout: 10 * time.Second}, nil
}

// EnsureTopics는 DLT 토픽이 없으면 만든다. 파티션 1개, 복제 수는 브로커 기본값이다.
func (p *DLTProducer) EnsureTopics(ctx context.Context, sourceTopics []string) error {
	admin, err := ckafka.NewAdminClientFromProducer(p.producer)
	if err != nil {
		return err
	}
	defer admin.Close()

	specs := make([]ckafka.TopicSpecification, 0, len(sourceTopics))
	for _, topic := range sourceTopics {
		specs = append(specs, ckafka.TopicSpecification{Topic: topic + dltSuffix, NumPartitions: 1, ReplicationFactor: -1})
	}
	results, err := admin.CreateTopics(ctx, specs, ckafka.SetAdminOperationTimeout(10*time.Second))
	if err != nil {
		return err
	}
	var errs []error
	for _, result := range results {
		if code := result.Error.Code(); code != ckafka.ErrNoError && code != ckafka.ErrTopicAlreadyExists {
			errs = append(errs, fmt.Errorf("create %s: %w", result.Topic, result.Error))
		}
	}
	return errors.Join(errs...)
}

func (p *DLTProducer) Publish(ctx context.Context, msg *ckafka.Message, cause error) error {
	topic := *msg.TopicPartition.Topic + dltSuffix
	headers := append([]ckafka.Header{}, msg.Headers...)
	headers = append(headers,
		ckafka.Header{Key: "dlt-original-topic", Value: []byte(*msg.TopicPartition.Topic)},
		ckafka.Header{Key: "dlt-original-partition", Value: []byte(strconv.Itoa(int(msg.TopicPartition.Partition)))},
		ckafka.Header{Key: "dlt-original-offset", Value: []byte(strconv.FormatInt(int64(msg.TopicPartition.Offset), 10))},
		ckafka.Header{Key: "dlt-exception-type", Value: []byte(fmt.Sprintf("%T", cause))},
		ckafka.Header{Key: "dlt-exception-message", Value: []byte(cause.Error())},
	)

	delivery := make(chan ckafka.Event, 1)
	err := p.producer.Produce(&ckafka.Message{
		TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: ckafka.PartitionAny},
		Key:            msg.Key,
		Value:          msg.Value,
		Headers:        headers,
	}, delivery)
	if err != nil {
		return err
	}

	timer := time.NewTimer(p.timeout)
	defer timer.Stop()
	select {
	case event := <-delivery:
		return event.(*ckafka.Message).TopicPartition.Error
	case <-timer.C:
		return fmt.Errorf("dlt publish to %s timed out", topic)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *DLTProducer) Close() {
	p.producer.Flush(5_000)
	p.producer.Close()
}
