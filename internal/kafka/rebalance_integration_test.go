package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"finvibe-profit-worker-go/internal/config"
	"finvibe-profit-worker-go/internal/metrics"
	"finvibe-profit-worker-go/internal/redisstore"
	"finvibe-profit-worker-go/internal/service"
	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// 여러 파티션에 컨슈머 둘을 붙이고 처리 중 리밸런스를 일으켜, 최종 Redis 상태가 기대값과 정확히 같은지 확인한다.
// 리밸런스는 커밋 전에 처리된 레코드를 재전달하므로, 마커가 없으면 값이 부풀고 처리가 끊기면 값이 모자란다.
// KAFKA_TEST_BROKERS와 REDIS_TEST_CLUSTER_NODES가 모두 있어야 실행된다.
const (
	e2ePartitions = 3
	e2ePortfolios = 3
	e2eTradesEach = 20
	e2eStockID    = 10
	e2eTradePrice = 100
	e2eTradeQty   = 2
)

func TestTradesSurviveRebalanceAcrossPartitions(t *testing.T) {
	brokers := brokersOrSkip(t)
	nodes := os.Getenv("REDIS_TEST_CLUSTER_NODES")
	if nodes == "" {
		t.Skip("REDIS_TEST_CLUSTER_NODES is not set")
	}
	client := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(nodes, ",")})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
		return master.FlushAll(ctx).Err()
	}); err != nil {
		t.Fatal(err)
	}

	topic := fmt.Sprintf("verify.rebalance.%d", time.Now().UnixNano())
	group := topic + ".group"
	dlt, err := NewDLTProducer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer dlt.Close()
	createTopic(t, dlt, topic, e2ePartitions)
	if err := dlt.EnsureTopics(ctx, []string{topic}); err != nil {
		t.Fatal(err)
	}
	produceTrades(t, brokers, topic)

	m := metrics.New(prometheus.NewRegistry())
	store := redisstore.New(client, m)
	consumers := New(config.Config{KafkaBrokers: []string{brokers}, MaxPollRecords: 5, BatchMaxWait: 200 * time.Millisecond},
		nil, service.NewCacheService(store, m), m, dlt)

	// 첫 컨슈머로 처리를 시작하고, 일부 반영된 뒤 두 번째 컨슈머를 붙여 리밸런스를 일으킨다.
	firstCtx, stopFirst := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumers.runGroup(firstCtx, 1, topic, group, "earliest", func(ctx context.Context, consumer kafkaConsumer, batch []message) {
			consumers.processRecords(ctx, consumer, batch, metrics.EventTrade, consumers.handleTradeRecord)
		})
		<-firstCtx.Done()
	}()
	waitForProgress(ctx, t, client, 1)

	secondCtx, stopSecond := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumers.runGroup(secondCtx, 1, topic, group, "earliest", func(ctx context.Context, consumer kafkaConsumer, batch []message) {
			consumers.processRecords(ctx, consumer, batch, metrics.EventTrade, consumers.handleTradeRecord)
		})
		<-secondCtx.Done()
	}()

	// 처리 도중 첫 컨슈머를 내려 두 번째 리밸런스를 일으킨다. 커밋 전이던 레코드는 재전달된다.
	time.Sleep(2 * time.Second)
	stopFirst()

	waitForCompletion(ctx, t, client)
	stopSecond()
	wg.Wait()

	// 매수 20건 × 2주 × 100원 = 포트폴리오마다 수량 40, 금액 4,000원. 한 번만 반영돼야 한다.
	for portfolio := 1; portfolio <= e2ePortfolios; portfolio++ {
		assertClusterString(t, ctx, client, fmt.Sprintf("portfolio:%d:stock:%d:quantity", portfolio, e2eStockID), "40")
		assertClusterField(t, ctx, client, fmt.Sprintf("pf:%d", portfolio), "pv", "4000")
		assertClusterField(t, ctx, client, fmt.Sprintf("pf:%d", portfolio), "cvp", "4000")
		assertClusterField(t, ctx, client, fmt.Sprintf("pf:%d", portfolio), "scv:10", "4000")
		assertClusterField(t, ctx, client, fmt.Sprintf("pf:%d", portfolio), "ac", "1")
		assertClusterField(t, ctx, client, fmt.Sprintf("usr:user-%d", portfolio), "pv", "4000")
		assertClusterField(t, ctx, client, fmt.Sprintf("usr:user-%d", portfolio), "cvp", "4000")
	}

	if dead := pollOptional(t, brokers, topic+dltSuffix); dead != nil {
		t.Fatalf("unexpected dlt message: %q", dead.Value)
	}

	// 리밸런스로 파티션을 물려받은 컨슈머는 커밋되지 않은 구간을 다시 읽는다.
	// 그 재전달을 그대로 재현해, 이미 반영된 이벤트가 한 번 더 더해지지 않는지 확인한다.
	replayAll(ctx, t, consumers, topic, group+".replay")

	for portfolio := 1; portfolio <= e2ePortfolios; portfolio++ {
		assertClusterString(t, ctx, client, fmt.Sprintf("portfolio:%d:stock:%d:quantity", portfolio, e2eStockID), "40")
		assertClusterField(t, ctx, client, fmt.Sprintf("pf:%d", portfolio), "pv", "4000")
		assertClusterField(t, ctx, client, fmt.Sprintf("pf:%d", portfolio), "cvp", "4000")
		assertClusterField(t, ctx, client, fmt.Sprintf("pf:%d", portfolio), "scv:10", "4000")
		assertClusterField(t, ctx, client, fmt.Sprintf("pf:%d", portfolio), "ac", "1")
		assertClusterField(t, ctx, client, fmt.Sprintf("usr:user-%d", portfolio), "pv", "4000")
		assertClusterField(t, ctx, client, fmt.Sprintf("usr:user-%d", portfolio), "cvp", "4000")
	}
}

// replayAll은 토픽의 모든 레코드를 처음부터 다시 처리한다. 커밋 전에 파티션을 잃은 컨슈머의 재전달과 같다.
func replayAll(ctx context.Context, t *testing.T, consumers *Consumers, topic, group string) {
	t.Helper()
	consumer, err := consumers.newConsumer(group, "earliest")
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	if err := consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		t.Fatal(err)
	}
	replayed := 0
	want := e2ePortfolios * e2eTradesEach
	for replayed < want {
		if ctx.Err() != nil {
			t.Fatalf("replayed %d records, want %d", replayed, want)
		}
		batch, err := consumers.fetchBatch(ctx, consumer)
		if err != nil {
			t.Fatal(err)
		}
		if len(batch) == 0 {
			continue
		}
		consumers.processRecords(ctx, consumer, batch, metrics.EventTrade, consumers.handleTradeRecord)
		replayed += len(batch)
	}
}

func createTopic(t *testing.T, producer *DLTProducer, topic string, partitions int) {
	t.Helper()
	admin, err := ckafka.NewAdminClientFromProducer(producer.producer)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	results, err := admin.CreateTopics(context.Background(),
		[]ckafka.TopicSpecification{{Topic: topic, NumPartitions: partitions, ReplicationFactor: -1}},
		ckafka.SetAdminOperationTimeout(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range results {
		if code := result.Error.Code(); code != ckafka.ErrNoError && code != ckafka.ErrTopicAlreadyExists {
			t.Fatal(result.Error)
		}
	}
}

func produceTrades(t *testing.T, brokers, topic string) {
	t.Helper()
	producer, err := ckafka.NewProducer(&ckafka.ConfigMap{"bootstrap.servers": brokers, "acks": "all"})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	tradeID := 1
	for portfolio := 1; portfolio <= e2ePortfolios; portfolio++ {
		for i := 0; i < e2eTradesEach; i++ {
			payload, err := json.Marshal(map[string]any{
				"tradeId":     tradeID,
				"userId":      fmt.Sprintf("user-%d", portfolio),
				"type":        "BUY",
				"amount":      e2eTradeQty,
				"price":       e2eTradePrice,
				"stockId":     e2eStockID,
				"portfolioId": portfolio,
			})
			if err != nil {
				t.Fatal(err)
			}
			delivery := make(chan ckafka.Event, 1)
			// 같은 포트폴리오의 이벤트는 한 파티션에 모이도록 key를 준다.
			if err := producer.Produce(&ckafka.Message{
				TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: ckafka.PartitionAny},
				Key:            []byte(fmt.Sprintf("%d", portfolio)),
				Value:          payload,
			}, delivery); err != nil {
				t.Fatal(err)
			}
			if report := (<-delivery).(*ckafka.Message); report.TopicPartition.Error != nil {
				t.Fatal(report.TopicPartition.Error)
			}
			tradeID++
		}
	}
}

func waitForProgress(ctx context.Context, t *testing.T, client *redis.ClusterClient, atLeast int64) {
	t.Helper()
	for {
		if ctx.Err() != nil {
			t.Fatal("no progress before timeout")
		}
		quantity, err := client.Get(ctx, fmt.Sprintf("portfolio:1:stock:%d:quantity", e2eStockID)).Result()
		if err == nil {
			if parsed, convErr := strconv.ParseInt(quantity, 10, 64); convErr == nil && parsed >= atLeast {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func waitForCompletion(ctx context.Context, t *testing.T, client *redis.ClusterClient) {
	t.Helper()
	for {
		if ctx.Err() != nil {
			t.Fatal("trades were not all applied before timeout")
		}
		done := 0
		for portfolio := 1; portfolio <= e2ePortfolios; portfolio++ {
			if value, err := client.HGet(ctx, fmt.Sprintf("pf:%d", portfolio), "pv").Result(); err == nil && value == "4000" {
				done++
			}
		}
		if done == e2ePortfolios {
			// 재전달된 레코드가 더 반영되지 않는지 잠깐 더 지켜본다.
			time.Sleep(3 * time.Second)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func pollOptional(t *testing.T, brokers, topic string) *ckafka.Message {
	t.Helper()
	consumer, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          topic + ".check",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	if err := consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if msg, ok := consumer.Poll(500).(*ckafka.Message); ok {
			return msg
		}
	}
	return nil
}

func assertClusterString(t *testing.T, ctx context.Context, client *redis.ClusterClient, key, want string) {
	t.Helper()
	got, err := client.Get(ctx, key).Result()
	if err != nil {
		t.Fatalf("%s: %v", key, err)
	}
	if got != want {
		t.Fatalf("%s got %q want %q", key, got, want)
	}
}

func assertClusterField(t *testing.T, ctx context.Context, client *redis.ClusterClient, key, field, want string) {
	t.Helper()
	got, err := client.HGet(ctx, key, field).Result()
	if err != nil {
		t.Fatalf("%s %s: %v", key, field, err)
	}
	if got != want {
		t.Fatalf("%s %s got %q want %q", key, field, got, want)
	}
}
