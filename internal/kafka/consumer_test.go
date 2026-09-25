package kafka

import (
	"context"
	"testing"
	"time"

	"finvibe-profit-worker-go/internal/config"
	"finvibe-profit-worker-go/internal/metrics"
	"finvibe-profit-worker-go/internal/redisstore"
	"finvibe-profit-worker-go/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

func TestRequiredWorkersIncludesStockPriceDLT(t *testing.T) {
	consumers := &Consumers{cfg: config.Config{
		StockConcurrency:         3,
		StockDLTConcurrency:      1,
		StockDLTEnabled:          true,
		TradeConcurrency:         1,
		PortfolioUserConcurrency: 1,
	}}

	if got := consumers.RequiredWorkers(); got != 6 {
		t.Fatalf("RequiredWorkers got %d want 6", got)
	}
}

func TestRequiredWorkersExcludesDisabledStockPriceDLT(t *testing.T) {
	consumers := &Consumers{cfg: config.Config{
		StockConcurrency:         3,
		StockDLTConcurrency:      1,
		StockDLTEnabled:          false,
		TradeConcurrency:         1,
		PortfolioUserConcurrency: 1,
	}}

	if got := consumers.RequiredWorkers(); got != 5 {
		t.Fatalf("RequiredWorkers got %d want 5", got)
	}
}

func TestStockPriceDLTStartsAtLatestOffset(t *testing.T) {
	consumers := &Consumers{cfg: config.Config{KafkaBrokers: []string{"kafka:9092"}}}

	cfg := consumers.consumerConfig("profit-worker-price-dlt", stockPriceDLTOffsetReset)

	if got := (*cfg)["auto.offset.reset"]; got != "latest" {
		t.Fatalf("auto.offset.reset got %v want latest", got)
	}
	if got := (*cfg)["group.id"]; got != "profit-worker-price-dlt" {
		t.Fatalf("group.id got %v", got)
	}
}

func TestStockPriceDLTUsesProfitCalculationPath(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	m := metrics.New(prometheus.NewRegistry())
	store := redisstore.New(rdb, m)
	profit := service.NewProfitService(store, m, 30*time.Second)
	consumers := New(config.Config{}, profit, nil, m, nil)

	if err := rdb.SAdd(ctx, "stock:10:portfolios", "100").Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, "portfolio:100:stock:10:quantity", "3", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, "portfolio:100:stock:10:current-value", "300", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(ctx, "pf:100", map[string]any{"pv": "240", "cvp": "300", "ac": "1", "u": "7"}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(ctx, "usr:7", map[string]any{"pv": "240", "cvp": "300", "pc": "1"}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.SAdd(ctx, "user:7:portfolios", "100").Err(); err != nil {
		t.Fatal(err)
	}

	err := consumers.handleStockDLT(ctx, []message{{value: []byte(`{"stockId":10,"price":120,"updatedAt":"2026-09-06T09:00:00"}`)}})
	if err != nil {
		t.Fatal(err)
	}

	if got, err := mr.Get("portfolio:100:stock:10:current-value"); err != nil || got != "360" {
		t.Fatalf("stock current value got %q err %v", got, err)
	}
	if got := mr.HGet("pf:100", "cvp"); got != "360" {
		t.Fatalf("portfolio current value got %q", got)
	}
	if got := mr.HGet("usr:7", "cvp"); got != "360" {
		t.Fatalf("user current value got %q", got)
	}
}

func TestStockBatchSelectsNewestTimestampInsteadOfLastMessage(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	m := metrics.New(prometheus.NewRegistry())
	store := redisstore.New(rdb, m)
	profit := service.NewProfitService(store, m, 30*time.Second)
	consumers := New(config.Config{}, profit, nil, m, nil)

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
		{value: []byte(`{"stockId":10,"price":120,"updatedAt":"2026-09-06T09:00:01Z"}`)},
		{value: []byte(`{"stockId":10,"price":90,"updatedAt":"2026-09-06T09:00:00Z"}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := mr.HGet("pf:100", "cvp"); got != "360" {
		t.Fatalf("portfolio current value got %q", got)
	}
}

func TestStockBatchSelectsHighestVersionWithinSameSecond(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	m := metrics.New(prometheus.NewRegistry())
	store := redisstore.New(rdb, m)
	profit := service.NewProfitService(store, m, 30*time.Second)
	consumers := New(config.Config{}, profit, nil, m, nil)

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

	// 같은 초의 두 틱이 역순으로 들어와도 버전이 큰 쪽을 고른다. 기존에는 충돌로 보고 나중 것을 버렸다.
	err := consumers.handleStock(ctx, []message{
		{value: []byte(`{"stockId":10,"price":121,"updatedAt":"2026-09-25T10:00:01","priceVersion":1790298001000001}`)},
		{value: []byte(`{"stockId":10,"price":120,"updatedAt":"2026-09-25T10:00:01","priceVersion":1790298001000000}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := mr.HGet("pf:100", "cvp"); got != "363" {
		t.Fatalf("portfolio current value got %q", got)
	}
}
