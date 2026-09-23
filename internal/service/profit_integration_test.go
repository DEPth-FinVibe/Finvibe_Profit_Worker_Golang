package service_test

import (
	"context"
	"testing"
	"time"

	"finvibe-profit-worker-go/internal/metrics"
	"finvibe-profit-worker-go/internal/model"
	"finvibe-profit-worker-go/internal/redisstore"
	"finvibe-profit-worker-go/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

func TestPriceUpdateFanoutUpdatesPortfolioSnapshots(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	m := metrics.New(prometheus.NewRegistry())
	store := redisstore.New(rdb, m)
	profit := service.NewProfitService(store, m, 30*time.Second)

	must(t, rdb.SAdd(ctx, "stock:10:portfolios", "100").Err())
	must(t, rdb.Set(ctx, "portfolio:100:stock:10:quantity", "3", 0).Err())
	must(t, rdb.Set(ctx, "portfolio:100:stock:10:current-value", "300", 0).Err())
	must(t, rdb.HSet(ctx, "pf:100", map[string]any{"pv": int64(240), "cvp": "300", "ac": int64(1), "u": "7"}).Err())
	must(t, rdb.HSet(ctx, "usr:7", map[string]any{"pv": int64(240), "cvp": "300", "pc": int64(1)}).Err())
	must(t, rdb.SAdd(ctx, "user:7:portfolios", "100").Err())
	eventAt := time.Date(2026, 9, 6, 9, 0, 0, 0, time.UTC)

	_, err := profit.UpdateProfitsByStockPriceChanges(ctx, []model.ProfitCalculationRequest{{StockID: 10, NewPrice: 120, Timestamp: eventAt}})
	must(t, err)

	stockCV, err := mr.Get("portfolio:100:stock:10:current-value")
	must(t, err)
	assertDecimal(t, stockCV, "360")
	assertHash(t, mr, "pf:100", "cvp", "360")
	assertHash(t, mr, "pf:100", "scv:10", "360")
	assertHash(t, mr, "pf:100", "pv", "240")
	assertHash(t, mr, "pf:100", "ac", "1")
	assertHash(t, mr, "usr:7", "cvp", "360")
	assertHash(t, mr, "usr:7", "pr", "50")
	assertUpdatedAt(t, mr, "pf:100")
	assertUpdatedAt(t, mr, "usr:7")
	if ok, err := mr.SIsMember("dirty:portfolio-valuations", "100"); err != nil || !ok {
		t.Fatal("portfolio dirty set missing 100")
	}
	if ok, err := mr.SIsMember("dirty:user-valuations", "7"); err != nil || !ok {
		t.Fatal("user dirty set missing 7")
	}

	// 같은 가격 이벤트를 재처리해도 종목 현재가의 delta가 0이므로 평가액이 중복 반영되지 않는다.
	result, err := profit.UpdateProfitsByStockPriceChanges(ctx, []model.ProfitCalculationRequest{{StockID: 10, NewPrice: 120, Timestamp: eventAt}})
	must(t, err)
	if result.Skipped[metrics.ReasonDuplicatePriceEvent] != 1 {
		t.Fatalf("duplicate skips got %v", result.Skipped)
	}
	assertHash(t, mr, "pf:100", "cvp", "360")
	assertHash(t, mr, "pf:100", "scv:10", "360")
	assertHash(t, mr, "usr:7", "cvp", "360")
}

func TestPriceUpdateFreshnessSkipsStaleAndSameTimestampConflict(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	m := metrics.New(prometheus.NewRegistry())
	store := redisstore.New(rdb, m)
	profit := service.NewProfitService(store, m, 30*time.Second)

	must(t, rdb.SAdd(ctx, "stock:10:portfolios", "100").Err())
	must(t, rdb.Set(ctx, "portfolio:100:stock:10:quantity", "3", 0).Err())
	must(t, rdb.Set(ctx, "portfolio:100:stock:10:current-value", "300", 0).Err())
	must(t, rdb.HSet(ctx, "pf:100", map[string]any{"pv": "240", "cvp": "300", "ac": "1", "u": "7"}).Err())
	must(t, rdb.HSet(ctx, "usr:7", map[string]any{"pv": "240", "cvp": "300", "pc": "1"}).Err())
	must(t, rdb.SAdd(ctx, "user:7:portfolios", "100").Err())
	latestAt := time.Date(2026, 9, 6, 9, 0, 0, 0, time.UTC)

	result, err := profit.UpdateProfitsByStockPriceChanges(ctx, []model.ProfitCalculationRequest{{StockID: 10, NewPrice: 120, Timestamp: latestAt}})
	must(t, err)
	if result.Applied != 1 {
		t.Fatalf("applied got %d", result.Applied)
	}

	result, err = profit.UpdateProfitsByStockPriceChanges(ctx, []model.ProfitCalculationRequest{{StockID: 10, NewPrice: 90, Timestamp: latestAt.Add(-time.Second)}})
	must(t, err)
	if result.Skipped[metrics.ReasonStalePriceEvent] != 1 {
		t.Fatalf("stale skips got %v", result.Skipped)
	}

	result, err = profit.UpdateProfitsByStockPriceChanges(ctx, []model.ProfitCalculationRequest{{StockID: 10, NewPrice: 130, Timestamp: latestAt}})
	must(t, err)
	if result.Skipped[metrics.ReasonPriceTimestampConflict] != 1 {
		t.Fatalf("conflict skips got %v", result.Skipped)
	}

	assertHash(t, mr, "pf:100", "cvp", "360")
	assertHash(t, mr, "pf:100", "scv:10", "360")
	assertHash(t, mr, "usr:7", "cvp", "360")

	result, err = profit.UpdateProfitsByStockPriceChanges(ctx, []model.ProfitCalculationRequest{{StockID: 10, NewPrice: 130, Timestamp: latestAt.Add(time.Second)}})
	must(t, err)
	if result.Applied != 1 {
		t.Fatalf("newer applied got %d", result.Applied)
	}
	assertHash(t, mr, "pf:100", "cvp", "390")
}

func TestPriceUpdateRecalculatesOneUserAcrossMultiplePortfolios(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	m := metrics.New(prometheus.NewRegistry())
	store := redisstore.New(rdb, m)
	profit := service.NewProfitService(store, m, 30*time.Second)

	must(t, rdb.SAdd(ctx, "stock:10:portfolios", "100", "200").Err())
	must(t, rdb.SAdd(ctx, "user:7:portfolios", "100", "200").Err())
	must(t, rdb.Set(ctx, "portfolio:100:stock:10:quantity", "3", 0).Err())
	must(t, rdb.Set(ctx, "portfolio:100:stock:10:current-value", "300", 0).Err())
	must(t, rdb.Set(ctx, "portfolio:200:stock:10:quantity", "2", 0).Err())
	must(t, rdb.Set(ctx, "portfolio:200:stock:10:current-value", "200", 0).Err())
	must(t, rdb.HSet(ctx, "pf:100", map[string]any{"pv": "240", "cvp": "300", "ac": "1", "u": "7"}).Err())
	must(t, rdb.HSet(ctx, "pf:200", map[string]any{"pv": "180", "cvp": "200", "ac": "1", "u": "7"}).Err())
	must(t, rdb.HSet(ctx, "usr:7", map[string]any{"pv": "420", "cvp": "500", "pc": "2"}).Err())

	_, err := profit.UpdateProfitsByStockPriceChanges(ctx, []model.ProfitCalculationRequest{
		{StockID: 10, NewPrice: 120, Timestamp: time.Now()},
	})
	must(t, err)

	assertHash(t, mr, "pf:100", "cvp", "360")
	assertHash(t, mr, "pf:100", "scv:10", "360")
	assertHash(t, mr, "pf:200", "cvp", "240")
	assertHash(t, mr, "pf:200", "scv:10", "240")
	assertHash(t, mr, "usr:7", "cvp", "600")
	assertHash(t, mr, "usr:7", "cv", "600")
}

func assertDecimal(t *testing.T, got, want string) {
	t.Helper()
	gd, err := decimal.NewFromString(got)
	must(t, err)
	wd, err := decimal.NewFromString(want)
	must(t, err)
	if !gd.Equal(wd) {
		t.Fatalf("decimal got %s want %s", got, want)
	}
}
func assertHash(t *testing.T, mr *miniredis.Miniredis, key, field, want string) {
	t.Helper()
	got := mr.HGet(key, field)
	assertDecimal(t, got, want)
}
func assertUpdatedAt(t *testing.T, mr *miniredis.Miniredis, key string) {
	t.Helper()
	if updatedAt := mr.HGet(key, "ua"); updatedAt == "" {
		t.Fatalf("updatedAt is missing from %s", key)
	} else if _, err := time.Parse(time.RFC3339Nano, updatedAt); err != nil {
		t.Fatalf("invalid updatedAt in %s: %v", key, err)
	}
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
