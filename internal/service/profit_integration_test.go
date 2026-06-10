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
	profit := service.NewProfitService(store, m)

	must(t, rdb.SAdd(ctx, "stock:10:portfolios", "100").Err())
	must(t, rdb.Set(ctx, "portfolio:100:stock:10:quantity", "3", 0).Err())
	must(t, rdb.Set(ctx, "portfolio:100:stock:10:current-value", "300", 0).Err())
	must(t, rdb.HSet(ctx, "pf:100", map[string]any{"pv": int64(240), "cvp": "300", "ac": int64(1)}).Err())

	err := profit.UpdateProfitsByStockPriceChanges(ctx, []model.ProfitCalculationRequest{{StockID: 10, NewPrice: 120, Timestamp: time.Now()}})
	must(t, err)

	stockCV, err := mr.Get("portfolio:100:stock:10:current-value")
	must(t, err)
	assertDecimal(t, stockCV, "360")
	assertHash(t, mr, "pf:100", "cvp", "360")
	assertHash(t, mr, "pf:100", "pv", "240")
	assertHash(t, mr, "pf:100", "ac", "1")
	if ok, err := mr.SIsMember("dirty:portfolio-valuations", "100"); err != nil || !ok {
		t.Fatal("portfolio dirty set missing 100")
	}
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
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
