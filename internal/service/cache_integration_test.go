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

func TestTradeUpdatesPortfolioAndUserSnapshotsOnce(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := redisstore.New(rdb, metrics.New(prometheus.NewRegistry()))
	cache := service.NewCacheService(store, metrics.New(prometheus.NewRegistry()))

	must(t, rdb.HSet(ctx, "pf:100", map[string]any{"u": "7", "pv": "0", "cvp": "0", "ac": "0"}).Err())
	must(t, rdb.HSet(ctx, "usr:7", map[string]any{"pv": "0", "cvp": "0", "pc": "1"}).Err())
	request := model.PortfolioCacheUpdateRequest{
		TradeID: 1, PortfolioID: 100, StockID: 10, UserID: "7",
		Type: model.StockBuy, Price: 120, Quantity: decimal.NewFromInt(3),
	}

	must(t, cache.UpdatePortfolioCaches(ctx, []model.PortfolioCacheUpdateRequest{request}))
	must(t, cache.UpdatePortfolioCaches(ctx, []model.PortfolioCacheUpdateRequest{request}))

	assertHash(t, mr, "pf:100", "pv", "360")
	assertHash(t, mr, "pf:100", "cv", "360")
	assertHash(t, mr, "pf:100", "scv:10", "360")
	assertHash(t, mr, "usr:7", "pv", "360")
	assertHash(t, mr, "usr:7", "cv", "360")
	assertUpdatedAt(t, mr, "pf:100")
	assertUpdatedAt(t, mr, "usr:7")
	if ok, _ := mr.SIsMember("dirty:portfolio-valuations", "100"); !ok {
		t.Fatal("portfolio dirty set missing 100")
	}
	if ok, _ := mr.SIsMember("dirty:user-valuations", "7"); !ok {
		t.Fatal("user dirty set missing 7")
	}
}

func TestPortfolioCreateAndDeleteAreIdempotent(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	m := metrics.New(prometheus.NewRegistry())
	store := redisstore.New(rdb, m)
	cache := service.NewCacheService(store, m)
	must(t, rdb.HSet(ctx, "pf:100", map[string]any{"pv": "240", "cvp": "300", "ac": "1"}).Err())

	created := model.UserCacheUpdateRequest{UserID: "7", PortfolioID: 100, Type: model.PortfolioCreated}
	must(t, cache.UpdateUserCaches(ctx, []model.UserCacheUpdateRequest{created, created}))
	assertHash(t, mr, "usr:7", "pv", "240")
	assertHash(t, mr, "usr:7", "cv", "300")
	assertHash(t, mr, "usr:7", "pc", "1")

	deleted := model.UserCacheUpdateRequest{UserID: "7", PortfolioID: 100, Type: model.PortfolioDeleted}
	must(t, cache.UpdateUserCaches(ctx, []model.UserCacheUpdateRequest{deleted, deleted}))
	assertHash(t, mr, "usr:7", "pv", "0")
	assertHash(t, mr, "usr:7", "cv", "0")
	assertHash(t, mr, "usr:7", "pc", "0")
	if ok, _ := mr.SIsMember("dirty:portfolio-valuation-deletions", "100"); !ok {
		t.Fatal("portfolio deletion dirty set missing 100")
	}
}

func TestPortfolioCreatedAfterTradeOnlyAddsPortfolioCount(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	m := metrics.New(prometheus.NewRegistry())
	store := redisstore.New(rdb, m)
	cache := service.NewCacheService(store, m)
	must(t, rdb.HSet(ctx, "pf:100", map[string]any{"pv": "0", "cvp": "0", "ac": "0"}).Err())
	must(t, rdb.HSet(ctx, "usr:7", map[string]any{"pv": "0", "cvp": "0", "pc": "0"}).Err())

	must(t, cache.UpdatePortfolioCaches(ctx, []model.PortfolioCacheUpdateRequest{{
		TradeID: 1, PortfolioID: 100, StockID: 10, UserID: "7",
		Type: model.StockBuy, Price: 120, Quantity: decimal.NewFromInt(3),
	}}))
	assertHash(t, mr, "usr:7", "pv", "360")
	assertHash(t, mr, "usr:7", "pc", "0")
	assertHash(t, mr, "pf:100", "ucp", "1")

	created := model.UserCacheUpdateRequest{UserID: "7", PortfolioID: 100, Type: model.PortfolioCreated}
	must(t, cache.UpdateUserCaches(ctx, []model.UserCacheUpdateRequest{created, created}))
	assertHash(t, mr, "usr:7", "pv", "360")
	assertHash(t, mr, "usr:7", "cv", "360")
	assertHash(t, mr, "usr:7", "pc", "1")
	if mr.HGet("pf:100", "ucp") != "" {
		t.Fatal("portfolio count pending marker was not cleared")
	}
}

func TestStockVersionKeptOnTradeDeltaAndRemovedWithHolding(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	m := metrics.New(prometheus.NewRegistry())
	store := redisstore.New(rdb, m)
	cache := service.NewCacheService(store, m)
	profit := service.NewProfitService(store, m, 30*time.Second)

	must(t, rdb.HSet(ctx, "pf:100", map[string]any{"u": "7", "pv": "0", "cvp": "0", "ac": "0"}).Err())
	must(t, rdb.HSet(ctx, "usr:7", map[string]any{"pv": "0", "cvp": "0", "pc": "1"}).Err())
	buy := model.PortfolioCacheUpdateRequest{
		TradeID: 1, PortfolioID: 100, StockID: 10, UserID: "7",
		Type: model.StockBuy, Price: 120, Quantity: decimal.NewFromInt(3),
	}
	must(t, cache.UpdatePortfolioCaches(ctx, []model.PortfolioCacheUpdateRequest{buy}))
	// 가격 틱이 오기 전에는 시세 근거가 없다.
	if mr.HGet("pf:100", "sv:10") != "" {
		t.Fatal("stock version should be absent before any price tick")
	}

	_, err := profit.UpdateProfitsByStockPriceChanges(ctx, []model.ProfitCalculationRequest{
		{StockID: 10, NewPrice: 130, Timestamp: time.Date(2026, 9, 25, 10, 0, 1, 0, time.UTC), Version: 1790298001000000},
	})
	must(t, err)
	assertHash(t, mr, "pf:100", "scv:10", "390")
	assertHash(t, mr, "pf:100", "sv:10", "1790298001000000")

	// 추가 매수는 체결가 증감분만 더하고 시세 근거 버전은 그대로 둔다.
	addBuy := buy
	addBuy.TradeID, addBuy.Price, addBuy.Quantity = 2, 125, decimal.NewFromInt(1)
	must(t, cache.UpdatePortfolioCaches(ctx, []model.PortfolioCacheUpdateRequest{addBuy}))
	assertHash(t, mr, "pf:100", "scv:10", "515")
	assertHash(t, mr, "pf:100", "sv:10", "1790298001000000")

	sellAll := buy
	sellAll.TradeID, sellAll.Type, sellAll.Price, sellAll.Quantity = 3, model.StockSell, 130, decimal.NewFromInt(4)
	must(t, cache.UpdatePortfolioCaches(ctx, []model.PortfolioCacheUpdateRequest{sellAll}))
	if mr.HGet("pf:100", "scv:10") != "" || mr.HGet("pf:100", "sv:10") != "" {
		t.Fatalf("stock value and version should be removed, got scv=%q sv=%q", mr.HGet("pf:100", "scv:10"), mr.HGet("pf:100", "sv:10"))
	}
}
