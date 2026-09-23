package service_test

import (
	"context"
	"testing"

	"finvibe-profit-worker-go/internal/metrics"
	"finvibe-profit-worker-go/internal/model"
	"finvibe-profit-worker-go/internal/redisstore"
	"finvibe-profit-worker-go/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

func newCacheFixture(t *testing.T) (context.Context, *miniredis.Miniredis, *redis.Client, *redisstore.Store, *service.CacheService) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	m := metrics.New(prometheus.NewRegistry())
	store := redisstore.New(rdb, m)
	return context.Background(), mr, rdb, store, service.NewCacheService(store, m)
}

// 포트폴리오 쪽 쓰기까지 끝나고 유저 쓰기 전에 끊긴 매수를 재처리해도 한 번만 반영된다.
func TestTradeRetryAfterPartialFailureAppliesOnce(t *testing.T) {
	ctx, mr, rdb, store, cache := newCacheFixture(t)
	must(t, rdb.HSet(ctx, "pf:100", map[string]any{"u": "7", "pv": "0", "cvp": "0", "ac": "0"}).Err())
	must(t, rdb.HSet(ctx, "usr:7", map[string]any{"pv": "0", "cvp": "0", "pc": "1"}).Err())
	amount := decimal.NewFromInt(360)

	// 첫 시도: 수량과 포트폴리오 해시까지 반영한 뒤 중단됐다.
	_, err := store.IncreaseStockQuantity(ctx, "trade:1", 10, 100, decimal.NewFromInt(3))
	must(t, err)
	must(t, store.ApplyPortfolioTradeTotals(ctx, "trade:1", 100, 10, redisstore.PortfolioTradeTotals{
		PurchasedValue: 360, CurrentValue: amount, StockCurrentValue: amount, AssetCount: 1,
	}))

	must(t, cache.UpdatePortfolioCaches(ctx, []model.PortfolioCacheUpdateRequest{{
		TradeID: 1, PortfolioID: 100, StockID: 10, UserID: "7",
		Type: model.StockBuy, Price: 120, Quantity: decimal.NewFromInt(3),
	}}))

	if got, _ := mr.Get("portfolio:100:stock:10:quantity"); got != "3" {
		t.Fatalf("quantity got %q", got)
	}
	assertHash(t, mr, "pf:100", "pv", "360")
	assertHash(t, mr, "pf:100", "cvp", "360")
	assertHash(t, mr, "pf:100", "scv:10", "360")
	assertHash(t, mr, "pf:100", "ac", "1")
	assertHash(t, mr, "usr:7", "pv", "360")
	assertHash(t, mr, "usr:7", "cvp", "360")
	if got, _ := mr.Get("portfolio:100:stock:10:current-value"); got != "360" {
		t.Fatalf("legacy current value got %q", got)
	}
}

// 매수 10 → 매도 10 → 매수 5 뒤 그 매도를 늦게 재처리해도 수량과 보유 인덱스가 유지된다.
func TestLateSellReplayKeepsHoldingBoughtAgain(t *testing.T) {
	ctx, mr, _, store, _ := newCacheFixture(t)

	_, err := store.IncreaseStockQuantity(ctx, "trade:1", 10, 100, decimal.NewFromInt(10))
	must(t, err)
	removed, err := store.DecreaseStockQuantity(ctx, "trade:2", 10, 100, decimal.NewFromInt(10))
	must(t, err)
	if !removed {
		t.Fatal("first sell should remove the holding")
	}
	_, err = store.IncreaseStockQuantity(ctx, "trade:3", 10, 100, decimal.NewFromInt(5))
	must(t, err)

	removed, err = store.DecreaseStockQuantity(ctx, "trade:2", 10, 100, decimal.NewFromInt(10))
	must(t, err)

	if !removed {
		t.Fatal("replay should return the first result")
	}
	if got, _ := mr.Get("portfolio:100:stock:10:quantity"); got != "5" {
		t.Fatalf("quantity got %q", got)
	}
	if ok, _ := mr.SIsMember("stock:10:portfolios", "100"); !ok {
		t.Fatal("reverse index lost portfolio 100")
	}
}

// 유저 합계만 반영하고 매핑 전에 끊긴 생성 이벤트를 재처리해도 합계가 두 번 더해지지 않는다.
func TestPortfolioCreateRetryAfterTotalsAppliedOnce(t *testing.T) {
	ctx, mr, rdb, store, cache := newCacheFixture(t)
	must(t, rdb.HSet(ctx, "pf:100", map[string]any{"pv": "240", "cvp": "300", "ac": "1"}).Err())
	must(t, store.ApplyUserTotals(ctx, "portfolio-user:100:CREATED", "7", 240, decimal.NewFromInt(300), 1))

	created := model.UserCacheUpdateRequest{UserID: "7", PortfolioID: 100, Type: model.PortfolioCreated}
	must(t, cache.UpdateUserCaches(ctx, []model.UserCacheUpdateRequest{created}))

	assertHash(t, mr, "usr:7", "pv", "240")
	assertHash(t, mr, "usr:7", "cvp", "300")
	assertHash(t, mr, "usr:7", "pc", "1")
	assertHash(t, mr, "pf:100", "u", "7")
}

// 매핑 필드만 지우고 유저 목록 제거 전에 끊긴 삭제 이벤트를 재처리하면 남은 정리가 끝난다.
func TestPortfolioDeleteRetryFinishesCleanup(t *testing.T) {
	ctx, mr, rdb, store, cache := newCacheFixture(t)
	must(t, rdb.HSet(ctx, "pf:100", map[string]any{"pv": "240", "cvp": "300", "ac": "1"}).Err())
	must(t, rdb.SAdd(ctx, "portfolio:100:stocks", "10").Err())
	created := model.UserCacheUpdateRequest{UserID: "7", PortfolioID: 100, Type: model.PortfolioCreated}
	must(t, cache.UpdateUserCaches(ctx, []model.UserCacheUpdateRequest{created}))

	// 첫 시도: 유저 합계를 빼고 매핑 필드만 지운 뒤 중단됐다.
	must(t, store.ApplyUserTotals(ctx, "portfolio-user:100:DELETED", "7", -240, decimal.NewFromInt(-300), -1))
	must(t, rdb.HDel(ctx, "pf:100", "u").Err())

	deleted := model.UserCacheUpdateRequest{UserID: "7", PortfolioID: 100, Type: model.PortfolioDeleted}
	must(t, cache.UpdateUserCaches(ctx, []model.UserCacheUpdateRequest{deleted}))

	assertHash(t, mr, "usr:7", "pv", "0")
	assertHash(t, mr, "usr:7", "pc", "0")
	if ok, _ := mr.SIsMember("user:7:portfolios", "100"); ok {
		t.Fatal("deleted portfolio is still in the user's portfolio list")
	}
	if mr.Exists("portfolio:100:stocks") {
		t.Fatal("portfolio state was not deleted")
	}
}
