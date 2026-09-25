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
)

func TestPriceGapCheckCountsOnlyPricesUnappliedBeyondGrace(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	m := metrics.New(prometheus.NewRegistry())
	checker := service.NewPriceGapChecker(redisstore.New(rdb, m), m, time.Second)
	// 1790298010 = 발행 시각. 점검은 30초 뒤에 한다.
	checker.SetClock(func() time.Time { return time.Unix(1790298040, 0) })
	at := time.Date(2026, 9, 25, 1, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)

	published := func(stockID, version, close string) {
		must(t, rdb.Set(ctx, "market:current-price-version:{stock:"+stockID+"}", version, 0).Err())
		must(t, rdb.Set(ctx, "market:current-price:{stock:"+stockID+"}", `{"stockId":`+stockID+`,"close":`+close+`,"priceVersion":`+version+`}`, 0).Err())
	}
	applied := func(stockID, version, price string) {
		must(t, rdb.HSet(ctx, "stock:{"+stockID+"}:price-application", map[string]any{"at": at, "price": price, "ver": version}).Err())
	}
	must(t, rdb.SAdd(ctx, "market:holding:stock-ids", "1", "2", "3", "4", "5", "6").Err())
	// 1: 최신까지 반영됨
	published("1", "1790298010000000", "100")
	applied("1", "1790298010000000", "100")
	// 2: 같은 가격의 틱만 이어져 버전은 앞서지만 Kafka로 발행되지 않았다
	published("2", "1790298010000003", "200.0")
	applied("2", "1790298001000000", "200")
	// 3: 가격이 바뀐 틱이 30초째 반영되지 않음. 직전 반영이 한참 전이어도 경과 시간만 본다
	published("3", "1790298010000000", "310")
	applied("3", "1790290000000000", "300")
	// 4: 한 번도 반영되지 않음
	published("4", "1790298010000000", "400")
	// 5: 모놀리식 현재가가 만료되어 판정 근거 없음
	must(t, rdb.Set(ctx, "market:current-price-version:{stock:5}", "1790298010000000", 0).Err())
	// 6: 3초 전에 발행되어 반영 중. 유예 시간 안이라 뒤처짐이 아니다
	published("6", "1790298037000000", "610")
	applied("6", "1790298001000000", "600")

	result, err := checker.Check(ctx)
	must(t, err)

	if result.BehindStocks != 2 {
		t.Fatalf("behind stocks got %d", result.BehindStocks)
	}
	if result.MaxUnappliedAgeSeconds != 30 {
		t.Fatalf("max unapplied age got %v", result.MaxUnappliedAgeSeconds)
	}
}

func TestPriceApplyLagRecordedFromVersionExecutionTime(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	profit := service.NewProfitService(redisstore.New(rdb, m), m, 30*time.Second)

	must(t, rdb.SAdd(ctx, "stock:10:portfolios", "100").Err())
	must(t, rdb.Set(ctx, "portfolio:100:stock:10:quantity", "3", 0).Err())
	must(t, rdb.HSet(ctx, "pf:100", map[string]any{"pv": "240", "cvp": "300", "ac": "1"}).Err())
	// 2초 전에 체결된 틱
	executedAt := time.Now().Add(-2 * time.Second).Truncate(time.Second)
	version := executedAt.Unix() * 1_000_000

	_, err := profit.UpdateProfitsByStockPriceChanges(ctx, []model.ProfitCalculationRequest{{StockID: 10, NewPrice: 120, Timestamp: executedAt, Version: version}})
	must(t, err)

	families, err := reg.Gather()
	must(t, err)
	for _, family := range families {
		if family.GetName() != "profit_worker_price_apply_lag_seconds" {
			continue
		}
		histogram := family.GetMetric()[0].GetHistogram()
		if histogram.GetSampleCount() != 1 || histogram.GetSampleSum() < 2 || histogram.GetSampleSum() > 10 {
			t.Fatalf("lag histogram got count=%d sum=%v", histogram.GetSampleCount(), histogram.GetSampleSum())
		}
		return
	}
	t.Fatal("price apply lag histogram not registered")
}
