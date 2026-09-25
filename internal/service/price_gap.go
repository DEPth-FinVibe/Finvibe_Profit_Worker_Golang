package service

import (
	"context"
	"log/slog"
	"time"

	"finvibe-profit-worker-go/internal/metrics"
	"finvibe-profit-worker-go/internal/model"
	"finvibe-profit-worker-go/internal/redisstore"
	"github.com/shopspring/decimal"
)

// PriceGapChecker는 모놀리식이 발행한 최신 시세를 수익률 반영이 따라잡았는지 주기적으로 점검한다.
// Kafka 유실이나 소비 정지처럼 이벤트가 오지 않아 생기는 어긋남은 소비 경로의 지표로는 보이지 않는다.
type PriceGapChecker struct {
	store    *redisstore.Store
	metrics  *metrics.Metrics
	interval time.Duration
	grace    time.Duration
	now      func() time.Time
}

type PriceGapResult struct {
	BehindStocks int
	// MaxUnappliedAgeSeconds는 발행됐지만 아직 반영되지 않은 시세 중 가장 오래 기다린 시간이다.
	MaxUnappliedAgeSeconds float64
}

// 발행 직후 반영 중인 틱은 뒤처짐이 아니다. 정상 반영 지연(p99 약 5초)보다 넉넉히 둔다.
const defaultPriceGapGrace = 10 * time.Second

func NewPriceGapChecker(store *redisstore.Store, m *metrics.Metrics, interval time.Duration) *PriceGapChecker {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &PriceGapChecker{store: store, metrics: m, interval: interval, grace: defaultPriceGapGrace, now: time.Now}
}

func (c *PriceGapChecker) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := c.Check(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("price version gap check failed", "err", err)
			}
		}
	}
}

// Check는 모놀리식 버전이 반영 버전보다 크고 가격도 다른 종목 중, 발행 후 유예 시간이 지나도록
// 반영되지 않은 종목을 뒤처진 것으로 센다.
// 모놀리식은 가격이 바뀐 틱만 Kafka로 보내므로, 가격이 같으면 버전이 앞서도 어긋난 것이 아니다.
// 버전 차이(발행 버전 − 반영 버전)는 같은 종목의 틱 간격이라 지연을 나타내지 못한다.
// 그래서 "발행된 시세가 반영되지 않은 채 지난 시간"을 잰다.
func (c *PriceGapChecker) Check(ctx context.Context) (PriceGapResult, error) {
	inputs, err := c.store.FetchPriceVersionGapInputs(ctx)
	if err != nil {
		c.metrics.RecordPriceVersionGapCheck(metrics.ResultFailure)
		return PriceGapResult{}, err
	}
	now := c.now()
	var result PriceGapResult
	for _, input := range inputs {
		applied := input.Applied
		if applied != nil && applied.Version >= input.PublishedVersion {
			continue
		}
		if applied != nil && decimal.NewFromInt(applied.Price).Equal(input.PublishedPrice) {
			continue
		}
		age := now.Sub(model.VersionTime(input.PublishedVersion))
		if age < c.grace {
			continue
		}
		result.BehindStocks++
		if seconds := age.Seconds(); seconds > result.MaxUnappliedAgeSeconds {
			result.MaxUnappliedAgeSeconds = seconds
		}
	}
	c.metrics.SetPriceVersionGap(result.BehindStocks, result.MaxUnappliedAgeSeconds)
	c.metrics.RecordPriceVersionGapCheck(metrics.ResultSuccess)
	return result, nil
}
