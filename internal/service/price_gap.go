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
}

type PriceGapResult struct {
	BehindStocks  int
	MaxGapSeconds float64
}

func NewPriceGapChecker(store *redisstore.Store, m *metrics.Metrics, interval time.Duration) *PriceGapChecker {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &PriceGapChecker{store: store, metrics: m, interval: interval}
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

// Check는 모놀리식 버전이 반영 버전보다 크고 가격도 다른 종목을 뒤처진 것으로 센다.
// 모놀리식은 가격이 바뀐 틱만 Kafka로 보내므로, 가격이 같으면 버전이 앞서도 어긋난 것이 아니다.
// 한 번도 반영되지 않은 종목은 뒤처짐에 세지만 격차(초)는 정의할 수 없어 최대 격차에서 뺀다.
func (c *PriceGapChecker) Check(ctx context.Context) (PriceGapResult, error) {
	inputs, err := c.store.FetchPriceVersionGapInputs(ctx)
	if err != nil {
		c.metrics.RecordPriceVersionGapCheck(metrics.ResultFailure)
		return PriceGapResult{}, err
	}
	var result PriceGapResult
	for _, input := range inputs {
		applied := input.Applied
		if applied != nil && applied.Version >= input.PublishedVersion {
			continue
		}
		if applied != nil && decimal.NewFromInt(applied.Price).Equal(input.PublishedPrice) {
			continue
		}
		result.BehindStocks++
		if applied == nil {
			continue
		}
		gap := model.VersionTime(input.PublishedVersion).Sub(model.VersionTime(applied.Version)).Seconds()
		if gap > result.MaxGapSeconds {
			result.MaxGapSeconds = gap
		}
	}
	c.metrics.SetPriceVersionGap(result.BehindStocks, result.MaxGapSeconds)
	c.metrics.RecordPriceVersionGapCheck(metrics.ResultSuccess)
	return result, nil
}
