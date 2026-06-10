package service

import (
	"context"
	"fmt"
	"time"

	"finvibe-profit-worker-go/internal/metrics"
	"finvibe-profit-worker-go/internal/model"
	"finvibe-profit-worker-go/internal/redisstore"

	"github.com/shopspring/decimal"
)

type CacheService struct {
	store   *redisstore.Store
	metrics *metrics.Metrics
}

func NewCacheService(s *redisstore.Store, m *metrics.Metrics) *CacheService {
	return &CacheService{s, m}
}

func (s *CacheService) UpdatePortfolioCaches(ctx context.Context, reqs []model.PortfolioCacheUpdateRequest) error {
	start := time.Now()
	result := metrics.ResultFailure
	defer func() { s.metrics.ObserveService(metrics.OpPortfolioCache, result, start) }()
	changedPF := map[int64]struct{}{}
	for _, r := range reqs {
		if err := s.applyPortfolio(ctx, r); err != nil {
			return err
		}
		changedPF[r.PortfolioID] = struct{}{}
	}
	for id := range changedPF {
		if err := s.savePortfolioSnapshot(ctx, id); err != nil {
			return err
		}
	}
	s.metrics.RecordAffectedPortfolios(metrics.OpPortfolioCache, len(reqs))
	result = metrics.ResultSuccess
	return nil
}

func (s *CacheService) applyPortfolio(ctx context.Context, r model.PortfolioCacheUpdateRequest) error {
	switch r.Type {
	case model.StockBuy:
		return s.buy(ctx, r)
	case model.StockSell:
		return s.sell(ctx, r)
	default:
		return fmt.Errorf("unsupported trade type %s", r.Type)
	}
}

func (s *CacheService) buy(ctx context.Context, r model.PortfolioCacheUpdateRequest) error {
	amount := decimal.NewFromInt(r.Price).Mul(r.Quantity)
	added, err := s.store.IncreaseStockQuantity(ctx, r.StockID, r.PortfolioID, r.Quantity)
	if err != nil {
		return err
	}
	if err = s.store.AddPortfolioPurchasedValue(ctx, r.PortfolioID, model.RoundToInt64(amount)); err != nil {
		return err
	}
	if err = s.store.AddPortfolioCurrentValue(ctx, r.PortfolioID, amount); err != nil {
		return err
	}
	if err = s.store.AddStockCurrentValue(ctx, r.StockID, r.PortfolioID, amount); err != nil {
		return err
	}
	if added {
		if err = s.store.IncreaseAssetCount(ctx, r.PortfolioID); err != nil {
			return err
		}
	}
	return nil
}

func (s *CacheService) sell(ctx context.Context, r model.PortfolioCacheUpdateRequest) error {
	amount := decimal.NewFromInt(r.Price).Mul(r.Quantity)
	removed, err := s.store.DecreaseStockQuantity(ctx, r.StockID, r.PortfolioID, r.Quantity)
	if err != nil {
		return err
	}
	if err = s.store.SubtractPortfolioPurchasedValue(ctx, r.PortfolioID, model.RoundToInt64(amount)); err != nil {
		return err
	}
	if err = s.store.SubtractPortfolioCurrentValue(ctx, r.PortfolioID, amount); err != nil {
		return err
	}
	if err = s.store.SubtractStockCurrentValue(ctx, r.StockID, r.PortfolioID, amount); err != nil {
		return err
	}
	if removed {
		if err = s.store.DecreaseAssetCount(ctx, r.PortfolioID); err != nil {
			return err
		}
	}
	return nil
}

func (s *CacheService) savePortfolioSnapshot(ctx context.Context, id int64) error {
	pv := s.store.FindPortfolioPurchasedValue(ctx, id)
	cv := s.store.FindPortfolioCurrentValue(ctx, id)
	return s.store.SavePortfolioValuation(ctx, model.PortfolioValuation{
		PortfolioID:    id,
		PurchasedValue: pv,
		CurrentValue:   model.RoundToInt64(cv),
		ProfitRate:     model.ProfitRate(pv, cv),
		AssetCount:     s.store.FindAssetCount(ctx, id),
	})
}
