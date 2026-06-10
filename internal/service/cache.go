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
	changedU := map[string]struct{}{}
	affected := 0
	for _, r := range reqs {
		user, err := s.applyPortfolio(ctx, r)
		if err != nil {
			return err
		}
		changedPF[r.PortfolioID] = struct{}{}
		if user != "" {
			changedU[user] = struct{}{}
			affected++
		}
	}
	for id := range changedPF {
		if err := s.savePortfolioSnapshot(ctx, id); err != nil {
			return err
		}
	}
	for id := range changedU {
		if err := s.saveUserSnapshot(ctx, id); err != nil {
			return err
		}
	}
	s.metrics.RecordAffectedPortfolios(metrics.OpPortfolioCache, len(reqs))
	s.metrics.RecordAffectedUsers(metrics.OpPortfolioCache, affected)
	result = metrics.ResultSuccess
	return nil
}
func (s *CacheService) UpdateUserCaches(ctx context.Context, reqs []model.UserCacheUpdateRequest) error {
	start := time.Now()
	result := metrics.ResultFailure
	defer func() { s.metrics.ObserveService(metrics.OpUserCache, result, start) }()
	changed := map[string]struct{}{}
	for _, r := range reqs {
		if err := s.applyUser(ctx, r); err != nil {
			return err
		}
		changed[r.UserID] = struct{}{}
	}
	for id := range changed {
		if err := s.saveUserSnapshot(ctx, id); err != nil {
			return err
		}
	}
	s.metrics.RecordAffectedPortfolios(metrics.OpUserCache, len(reqs))
	s.metrics.RecordAffectedUsers(metrics.OpUserCache, len(reqs))
	result = metrics.ResultSuccess
	return nil
}

func (s *CacheService) applyPortfolio(ctx context.Context, r model.PortfolioCacheUpdateRequest) (string, error) {
	switch r.Type {
	case model.StockBuy:
		return s.buy(ctx, r)
	case model.StockSell:
		return s.sell(ctx, r)
	default:
		return "", fmt.Errorf("unsupported trade type %s", r.Type)
	}
}
func (s *CacheService) buy(ctx context.Context, r model.PortfolioCacheUpdateRequest) (string, error) {
	amount := decimal.NewFromInt(r.Price).Mul(r.Quantity)
	added, err := s.store.IncreaseStockQuantity(ctx, r.StockID, r.PortfolioID, r.Quantity)
	if err != nil {
		return "", err
	}
	if err = s.store.AddPortfolioPurchasedValue(ctx, r.PortfolioID, model.RoundToInt64(amount)); err != nil {
		return "", err
	}
	if err = s.store.AddPortfolioCurrentValue(ctx, r.PortfolioID, amount); err != nil {
		return "", err
	}
	if err = s.store.AddStockCurrentValue(ctx, r.StockID, r.PortfolioID, amount); err != nil {
		return "", err
	}
	user := s.store.FindUserIDByPortfolioID(ctx, r.PortfolioID)
	if added {
		if err = s.store.IncreaseAssetCount(ctx, r.PortfolioID); err != nil {
			return "", err
		}
	}
	if user != "" {
		err = s.store.AddUserPurchasedValue(ctx, user, model.RoundToInt64(amount))
	}
	return user, err
}
func (s *CacheService) sell(ctx context.Context, r model.PortfolioCacheUpdateRequest) (string, error) {
	amount := decimal.NewFromInt(r.Price).Mul(r.Quantity)
	removed, err := s.store.DecreaseStockQuantity(ctx, r.StockID, r.PortfolioID, r.Quantity)
	if err != nil {
		return "", err
	}
	if err = s.store.SubtractPortfolioPurchasedValue(ctx, r.PortfolioID, model.RoundToInt64(amount)); err != nil {
		return "", err
	}
	if err = s.store.SubtractPortfolioCurrentValue(ctx, r.PortfolioID, amount); err != nil {
		return "", err
	}
	if err = s.store.SubtractStockCurrentValue(ctx, r.StockID, r.PortfolioID, amount); err != nil {
		return "", err
	}
	user := s.store.FindUserIDByPortfolioID(ctx, r.PortfolioID)
	if removed {
		if err = s.store.DecreaseAssetCount(ctx, r.PortfolioID); err != nil {
			return "", err
		}
	}
	if user != "" {
		err = s.store.SubtractUserPurchasedValue(ctx, user, model.RoundToInt64(amount))
	}
	return user, err
}
func (s *CacheService) applyUser(ctx context.Context, r model.UserCacheUpdateRequest) error {
	pv := s.store.FindPortfolioPurchasedValue(ctx, r.PortfolioID)
	switch r.Type {
	case model.PortfolioCreated:
		if err := s.store.MapPortfolioToUser(ctx, r.PortfolioID, r.UserID); err != nil {
			return err
		}
		if err := s.store.AddUserPurchasedValue(ctx, r.UserID, pv); err != nil {
			return err
		}
		return s.store.IncreasePortfolioCount(ctx, r.UserID)
	case model.PortfolioDeleted:
		if err := s.store.RemovePortfolioUserMapping(ctx, r.PortfolioID); err != nil {
			return err
		}
		if err := s.store.SubtractUserPurchasedValue(ctx, r.UserID, pv); err != nil {
			return err
		}
		if err := s.store.DecreasePortfolioCount(ctx, r.UserID); err != nil {
			return err
		}
		if err := s.store.MarkPortfolioValuationDeleted(ctx, r.PortfolioID); err != nil {
			return err
		}
		return s.store.DeletePortfolioState(ctx, r.PortfolioID)
	default:
		return fmt.Errorf("unsupported user cache change %s", r.Type)
	}
}
func (s *CacheService) savePortfolioSnapshot(ctx context.Context, id int64) error {
	pv := s.store.FindPortfolioPurchasedValue(ctx, id)
	cv := s.store.FindPortfolioCurrentValue(ctx, id)
	return s.store.SavePortfolioValuation(ctx, model.PortfolioValuation{PortfolioID: id, PurchasedValue: pv, CurrentValue: model.RoundToInt64(cv), ProfitRate: model.ProfitRate(pv, cv), AssetCount: s.store.FindAssetCount(ctx, id)})
}
func (s *CacheService) saveUserSnapshot(ctx context.Context, id string) error {
	pv := s.store.FindUserPurchasedValue(ctx, id)
	cv := s.store.CalculateUserCurrentValue(ctx, id)
	return s.store.SaveUserValuation(ctx, model.UserValuation{UserID: id, PurchasedValue: pv, CurrentValue: model.RoundToInt64(cv), ProfitRate: model.ProfitRate(pv, cv), PortfolioCount: s.store.FindUserPortfolioCount(ctx, id)})
}
