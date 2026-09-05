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
	changedUsers := map[string]struct{}{}
	for _, r := range reqs {
		if r.TradeID != 0 {
			processed, err := s.store.IsTradeProcessed(ctx, r.TradeID)
			if err != nil {
				return err
			}
			if processed {
				changedPF[r.PortfolioID] = struct{}{}
				if r.UserID != "" {
					changedUsers[r.UserID] = struct{}{}
				}
				continue
			}
		}
		userID, err := s.applyPortfolio(ctx, r)
		if err != nil {
			return err
		}
		changedPF[r.PortfolioID] = struct{}{}
		if userID != "" {
			changedUsers[userID] = struct{}{}
		}
		if r.TradeID != 0 {
			if err := s.store.MarkTradeProcessed(ctx, r.TradeID); err != nil {
				return err
			}
		}
	}
	for id := range changedPF {
		if err := s.savePortfolioSnapshot(ctx, id); err != nil {
			return err
		}
	}
	for userID := range changedUsers {
		if err := s.saveUserSnapshot(ctx, userID); err != nil {
			return err
		}
	}
	s.metrics.RecordAffectedPortfolios(metrics.OpPortfolioCache, len(reqs))
	s.metrics.RecordAffectedUsers(metrics.OpPortfolioCache, len(changedUsers))
	result = metrics.ResultSuccess
	return nil
}

func (s *CacheService) applyPortfolio(ctx context.Context, r model.PortfolioCacheUpdateRequest) (string, error) {
	userID := r.UserID
	if userID == "" {
		userID = s.store.FindUserIDByPortfolioID(ctx, r.PortfolioID)
	}
	if userID != "" && s.store.FindUserIDByPortfolioID(ctx, r.PortfolioID) == "" {
		if err := s.store.MapPortfolioToUserFromTrade(ctx, r.PortfolioID, userID); err != nil {
			return "", err
		}
	}

	var err error
	switch r.Type {
	case model.StockBuy:
		err = s.buy(ctx, r, userID)
	case model.StockSell:
		err = s.sell(ctx, r, userID)
	default:
		err = fmt.Errorf("unsupported trade type %s", r.Type)
	}
	return userID, err
}

func (s *CacheService) buy(ctx context.Context, r model.PortfolioCacheUpdateRequest, userID string) error {
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
	if userID != "" {
		if err = s.store.AddUserPurchasedValue(ctx, userID, model.RoundToInt64(amount)); err != nil {
			return err
		}
		if err = s.store.AddUserCurrentValue(ctx, userID, amount); err != nil {
			return err
		}
	}
	return nil
}

func (s *CacheService) sell(ctx context.Context, r model.PortfolioCacheUpdateRequest, userID string) error {
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
	if userID != "" {
		if err = s.store.SubtractUserPurchasedValue(ctx, userID, model.RoundToInt64(amount)); err != nil {
			return err
		}
		if err = s.store.SubtractUserCurrentValue(ctx, userID, amount); err != nil {
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

func (s *CacheService) UpdateUserCaches(ctx context.Context, reqs []model.UserCacheUpdateRequest) error {
	start := time.Now()
	result := metrics.ResultFailure
	defer func() { s.metrics.ObserveService(metrics.OpUserCache, result, start) }()

	changedUsers := map[string]struct{}{}
	for _, request := range reqs {
		switch request.Type {
		case model.PortfolioCreated:
			changed, err := s.createPortfolioMapping(ctx, request)
			if err != nil {
				return err
			}
			if changed {
				changedUsers[request.UserID] = struct{}{}
			}
		case model.PortfolioDeleted:
			changed, err := s.deletePortfolioMapping(ctx, request)
			if err != nil {
				return err
			}
			if changed {
				changedUsers[request.UserID] = struct{}{}
			}
		default:
			continue
		}
	}

	for userID := range changedUsers {
		if err := s.saveUserSnapshot(ctx, userID); err != nil {
			return err
		}
	}
	s.metrics.RecordAffectedUsers(metrics.OpUserCache, len(changedUsers))
	result = metrics.ResultSuccess
	return nil
}

func (s *CacheService) createPortfolioMapping(ctx context.Context, request model.UserCacheUpdateRequest) (bool, error) {
	if existing := s.store.FindUserIDByPortfolioID(ctx, request.PortfolioID); existing != "" {
		if !s.store.IsPortfolioCountPending(ctx, request.PortfolioID) {
			return false, nil
		}
		if err := s.store.IncreasePortfolioCount(ctx, request.UserID); err != nil {
			return false, err
		}
		if err := s.store.ClearPortfolioCountPending(ctx, request.PortfolioID); err != nil {
			return false, err
		}
		return true, nil
	}
	purchasedValue := s.store.FindPortfolioPurchasedValue(ctx, request.PortfolioID)
	currentValue := s.store.FindPortfolioCurrentValue(ctx, request.PortfolioID)
	if err := s.store.MapPortfolioToUser(ctx, request.PortfolioID, request.UserID); err != nil {
		return false, err
	}
	if err := s.store.AddUserPurchasedValue(ctx, request.UserID, purchasedValue); err != nil {
		return false, err
	}
	if err := s.store.AddUserCurrentValue(ctx, request.UserID, currentValue); err != nil {
		return false, err
	}
	if err := s.store.IncreasePortfolioCount(ctx, request.UserID); err != nil {
		return false, err
	}
	return true, nil
}

func (s *CacheService) deletePortfolioMapping(ctx context.Context, request model.UserCacheUpdateRequest) (bool, error) {
	if existing := s.store.FindUserIDByPortfolioID(ctx, request.PortfolioID); existing == "" {
		return false, s.store.MarkPortfolioValuationDeleted(ctx, request.PortfolioID)
	}
	purchasedValue := s.store.FindPortfolioPurchasedValue(ctx, request.PortfolioID)
	currentValue := s.store.FindPortfolioCurrentValue(ctx, request.PortfolioID)
	countPending := s.store.IsPortfolioCountPending(ctx, request.PortfolioID)
	if err := s.store.SubtractUserPurchasedValue(ctx, request.UserID, purchasedValue); err != nil {
		return false, err
	}
	if err := s.store.SubtractUserCurrentValue(ctx, request.UserID, currentValue); err != nil {
		return false, err
	}
	if !countPending {
		if err := s.store.DecreasePortfolioCount(ctx, request.UserID); err != nil {
			return false, err
		}
	}
	if err := s.store.RemovePortfolioUserMapping(ctx, request.PortfolioID); err != nil {
		return false, err
	}
	if err := s.store.MarkPortfolioValuationDeleted(ctx, request.PortfolioID); err != nil {
		return false, err
	}
	if err := s.store.DeletePortfolioState(ctx, request.PortfolioID); err != nil {
		return false, err
	}
	return true, nil
}

func (s *CacheService) saveUserSnapshot(ctx context.Context, userID string) error {
	purchasedValue := s.store.FindUserPurchasedValue(ctx, userID)
	currentValue := s.store.FindUserCurrentValue(ctx, userID)
	return s.store.SaveUserValuation(ctx, model.UserValuation{
		UserID:         userID,
		PurchasedValue: purchasedValue,
		CurrentValue:   model.RoundToInt64(currentValue),
		ProfitRate:     model.ProfitRate(purchasedValue, currentValue),
		PortfolioCount: s.store.FindUserPortfolioCount(ctx, userID),
	})
}
