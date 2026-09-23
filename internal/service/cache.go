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

// buy와 sell의 각 저장소 쓰기는 이벤트 단위로 원자적이고 멱등하다.
// 도중에 실패해 재시도하면 끝난 쓰기는 건너뛰고 남은 쓰기만 반영된다.
func (s *CacheService) buy(ctx context.Context, r model.PortfolioCacheUpdateRequest, userID string) error {
	eventKey := tradeEventKey(r.TradeID)
	amount := decimal.NewFromInt(r.Price).Mul(r.Quantity)
	added, err := s.store.IncreaseStockQuantity(ctx, eventKey, r.StockID, r.PortfolioID, r.Quantity)
	if err != nil {
		return err
	}
	assetCount := int64(0)
	if added {
		assetCount = 1
	}
	if err = s.store.ApplyPortfolioTradeTotals(ctx, eventKey, r.PortfolioID, r.StockID, redisstore.PortfolioTradeTotals{
		PurchasedValue:    model.RoundToInt64(amount),
		CurrentValue:      amount,
		StockCurrentValue: amount,
		AssetCount:        assetCount,
	}); err != nil {
		return err
	}
	if userID != "" {
		return s.store.ApplyUserTotals(ctx, eventKey, userID, model.RoundToInt64(amount), amount, 0)
	}
	return nil
}

func (s *CacheService) sell(ctx context.Context, r model.PortfolioCacheUpdateRequest, userID string) error {
	eventKey := tradeEventKey(r.TradeID)
	amount := decimal.NewFromInt(r.Price).Mul(r.Quantity)
	removed, err := s.store.DecreaseStockQuantity(ctx, eventKey, r.StockID, r.PortfolioID, r.Quantity)
	if err != nil {
		return err
	}
	assetCount := int64(0)
	if removed {
		assetCount = -1
	}
	if err = s.store.ApplyPortfolioTradeTotals(ctx, eventKey, r.PortfolioID, r.StockID, redisstore.PortfolioTradeTotals{
		PurchasedValue:              -model.RoundToInt64(amount),
		CurrentValue:                amount.Neg(),
		StockCurrentValue:           amount.Neg(),
		RemoveStockCurrentValue:     removed,
		DeleteNonPositiveStockValue: true,
		AssetCount:                  assetCount,
	}); err != nil {
		return err
	}
	if userID != "" {
		return s.store.ApplyUserTotals(ctx, eventKey, userID, -model.RoundToInt64(amount), amount.Neg(), 0)
	}
	return nil
}

func tradeEventKey(tradeID int64) string {
	if tradeID == 0 {
		return ""
	}
	return fmt.Sprintf("trade:%d", tradeID)
}

func portfolioUserEventKey(portfolioID int64, typ model.UserChangeType) string {
	// 포트폴리오는 한 번 생성되고 한 번 삭제되므로 (portfolioId, 변경 유형)이 곧 이벤트 식별자다.
	return fmt.Sprintf("portfolio-user:%d:%s", portfolioID, typ)
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

// createPortfolioMapping은 유저 합계를 먼저 멱등하게 반영하고 매핑을 만든다.
// 매핑을 먼저 만들면, 매핑 직후 끊긴 재시도가 "이미 생성됨"으로 빠져 유저 합계가 반영되지 않는다.
func (s *CacheService) createPortfolioMapping(ctx context.Context, request model.UserCacheUpdateRequest) (bool, error) {
	eventKey := portfolioUserEventKey(request.PortfolioID, model.PortfolioCreated)
	if existing := s.store.FindUserIDByPortfolioID(ctx, request.PortfolioID); existing != "" {
		if !s.store.IsPortfolioCountPending(ctx, request.PortfolioID) {
			// 이미 반영됐다. 이전 시도에서 snapshot 저장만 실패했을 수 있으므로 다시 저장하게 한다.
			return true, nil
		}
		// 매매 이벤트가 생성 이벤트보다 먼저 와서 매핑만 있고 포트폴리오 수는 아직 세지 않은 상태다.
		if err := s.store.ApplyUserTotals(ctx, eventKey, request.UserID, 0, decimal.Zero, 1); err != nil {
			return false, err
		}
		if err := s.store.ClearPortfolioCountPending(ctx, request.PortfolioID); err != nil {
			return false, err
		}
		return true, nil
	}
	purchasedValue := s.store.FindPortfolioPurchasedValue(ctx, request.PortfolioID)
	currentValue := s.store.FindPortfolioCurrentValue(ctx, request.PortfolioID)
	if err := s.store.ApplyUserTotals(ctx, eventKey, request.UserID, purchasedValue, currentValue, 1); err != nil {
		return false, err
	}
	if err := s.store.MapPortfolioToUser(ctx, request.PortfolioID, request.UserID); err != nil {
		return false, err
	}
	return true, nil
}

// deletePortfolioMapping은 유저 합계를 멱등하게 빼고 매핑과 상태를 지운다.
// 매핑이 이미 없는 재시도에서도 남은 정리 단계를 다시 실행한다. 모두 멱등한 연산이다.
func (s *CacheService) deletePortfolioMapping(ctx context.Context, request model.UserCacheUpdateRequest) (bool, error) {
	if existing := s.store.FindUserIDByPortfolioID(ctx, request.PortfolioID); existing != "" {
		purchasedValue := s.store.FindPortfolioPurchasedValue(ctx, request.PortfolioID)
		currentValue := s.store.FindPortfolioCurrentValue(ctx, request.PortfolioID)
		portfolioCount := int64(-1)
		if s.store.IsPortfolioCountPending(ctx, request.PortfolioID) {
			portfolioCount = 0
		}
		eventKey := portfolioUserEventKey(request.PortfolioID, model.PortfolioDeleted)
		if err := s.store.ApplyUserTotals(ctx, eventKey, request.UserID, -purchasedValue, currentValue.Neg(), portfolioCount); err != nil {
			return false, err
		}
	}
	if err := s.store.RemovePortfolioUserMapping(ctx, request.PortfolioID, request.UserID); err != nil {
		return false, err
	}
	if err := s.store.MarkPortfolioValuationDeleted(ctx, request.PortfolioID); err != nil {
		return false, err
	}
	if err := s.store.DeletePortfolioState(ctx, request.PortfolioID); err != nil {
		return false, err
	}
	return request.UserID != "", nil
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
