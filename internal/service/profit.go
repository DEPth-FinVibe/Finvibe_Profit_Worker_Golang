package service

import (
	"context"
	"time"

	"finvibe-profit-worker-go/internal/metrics"
	"finvibe-profit-worker-go/internal/model"
	"finvibe-profit-worker-go/internal/redisstore"
	"github.com/shopspring/decimal"
)

type ProfitService struct {
	store   *redisstore.Store
	metrics *metrics.Metrics
}

func NewProfitService(s *redisstore.Store, m *metrics.Metrics) *ProfitService {
	return &ProfitService{s, m}
}

type recalcTask struct{ portfolioID, stockID, newPrice int64 }

func (s *ProfitService) UpdateProfitsByStockPriceChanges(ctx context.Context, reqs []model.ProfitCalculationRequest) error {
	start := time.Now()
	result := metrics.ResultFailure
	defer func() { s.metrics.ObserveService(metrics.OpStockRecalc, result, start) }()
	phase := time.Now()
	priceByStock := make(map[int64]int64, len(reqs))
	stockIDs := make([]int64, 0, len(reqs))
	for _, r := range reqs {
		priceByStock[r.StockID] = r.NewPrice
		stockIDs = append(stockIDs, r.StockID)
	}
	portfoliosByStock, err := s.store.BulkFindPortfolioIDsByStockIDs(ctx, stockIDs)
	if err != nil {
		return err
	}
	s.metrics.ObservePhase(metrics.OpStockRecalc, "reverse_index_lookup", metrics.ResultSuccess, phase)
	tasks := make([]recalcTask, 0)
	for _, stockID := range stockIDs {
		for _, pf := range portfoliosByStock[stockID] {
			tasks = append(tasks, recalcTask{pf, stockID, priceByStock[stockID]})
		}
	}
	if len(tasks) == 0 {
		result = metrics.ResultSuccess
		return nil
	}
	phase = time.Now()
	keys := make([]model.StockHoldingKey, len(tasks))
	for i, t := range tasks {
		keys[i] = model.StockHoldingKey{PortfolioID: t.portfolioID, StockID: t.stockID}
	}
	holdings, err := s.store.BulkFetchStockHoldings(ctx, keys)
	if err != nil {
		return err
	}
	s.metrics.ObservePhase(metrics.OpStockRecalc, "bulk_prefetch", metrics.ResultSuccess, phase)
	phase = time.Now()
	replacements := make(map[int64][]redisstore.StockCurrentValueReplacement)
	stockCV := make(map[string]decimal.Decimal)
	for _, t := range tasks {
		h := holdings[model.StockHoldingKey{PortfolioID: t.portfolioID, StockID: t.stockID}.String()]
		if h.Quantity.IsZero() {
			continue
		}
		newCV := decimal.NewFromInt(t.newPrice).Mul(h.Quantity)
		replacements[t.portfolioID] = append(replacements[t.portfolioID], redisstore.StockCurrentValueReplacement{
			StockID:       t.stockID,
			PreviousValue: h.CurrentValue,
			CurrentValue:  newCV,
		})
		stockCV[s.store.StockCurrentValueKey(t.portfolioID, t.stockID)] = newCV
	}
	s.metrics.ObservePhase(metrics.OpStockRecalc, "in_memory_compute", metrics.ResultSuccess, phase)
	if len(replacements) == 0 {
		result = metrics.ResultSuccess
		return nil
	}
	phase = time.Now()
	states, err := s.store.BulkReplaceStockCurrentValuesAndFetchMetadata(ctx, replacements)
	if err != nil {
		return err
	}
	s.metrics.ObservePhase(metrics.OpStockRecalc, "atomic_stock_cv_replace", metrics.ResultSuccess, phase)
	phase = time.Now()
	if len(stockCV) > 0 {
		if err := s.store.BulkSetStockCurrentValues(ctx, stockCV); err != nil {
			return err
		}
	}
	s.metrics.ObservePhase(metrics.OpStockRecalc, "pipeline_stock_cv_set", metrics.ResultSuccess, phase)
	phase = time.Now()
	vals := make([]model.PortfolioValuation, 0, len(replacements))
	for pf := range replacements {
		st := states[pf]
		vals = append(vals, model.PortfolioValuation{
			PortfolioID:    pf,
			PurchasedValue: st.Metadata.PurchasedValue,
			CurrentValue:   model.RoundToInt64(st.CurrentValue),
			ProfitRate:     model.ProfitRate(st.Metadata.PurchasedValue, st.CurrentValue),
			AssetCount:     st.Metadata.AssetCount,
		})
	}
	if err := s.store.BulkSavePortfolioValuations(ctx, vals); err != nil {
		return err
	}
	s.metrics.ObservePhase(metrics.OpStockRecalc, "portfolio_fanout", metrics.ResultSuccess, phase)
	s.metrics.RecordAffectedPortfolios(metrics.OpStockRecalc, len(tasks))

	phase = time.Now()
	affectedUsers := make(map[string]struct{})
	for portfolioID := range replacements {
		userID := states[portfolioID].Metadata.UserID
		if userID == "" {
			continue
		}
		affectedUsers[userID] = struct{}{}
	}
	if len(affectedUsers) > 0 {
		userIDs := make([]string, 0, len(affectedUsers))
		for userID := range affectedUsers {
			userIDs = append(userIDs, userID)
		}
		userStates, err := s.store.BulkRecalculateUserCurrentValuesAndFetchMetadata(ctx, userIDs)
		if err != nil {
			return err
		}
		userVals := make([]model.UserValuation, 0, len(userStates))
		for userID, state := range userStates {
			userVals = append(userVals, model.UserValuation{
				UserID:         userID,
				PurchasedValue: state.Metadata.PurchasedValue,
				CurrentValue:   model.RoundToInt64(state.CurrentValue),
				ProfitRate:     model.ProfitRate(state.Metadata.PurchasedValue, state.CurrentValue),
				PortfolioCount: state.Metadata.PortfolioCount,
			})
		}
		if err := s.store.BulkSaveUserValuations(ctx, userVals); err != nil {
			return err
		}
	}
	s.metrics.ObservePhase(metrics.OpStockRecalc, "user_fanout", metrics.ResultSuccess, phase)
	s.metrics.RecordAffectedUsers(metrics.OpStockRecalc, len(affectedUsers))
	result = metrics.ResultSuccess
	return nil
}
