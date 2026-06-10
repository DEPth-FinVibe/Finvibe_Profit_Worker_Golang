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
	portfolioDelta := make(map[int64]decimal.Decimal)
	stockCV := make(map[string]decimal.Decimal)
	for _, t := range tasks {
		h := holdings[model.StockHoldingKey{PortfolioID: t.portfolioID, StockID: t.stockID}.String()]
		if h.Quantity.IsZero() {
			continue
		}
		newCV := decimal.NewFromInt(t.newPrice).Mul(h.Quantity)
		delta := newCV.Sub(h.CurrentValue)
		if old, ok := portfolioDelta[t.portfolioID]; ok {
			portfolioDelta[t.portfolioID] = old.Add(delta)
		} else {
			portfolioDelta[t.portfolioID] = delta
		}
		stockCV[s.store.StockCurrentValueKey(t.portfolioID, t.stockID)] = newCV
	}
	s.metrics.ObservePhase(metrics.OpStockRecalc, "in_memory_compute", metrics.ResultSuccess, phase)
	phase = time.Now()
	states, err := s.store.BulkIncrementPortfolioCurrentValuesAndFetchMetadata(ctx, portfolioDelta)
	if err != nil {
		return err
	}
	s.metrics.ObservePhase(metrics.OpStockRecalc, "pipeline_portfolio_incr", metrics.ResultSuccess, phase)
	phase = time.Now()
	if len(stockCV) > 0 {
		if err := s.store.BulkSetStockCurrentValues(ctx, stockCV); err != nil {
			return err
		}
	}
	s.metrics.ObservePhase(metrics.OpStockRecalc, "pipeline_stock_cv_set", metrics.ResultSuccess, phase)
	phase = time.Now()
	vals := make([]model.PortfolioValuation, 0, len(portfolioDelta))
	for pf := range portfolioDelta {
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
	result = metrics.ResultSuccess
	return nil
}
