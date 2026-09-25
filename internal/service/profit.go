package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"finvibe-profit-worker-go/internal/metrics"
	"finvibe-profit-worker-go/internal/model"
	"finvibe-profit-worker-go/internal/redisstore"
	"github.com/shopspring/decimal"
)

type ProfitService struct {
	store        *redisstore.Store
	metrics      *metrics.Metrics
	priceLockTTL time.Duration
}

func NewProfitService(s *redisstore.Store, m *metrics.Metrics, priceLockTTL time.Duration) *ProfitService {
	if priceLockTTL <= 0 {
		priceLockTTL = 30 * time.Second
	}
	return &ProfitService{store: s, metrics: m, priceLockTTL: priceLockTTL}
}

type recalcTask struct{ portfolioID, stockID, newPrice, version int64 }

type PriceUpdateResult struct {
	Applied int
	Skipped map[string]int
}

func (s *ProfitService) UpdateProfitsByStockPriceChanges(ctx context.Context, reqs []model.ProfitCalculationRequest) (PriceUpdateResult, error) {
	start := time.Now()
	result := metrics.ResultFailure
	defer func() { s.metrics.ObserveService(metrics.OpStockRecalc, result, start) }()
	outcome := PriceUpdateResult{Skipped: make(map[string]int)}
	if len(reqs) == 0 {
		result = metrics.ResultSuccess
		return outcome, nil
	}

	stockIDs := make([]int64, 0, len(reqs))
	for _, request := range reqs {
		if request.Timestamp.IsZero() {
			return outcome, fmt.Errorf("stock %d price timestamp is empty", request.StockID)
		}
		stockIDs = append(stockIDs, request.StockID)
	}
	locks, err := s.store.AcquireStockPriceLocks(ctx, stockIDs, s.priceLockTTL)
	if err != nil {
		return outcome, err
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = locks.Release(releaseCtx)
	}()

	applyCtx, cancelApply := context.WithCancel(ctx)
	defer cancelApply()
	stopRenewal, renewalDone := s.renewPriceLocks(applyCtx, cancelApply, locks)
	renewalStopped := false
	stopAndCheckRenewal := func() error {
		if !renewalStopped {
			close(stopRenewal)
			renewalStopped = true
		}
		return <-renewalDone
	}
	defer func() {
		if !renewalStopped {
			close(stopRenewal)
			<-renewalDone
		}
	}()

	appliedStates, err := s.store.BulkFetchAppliedStockPrices(applyCtx, stockIDs)
	if err != nil {
		return outcome, err
	}
	accepted := make([]model.ProfitCalculationRequest, 0, len(reqs))
	for _, request := range reqs {
		request.Version = request.EffectiveVersion()
		applied, exists := appliedStates[request.StockID]
		if !exists || request.Version > applied.Version {
			accepted = append(accepted, request)
			continue
		}
		if request.Version < applied.Version {
			outcome.Skipped[metrics.ReasonStalePriceEvent]++
			continue
		}
		if request.NewPrice == applied.Price {
			outcome.Skipped[metrics.ReasonDuplicatePriceEvent]++
			continue
		}
		outcome.Skipped[metrics.ReasonPriceTimestampConflict]++
		slog.Warn("stock price version conflict",
			"stock_id", request.StockID,
			"version", request.Version,
			"updated_at", request.Timestamp,
			"applied_price", applied.Price,
			"incoming_price", request.NewPrice,
		)
	}

	if len(accepted) > 0 {
		if err := s.applyStockPriceChanges(applyCtx, accepted); err != nil {
			return outcome, err
		}
	}
	if err := stopAndCheckRenewal(); err != nil {
		return outcome, err
	}
	if err := locks.Refresh(ctx); err != nil {
		return outcome, err
	}
	states := make([]redisstore.AppliedStockPrice, 0, len(accepted))
	for _, request := range accepted {
		states = append(states, redisstore.AppliedStockPrice{
			StockID: request.StockID, Price: request.NewPrice, Timestamp: request.Timestamp, Version: request.Version,
		})
	}
	if err := locks.Commit(ctx, states); err != nil {
		return outcome, err
	}
	outcome.Applied = len(accepted)
	result = metrics.ResultSuccess
	return outcome, nil
}

func (s *ProfitService) renewPriceLocks(ctx context.Context, cancelApply context.CancelFunc, locks *redisstore.StockPriceLockSet) (chan struct{}, chan error) {
	stop := make(chan struct{})
	done := make(chan error, 1)
	interval := s.priceLockTTL / 3
	if interval < time.Second {
		interval = time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				done <- ctx.Err()
				return
			case <-stop:
				done <- nil
				return
			case <-ticker.C:
				if err := locks.Refresh(ctx); err != nil {
					cancelApply()
					done <- err
					return
				}
			}
		}
	}()
	return stop, done
}

func (s *ProfitService) applyStockPriceChanges(ctx context.Context, reqs []model.ProfitCalculationRequest) error {
	phase := time.Now()
	priceByStock := make(map[int64]int64, len(reqs))
	versionByStock := make(map[int64]int64, len(reqs))
	stockIDs := make([]int64, 0, len(reqs))
	for _, r := range reqs {
		priceByStock[r.StockID] = r.NewPrice
		versionByStock[r.StockID] = r.Version
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
			tasks = append(tasks, recalcTask{pf, stockID, priceByStock[stockID], versionByStock[stockID]})
		}
	}
	if len(tasks) == 0 {
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
			Version:       t.version,
		})
		stockCV[s.store.StockCurrentValueKey(t.portfolioID, t.stockID)] = newCV
	}
	s.metrics.ObservePhase(metrics.OpStockRecalc, "in_memory_compute", metrics.ResultSuccess, phase)
	if len(replacements) == 0 {
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
	return nil
}
