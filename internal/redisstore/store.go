package redisstore

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"finvibe-profit-worker-go/internal/metrics"
	"finvibe-profit-worker-go/internal/model"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

type Store struct {
	rdb     redis.UniversalClient
	metrics *metrics.Metrics
}

func New(rdb redis.UniversalClient, m *metrics.Metrics) *Store { return &Store{rdb: rdb, metrics: m} }

type PipelineResultMismatchError struct {
	Operation        string
	Expected, Actual int
}

func (e PipelineResultMismatchError) Error() string {
	return fmt.Sprintf("pipeline result mismatch for %s: expected %d, actual %d", e.Operation, e.Expected, e.Actual)
}
func check(op string, expected, actual int) error {
	if expected != actual {
		return PipelineResultMismatchError{op, expected, actual}
	}
	return nil
}

const (
	fPV  = "pv"
	fCV  = "cv"
	fCVP = "cvp"
	fAC  = "ac"
	fU   = "u"
	fUCP = "ucp"
	fDel = "del"
	fDA  = "da"
	fUA  = "ua"
	fPR  = "pr"
	fPC  = "pc"
)

func pfKey(id int64) string              { return fmt.Sprintf("pf:%d", id) }
func usrKey(id string) string            { return "usr:" + id }
func userPortfoliosKey(id string) string { return "user:" + id + ":portfolios" }
func portfolioStocksKey(id int64) string { return fmt.Sprintf("portfolio:%d:stocks", id) }
func stockPortfoliosKey(id int64) string { return fmt.Sprintf("stock:%d:portfolios", id) }
func quantityKey(portfolioID, stockID int64) string {
	return fmt.Sprintf("portfolio:%d:stock:%d:quantity", portfolioID, stockID)
}
func currentValueKey(portfolioID, stockID int64) string {
	return fmt.Sprintf("portfolio:%d:stock:%d:current-value", portfolioID, stockID)
}
func stockCurrentValueField(stockID int64) string { return fmt.Sprintf("scv:%d", stockID) }
func processedTradeKey(tradeID int64) string      { return fmt.Sprintf("processed:trade:%d", tradeID) }
func (s *Store) StockCurrentValueKey(portfolioID, stockID int64) string {
	return currentValueKey(portfolioID, stockID)
}

func (s *Store) IsTradeProcessed(ctx context.Context, tradeID int64) (bool, error) {
	count, err := s.rdb.Exists(ctx, processedTradeKey(tradeID)).Result()
	return count > 0, err
}

func (s *Store) MarkTradeProcessed(ctx context.Context, tradeID int64) error {
	return s.rdb.Set(ctx, processedTradeKey(tradeID), "1", 7*24*time.Hour).Err()
}

func (s *Store) FindPortfolioIDsByStockID(ctx context.Context, stockID int64) ([]int64, error) {
	start := time.Now()
	members, err := s.rdb.SMembers(ctx, stockPortfoliosKey(stockID)).Result()
	s.observe("smembers", err, start)
	if err != nil {
		return nil, err
	}
	out := make([]int64, 0, len(members))
	for _, m := range members {
		if v, err := strconv.ParseInt(m, 10, 64); err == nil {
			out = append(out, v)
		}
	}
	return out, nil
}
func (s *Store) BulkFindPortfolioIDsByStockIDs(ctx context.Context, stockIDs []int64) (map[int64][]int64, error) {
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.StringSliceCmd, 0, len(stockIDs))
	start := time.Now()
	for _, id := range stockIDs {
		cmds = append(cmds, pipe.SMembers(ctx, stockPortfoliosKey(id)))
	}
	_, err := pipe.Exec(ctx)
	s.observe("pipeline_smembers", err, start)
	if err != nil && err != redis.Nil {
		return nil, err
	}
	if err := check("bulkFindPortfolioIdsByStockIds", len(stockIDs), len(cmds)); err != nil {
		return nil, err
	}
	out := make(map[int64][]int64, len(stockIDs))
	for i, cmd := range cmds {
		vals := cmd.Val()
		ids := make([]int64, 0, len(vals))
		for _, v := range vals {
			if n, e := strconv.ParseInt(v, 10, 64); e == nil {
				ids = append(ids, n)
			}
		}
		out[stockIDs[i]] = ids
	}
	return out, nil
}
func (s *Store) BulkFetchStockHoldings(ctx context.Context, keys []model.StockHoldingKey) (map[string]model.StockHolding, error) {
	pipe := s.rdb.Pipeline()
	qcmd := make([]*redis.StringCmd, len(keys))
	hcmd := make([]*redis.StringCmd, len(keys))
	ccmd := make([]*redis.StringCmd, len(keys))
	start := time.Now()
	for i, k := range keys {
		qcmd[i] = pipe.Get(ctx, quantityKey(k.PortfolioID, k.StockID))
		hcmd[i] = pipe.HGet(ctx, pfKey(k.PortfolioID), stockCurrentValueField(k.StockID))
		ccmd[i] = pipe.Get(ctx, currentValueKey(k.PortfolioID, k.StockID))
	}
	_, err := pipe.Exec(ctx)
	s.observe("pipeline_get_stock_holdings", err, start)
	if err != nil && err != redis.Nil {
		return nil, err
	}
	if err := check("bulkFetchStockHoldings", 3*len(keys), len(qcmd)+len(hcmd)+len(ccmd)); err != nil {
		return nil, err
	}
	out := make(map[string]model.StockHolding, len(keys))
	for i, k := range keys {
		currentValue := hcmd[i].Val()
		if currentValue == "" {
			currentValue = ccmd[i].Val()
		}
		out[k.String()] = model.StockHolding{Quantity: parseDecimal(qcmd[i].Val()), CurrentValue: parseDecimal(currentValue)}
	}
	return out, nil
}
func (s *Store) BulkSetStockCurrentValues(ctx context.Context, updates map[string]decimal.Decimal) error {
	pipe := s.rdb.Pipeline()
	start := time.Now()
	for k, v := range updates {
		pipe.Set(ctx, k, v.String(), 0)
	}
	_, err := pipe.Exec(ctx)
	s.observe("pipeline_set_stock_cv", err, start)
	if err == redis.Nil {
		return nil
	}
	return err
}

type StockCurrentValueReplacement struct {
	StockID       int64
	PreviousValue decimal.Decimal
	CurrentValue  decimal.Decimal
}

const replaceStockCurrentValuesScript = `
local portfolio_key = KEYS[1]
local portfolio_current = redis.call('HGET', portfolio_key, 'cvp')
if not portfolio_current then
    portfolio_current = redis.call('HGET', portfolio_key, 'cv') or '0'
    redis.call('HSET', portfolio_key, 'cvp', portfolio_current)
end

local total_delta = 0
for i = 1, #ARGV, 3 do
    local field = ARGV[i]
    local new_value = ARGV[i + 1]
    local previous_value = redis.call('HGET', portfolio_key, field)
    if not previous_value then
        previous_value = ARGV[i + 2]
    end
    total_delta = total_delta + (tonumber(new_value) - tonumber(previous_value))
    redis.call('HSET', portfolio_key, field, new_value)
end

if total_delta ~= 0 then
    portfolio_current = redis.call('HINCRBYFLOAT', portfolio_key, 'cvp', total_delta)
else
    portfolio_current = redis.call('HGET', portfolio_key, 'cvp')
end

return {
    portfolio_current,
    redis.call('HGET', portfolio_key, 'pv') or '',
    redis.call('HGET', portfolio_key, 'ac') or '',
    redis.call('HGET', portfolio_key, 'u') or ''
}
`

func (s *Store) BulkReplaceStockCurrentValuesAndFetchMetadata(ctx context.Context, replacements map[int64][]StockCurrentValueReplacement) (map[int64]model.PortfolioStateSnapshot, error) {
	opStart := time.Now()
	opResult := metrics.ResultFailure
	defer func() { s.observeOperation(metrics.OpPortfolioCurrent, opResult, opStart) }()

	pipe := s.rdb.Pipeline()
	ids := make([]int64, 0, len(replacements))
	cmds := make([]*redis.Cmd, 0, len(replacements))
	start := time.Now()
	for id, portfolioReplacements := range replacements {
		ids = append(ids, id)
		args := make([]any, 0, 3*len(portfolioReplacements))
		for _, replacement := range portfolioReplacements {
			args = append(args,
				stockCurrentValueField(replacement.StockID),
				replacement.CurrentValue.String(),
				replacement.PreviousValue.String(),
			)
		}
		cmds = append(cmds, pipe.Eval(ctx, replaceStockCurrentValuesScript, []string{pfKey(id)}, args...))
	}
	_, err := pipe.Exec(ctx)
	s.observe("pipeline_eval_replace_stock_cv", err, start)
	if err != nil && err != redis.Nil {
		return nil, err
	}
	if err := check("bulkReplaceStockCurrentValuesAndFetchMetadata", len(ids), len(cmds)); err != nil {
		return nil, err
	}
	out := make(map[int64]model.PortfolioStateSnapshot, len(ids))
	for i, id := range ids {
		vals, resultErr := cmds[i].Slice()
		if resultErr != nil {
			return nil, resultErr
		}
		currentValue := parseDecimal(asString(vals, 0))
		out[id] = model.PortfolioStateSnapshot{
			CurrentValue: currentValue,
			Metadata: model.PortfolioMetadata{
				PurchasedValue: asInt(vals, 1),
				AssetCount:     asInt(vals, 2),
				UserID:         asString(vals, 3),
				CurrentValue:   currentValue,
			},
		}
	}
	opResult = metrics.ResultSuccess
	return out, nil
}
func (s *Store) BulkIncrementUserCurrentValuesAndFetchMetadata(ctx context.Context, deltas map[string]decimal.Decimal) (map[string]model.UserStateSnapshot, error) {
	opStart := time.Now()
	opResult := metrics.ResultFailure
	defer func() { s.observeOperation(metrics.OpUserCurrent, opResult, opStart) }()

	pipe := s.rdb.Pipeline()
	ids := make([]string, 0, len(deltas))
	incr := make([]*redis.FloatCmd, 0, len(deltas))
	meta := make([]*redis.SliceCmd, 0, len(deltas))
	start := time.Now()
	for id, d := range deltas {
		ids = append(ids, id)
		incr = append(incr, pipe.HIncrByFloat(ctx, usrKey(id), fCVP, toFloat(d)))
		meta = append(meta, pipe.HMGet(ctx, usrKey(id), fPV, fPC))
	}
	_, err := pipe.Exec(ctx)
	s.observe("pipeline_hincrbyfloat_hmget_user", err, start)
	if err != nil && err != redis.Nil {
		return nil, err
	}
	if err := check("bulkIncrementCurrentValuesAndFetchMetadata(user)", 2*len(ids), len(incr)+len(meta)); err != nil {
		return nil, err
	}
	out := make(map[string]model.UserStateSnapshot, len(ids))
	for i, id := range ids {
		vals := meta[i].Val()
		out[id] = model.UserStateSnapshot{CurrentValue: decimal.NewFromFloat(incr[i].Val()), Metadata: model.UserMetadata{PurchasedValue: asInt(vals, 0), PortfolioCount: asInt(vals, 1)}}
	}
	opResult = metrics.ResultSuccess
	return out, nil
}

func (s *Store) BulkRecalculateUserCurrentValuesAndFetchMetadata(ctx context.Context, userIDs []string) (map[string]model.UserStateSnapshot, error) {
	opStart := time.Now()
	opResult := metrics.ResultFailure
	defer func() { s.observeOperation(metrics.OpUserCurrent, opResult, opStart) }()

	portfolioSets := make([]*redis.StringSliceCmd, len(userIDs))
	pipe := s.rdb.Pipeline()
	for i, userID := range userIDs {
		portfolioSets[i] = pipe.SMembers(ctx, userPortfoliosKey(userID))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	type holding struct {
		userID string
		cmd    *redis.SliceCmd
	}
	holdings := make([]holding, 0)
	pipe = s.rdb.Pipeline()
	for i, userID := range userIDs {
		for _, portfolioID := range portfolioSets[i].Val() {
			id, err := strconv.ParseInt(portfolioID, 10, 64)
			if err != nil {
				continue
			}
			holdings = append(holdings, holding{
				userID: userID,
				cmd:    pipe.HMGet(ctx, pfKey(id), fCVP, fCV),
			})
		}
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	totals := make(map[string]decimal.Decimal, len(userIDs))
	for _, userID := range userIDs {
		totals[userID] = decimal.Zero
	}
	for _, holding := range holdings {
		values := holding.cmd.Val()
		currentValue := parseDecimal(asString(values, 0))
		if asString(values, 0) == "" {
			currentValue = parseDecimal(asString(values, 1))
		}
		totals[holding.userID] = totals[holding.userID].Add(currentValue)
	}

	metadata := make([]*redis.SliceCmd, len(userIDs))
	pipe = s.rdb.Pipeline()
	for i, userID := range userIDs {
		pipe.HSet(ctx, usrKey(userID), fCVP, totals[userID].String())
		metadata[i] = pipe.HMGet(ctx, usrKey(userID), fPV, fPC)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	out := make(map[string]model.UserStateSnapshot, len(userIDs))
	for i, userID := range userIDs {
		values := metadata[i].Val()
		out[userID] = model.UserStateSnapshot{
			CurrentValue: totals[userID],
			Metadata: model.UserMetadata{
				PurchasedValue: asInt(values, 0),
				PortfolioCount: asInt(values, 1),
			},
		}
	}
	opResult = metrics.ResultSuccess
	return out, nil
}

func (s *Store) DeletePortfolioState(ctx context.Context, id int64) error {
	stocks, _ := s.rdb.SMembers(ctx, portfolioStocksKey(id)).Result()
	pipe := s.rdb.Pipeline()
	for _, stock := range stocks {
		sid, _ := strconv.ParseInt(stock, 10, 64)
		pipe.Del(ctx, quantityKey(id, sid), currentValueKey(id, sid))
		pipe.HDel(ctx, pfKey(id), stockCurrentValueField(sid))
		pipe.SRem(ctx, stockPortfoliosKey(sid), strconv.FormatInt(id, 10))
	}
	pipe.Del(ctx, portfolioStocksKey(id))
	_, err := pipe.Exec(ctx)
	return err
}
func (s *Store) FindPortfolioPurchasedValue(ctx context.Context, id int64) int64 {
	return s.hint(ctx, pfKey(id), fPV)
}
func (s *Store) FindPortfolioCurrentValue(ctx context.Context, id int64) decimal.Decimal {
	start := time.Now()
	defer func() { s.observeOperation(metrics.OpPortfolioCurrent, metrics.ResultSuccess, start) }()
	return s.hdecFallback(ctx, pfKey(id), fCVP, fCV)
}
func (s *Store) FindAssetCount(ctx context.Context, id int64) int64 {
	return s.hint(ctx, pfKey(id), fAC)
}

func (s *Store) FindUserIDByPortfolioID(ctx context.Context, portfolioID int64) string {
	return s.rdb.HGet(ctx, pfKey(portfolioID), fU).Val()
}
func (s *Store) MapPortfolioToUser(ctx context.Context, portfolioID int64, userID string) error {
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, pfKey(portfolioID), fU, userID)
	pipe.SAdd(ctx, userPortfoliosKey(userID), strconv.FormatInt(portfolioID, 10))
	_, err := pipe.Exec(ctx)
	return err
}
func (s *Store) MapPortfolioToUserFromTrade(ctx context.Context, portfolioID int64, userID string) error {
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, pfKey(portfolioID), fU, userID, fUCP, "1")
	pipe.SAdd(ctx, userPortfoliosKey(userID), strconv.FormatInt(portfolioID, 10))
	_, err := pipe.Exec(ctx)
	return err
}
func (s *Store) IsPortfolioCountPending(ctx context.Context, portfolioID int64) bool {
	return s.rdb.HGet(ctx, pfKey(portfolioID), fUCP).Val() == "1"
}
func (s *Store) ClearPortfolioCountPending(ctx context.Context, portfolioID int64) error {
	return s.rdb.HDel(ctx, pfKey(portfolioID), fUCP).Err()
}
func (s *Store) RemovePortfolioUserMapping(ctx context.Context, portfolioID int64, userID string) error {
	pipe := s.rdb.Pipeline()
	pipe.HDel(ctx, pfKey(portfolioID), fU, fUCP)
	if userID != "" {
		pipe.SRem(ctx, userPortfoliosKey(userID), strconv.FormatInt(portfolioID, 10))
	}
	_, err := pipe.Exec(ctx)
	return err
}
func (s *Store) FindUserPurchasedValue(ctx context.Context, id string) int64 {
	return s.hint(ctx, usrKey(id), fPV)
}
func (s *Store) FindUserPortfolioCount(ctx context.Context, id string) int64 {
	return s.hint(ctx, usrKey(id), fPC)
}
func (s *Store) FindUserCurrentValue(ctx context.Context, id string) decimal.Decimal {
	start := time.Now()
	defer func() { s.observeOperation(metrics.OpUserCurrent, metrics.ResultSuccess, start) }()
	return s.hdecFallback(ctx, usrKey(id), fCVP, fCV)
}
func (s *Store) CalculateUserCurrentValue(ctx context.Context, id string) decimal.Decimal {
	start := time.Now()
	defer func() { s.observeOperation(metrics.OpUserCurrent, metrics.ResultSuccess, start) }()
	if v := s.hdecFallback(ctx, usrKey(id), fCVP, fCV); !v.IsZero() {
		return v
	}
	members := s.rdb.SMembers(ctx, userPortfoliosKey(id)).Val()
	total := decimal.Zero
	for _, m := range members {
		pid, _ := strconv.ParseInt(m, 10, 64)
		total = total.Add(s.hdecFallback(ctx, pfKey(pid), fCVP, fCV))
	}
	return total
}

func (s *Store) SavePortfolioValuation(ctx context.Context, v model.PortfolioValuation) error {
	start := time.Now()
	var err error
	defer func() { s.observeOperation(metrics.OpPortfolioValuation, resultFromError(err), start) }()
	_, err = s.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, pfKey(v.PortfolioID), map[string]any{fPV: v.PurchasedValue, fCV: v.CurrentValue, fPR: v.ProfitRate, fAC: v.AssetCount, fDel: "0", fUA: time.Now().UTC().Format(time.RFC3339Nano)})
		pipe.SAdd(ctx, "dirty:portfolio-valuations", strconv.FormatInt(v.PortfolioID, 10))
		return nil
	})
	return err
}
func (s *Store) SaveUserValuation(ctx context.Context, v model.UserValuation) error {
	start := time.Now()
	var err error
	defer func() { s.observeOperation(metrics.OpUserValuation, resultFromError(err), start) }()
	_, err = s.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, usrKey(v.UserID), map[string]any{fPV: v.PurchasedValue, fCV: v.CurrentValue, fPR: v.ProfitRate, fPC: v.PortfolioCount, fUA: time.Now().UTC().Format(time.RFC3339Nano)})
		pipe.SAdd(ctx, "dirty:user-valuations", v.UserID)
		return nil
	})
	return err
}
func (s *Store) BulkSavePortfolioValuations(ctx context.Context, vals []model.PortfolioValuation) error {
	start := time.Now()
	var err error
	defer func() { s.observeOperation(metrics.OpPortfolioValuation, resultFromError(err), start) }()
	pipe := s.rdb.Pipeline()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	dirty := make([]any, 0, len(vals))
	for _, v := range vals {
		portfolioID := strconv.FormatInt(v.PortfolioID, 10)
		pipe.HSet(ctx, pfKey(v.PortfolioID), map[string]any{fPV: v.PurchasedValue, fCV: v.CurrentValue, fPR: v.ProfitRate, fAC: v.AssetCount, fDel: "0", fUA: now})
		dirty = append(dirty, portfolioID)
	}
	if len(dirty) > 0 {
		pipe.SAdd(ctx, "dirty:portfolio-valuations", dirty...)
	}
	_, err = pipe.Exec(ctx)
	return err
}
func (s *Store) BulkSaveUserValuations(ctx context.Context, vals []model.UserValuation) error {
	start := time.Now()
	var err error
	defer func() { s.observeOperation(metrics.OpUserValuation, resultFromError(err), start) }()
	pipe := s.rdb.Pipeline()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	dirty := make([]any, 0, len(vals))
	for _, v := range vals {
		pipe.HSet(ctx, usrKey(v.UserID), map[string]any{fPV: v.PurchasedValue, fCV: v.CurrentValue, fPR: v.ProfitRate, fPC: v.PortfolioCount, fUA: now})
		dirty = append(dirty, v.UserID)
	}
	if len(dirty) > 0 {
		pipe.SAdd(ctx, "dirty:user-valuations", dirty...)
	}
	_, err = pipe.Exec(ctx)
	return err
}
func (s *Store) MarkPortfolioValuationDeleted(ctx context.Context, id int64) error {
	_, err := s.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, pfKey(id), map[string]any{fDel: "1", fDA: time.Now().UTC().Format(time.RFC3339Nano)})
		pipe.SAdd(ctx, "dirty:portfolio-valuation-deletions", strconv.FormatInt(id, 10))
		return nil
	})
	return err
}

func (s *Store) getDec(ctx context.Context, key string) decimal.Decimal {
	return parseDecimal(s.rdb.Get(ctx, key).Val())
}
func (s *Store) hint(ctx context.Context, key, field string) int64 {
	v := s.rdb.HGet(ctx, key, field).Val()
	n, _ := strconv.ParseInt(v, 10, 64)
	return n
}
func (s *Store) hdecFallback(ctx context.Context, key, primary, fallback string) decimal.Decimal {
	v := s.rdb.HGet(ctx, key, primary).Val()
	if v == "" {
		v = s.rdb.HGet(ctx, key, fallback).Val()
	}
	return parseDecimal(v)
}
func parseDecimal(s string) decimal.Decimal {
	if s == "" {
		return decimal.Zero
	}
	v, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Zero
	}
	return v
}
func toFloat(d decimal.Decimal) float64 { f, _ := d.Float64(); return f }
func asString(vals []any, i int) string {
	if i >= len(vals) || vals[i] == nil {
		return ""
	}
	return fmt.Sprint(vals[i])
}
func asInt(vals []any, i int) int64 { n, _ := strconv.ParseInt(asString(vals, i), 10, 64); return n }
func resultFromError(err error) string {
	if err != nil && err != redis.Nil {
		return metrics.ResultFailure
	}
	return metrics.ResultSuccess
}

func (s *Store) observeOperation(operation, result string, start time.Time) {
	if s.metrics == nil {
		return
	}
	s.metrics.ObserveRedisOperation(operation, result, start)
}

func (s *Store) observe(cmd string, err error, start time.Time) {
	if s.metrics == nil {
		return
	}
	res := metrics.ResultSuccess
	if err != nil && err != redis.Nil {
		res = metrics.ResultFailure
	}
	s.metrics.ObserveRedis(cmd, res, start)
}
