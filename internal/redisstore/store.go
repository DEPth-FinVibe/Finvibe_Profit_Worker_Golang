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
func (s *Store) StockCurrentValueKey(portfolioID, stockID int64) string {
	return currentValueKey(portfolioID, stockID)
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
	ccmd := make([]*redis.StringCmd, len(keys))
	start := time.Now()
	for i, k := range keys {
		qcmd[i] = pipe.Get(ctx, quantityKey(k.PortfolioID, k.StockID))
		ccmd[i] = pipe.Get(ctx, currentValueKey(k.PortfolioID, k.StockID))
	}
	_, err := pipe.Exec(ctx)
	s.observe("pipeline_get_stock_holdings", err, start)
	if err != nil && err != redis.Nil {
		return nil, err
	}
	if err := check("bulkFetchStockHoldings", 2*len(keys), len(qcmd)+len(ccmd)); err != nil {
		return nil, err
	}
	out := make(map[string]model.StockHolding, len(keys))
	for i, k := range keys {
		out[k.String()] = model.StockHolding{Quantity: parseDecimal(qcmd[i].Val()), CurrentValue: parseDecimal(ccmd[i].Val())}
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
func (s *Store) BulkIncrementPortfolioCurrentValuesAndFetchMetadata(ctx context.Context, deltas map[int64]decimal.Decimal) (map[int64]model.PortfolioStateSnapshot, error) {
	pipe := s.rdb.Pipeline()
	ids := make([]int64, 0, len(deltas))
	incr := make([]*redis.FloatCmd, 0, len(deltas))
	meta := make([]*redis.SliceCmd, 0, len(deltas))
	start := time.Now()
	for id, d := range deltas {
		ids = append(ids, id)
		incr = append(incr, pipe.HIncrByFloat(ctx, pfKey(id), fCVP, toFloat(d)))
		meta = append(meta, pipe.HMGet(ctx, pfKey(id), fPV, fAC, fU))
	}
	_, err := pipe.Exec(ctx)
	s.observe("pipeline_hincrbyfloat_hmget_portfolio", err, start)
	if err != nil && err != redis.Nil {
		return nil, err
	}
	if err := check("bulkIncrementCurrentValuesAndFetchMetadata(portfolio)", 2*len(ids), len(incr)+len(meta)); err != nil {
		return nil, err
	}
	out := make(map[int64]model.PortfolioStateSnapshot, len(ids))
	for i, id := range ids {
		vals := meta[i].Val()
		out[id] = model.PortfolioStateSnapshot{CurrentValue: decimal.NewFromFloat(incr[i].Val()), Metadata: model.PortfolioMetadata{PurchasedValue: asInt(vals, 0), AssetCount: asInt(vals, 1), UserID: asString(vals, 2), CurrentValue: decimal.NewFromFloat(incr[i].Val())}}
	}
	return out, nil
}
func (s *Store) BulkIncrementUserCurrentValuesAndFetchMetadata(ctx context.Context, deltas map[string]decimal.Decimal) (map[string]model.UserStateSnapshot, error) {
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
	return out, nil
}

func (s *Store) IncreaseStockQuantity(ctx context.Context, stockID, portfolioID int64, qty decimal.Decimal) (bool, error) {
	old := s.getDec(ctx, quantityKey(portfolioID, stockID))
	added := old.IsZero()
	neu := old.Add(qty)
	pipe := s.rdb.Pipeline()
	pipe.Set(ctx, quantityKey(portfolioID, stockID), neu.String(), 0)
	pipe.SAdd(ctx, stockPortfoliosKey(stockID), strconv.FormatInt(portfolioID, 10))
	pipe.SAdd(ctx, portfolioStocksKey(portfolioID), strconv.FormatInt(stockID, 10))
	_, err := pipe.Exec(ctx)
	return added, err
}
func (s *Store) DecreaseStockQuantity(ctx context.Context, stockID, portfolioID int64, qty decimal.Decimal) (bool, error) {
	old := s.getDec(ctx, quantityKey(portfolioID, stockID))
	neu := old.Sub(qty)
	if neu.Sign() <= 0 {
		pipe := s.rdb.Pipeline()
		pipe.Del(ctx, quantityKey(portfolioID, stockID), currentValueKey(portfolioID, stockID))
		pipe.SRem(ctx, stockPortfoliosKey(stockID), strconv.FormatInt(portfolioID, 10))
		pipe.SRem(ctx, portfolioStocksKey(portfolioID), strconv.FormatInt(stockID, 10))
		_, err := pipe.Exec(ctx)
		return true, err
	}
	return false, s.rdb.Set(ctx, quantityKey(portfolioID, stockID), neu.String(), 0).Err()
}
func (s *Store) AddPortfolioPurchasedValue(ctx context.Context, id, amount int64) error {
	return s.rdb.HIncrBy(ctx, pfKey(id), fPV, amount).Err()
}
func (s *Store) SubtractPortfolioPurchasedValue(ctx context.Context, id, amount int64) error {
	return s.rdb.HIncrBy(ctx, pfKey(id), fPV, -amount).Err()
}
func (s *Store) AddPortfolioCurrentValue(ctx context.Context, id int64, amount decimal.Decimal) error {
	return s.rdb.HIncrByFloat(ctx, pfKey(id), fCVP, toFloat(amount)).Err()
}
func (s *Store) SubtractPortfolioCurrentValue(ctx context.Context, id int64, amount decimal.Decimal) error {
	return s.rdb.HIncrByFloat(ctx, pfKey(id), fCVP, -toFloat(amount)).Err()
}
func (s *Store) AddStockCurrentValue(ctx context.Context, stockID, portfolioID int64, amount decimal.Decimal) error {
	v := s.getDec(ctx, currentValueKey(portfolioID, stockID)).Add(amount)
	return s.rdb.Set(ctx, currentValueKey(portfolioID, stockID), v.String(), 0).Err()
}
func (s *Store) SubtractStockCurrentValue(ctx context.Context, stockID, portfolioID int64, amount decimal.Decimal) error {
	v := s.getDec(ctx, currentValueKey(portfolioID, stockID)).Sub(amount)
	if v.Sign() <= 0 {
		return s.rdb.Del(ctx, currentValueKey(portfolioID, stockID)).Err()
	}
	return s.rdb.Set(ctx, currentValueKey(portfolioID, stockID), v.String(), 0).Err()
}
func (s *Store) IncreaseAssetCount(ctx context.Context, id int64) error {
	return s.rdb.HIncrBy(ctx, pfKey(id), fAC, 1).Err()
}
func (s *Store) DecreaseAssetCount(ctx context.Context, id int64) error {
	return s.rdb.HIncrBy(ctx, pfKey(id), fAC, -1).Err()
}
func (s *Store) DeletePortfolioState(ctx context.Context, id int64) error {
	stocks, _ := s.rdb.SMembers(ctx, portfolioStocksKey(id)).Result()
	pipe := s.rdb.Pipeline()
	for _, stock := range stocks {
		sid, _ := strconv.ParseInt(stock, 10, 64)
		pipe.Del(ctx, quantityKey(id, sid), currentValueKey(id, sid))
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
func (s *Store) RemovePortfolioUserMapping(ctx context.Context, portfolioID int64) error {
	userID := s.FindUserIDByPortfolioID(ctx, portfolioID)
	pipe := s.rdb.Pipeline()
	pipe.HDel(ctx, pfKey(portfolioID), fU)
	if userID != "" {
		pipe.SRem(ctx, userPortfoliosKey(userID), strconv.FormatInt(portfolioID, 10))
	}
	_, err := pipe.Exec(ctx)
	return err
}
func (s *Store) AddUserPurchasedValue(ctx context.Context, id string, amount int64) error {
	return s.rdb.HIncrBy(ctx, usrKey(id), fPV, amount).Err()
}
func (s *Store) SubtractUserPurchasedValue(ctx context.Context, id string, amount int64) error {
	return s.rdb.HIncrBy(ctx, usrKey(id), fPV, -amount).Err()
}
func (s *Store) IncreasePortfolioCount(ctx context.Context, id string) error {
	return s.rdb.HIncrBy(ctx, usrKey(id), fPC, 1).Err()
}
func (s *Store) DecreasePortfolioCount(ctx context.Context, id string) error {
	return s.rdb.HIncrBy(ctx, usrKey(id), fPC, -1).Err()
}
func (s *Store) FindUserPurchasedValue(ctx context.Context, id string) int64 {
	return s.hint(ctx, usrKey(id), fPV)
}
func (s *Store) FindUserPortfolioCount(ctx context.Context, id string) int64 {
	return s.hint(ctx, usrKey(id), fPC)
}
func (s *Store) FindUserCurrentValue(ctx context.Context, id string) decimal.Decimal {
	return s.hdecFallback(ctx, usrKey(id), fCVP, fCV)
}
func (s *Store) CalculateUserCurrentValue(ctx context.Context, id string) decimal.Decimal {
	if v := s.FindUserCurrentValue(ctx, id); !v.IsZero() {
		return v
	}
	members := s.rdb.SMembers(ctx, userPortfoliosKey(id)).Val()
	total := decimal.Zero
	for _, m := range members {
		pid, _ := strconv.ParseInt(m, 10, 64)
		total = total.Add(s.FindPortfolioCurrentValue(ctx, pid))
	}
	return total
}

func (s *Store) SavePortfolioValuation(ctx context.Context, v model.PortfolioValuation) error {
	_, err := s.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, pfKey(v.PortfolioID), map[string]any{fPV: v.PurchasedValue, fCV: v.CurrentValue, fPR: v.ProfitRate, fAC: v.AssetCount, fDel: "0", fUA: time.Now().UTC().Format(time.RFC3339Nano)})
		pipe.SAdd(ctx, "dirty:portfolio-valuations", strconv.FormatInt(v.PortfolioID, 10))
		return nil
	})
	return err
}
func (s *Store) SaveUserValuation(ctx context.Context, v model.UserValuation) error {
	_, err := s.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, usrKey(v.UserID), map[string]any{fPV: v.PurchasedValue, fCV: v.CurrentValue, fPR: v.ProfitRate, fPC: v.PortfolioCount, fUA: time.Now().UTC().Format(time.RFC3339Nano)})
		pipe.SAdd(ctx, "dirty:user-valuations", v.UserID)
		return nil
	})
	return err
}
func (s *Store) BulkSavePortfolioValuations(ctx context.Context, vals []model.PortfolioValuation) error {
	pipe := s.rdb.Pipeline()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, v := range vals {
		pipe.HSet(ctx, pfKey(v.PortfolioID), map[string]any{fPV: v.PurchasedValue, fCV: v.CurrentValue, fPR: v.ProfitRate, fAC: v.AssetCount, fDel: "0", fUA: now})
		pipe.SAdd(ctx, "dirty:portfolio-valuations", strconv.FormatInt(v.PortfolioID, 10))
	}
	_, err := pipe.Exec(ctx)
	return err
}
func (s *Store) BulkSaveUserValuations(ctx context.Context, vals []model.UserValuation) error {
	pipe := s.rdb.Pipeline()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, v := range vals {
		pipe.HSet(ctx, usrKey(v.UserID), map[string]any{fPV: v.PurchasedValue, fCV: v.CurrentValue, fPR: v.ProfitRate, fPC: v.PortfolioCount, fUA: now})
		pipe.SAdd(ctx, "dirty:user-valuations", v.UserID)
	}
	_, err := pipe.Exec(ctx)
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
