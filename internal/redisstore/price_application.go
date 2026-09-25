package redisstore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"finvibe-profit-worker-go/internal/model"
	"github.com/redis/go-redis/v9"
)

var (
	ErrStockPriceLockBusy = errors.New("stock price application lock is busy")
	ErrStockPriceLockLost = errors.New("stock price application lock ownership was lost")
)

type AppliedStockPrice struct {
	StockID   int64
	Price     int64
	Timestamp time.Time
	// ver가 없는 기존 반영 상태는 Timestamp로 유도한다.
	Version int64
}

type stockPriceLock struct {
	stockID int64
	key     string
	token   string
}

type StockPriceLockSet struct {
	store *Store
	locks []stockPriceLock
	ttl   time.Duration
}

func stockPriceApplicationKey(stockID int64) string {
	return fmt.Sprintf("stock:{%d}:price-application", stockID)
}

func stockPriceApplicationLockKey(stockID int64) string {
	return fmt.Sprintf("stock:{%d}:price-application-lock", stockID)
}

func (s *Store) AcquireStockPriceLocks(ctx context.Context, stockIDs []int64, ttl time.Duration) (*StockPriceLockSet, error) {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	ids := append([]int64(nil), stockIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	uniqueIDs := make([]int64, 0, len(ids))
	for i, stockID := range ids {
		if i > 0 && stockID == ids[i-1] {
			continue
		}
		uniqueIDs = append(uniqueIDs, stockID)
	}

	lockSet := &StockPriceLockSet{store: s, ttl: ttl}
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.BoolCmd, 0, len(uniqueIDs))
	candidates := make([]stockPriceLock, 0, len(uniqueIDs))
	for _, stockID := range uniqueIDs {
		token, err := randomToken()
		if err != nil {
			return nil, err
		}
		key := stockPriceApplicationLockKey(stockID)
		candidates = append(candidates, stockPriceLock{stockID: stockID, key: key, token: token})
		cmds = append(cmds, pipe.SetNX(ctx, key, token, ttl))
	}
	_, execErr := pipe.Exec(ctx)
	var busyStockID int64
	var commandErr error
	busy := false
	for i, cmd := range cmds {
		acquired, err := cmd.Result()
		if err != nil {
			if commandErr == nil {
				commandErr = err
			}
			continue
		}
		if acquired {
			lockSet.locks = append(lockSet.locks, candidates[i])
		} else if !busy {
			busyStockID = candidates[i].stockID
			busy = true
		}
	}
	if execErr != nil && execErr != redis.Nil {
		_ = lockSet.Release(context.Background())
		return nil, execErr
	}
	if commandErr != nil {
		_ = lockSet.Release(context.Background())
		return nil, commandErr
	}
	if busy {
		_ = lockSet.Release(context.Background())
		return nil, fmt.Errorf("%w: stock_id=%d", ErrStockPriceLockBusy, busyStockID)
	}
	return lockSet, nil
}

func (s *Store) BulkFetchAppliedStockPrices(ctx context.Context, stockIDs []int64) (map[int64]AppliedStockPrice, error) {
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(stockIDs))
	for i, stockID := range stockIDs {
		cmds[i] = pipe.HGetAll(ctx, stockPriceApplicationKey(stockID))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	states := make(map[int64]AppliedStockPrice, len(stockIDs))
	for i, stockID := range stockIDs {
		values := cmds[i].Val()
		if values["at"] == "" {
			continue
		}
		timestamp, err := time.Parse(time.RFC3339Nano, values["at"])
		if err != nil {
			return nil, fmt.Errorf("parse applied stock price timestamp for stock %d: %w", stockID, err)
		}
		price, err := strconv.ParseInt(values["price"], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse applied stock price for stock %d: %w", stockID, err)
		}
		version := model.VersionFromWallClock(timestamp)
		if raw := values["ver"]; raw != "" {
			version, err = strconv.ParseInt(raw, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parse applied stock price version for stock %d: %w", stockID, err)
			}
		}
		states[stockID] = AppliedStockPrice{StockID: stockID, Price: price, Timestamp: timestamp, Version: version}
	}
	return states, nil
}

const refreshStockPriceLockScript = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
return redis.call('PEXPIRE', KEYS[1], ARGV[2])
`

func (l *StockPriceLockSet) Refresh(ctx context.Context) error {
	pipe := l.store.rdb.Pipeline()
	cmds := make([]*redis.Cmd, len(l.locks))
	for i, lock := range l.locks {
		cmds[i] = pipe.Eval(ctx, refreshStockPriceLockScript, []string{lock.key}, lock.token, l.ttl.Milliseconds())
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return err
	}
	for i, cmd := range cmds {
		refreshed, err := cmd.Int64()
		if err != nil {
			return err
		}
		if refreshed != 1 {
			return fmt.Errorf("%w: stock_id=%d", ErrStockPriceLockLost, l.locks[i].stockID)
		}
	}
	return nil
}

const commitStockPriceApplicationScript = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
redis.call('HSET', KEYS[2], 'at', ARGV[2], 'price', ARGV[3], 'ver', ARGV[4])
redis.call('DEL', KEYS[1])
return 1
`

func (l *StockPriceLockSet) Commit(ctx context.Context, applied []AppliedStockPrice) error {
	byStock := make(map[int64]AppliedStockPrice, len(applied))
	for _, state := range applied {
		byStock[state.StockID] = state
	}
	pipe := l.store.rdb.Pipeline()
	cmds := make([]*redis.Cmd, 0, len(applied))
	stockIDs := make([]int64, 0, len(applied))
	for _, lock := range l.locks {
		state, ok := byStock[lock.stockID]
		if !ok {
			continue
		}
		cmds = append(cmds, pipe.Eval(ctx, commitStockPriceApplicationScript,
			[]string{lock.key, stockPriceApplicationKey(lock.stockID)},
			lock.token,
			state.Timestamp.UTC().Format(time.RFC3339Nano),
			strconv.FormatInt(state.Price, 10),
			strconv.FormatInt(state.Version, 10),
		))
		stockIDs = append(stockIDs, lock.stockID)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return err
	}
	for i, cmd := range cmds {
		committed, err := cmd.Int64()
		if err != nil {
			return err
		}
		if committed != 1 {
			return fmt.Errorf("%w: stock_id=%d", ErrStockPriceLockLost, stockIDs[i])
		}
	}
	return nil
}

const releaseStockPriceLockScript = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
return redis.call('DEL', KEYS[1])
`

func (l *StockPriceLockSet) Release(ctx context.Context) error {
	if len(l.locks) == 0 {
		return nil
	}
	pipe := l.store.rdb.Pipeline()
	for _, lock := range l.locks {
		pipe.Eval(ctx, releaseStockPriceLockScript, []string{lock.key}, lock.token)
	}
	_, err := pipe.Exec(ctx)
	if err == redis.Nil {
		return nil
	}
	return err
}

func randomToken() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}
