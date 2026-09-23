package redisstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestStockPriceLockCommitStoresApplicationStateAndReleasesLock(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := New(rdb, nil)
	lockTTL := 30 * time.Second
	appliedAt := time.Date(2026, 9, 6, 9, 0, 0, 123, time.UTC)

	locks, err := store.AcquireStockPriceLocks(ctx, []int64{20, 10}, lockTTL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireStockPriceLocks(ctx, []int64{10}, lockTTL); !errors.Is(err, ErrStockPriceLockBusy) {
		t.Fatalf("second acquire error got %v", err)
	}
	if err := locks.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if err := locks.Commit(ctx, []AppliedStockPrice{{StockID: 10, Price: 120, Timestamp: appliedAt}}); err != nil {
		t.Fatal(err)
	}
	if err := locks.Release(ctx); err != nil {
		t.Fatal(err)
	}

	states, err := store.BulkFetchAppliedStockPrices(ctx, []int64{10, 20})
	if err != nil {
		t.Fatal(err)
	}
	if got := states[10]; got.Price != 120 || !got.Timestamp.Equal(appliedAt) {
		t.Fatalf("applied state got %+v", got)
	}
	if _, exists := states[20]; exists {
		t.Fatalf("unexpected state for stock 20: %+v", states[20])
	}
	if mr.Exists(stockPriceApplicationLockKey(10)) || mr.Exists(stockPriceApplicationLockKey(20)) {
		t.Fatal("price application lock was not released")
	}
}

func TestStockPriceLockCannotCommitAfterOwnershipExpires(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := New(rdb, nil)

	locks, err := store.AcquireStockPriceLocks(ctx, []int64{10}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	mr.FastForward(2 * time.Second)
	err = locks.Commit(ctx, []AppliedStockPrice{{StockID: 10, Price: 120, Timestamp: time.Now()}})
	if !errors.Is(err, ErrStockPriceLockLost) {
		t.Fatalf("commit error got %v", err)
	}
	if mr.Exists(stockPriceApplicationKey(10)) {
		t.Fatal("application state was stored without lock ownership")
	}
}

func TestStockPriceLockRefreshExtendsOwnership(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := New(rdb, nil)

	locks, err := store.AcquireStockPriceLocks(ctx, []int64{10}, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	mr.FastForward(1500 * time.Millisecond)
	if err := locks.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	mr.FastForward(1500 * time.Millisecond)
	if err := locks.Commit(ctx, []AppliedStockPrice{{StockID: 10, Price: 120, Timestamp: time.Now()}}); err != nil {
		t.Fatalf("commit after refresh failed: %v", err)
	}
}
