package redisstore_test

import (
	"context"
	"testing"

	"finvibe-profit-worker-go/internal/redisstore"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

func TestBulkReplaceStockCurrentValuesIsRetrySafeAfterAtomicMutation(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := redisstore.New(rdb, nil)

	must(t, rdb.HSet(ctx, "pf:100", map[string]any{"pv": "420", "cvp": "500", "ac": "2", "u": "7"}).Err())
	must(t, rdb.Set(ctx, store.StockCurrentValueKey(100, 10), "300", 0).Err())
	must(t, rdb.Set(ctx, store.StockCurrentValueKey(100, 20), "200", 0).Err())
	replacements := map[int64][]redisstore.StockCurrentValueReplacement{
		100: {
			{StockID: 10, PreviousValue: decimal.NewFromInt(300), CurrentValue: decimal.NewFromInt(360)},
			{StockID: 20, PreviousValue: decimal.NewFromInt(200), CurrentValue: decimal.NewFromInt(240)},
		},
	}

	states, err := store.BulkReplaceStockCurrentValuesAndFetchMetadata(ctx, replacements)
	must(t, err)
	assertDecimal(t, states[100].CurrentValue, "600")
	assertHash(t, mr, "pf:100", "cvp", "600")
	assertHash(t, mr, "pf:100", "scv:10", "360")
	assertHash(t, mr, "pf:100", "scv:20", "240")

	// 호환 key projection 전에 중단된 상황처럼 기존 key는 이전 값을 유지한다.
	assertString(t, mr, store.StockCurrentValueKey(100, 10), "300")
	assertString(t, mr, store.StockCurrentValueKey(100, 20), "200")

	states, err = store.BulkReplaceStockCurrentValuesAndFetchMetadata(ctx, replacements)
	must(t, err)
	assertDecimal(t, states[100].CurrentValue, "600")
	assertHash(t, mr, "pf:100", "cvp", "600")
}

func TestBulkReplaceStockCurrentValuesPreservesConcurrentStockDeltas(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := redisstore.New(rdb, nil)

	must(t, rdb.HSet(ctx, "pf:100", map[string]any{"pv": "420", "cvp": "500", "ac": "2", "u": "7"}).Err())

	requests := []map[int64][]redisstore.StockCurrentValueReplacement{
		{100: {{StockID: 10, PreviousValue: decimal.NewFromInt(300), CurrentValue: decimal.NewFromInt(360)}}},
		{100: {{StockID: 20, PreviousValue: decimal.NewFromInt(200), CurrentValue: decimal.NewFromInt(240)}}},
	}
	errs := make(chan error, len(requests))
	for _, request := range requests {
		request := request
		go func() {
			_, err := store.BulkReplaceStockCurrentValuesAndFetchMetadata(ctx, request)
			errs <- err
		}()
	}
	for range requests {
		must(t, <-errs)
	}

	assertHash(t, mr, "pf:100", "cvp", "600")
	assertHash(t, mr, "pf:100", "scv:10", "360")
	assertHash(t, mr, "pf:100", "scv:20", "240")
}

func assertHash(t *testing.T, mr *miniredis.Miniredis, key, field, want string) {
	t.Helper()
	got := mr.HGet(key, field)
	parsed, err := decimal.NewFromString(got)
	must(t, err)
	assertDecimal(t, parsed, want)
}

func assertString(t *testing.T, mr *miniredis.Miniredis, key, want string) {
	t.Helper()
	got, err := mr.Get(key)
	must(t, err)
	if got != want {
		t.Fatalf("%s got %s want %s", key, got, want)
	}
}

func assertDecimal(t *testing.T, got decimal.Decimal, want string) {
	t.Helper()
	wantDecimal, err := decimal.NewFromString(want)
	must(t, err)
	if !got.Equal(wantDecimal) {
		t.Fatalf("decimal got %s want %s", got, want)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
