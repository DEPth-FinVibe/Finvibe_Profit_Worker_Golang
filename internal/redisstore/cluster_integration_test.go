package redisstore

import (
	"context"
	"os"
	"strings"
	"testing"

	"finvibe-profit-worker-go/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

// 실제 Redis Cluster에서 멱등 쓰기 Lua가 CROSSSLOT 없이 실행되고 한 번만 반영되는지 확인한다.
// REDIS_TEST_CLUSTER_NODES=127.0.0.1:7001,127.0.0.1:7002 처럼 노드를 주면 실행된다.
func clusterStoreOrSkip(t *testing.T) (context.Context, *redis.ClusterClient, *Store) {
	t.Helper()
	nodes := os.Getenv("REDIS_TEST_CLUSTER_NODES")
	if nodes == "" {
		t.Skip("REDIS_TEST_CLUSTER_NODES is not set")
	}
	client := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(nodes, ",")})
	ctx := context.Background()
	if err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
		return master.FlushAll(ctx).Err()
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return ctx, client, New(client, metrics.New(prometheus.NewRegistry()))
}

func TestIdempotentWritesRunOnRealCluster(t *testing.T) {
	ctx, client, store := clusterStoreOrSkip(t)
	amount := decimal.NewFromInt(360)

	for attempt := 0; attempt < 3; attempt++ {
		added, err := store.IncreaseStockQuantity(ctx, "trade:1", 10, 100, decimal.NewFromInt(3))
		if err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		if !added {
			t.Fatalf("attempt %d: added got false, want the first result", attempt)
		}
		if err := store.ApplyPortfolioTradeTotals(ctx, "trade:1", 100, 10, PortfolioTradeTotals{
			PurchasedValue: 360, CurrentValue: amount, StockCurrentValue: amount, AssetCount: 1,
		}); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		if err := store.ApplyUserTotals(ctx, "trade:1", "7", 360, amount, 0); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
	}

	assertClusterValue(t, ctx, client, "portfolio:100:stock:10:quantity", "3")
	assertClusterValue(t, ctx, client, "portfolio:100:stock:10:current-value", "360")
	assertClusterHash(t, ctx, client, "pf:100", "pv", "360")
	assertClusterHash(t, ctx, client, "pf:100", "cvp", "360")
	assertClusterHash(t, ctx, client, "pf:100", "scv:10", "360")
	assertClusterHash(t, ctx, client, "pf:100", "ac", "1")
	assertClusterHash(t, ctx, client, "usr:7", "pv", "360")
	assertClusterHash(t, ctx, client, "usr:7", "cvp", "360")

	if member, err := client.SIsMember(ctx, "stock:10:portfolios", "100").Result(); err != nil || !member {
		t.Fatalf("reverse index got %v err %v", member, err)
	}
}

func TestSellRemovesStockValueOnRealCluster(t *testing.T) {
	ctx, client, store := clusterStoreOrSkip(t)
	amount := decimal.NewFromInt(360)
	if _, err := store.IncreaseStockQuantity(ctx, "trade:1", 10, 100, decimal.NewFromInt(3)); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyPortfolioTradeTotals(ctx, "trade:1", 100, 10, PortfolioTradeTotals{
		PurchasedValue: 360, CurrentValue: amount, StockCurrentValue: amount, AssetCount: 1,
	}); err != nil {
		t.Fatal(err)
	}

	removed, err := store.DecreaseStockQuantity(ctx, "trade:2", 10, 100, decimal.NewFromInt(3))
	if err != nil {
		t.Fatal(err)
	}
	if !removed {
		t.Fatal("selling the whole holding should remove it")
	}
	if err := store.ApplyPortfolioTradeTotals(ctx, "trade:2", 100, 10, PortfolioTradeTotals{
		PurchasedValue: -360, CurrentValue: amount.Neg(), StockCurrentValue: amount.Neg(),
		RemoveStockCurrentValue: true, DeleteNonPositiveStockValue: true, AssetCount: -1,
	}); err != nil {
		t.Fatal(err)
	}

	if exists, err := client.Exists(ctx, "portfolio:100:stock:10:quantity").Result(); err != nil || exists != 0 {
		t.Fatalf("quantity key exists=%d err=%v", exists, err)
	}
	if exists, err := client.HExists(ctx, "pf:100", "scv:10").Result(); err != nil || exists {
		t.Fatalf("scv field exists=%v err=%v", exists, err)
	}
	assertClusterHash(t, ctx, client, "pf:100", "pv", "0")
	assertClusterHash(t, ctx, client, "pf:100", "ac", "0")
	if member, err := client.SIsMember(ctx, "stock:10:portfolios", "100").Result(); err != nil || member {
		t.Fatalf("reverse index still has the portfolio: %v err %v", member, err)
	}
}

func assertClusterValue(t *testing.T, ctx context.Context, client *redis.ClusterClient, key, want string) {
	t.Helper()
	got, err := client.Get(ctx, key).Result()
	if err != nil {
		t.Fatalf("%s: %v", key, err)
	}
	if got != want {
		t.Fatalf("%s got %q want %q", key, got, want)
	}
}

func assertClusterHash(t *testing.T, ctx context.Context, client *redis.ClusterClient, key, field, want string) {
	t.Helper()
	got, err := client.HGet(ctx, key, field).Result()
	if err != nil {
		t.Fatalf("%s %s: %v", key, field, err)
	}
	if got != want {
		t.Fatalf("%s %s got %q want %q", key, field, got, want)
	}
}
