package config

import (
	"testing"
	"time"
)

func TestLoadStockPriceDLTDefaults(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_STOCK_PRICE_UPDATED_DLT", "")
	t.Setenv("KAFKA_GROUP_STOCK_PRICE_DLT", "")
	t.Setenv("KAFKA_CONCURRENCY_STOCK_PRICE_DLT", "")
	t.Setenv("KAFKA_STOCK_PRICE_DLT_ENABLED", "")

	cfg := Load()

	if cfg.StockDLTTopic != "market.stock-price-updated.v1.DLT" {
		t.Fatalf("StockDLTTopic got %q", cfg.StockDLTTopic)
	}
	if cfg.StockDLTGroup != "profit-worker-price-dlt" {
		t.Fatalf("StockDLTGroup got %q", cfg.StockDLTGroup)
	}
	if cfg.StockDLTConcurrency != 1 {
		t.Fatalf("StockDLTConcurrency got %d", cfg.StockDLTConcurrency)
	}
	if !cfg.StockDLTEnabled {
		t.Fatal("StockDLTEnabled got false")
	}
}

func TestLoadStockPriceDLTOverrides(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_STOCK_PRICE_UPDATED_DLT", "custom.price-dlt")
	t.Setenv("KAFKA_GROUP_STOCK_PRICE_DLT", "custom-price-dlt-group")
	t.Setenv("KAFKA_CONCURRENCY_STOCK_PRICE_DLT", "3")
	t.Setenv("KAFKA_STOCK_PRICE_DLT_ENABLED", "false")

	cfg := Load()

	if cfg.StockDLTTopic != "custom.price-dlt" {
		t.Fatalf("StockDLTTopic got %q", cfg.StockDLTTopic)
	}
	if cfg.StockDLTGroup != "custom-price-dlt-group" {
		t.Fatalf("StockDLTGroup got %q", cfg.StockDLTGroup)
	}
	if cfg.StockDLTConcurrency != 3 {
		t.Fatalf("StockDLTConcurrency got %d", cfg.StockDLTConcurrency)
	}
	if cfg.StockDLTEnabled {
		t.Fatal("StockDLTEnabled got true")
	}
}

func TestLoadPriceApplicationLockTTL(t *testing.T) {
	t.Setenv("PRICE_APPLICATION_LOCK_TTL_SECONDS", "45")

	cfg := Load()

	if cfg.PriceApplicationLockTTL != 45*time.Second {
		t.Fatalf("PriceApplicationLockTTL got %s", cfg.PriceApplicationLockTTL)
	}
}
