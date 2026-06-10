package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"finvibe-profit-worker-go/internal/config"
	kconsumer "finvibe-profit-worker-go/internal/kafka"
	"finvibe-profit-worker-go/internal/metrics"
	"finvibe-profit-worker-go/internal/redisstore"
	"finvibe-profit-worker-go/internal/service"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	rdb := newRedisClient(cfg)
	store := redisstore.New(rdb, m)
	profit := service.NewProfitService(store, m)
	cache := service.NewCacheService(store, m)
	consumers := kconsumer.New(cfg, profit, cache, m)
	mux := http.NewServeMux()
	mux.Handle("/actuator/prometheus", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/actuator/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"UP"}`))
	})
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		slog.Info("http listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server", "err", err)
			stop()
		}
	}()
	consumers.Run(ctx)
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	_ = rdb.Close()
	slog.Info("profit worker stopped")
}

func newRedisClient(cfg config.Config) redis.UniversalClient {
	if cfg.RedisMode == "cluster" && len(cfg.RedisClusterNodes) > 0 {
		return redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    cfg.RedisClusterNodes,
			Password: cfg.RedisPassword,
		})
	}
	return redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
}

func init() { slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil))) }
