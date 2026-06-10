package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
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
	go m.RunPeriodicDump(ctx)
	rdb := newRedisClient(cfg)
	store := redisstore.New(rdb, m)
	profit := service.NewProfitService(store, m)
	cache := service.NewCacheService(store, m)
	consumers := kconsumer.New(cfg, profit, cache, m)
	mux := http.NewServeMux()
	mux.Handle("/actuator/prometheus", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/actuator/health", livenessHandler(ctx))
	mux.HandleFunc("/actuator/health/liveness", livenessHandler(ctx))
	mux.HandleFunc("/actuator/health/readiness", readinessHandler(ctx, rdb, consumers))
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

type consumerReadiness interface {
	Ready() bool
	ActiveWorkers() int64
	RequiredWorkers() int64
}

func livenessHandler(ctx context.Context) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if ctx.Err() != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"DOWN"}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"UP"}`))
	}
}

func readinessHandler(ctx context.Context, rdb redis.UniversalClient, consumers consumerReadiness) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if ctx.Err() != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"DOWN","shutdown":"true"}`))
			return
		}

		pingCtx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
		defer cancel()
		if err := rdb.Ping(pingCtx).Err(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"DOWN","redis":"DOWN"}`))
			return
		}

		if !consumers.Ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(fmt.Sprintf(`{"status":"DOWN","kafka":"DOWN","activeWorkers":%d,"requiredWorkers":%d}`, consumers.ActiveWorkers(), consumers.RequiredWorkers())))
			return
		}

		_, _ = w.Write([]byte(`{"status":"UP","redis":"UP","kafka":"UP"}`))
	}
}

func newRedisClient(cfg config.Config) redis.UniversalClient {
	if cfg.RedisMode == "cluster" && len(cfg.RedisClusterNodes) > 0 {
		addressMap := redisClusterAddressMap(cfg.RedisClusterNodes)
		return redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    cfg.RedisClusterNodes,
			Password: cfg.RedisPassword,
			NewClient: func(opt *redis.Options) *redis.Client {
				if replacement, ok := addressMap[opt.Addr]; ok {
					slog.Info("redis cluster address remapped", "from", opt.Addr, "to", replacement)
					opt.Addr = replacement
				}
				return redis.NewClient(opt)
			},
		})
	}
	return redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
}

func redisClusterAddressMap(addrs []string) map[string]string {
	mapping := make(map[string]string)
	for _, addr := range addrs {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			slog.Warn("redis cluster seed address ignored", "addr", addr, "err", err)
			continue
		}
		ips, err := net.LookupHost(host)
		if err != nil {
			slog.Warn("redis cluster seed lookup failed", "addr", addr, "err", err)
			continue
		}
		for _, ip := range ips {
			mapping[net.JoinHostPort(ip, port)] = addr
		}
	}
	return mapping
}

func init() { slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil))) }
