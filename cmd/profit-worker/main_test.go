package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type fakeConsumers struct {
	ready    bool
	active   int64
	required int64
}

func (f fakeConsumers) Ready() bool            { return f.ready }
func (f fakeConsumers) ActiveWorkers() int64   { return f.active }
func (f fakeConsumers) RequiredWorkers() int64 { return f.required }

func TestLivenessReportsDownDuringShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	rec := httptest.NewRecorder()
	livenessHandler(ctx)(rec, httptest.NewRequest(http.MethodGet, "/actuator/health/liveness", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status got %d want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestReadinessRequiresRedis(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer rdb.Close()

	rec := httptest.NewRecorder()
	readinessHandler(context.Background(), rdb, fakeConsumers{ready: true, active: 2, required: 2})(rec, httptest.NewRequest(http.MethodGet, "/actuator/health/readiness", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status got %d want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestReadinessRequiresKafkaWorkers(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	rec := httptest.NewRecorder()
	readinessHandler(context.Background(), rdb, fakeConsumers{ready: false, active: 1, required: 2})(rec, httptest.NewRequest(http.MethodGet, "/actuator/health/readiness", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status got %d want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestReadinessReportsUpWhenDependenciesReady(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	rec := httptest.NewRecorder()
	readinessHandler(context.Background(), rdb, fakeConsumers{ready: true, active: 2, required: 2})(rec, httptest.NewRequest(http.MethodGet, "/actuator/health/readiness", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status got %d want %d", rec.Code, http.StatusOK)
	}
}
