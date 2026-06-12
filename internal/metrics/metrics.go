package metrics

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	ResultSuccess = "success"
	ResultFailure = "failure"
	ResultSkipped = "skipped"

	EventStockPrice    = "stock_price_updated"
	EventTrade         = "portfolio_trade"
	EventPortfolioUser = "portfolio_user"

	ReasonUpdatedEventIgnored = "updated_event_ignored"

	OpStockRecalc        = "stock_price_recalculation"
	OpPortfolioCache     = "portfolio_cache_update"
	OpUserCache          = "user_cache_update"
	OpPortfolioCurrent   = "portfolio_current_value"
	OpUserCurrent        = "user_current_value"
	OpPortfolioValuation = "portfolio_valuation_save"
	OpUserValuation      = "user_valuation_save"
)

type Metrics struct {
	listener           *prometheus.HistogramVec
	service            *prometheus.HistogramVec
	phase              *prometheus.HistogramVec
	redisOperation     *prometheus.HistogramVec
	redisCommand       *prometheus.HistogramVec
	batch              *prometheus.SummaryVec
	dedup              *prometheus.SummaryVec
	eventAge           *prometheus.HistogramVec
	lastListener       *prometheus.GaugeVec
	lastService        *prometheus.GaugeVec
	lastEventAge       *prometheus.GaugeVec
	consumed           *prometheus.CounterVec
	skipped            *prometheus.CounterVec
	affectedPortfolios *prometheus.SummaryVec
	affectedUsers      *prometheus.SummaryVec

	// lightweight per-minute counters for console dump
	consumedTotal atomic.Int64
	failedTotal   atomic.Int64
	skippedTotal  atomic.Int64

	listenerDurationSum atomic.Int64
	listenerDurationCnt atomic.Int64
	serviceDurationSum  atomic.Int64
	serviceDurationCnt  atomic.Int64
}

func New(reg *prometheus.Registry) *Metrics {
	wrapped := prometheus.WrapRegistererWith(prometheus.Labels{"worker_runtime": "golang"}, reg)
	m := &Metrics{
		listener:           prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "profit_worker_listener_duration_seconds", Help: "Kafka listener duration"}, []string{"event_type", "result"}),
		service:            prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "profit_worker_service_duration_seconds", Help: "Application service duration"}, []string{"operation", "result"}),
		phase:              prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "profit_worker_phase_duration_seconds", Help: "Processing phase duration"}, []string{"operation", "phase", "result"}),
		redisOperation:     prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "profit_worker_redis_operation_duration_seconds", Help: "Redis operation duration"}, []string{"operation", "result"}),
		redisCommand:       prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "profit_worker_redis_command_duration_seconds", Help: "Redis command duration"}, []string{"command", "result"}),
		batch:              prometheus.NewSummaryVec(prometheus.SummaryOpts{Name: "profit_worker_batch_size", Help: "Kafka batch size"}, []string{"event_type"}),
		dedup:              prometheus.NewSummaryVec(prometheus.SummaryOpts{Name: "profit_worker_batch_deduplicated_size", Help: "Deduplicated batch size"}, []string{"event_type"}),
		eventAge:           prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "profit_worker_event_age_seconds", Help: "Event age"}, []string{"event_type"}),
		lastListener:       prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "profit_worker_listener_last_duration", Help: "Last Kafka listener duration"}, []string{"event_type", "result"}),
		lastService:        prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "profit_worker_service_last_duration", Help: "Last application service duration"}, []string{"operation", "result"}),
		lastEventAge:       prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "profit_worker_event_last_age", Help: "Last event age"}, []string{"event_type"}),
		consumed:           prometheus.NewCounterVec(prometheus.CounterOpts{Name: "profit_worker_events_consumed_total", Help: "Consumed events"}, []string{"event_type", "result"}),
		skipped:            prometheus.NewCounterVec(prometheus.CounterOpts{Name: "profit_worker_events_skipped_total", Help: "Skipped events"}, []string{"event_type", "reason"}),
		affectedPortfolios: prometheus.NewSummaryVec(prometheus.SummaryOpts{Name: "profit_worker_affected_portfolios", Help: "Affected portfolios"}, []string{"operation"}),
		affectedUsers:      prometheus.NewSummaryVec(prometheus.SummaryOpts{Name: "profit_worker_affected_users", Help: "Affected users"}, []string{"operation"}),
	}
	wrapped.MustRegister(
		m.listener,
		m.service,
		m.phase,
		m.redisOperation,
		m.redisCommand,
		m.batch,
		m.dedup,
		m.eventAge,
		m.lastListener,
		m.lastService,
		m.lastEventAge,
		m.consumed,
		m.skipped,
		m.affectedPortfolios,
		m.affectedUsers,
	)
	return m
}

func (m *Metrics) RunPeriodicDump(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.dump()
		}
	}
}

func (m *Metrics) dump() {
	consumed := m.consumedTotal.Swap(0)
	failed := m.failedTotal.Swap(0)
	skipped := m.skippedTotal.Swap(0)
	lSum := m.listenerDurationSum.Swap(0)
	lCnt := m.listenerDurationCnt.Swap(0)
	sSum := m.serviceDurationSum.Swap(0)
	sCnt := m.serviceDurationCnt.Swap(0)

	avgListener := 0.0
	if lCnt > 0 {
		avgListener = float64(lSum) / float64(lCnt) / 1e9
	}
	avgService := 0.0
	if sCnt > 0 {
		avgService = float64(sSum) / float64(sCnt) / 1e9
	}

	slog.Info("throughput stats",
		"consumed_per_min", consumed,
		"consumed_per_sec", float64(consumed)/60.0,
		"failed_per_min", failed,
		"skipped_per_min", skipped,
		"avg_listener_sec", avgListener,
		"avg_service_sec", avgService,
	)
}

func (m *Metrics) ObserveListener(event, result string, start time.Time) {
	seconds := time.Since(start).Seconds()
	m.listener.WithLabelValues(event, result).Observe(seconds)
	m.lastListener.WithLabelValues(event, result).Set(seconds)

	m.listenerDurationSum.Add(time.Since(start).Nanoseconds())
	m.listenerDurationCnt.Add(1)
}

func (m *Metrics) ObserveService(op, result string, start time.Time) {
	seconds := time.Since(start).Seconds()
	m.service.WithLabelValues(op, result).Observe(seconds)
	m.lastService.WithLabelValues(op, result).Set(seconds)

	m.serviceDurationSum.Add(time.Since(start).Nanoseconds())
	m.serviceDurationCnt.Add(1)
}

func (m *Metrics) ObservePhase(op, phase, result string, start time.Time) {
	m.phase.WithLabelValues(op, phase, result).Observe(time.Since(start).Seconds())
}

func (m *Metrics) ObserveRedisOperation(op, result string, start time.Time) {
	m.redisOperation.WithLabelValues(op, result).Observe(time.Since(start).Seconds())
}

func (m *Metrics) ObserveRedis(cmd, result string, start time.Time) {
	m.redisCommand.WithLabelValues(cmd, result).Observe(time.Since(start).Seconds())
}

func (m *Metrics) RecordBatch(event string, size, dedup int) {
	m.batch.WithLabelValues(event).Observe(float64(size))
	m.dedup.WithLabelValues(event).Observe(float64(dedup))
}

func (m *Metrics) RecordAge(event string, age time.Duration) {
	if age >= 0 {
		seconds := age.Seconds()
		m.eventAge.WithLabelValues(event).Observe(seconds)
		m.lastEventAge.WithLabelValues(event).Set(seconds)
	}
}

func (m *Metrics) RecordConsumed(event, result string) {
	m.consumed.WithLabelValues(event, result).Inc()
	m.consumedTotal.Add(1)
	switch result {
	case ResultFailure:
		m.failedTotal.Add(1)
	case ResultSkipped:
		m.skippedTotal.Add(1)
	}
}

func (m *Metrics) RecordSkipped(event, reason string) {
	m.skipped.WithLabelValues(event, reason).Inc()
	m.skippedTotal.Add(1)
}

func (m *Metrics) RecordAffectedPortfolios(op string, n int) {
	m.affectedPortfolios.WithLabelValues(op).Observe(float64(n))
}

func (m *Metrics) RecordAffectedUsers(op string, n int) {
	m.affectedUsers.WithLabelValues(op).Observe(float64(n))
}
