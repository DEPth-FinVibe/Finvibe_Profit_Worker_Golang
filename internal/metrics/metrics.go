package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	ResultSuccess      = "success"
	ResultFailure      = "failure"
	ResultSkipped      = "skipped"
	EventStockPrice    = "stock_price_updated"
	EventTrade         = "portfolio_trade"
	EventPortfolioUser = "portfolio_user"
	OpStockRecalc      = "stock_price_recalculation"
	OpPortfolioCache   = "portfolio_cache_update"
	OpUserCache        = "user_cache_update"
)

type Metrics struct {
	listener           *prometheus.HistogramVec
	service            *prometheus.HistogramVec
	phase              *prometheus.HistogramVec
	redis              *prometheus.HistogramVec
	batch              *prometheus.GaugeVec
	dedup              *prometheus.GaugeVec
	eventAge           *prometheus.HistogramVec
	consumed           *prometheus.CounterVec
	affectedPortfolios *prometheus.CounterVec
	affectedUsers      *prometheus.CounterVec
}

func New(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		listener:           prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "profit_worker_listener_duration_seconds", Help: "Kafka listener duration"}, []string{"event_type", "result"}),
		service:            prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "profit_worker_service_duration_seconds", Help: "Application service duration"}, []string{"operation", "result"}),
		phase:              prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "profit_worker_phase_duration_seconds", Help: "Processing phase duration"}, []string{"operation", "phase", "result"}),
		redis:              prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "profit_worker_redis_command_duration_seconds", Help: "Redis command duration"}, []string{"command", "result"}),
		batch:              prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "profit_worker_batch_size", Help: "Kafka batch size"}, []string{"event_type"}),
		dedup:              prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "profit_worker_deduplicated_size", Help: "Deduplicated batch size"}, []string{"event_type"}),
		eventAge:           prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "profit_worker_event_age_seconds", Help: "Event age"}, []string{"event_type"}),
		consumed:           prometheus.NewCounterVec(prometheus.CounterOpts{Name: "profit_worker_events_consumed_total", Help: "Consumed events"}, []string{"event_type", "result"}),
		affectedPortfolios: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "profit_worker_affected_portfolios_total", Help: "Affected portfolios"}, []string{"operation"}),
		affectedUsers:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "profit_worker_affected_users_total", Help: "Affected users"}, []string{"operation"}),
	}
	reg.MustRegister(m.listener, m.service, m.phase, m.redis, m.batch, m.dedup, m.eventAge, m.consumed, m.affectedPortfolios, m.affectedUsers)
	return m
}

func (m *Metrics) ObserveListener(event, result string, start time.Time) {
	m.listener.WithLabelValues(event, result).Observe(time.Since(start).Seconds())
}
func (m *Metrics) ObserveService(op, result string, start time.Time) {
	m.service.WithLabelValues(op, result).Observe(time.Since(start).Seconds())
}
func (m *Metrics) ObservePhase(op, phase, result string, start time.Time) {
	m.phase.WithLabelValues(op, phase, result).Observe(time.Since(start).Seconds())
}
func (m *Metrics) ObserveRedis(cmd, result string, start time.Time) {
	m.redis.WithLabelValues(cmd, result).Observe(time.Since(start).Seconds())
}
func (m *Metrics) RecordBatch(event string, size, dedup int) {
	m.batch.WithLabelValues(event).Set(float64(size))
	m.dedup.WithLabelValues(event).Set(float64(dedup))
}
func (m *Metrics) RecordAge(event string, age time.Duration) {
	if age >= 0 {
		m.eventAge.WithLabelValues(event).Observe(age.Seconds())
	}
}
func (m *Metrics) RecordConsumed(event, result string) {
	m.consumed.WithLabelValues(event, result).Inc()
}
func (m *Metrics) RecordAffectedPortfolios(op string, n int) {
	m.affectedPortfolios.WithLabelValues(op).Add(float64(n))
}
func (m *Metrics) RecordAffectedUsers(op string, n int) {
	m.affectedUsers.WithLabelValues(op).Add(float64(n))
}
