package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"finvibe-profit-worker-go/internal/config"
	"finvibe-profit-worker-go/internal/metrics"
	"finvibe-profit-worker-go/internal/model"
	"finvibe-profit-worker-go/internal/service"
	"github.com/segmentio/kafka-go"
)

type Consumers struct {
	cfg     config.Config
	profit  *service.ProfitService
	cache   *service.CacheService
	metrics *metrics.Metrics
}

func New(cfg config.Config, p *service.ProfitService, c *service.CacheService, m *metrics.Metrics) *Consumers {
	return &Consumers{cfg, p, c, m}
}
func (c *Consumers) Run(ctx context.Context) {
	c.runGroup(ctx, c.cfg.StockConcurrency, c.cfg.StockTopic, c.cfg.StockGroup, c.handleStock)
	c.runGroup(ctx, c.cfg.TradeConcurrency, c.cfg.TradeTopic, c.cfg.TradeGroup, c.handleTrade)
	c.runGroup(ctx, c.cfg.PortfolioUserConcurrency, c.cfg.PortfolioUserTopic, c.cfg.PortfolioUserGroup, c.handlePortfolioUser)
}
func (c *Consumers) runGroup(ctx context.Context, n int, topic, group string, handler func(context.Context, []kafka.Message) error) {
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		go func(worker int) {
			r := kafka.NewReader(kafka.ReaderConfig{Brokers: c.cfg.KafkaBrokers, Topic: topic, GroupID: group, MinBytes: 1, MaxBytes: 10e6, MaxWait: 500 * time.Millisecond})
			defer r.Close()
			for {
				batch, err := c.fetchBatch(ctx, r)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					slog.Error("kafka fetch", "topic", topic, "err", err)
					continue
				}
				if err := handler(ctx, batch); err != nil {
					slog.Error("kafka handle", "topic", topic, "err", err)
					continue
				}
				if err := r.CommitMessages(ctx, batch...); err != nil {
					slog.Error("kafka commit", "topic", topic, "err", err)
				}
			}
		}(i)
	}
}

func (c *Consumers) fetchBatch(ctx context.Context, r *kafka.Reader) ([]kafka.Message, error) {
	limit := c.cfg.MaxPollRecords
	if limit < 1 {
		limit = 1
	}

	first, err := r.FetchMessage(ctx)
	if err != nil {
		return nil, err
	}

	batch := make([]kafka.Message, 0, limit)
	batch = append(batch, first)
	if limit == 1 {
		return batch, nil
	}

	wait := c.cfg.BatchMaxWait
	if wait <= 0 {
		wait = 50 * time.Millisecond
	}
	deadline := time.Now().Add(wait)

	for len(batch) < limit {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return batch, nil
		}
		fetchCtx, cancel := context.WithTimeout(ctx, remaining)
		msg, err := r.FetchMessage(fetchCtx)
		timedOut := fetchCtx.Err() != nil && ctx.Err() == nil
		cancel()
		if err != nil {
			if timedOut {
				return batch, nil
			}
			return nil, err
		}
		batch = append(batch, msg)
	}
	return batch, nil
}

func (c *Consumers) handleStock(ctx context.Context, msgs []kafka.Message) error {
	start := time.Now()
	result := metrics.ResultFailure
	defer func() { c.metrics.ObserveListener(metrics.EventStockPrice, result, start) }()
	latest := map[int64]model.StockPriceUpdatedEvent{}
	order := make([]int64, 0, len(msgs))
	for _, m := range msgs {
		ev, err := model.Decode[model.StockPriceUpdatedEvent](m.Value)
		if err != nil {
			c.recordMany(metrics.EventStockPrice, metrics.ResultFailure, len(msgs))
			return err
		}
		if _, seen := latest[ev.StockID]; !seen {
			order = append(order, ev.StockID)
		}
		latest[ev.StockID] = ev
	}
	c.metrics.RecordBatch(metrics.EventStockPrice, len(msgs), len(latest))
	reqs := make([]model.ProfitCalculationRequest, 0, len(latest))
	for _, id := range order {
		ev := latest[id]
		price, err := ev.Price.Int64Exact()
		if err != nil {
			c.recordMany(metrics.EventStockPrice, metrics.ResultFailure, len(latest))
			return err
		}
		c.metrics.RecordAge(metrics.EventStockPrice, time.Since(ev.UpdatedAt.Time))
		reqs = append(reqs, model.ProfitCalculationRequest{StockID: ev.StockID, NewPrice: price, Timestamp: ev.UpdatedAt.Time})
	}
	if err := c.profit.UpdateProfitsByStockPriceChanges(ctx, reqs); err != nil {
		c.recordMany(metrics.EventStockPrice, metrics.ResultFailure, len(reqs))
		return err
	}
	c.recordMany(metrics.EventStockPrice, metrics.ResultSuccess, len(reqs))
	result = metrics.ResultSuccess
	return nil
}
func (c *Consumers) handleTrade(ctx context.Context, msgs []kafka.Message) error {
	start := time.Now()
	result := metrics.ResultFailure
	defer func() { c.metrics.ObserveListener(metrics.EventTrade, result, start) }()
	reqs := make([]model.PortfolioCacheUpdateRequest, 0, len(msgs))
	for _, m := range msgs {
		ev, err := model.Decode[model.PortfolioTradeEvent](m.Value)
		if err != nil {
			return err
		}
		qty := ev.Amount.Decimal
		if qty.Sign() <= 0 {
			return fmt.Errorf("trade amount must be positive: %s", qty)
		}
		typ := model.StockBuy
		if ev.Type == "SELL" {
			typ = model.StockSell
		} else if ev.Type != "BUY" {
			return fmt.Errorf("unsupported trade type: %s", ev.Type)
		}
		reqs = append(reqs, model.PortfolioCacheUpdateRequest{PortfolioID: ev.PortfolioID, StockID: ev.StockID, Type: typ, Price: ev.Price, Quantity: qty})
	}
	if err := c.cache.UpdatePortfolioCaches(ctx, reqs); err != nil {
		c.recordMany(metrics.EventTrade, metrics.ResultFailure, len(reqs))
		return err
	}
	c.recordMany(metrics.EventTrade, metrics.ResultSuccess, len(reqs))
	result = metrics.ResultSuccess
	return nil
}
func (c *Consumers) handlePortfolioUser(ctx context.Context, msgs []kafka.Message) error {
	start := time.Now()
	result := metrics.ResultFailure
	defer func() { c.metrics.ObserveListener(metrics.EventPortfolioUser, result, start) }()
	reqs := make([]model.UserCacheUpdateRequest, 0, len(msgs))
	skipped := 0
	for _, m := range msgs {
		ev, err := model.Decode[model.PortfolioUserEvent](m.Value)
		if err != nil {
			return err
		}
		switch ev.EventType {
		case "CREATED":
			reqs = append(reqs, model.UserCacheUpdateRequest{UserID: ev.UserID, PortfolioID: ev.PortfolioID, Type: model.PortfolioCreated})
		case "DELETED":
			reqs = append(reqs, model.UserCacheUpdateRequest{UserID: ev.UserID, PortfolioID: ev.PortfolioID, Type: model.PortfolioDeleted})
		case "UPDATED":
			skipped++
		default:
			return fmt.Errorf("unsupported portfolio user event type: %s", ev.EventType)
		}
	}
	if len(reqs) > 0 {
		if err := c.cache.UpdateUserCaches(ctx, reqs); err != nil {
			c.recordMany(metrics.EventPortfolioUser, metrics.ResultFailure, len(reqs))
			return err
		}
	}
	c.recordMany(metrics.EventPortfolioUser, metrics.ResultSuccess, len(reqs))
	c.recordMany(metrics.EventPortfolioUser, metrics.ResultSkipped, skipped)
	result = metrics.ResultSuccess
	return nil
}
func (c *Consumers) recordMany(event, result string, n int) {
	for i := 0; i < n; i++ {
		c.metrics.RecordConsumed(event, result)
	}
}
