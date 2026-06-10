package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"finvibe-profit-worker-go/internal/config"
	"finvibe-profit-worker-go/internal/metrics"
	"finvibe-profit-worker-go/internal/model"
	"finvibe-profit-worker-go/internal/service"
	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Consumers struct {
	cfg     config.Config
	profit  *service.ProfitService
	cache   *service.CacheService
	metrics *metrics.Metrics
}

type message struct {
	value []byte
	raw   *ckafka.Message
}

func New(cfg config.Config, p *service.ProfitService, c *service.CacheService, m *metrics.Metrics) *Consumers {
	return &Consumers{cfg: cfg, profit: p, cache: c, metrics: m}
}

func (c *Consumers) Run(ctx context.Context) {
	c.runGroup(ctx, c.cfg.StockConcurrency, c.cfg.StockTopic, c.cfg.StockGroup, c.handleStock)
	c.runGroup(ctx, c.cfg.TradeConcurrency, c.cfg.TradeTopic, c.cfg.TradeGroup, c.handleTrade)
	c.runGroup(ctx, c.cfg.PortfolioUserConcurrency, c.cfg.PortfolioUserTopic, c.cfg.PortfolioUserGroup, c.handlePortfolioUser)
}

func (c *Consumers) runGroup(ctx context.Context, n int, topic, group string, handler func(context.Context, []message) error) {
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		go func(worker int) {
			consumer, err := c.newConsumer(group)
			if err != nil {
				slog.Error("kafka consumer create", "topic", topic, "group", group, "err", err)
				return
			}
			defer consumer.Close()

			if err := consumer.SubscribeTopics([]string{topic}, nil); err != nil {
				slog.Error("kafka subscribe", "topic", topic, "group", group, "err", err)
				return
			}

			for ctx.Err() == nil {
				batch, err := c.fetchBatch(ctx, consumer)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					slog.Error("kafka fetch", "topic", topic, "group", group, "err", err)
					time.Sleep(500 * time.Millisecond)
					continue
				}
				if len(batch) == 0 {
					continue
				}
				if err := handler(ctx, batch); err != nil {
					slog.Error("kafka handle", "topic", topic, "group", group, "err", err)
					continue
				}
				for _, msg := range batch {
					if _, err := consumer.StoreMessage(msg.raw); err != nil {
						slog.Error("kafka store offset", "topic", topic, "group", group, "err", err)
						break
					}
				}
				if _, err := consumer.Commit(); err != nil {
					slog.Error("kafka commit", "topic", topic, "group", group, "err", err)
				}
			}
		}(i)
	}
}

func (c *Consumers) newConsumer(group string) (*ckafka.Consumer, error) {
	cfg := &ckafka.ConfigMap{
		"bootstrap.servers":        strings.Join(c.cfg.KafkaBrokers, ","),
		"group.id":                 group,
		"auto.offset.reset":        "latest",
		"enable.auto.commit":       false,
		"enable.auto.offset.store": false,
	}
	if c.cfg.KafkaGroupProtocol != "" {
		_ = cfg.SetKey("group.protocol", c.cfg.KafkaGroupProtocol)
	}
	return ckafka.NewConsumer(cfg)
}

func (c *Consumers) fetchBatch(ctx context.Context, consumer *ckafka.Consumer) ([]message, error) {
	limit := c.cfg.MaxPollRecords
	if limit < 1 {
		limit = 1
	}
	wait := c.cfg.BatchMaxWait
	if wait <= 0 {
		wait = 50 * time.Millisecond
	}

	batch := make([]message, 0, limit)
	deadline := time.Now().Add(wait)
	for len(batch) < limit && ctx.Err() == nil {
		remaining := time.Until(deadline)
		if len(batch) > 0 && remaining <= 0 {
			return batch, nil
		}
		pollMS := int(remaining.Milliseconds())
		if len(batch) == 0 || pollMS < 1 {
			pollMS = 100
		}
		event := consumer.Poll(pollMS)
		if event == nil {
			if len(batch) > 0 {
				return batch, nil
			}
			continue
		}
		switch ev := event.(type) {
		case *ckafka.Message:
			batch = append(batch, message{value: ev.Value, raw: ev})
		case ckafka.Error:
			if ev.IsTimeout() {
				if len(batch) > 0 {
					return batch, nil
				}
				continue
			}
			return nil, ev
		default:
			// Rebalance/stat/log events are not data. Poll again.
		}
	}
	return batch, ctx.Err()
}

func (c *Consumers) handleStock(ctx context.Context, msgs []message) error {
	start := time.Now()
	result := metrics.ResultFailure
	defer func() { c.metrics.ObserveListener(metrics.EventStockPrice, result, start) }()
	latest := map[int64]model.StockPriceUpdatedEvent{}
	order := make([]int64, 0, len(msgs))
	for _, m := range msgs {
		ev, err := model.Decode[model.StockPriceUpdatedEvent](m.value)
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

func (c *Consumers) handleTrade(ctx context.Context, msgs []message) error {
	start := time.Now()
	result := metrics.ResultFailure
	defer func() { c.metrics.ObserveListener(metrics.EventTrade, result, start) }()
	reqs := make([]model.PortfolioCacheUpdateRequest, 0, len(msgs))
	for _, m := range msgs {
		ev, err := model.Decode[model.PortfolioTradeEvent](m.value)
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

func (c *Consumers) handlePortfolioUser(ctx context.Context, msgs []message) error {
	start := time.Now()
	result := metrics.ResultFailure
	defer func() { c.metrics.ObserveListener(metrics.EventPortfolioUser, result, start) }()
	reqs := make([]model.UserCacheUpdateRequest, 0, len(msgs))
	skipped := 0
	for _, m := range msgs {
		ev, err := model.Decode[model.PortfolioUserEvent](m.value)
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
		if !ev.OccurredAt.IsZero() {
			c.metrics.RecordAge(metrics.EventPortfolioUser, time.Since(ev.OccurredAt))
		}
	}
	if len(reqs) > 0 {
		if err := c.cache.UpdateUserCaches(ctx, reqs); err != nil {
			c.recordMany(metrics.EventPortfolioUser, metrics.ResultFailure, len(reqs))
			return err
		}
	}
	c.recordMany(metrics.EventPortfolioUser, metrics.ResultSuccess, len(reqs))
	for i := 0; i < skipped; i++ {
		c.metrics.RecordSkipped(metrics.EventPortfolioUser, metrics.ReasonUpdatedEventIgnored)
		c.metrics.RecordConsumed(metrics.EventPortfolioUser, metrics.ResultSkipped)
	}
	result = metrics.ResultSuccess
	return nil
}

func (c *Consumers) recordMany(event, result string, n int) {
	for i := 0; i < n; i++ {
		c.metrics.RecordConsumed(event, result)
	}
}
