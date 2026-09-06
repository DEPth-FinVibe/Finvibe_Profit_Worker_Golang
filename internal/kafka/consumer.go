package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
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
	active  atomic.Int64
}

type message struct {
	value []byte
	raw   *ckafka.Message
}

func New(cfg config.Config, p *service.ProfitService, c *service.CacheService, m *metrics.Metrics) *Consumers {
	return &Consumers{cfg: cfg, profit: p, cache: c, metrics: m}
}

func (c *Consumers) Run(ctx context.Context) {
	c.runGroup(ctx, c.cfg.StockConcurrency, c.cfg.StockTopic, c.cfg.StockGroup, "latest", c.handleStock)
	if c.cfg.StockDLTEnabled {
		c.runGroup(ctx, c.cfg.StockDLTConcurrency, c.cfg.StockDLTTopic, c.cfg.StockDLTGroup, "earliest", c.handleStockDLT)
	}
	c.runGroup(ctx, c.cfg.TradeConcurrency, c.cfg.TradeTopic, c.cfg.TradeGroup, "latest", c.handleTrade)
	c.runGroup(ctx, c.cfg.PortfolioUserConcurrency, c.cfg.PortfolioUserTopic, c.cfg.PortfolioUserGroup, "latest", c.handlePortfolioUser)
}

func (c *Consumers) Ready() bool {
	return c.ActiveWorkers() >= c.RequiredWorkers()
}

func (c *Consumers) ActiveWorkers() int64 {
	return c.active.Load()
}

func (c *Consumers) RequiredWorkers() int64 {
	required := normalizeConcurrency(c.cfg.StockConcurrency) + normalizeConcurrency(c.cfg.TradeConcurrency) + normalizeConcurrency(c.cfg.PortfolioUserConcurrency)
	if c.cfg.StockDLTEnabled {
		required += normalizeConcurrency(c.cfg.StockDLTConcurrency)
	}
	return int64(required)
}

func normalizeConcurrency(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

func (c *Consumers) runGroup(ctx context.Context, n int, topic, group, offsetReset string, handler func(context.Context, []message) error) {
	n = normalizeConcurrency(n)
	for i := 0; i < n; i++ {
		go func(worker int) {
			consumer, err := c.newConsumer(group, offsetReset)
			if err != nil {
				slog.Error("kafka consumer create", "topic", topic, "group", group, "err", err)
				return
			}
			defer consumer.Close()

			if err := consumer.SubscribeTopics([]string{topic}, nil); err != nil {
				slog.Error("kafka subscribe", "topic", topic, "group", group, "err", err)
				return
			}

			c.active.Add(1)
			defer c.active.Add(-1)
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
					if seekErr := rewindBatch(consumer, batch); seekErr != nil {
						slog.Error("kafka rewind", "topic", topic, "group", group, "err", seekErr)
					}
					continue
				}
				stored := true
				for _, msg := range batch {
					if _, err := consumer.StoreMessage(msg.raw); err != nil {
						slog.Error("kafka store offset", "topic", topic, "group", group, "err", err)
						stored = false
						break
					}
				}
				if !stored {
					if seekErr := rewindBatch(consumer, batch); seekErr != nil {
						slog.Error("kafka rewind after offset store failure", "topic", topic, "group", group, "err", seekErr)
					}
					continue
				}
				if _, err := consumer.Commit(); err != nil {
					slog.Error("kafka commit", "topic", topic, "group", group, "err", err)
					if seekErr := rewindBatch(consumer, batch); seekErr != nil {
						slog.Error("kafka rewind after commit failure", "topic", topic, "group", group, "err", seekErr)
					}
				}
			}
		}(i)
	}
}

func (c *Consumers) newConsumer(group, offsetReset string) (*ckafka.Consumer, error) {
	cfg := c.consumerConfig(group, offsetReset)
	return ckafka.NewConsumer(cfg)
}

func (c *Consumers) consumerConfig(group, offsetReset string) *ckafka.ConfigMap {
	if offsetReset == "" {
		offsetReset = "latest"
	}
	cfg := &ckafka.ConfigMap{
		"bootstrap.servers":        strings.Join(c.cfg.KafkaBrokers, ","),
		"group.id":                 group,
		"auto.offset.reset":        offsetReset,
		"enable.auto.commit":       false,
		"enable.auto.offset.store": false,
	}
	if c.cfg.KafkaGroupProtocol != "" {
		_ = cfg.SetKey("group.protocol", c.cfg.KafkaGroupProtocol)
	}
	return cfg
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
	return c.handleStockMessages(ctx, msgs, metrics.EventStockPrice)
}

func (c *Consumers) handleStockDLT(ctx context.Context, msgs []message) error {
	return c.handleStockMessages(ctx, msgs, metrics.EventStockPriceDLT)
}

func (c *Consumers) handleStockMessages(ctx context.Context, msgs []message, eventType string) error {
	start := time.Now()
	result := metrics.ResultFailure
	defer func() { c.metrics.ObserveListener(eventType, result, start) }()
	latest := map[int64]model.StockPriceUpdatedEvent{}
	order := make([]int64, 0, len(msgs))
	for _, m := range msgs {
		ev, err := model.Decode[model.StockPriceUpdatedEvent](m.value)
		if err != nil {
			c.recordMany(eventType, metrics.ResultFailure, len(msgs))
			return err
		}
		current, seen := latest[ev.StockID]
		if !seen {
			order = append(order, ev.StockID)
			latest[ev.StockID] = ev
			continue
		}
		switch {
		case ev.UpdatedAt.After(current.UpdatedAt.Time):
			c.metrics.RecordSkipped(eventType, metrics.ReasonStalePriceEvent)
			latest[ev.StockID] = ev
		case ev.UpdatedAt.Before(current.UpdatedAt.Time):
			c.metrics.RecordSkipped(eventType, metrics.ReasonStalePriceEvent)
		case ev.Price.Equal(current.Price.Decimal):
			c.metrics.RecordSkipped(eventType, metrics.ReasonDuplicatePriceEvent)
		default:
			c.metrics.RecordSkipped(eventType, metrics.ReasonPriceTimestampConflict)
			slog.Warn("stock price timestamp conflict in batch", "event_type", eventType, "stock_id", ev.StockID, "updated_at", ev.UpdatedAt.Time)
		}
	}
	c.metrics.RecordBatch(eventType, len(msgs), len(latest))
	reqs := make([]model.ProfitCalculationRequest, 0, len(latest))
	for _, id := range order {
		ev := latest[id]
		price, err := ev.Price.Int64Exact()
		if err != nil {
			c.recordMany(eventType, metrics.ResultFailure, len(latest))
			return err
		}
		c.metrics.RecordAge(eventType, time.Since(ev.UpdatedAt.Time))
		reqs = append(reqs, model.ProfitCalculationRequest{StockID: ev.StockID, NewPrice: price, Timestamp: ev.UpdatedAt.Time})
	}
	outcome, err := c.profit.UpdateProfitsByStockPriceChanges(ctx, reqs)
	if err != nil {
		c.recordMany(eventType, metrics.ResultFailure, len(reqs))
		return err
	}
	for reason, count := range outcome.Skipped {
		for i := 0; i < count; i++ {
			c.metrics.RecordSkipped(eventType, reason)
		}
	}
	c.recordMany(eventType, metrics.ResultSuccess, len(reqs))
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
		reqs = append(reqs, model.PortfolioCacheUpdateRequest{TradeID: ev.TradeID, PortfolioID: ev.PortfolioID, StockID: ev.StockID, UserID: ev.UserID, Type: typ, Price: ev.Price, Quantity: qty})
	}
	for _, request := range reqs {
		if err := c.cache.UpdatePortfolioCaches(ctx, []model.PortfolioCacheUpdateRequest{request}); err != nil {
			c.recordMany(metrics.EventTrade, metrics.ResultFailure, 1)
			return err
		}
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
	for _, m := range msgs {
		ev, err := model.Decode[model.PortfolioUserEvent](m.value)
		if err != nil {
			c.recordMany(metrics.EventPortfolioUser, metrics.ResultFailure, len(msgs))
			return err
		}
		c.metrics.RecordAge(metrics.EventPortfolioUser, time.Since(ev.OccurredAt))
		switch ev.EventType {
		case "CREATED":
			reqs = append(reqs, model.UserCacheUpdateRequest{UserID: ev.UserID, PortfolioID: ev.PortfolioID, Type: model.PortfolioCreated})
		case "DELETED":
			reqs = append(reqs, model.UserCacheUpdateRequest{UserID: ev.UserID, PortfolioID: ev.PortfolioID, Type: model.PortfolioDeleted})
		case "UPDATED":
			c.metrics.RecordSkipped(metrics.EventPortfolioUser, metrics.ReasonUpdatedEventIgnored)
		default:
			c.recordMany(metrics.EventPortfolioUser, metrics.ResultFailure, len(msgs))
			return fmt.Errorf("unsupported portfolio user event type %s", ev.EventType)
		}
	}
	if err := c.cache.UpdateUserCaches(ctx, reqs); err != nil {
		c.recordMany(metrics.EventPortfolioUser, metrics.ResultFailure, len(reqs))
		return err
	}
	c.recordMany(metrics.EventPortfolioUser, metrics.ResultSuccess, len(reqs))
	result = metrics.ResultSuccess
	return nil
}

func rewindBatch(consumer *ckafka.Consumer, batch []message) error {
	earliest := make(map[string]ckafka.TopicPartition)
	for _, msg := range batch {
		tp := msg.raw.TopicPartition
		key := fmt.Sprintf("%s:%d", *tp.Topic, tp.Partition)
		current, exists := earliest[key]
		if !exists || tp.Offset < current.Offset {
			earliest[key] = tp
		}
	}
	for _, partition := range earliest {
		if err := consumer.Seek(partition, 5_000); err != nil {
			return err
		}
	}
	return nil
}

func (c *Consumers) recordMany(event, result string, n int) {
	for i := 0; i < n; i++ {
		c.metrics.RecordConsumed(event, result)
	}
}
