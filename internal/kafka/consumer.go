package kafka

import (
	"context"
	"errors"
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
	dlt     DeadLetterPublisher
	retry   retryPolicy
	active  atomic.Int64
}

type message struct {
	value []byte
	raw   *ckafka.Message
}

// kafkaConsumer는 재시도 흐름을 테스트할 수 있도록 *ckafka.Consumer에서 쓰는 메서드만 모은다.
type kafkaConsumer interface {
	Poll(timeoutMs int) ckafka.Event
	Assignment() ([]ckafka.TopicPartition, error)
	Pause(partitions []ckafka.TopicPartition) error
	Resume(partitions []ckafka.TopicPartition) error
	Seek(partition ckafka.TopicPartition, ignoredTimeoutMs int) error
	StoreMessage(m *ckafka.Message) ([]ckafka.TopicPartition, error)
	Commit() ([]ckafka.TopicPartition, error)
}

// retryPolicy는 일시 장애의 재시도 간격과 한도다.
// 가격은 다음 틱이 최신값을 다시 싣고 오므로 짧게 재시도하고 버린다.
// 매매·포트폴리오는 증감분 누적이라 유실되면 복구되지 않으므로 길게 재시도하고 DLT로 보낸다.
type retryPolicy struct {
	priceInterval time.Duration
	priceRetries  int
	recordInitial time.Duration
	recordMax     time.Duration
	recordElapsed time.Duration
}

var defaultRetryPolicy = retryPolicy{
	priceInterval: 200 * time.Millisecond,
	priceRetries:  2,
	recordInitial: time.Second,
	recordMax:     30 * time.Second,
	recordElapsed: 5 * time.Minute,
}

// errInvalidPayload로 감싼 에러는 다시 시도해도 결과가 같으므로 재시도하지 않는다.
var errInvalidPayload = errors.New("invalid payload")

func invalidPayload(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errInvalidPayload, fmt.Sprintf(format, args...))
}

const stockPriceDLTOffsetReset = "latest"

func New(cfg config.Config, p *service.ProfitService, c *service.CacheService, m *metrics.Metrics, dlt DeadLetterPublisher) *Consumers {
	return &Consumers{cfg: cfg, profit: p, cache: c, metrics: m, dlt: dlt, retry: defaultRetryPolicy}
}

func (c *Consumers) Run(ctx context.Context) {
	c.runGroup(ctx, c.cfg.StockConcurrency, c.cfg.StockTopic, c.cfg.StockGroup, "latest", func(ctx context.Context, consumer kafkaConsumer, batch []message) {
		c.processPriceBatch(ctx, consumer, batch, metrics.EventStockPrice, c.handleStock)
	})
	if c.cfg.StockDLTEnabled {
		c.runGroup(ctx, c.cfg.StockDLTConcurrency, c.cfg.StockDLTTopic, c.cfg.StockDLTGroup, stockPriceDLTOffsetReset, func(ctx context.Context, consumer kafkaConsumer, batch []message) {
			c.processPriceBatch(ctx, consumer, batch, metrics.EventStockPriceDLT, c.handleStockDLT)
		})
	}
	c.runGroup(ctx, c.cfg.TradeConcurrency, c.cfg.TradeTopic, c.cfg.TradeGroup, "latest", func(ctx context.Context, consumer kafkaConsumer, batch []message) {
		c.processRecords(ctx, consumer, batch, metrics.EventTrade, c.handleTradeRecord)
	})
	c.runGroup(ctx, c.cfg.PortfolioUserConcurrency, c.cfg.PortfolioUserTopic, c.cfg.PortfolioUserGroup, "latest", func(ctx context.Context, consumer kafkaConsumer, batch []message) {
		c.processRecords(ctx, consumer, batch, metrics.EventPortfolioUser, c.handlePortfolioUserRecord)
	})
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

func (c *Consumers) runGroup(ctx context.Context, n int, topic, group, offsetReset string, process func(context.Context, kafkaConsumer, []message)) {
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
				process(ctx, consumer, batch)
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
			// 깨진 가격 메시지는 재시도해도 같으므로 버린다. 다음 틱이 최신 가격을 다시 싣고 온다.
			slog.Error("dropping invalid stock price event", "event_type", eventType, "err", err)
			c.metrics.RecordSkipped(eventType, metrics.ReasonInvalidPayload)
			c.metrics.RecordRecovered(eventType, metrics.ActionDropped)
			continue
		}
		if ev.StockID == 0 || ev.UpdatedAt.IsZero() {
			slog.Error("dropping stock price event missing stockId or updatedAt", "event_type", eventType)
			c.metrics.RecordSkipped(eventType, metrics.ReasonInvalidPayload)
			c.metrics.RecordRecovered(eventType, metrics.ActionDropped)
			continue
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
			slog.Error("dropping invalid stock price event", "event_type", eventType, "stock_id", ev.StockID, "err", err)
			c.metrics.RecordSkipped(eventType, metrics.ReasonInvalidPayload)
			c.metrics.RecordRecovered(eventType, metrics.ActionDropped)
			continue
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

// processPriceBatch는 가격 배치를 짧게 재시도하고, 그래도 실패하면 버린다.
// 다음 틱이 최신 가격을 다시 싣고 오므로 지난 가격을 붙잡고 있지 않는다.
func (c *Consumers) processPriceBatch(ctx context.Context, consumer kafkaConsumer, batch []message, event string, handler func(context.Context, []message) error) {
	for attempt := 0; ; attempt++ {
		err := handler(ctx, batch)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return
		}
		if attempt >= c.retry.priceRetries {
			slog.Error("dropping stock price batch after retries", "event_type", event, "size", len(batch), "err", err)
			for range batch {
				c.metrics.RecordRecovered(event, metrics.ActionDropped)
			}
			break
		}
		if !sleepContext(ctx, c.retry.priceInterval) {
			return
		}
	}
	for _, msg := range batch {
		c.storeOffset(consumer, msg)
	}
	c.commit(consumer)
}

// processRecords는 매매·포트폴리오 레코드를 순서대로 하나씩 처리한다.
func (c *Consumers) processRecords(ctx context.Context, consumer kafkaConsumer, batch []message, event string, handler func(context.Context, message) error) {
	for i, msg := range batch {
		if !c.processRecord(ctx, consumer, msg, event, handler) {
			// 종료 중이거나 파티션이 회수됐다. 처리하지 않은 레코드는 되감아 다시 읽게 한다.
			if err := rewindBatch(consumer, batch[i:]); err != nil {
				slog.Warn("kafka rewind unprocessed records", "event_type", event, "err", err)
			}
			return
		}
	}
	c.commit(consumer)
}

// processRecord는 레코드 하나를 반영한다. 깨진 레코드는 곧바로, 일시 장애는 재시도 한도를 넘기면 DLT로 보낸다.
// 반영에 성공했거나 DLT로 보냈으면 true, 종료 중이거나 파티션을 잃어 멈췄으면 false를 돌려준다.
func (c *Consumers) processRecord(ctx context.Context, consumer kafkaConsumer, msg message, event string, handler func(context.Context, message) error) bool {
	started := time.Now()
	wait := c.retry.recordInitial
	for {
		err := handler(ctx, msg)
		if err == nil {
			c.storeOffset(consumer, msg)
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		if errors.Is(err, errInvalidPayload) || time.Since(started) >= c.retry.recordElapsed {
			publishErr := c.publishDeadLetter(ctx, msg, err)
			if publishErr == nil {
				slog.Error("sent record to dlt", "event_type", event, "err", err)
				c.metrics.RecordRecovered(event, metrics.ActionDeadLettered)
				c.storeOffset(consumer, msg)
				return true
			}
			slog.Error("dlt publish failed, retrying", "event_type", event, "err", err, "publish_err", publishErr)
		} else {
			slog.Warn("record failed, retrying", "event_type", event, "wait", wait, "err", err)
		}

		// 대기가 길어질 수 있으므로 앞 레코드까지 먼저 커밋한다.
		c.commit(consumer)
		if !c.waitPaused(ctx, consumer, msg, wait) {
			return false
		}
		wait = min(wait*2, c.retry.recordMax)
	}
}

// waitPaused는 할당된 파티션을 멈춘 채 Poll을 계속 불러 max.poll.interval.ms를 지키며 기다린다.
// 대기 중 레코드의 파티션이 회수되면 false를 돌려준다.
func (c *Consumers) waitPaused(ctx context.Context, consumer kafkaConsumer, msg message, wait time.Duration) bool {
	pauseAssigned(consumer)
	defer func() {
		if assigned, err := consumer.Assignment(); err == nil {
			_ = consumer.Resume(assigned)
		}
	}()

	deadline := time.Now().Add(wait)
	for remaining := time.Until(deadline); remaining > 0; remaining = time.Until(deadline) {
		if ctx.Err() != nil {
			return false
		}
		if event, ok := consumer.Poll(int(min(remaining, 100*time.Millisecond).Milliseconds()) + 1).(*ckafka.Message); ok {
			// 대기 중 새로 할당된 파티션에서 온 메시지는 잃지 않도록 그 위치로 되돌린다.
			_ = consumer.Seek(event.TopicPartition, 0)
		}
		if !isAssigned(consumer, msg.raw.TopicPartition) {
			return false
		}
		pauseAssigned(consumer)
	}
	return true
}

func pauseAssigned(consumer kafkaConsumer) {
	if assigned, err := consumer.Assignment(); err == nil && len(assigned) > 0 {
		_ = consumer.Pause(assigned)
	}
}

func isAssigned(consumer kafkaConsumer, tp ckafka.TopicPartition) bool {
	assigned, err := consumer.Assignment()
	if err != nil {
		return true
	}
	for _, partition := range assigned {
		if *partition.Topic == *tp.Topic && partition.Partition == tp.Partition {
			return true
		}
	}
	return false
}

func (c *Consumers) publishDeadLetter(ctx context.Context, msg message, cause error) error {
	if c.dlt == nil {
		return errors.New("dlt publisher is not configured")
	}
	return c.dlt.Publish(ctx, msg.raw, cause)
}

func (c *Consumers) storeOffset(consumer kafkaConsumer, msg message) {
	if _, err := consumer.StoreMessage(msg.raw); err != nil {
		slog.Error("kafka store offset", "err", err)
	}
}

func (c *Consumers) commit(consumer kafkaConsumer) {
	if _, err := consumer.Commit(); err != nil {
		var kafkaErr ckafka.Error
		if errors.As(err, &kafkaErr) && kafkaErr.Code() == ckafka.ErrNoOffset {
			return
		}
		// 커밋에 실패해도 저장한 offset은 다음 커밋에 실린다. 재전달돼도 반영은 멱등하다.
		slog.Error("kafka commit", "err", err)
	}
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *Consumers) handleTradeRecord(ctx context.Context, msg message) error {
	start := time.Now()
	result := metrics.ResultFailure
	defer func() {
		c.metrics.ObserveListener(metrics.EventTrade, result, start)
		c.metrics.RecordConsumed(metrics.EventTrade, result)
	}()

	ev, err := model.Decode[model.PortfolioTradeEvent](msg.value)
	if err != nil {
		return invalidPayload("decode trade event: %v", err)
	}
	if ev.PortfolioID == 0 || ev.StockID == 0 {
		return invalidPayload("trade event is missing portfolioId or stockId: tradeId=%d", ev.TradeID)
	}
	qty := ev.Amount.Decimal
	if qty.Sign() <= 0 {
		return invalidPayload("trade amount must be positive: %s", qty)
	}
	typ := model.StockBuy
	if ev.Type == "SELL" {
		typ = model.StockSell
	} else if ev.Type != "BUY" {
		return invalidPayload("unsupported trade type: %s", ev.Type)
	}
	request := model.PortfolioCacheUpdateRequest{TradeID: ev.TradeID, PortfolioID: ev.PortfolioID, StockID: ev.StockID, UserID: ev.UserID, Type: typ, Price: ev.Price, Quantity: qty}
	if err := c.cache.UpdatePortfolioCaches(ctx, []model.PortfolioCacheUpdateRequest{request}); err != nil {
		return err
	}
	result = metrics.ResultSuccess
	return nil
}

func (c *Consumers) handlePortfolioUserRecord(ctx context.Context, msg message) error {
	start := time.Now()
	result := metrics.ResultFailure
	defer func() {
		c.metrics.ObserveListener(metrics.EventPortfolioUser, result, start)
		c.metrics.RecordConsumed(metrics.EventPortfolioUser, result)
	}()

	ev, err := model.Decode[model.PortfolioUserEvent](msg.value)
	if err != nil {
		return invalidPayload("decode portfolio user event: %v", err)
	}
	if ev.PortfolioID == 0 || ev.UserID == "" {
		return invalidPayload("portfolio user event is missing portfolioId or userId")
	}
	c.metrics.RecordAge(metrics.EventPortfolioUser, time.Since(ev.OccurredAt))

	var typ model.UserChangeType
	switch ev.EventType {
	case "CREATED":
		typ = model.PortfolioCreated
	case "DELETED":
		typ = model.PortfolioDeleted
	case "UPDATED":
		c.metrics.RecordSkipped(metrics.EventPortfolioUser, metrics.ReasonUpdatedEventIgnored)
		result = metrics.ResultSkipped
		return nil
	default:
		return invalidPayload("unsupported portfolio user event type %s", ev.EventType)
	}
	if err := c.cache.UpdateUserCaches(ctx, []model.UserCacheUpdateRequest{{UserID: ev.UserID, PortfolioID: ev.PortfolioID, Type: typ}}); err != nil {
		return err
	}
	result = metrics.ResultSuccess
	return nil
}

func rewindBatch(consumer kafkaConsumer, batch []message) error {
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
