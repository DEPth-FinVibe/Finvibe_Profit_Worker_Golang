package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr                 string
	RedisAddr                string
	RedisPassword            string
	RedisMode                string
	RedisClusterNodes        []string
	RedisDB                  int
	KafkaBrokers             []string
	StockTopic               string
	StockDLTTopic            string
	TradeTopic               string
	PortfolioUserTopic       string
	StockGroup               string
	StockDLTGroup            string
	TradeGroup               string
	PortfolioUserGroup       string
	StockConcurrency         int
	StockDLTConcurrency      int
	StockDLTEnabled          bool
	TradeConcurrency         int
	PortfolioUserConcurrency int
	KafkaGroupProtocol       string
	MaxPollRecords           int
	BatchMaxWait             time.Duration
	ShutdownTimeout          time.Duration
}

func Load() Config {
	return Config{
		HTTPAddr:                 getenv("HTTP_ADDR", ":8080"),
		RedisAddr:                getenv("REDIS_ADDR", getenv("REDIS_HOST", "localhost")+":"+getenv("REDIS_PORT", "6379")),
		RedisMode:                getenv("REDIS_MODE", "standalone"),
		RedisClusterNodes:        split(getenv("REDIS_CLUSTER_NODES", getenv("SPRING_DATA_REDIS_CLUSTER_NODES", getenv("SPRING_REDIS_CLUSTER_NODES", "")))),
		RedisPassword:            getenv("REDIS_PASSWORD", ""),
		RedisDB:                  getint("REDIS_DB", 0),
		KafkaBrokers:             split(getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")),
		StockTopic:               getenv("KAFKA_TOPIC_STOCK_PRICE_UPDATED", "market.stock-price-updated.v1"),
		StockDLTTopic:            getenv("KAFKA_TOPIC_STOCK_PRICE_UPDATED_DLT", "market.stock-price-updated.v1.DLT"),
		TradeTopic:               getenv("KAFKA_TOPIC_PORTFOLIO_TRADE", "trade.trade-executed.v1"),
		PortfolioUserTopic:       getenv("KAFKA_TOPIC_PORTFOLIO_USER", "asset.portfolio-group-changed.v1"),
		StockGroup:               getenv("KAFKA_GROUP_STOCK_PRICE", "profit-worker-price"),
		StockDLTGroup:            getenv("KAFKA_GROUP_STOCK_PRICE_DLT", "profit-worker-price-dlt"),
		TradeGroup:               getenv("KAFKA_GROUP_TRADE", "profit-worker-trade"),
		PortfolioUserGroup:       getenv("KAFKA_GROUP_PORTFOLIO", "profit-worker-portfolio"),
		StockConcurrency:         getint("KAFKA_CONCURRENCY_STOCK_PRICE", 2),
		StockDLTConcurrency:      getint("KAFKA_CONCURRENCY_STOCK_PRICE_DLT", 1),
		StockDLTEnabled:          getbool("KAFKA_STOCK_PRICE_DLT_ENABLED", true),
		TradeConcurrency:         getint("KAFKA_CONCURRENCY_TRADE", 1),
		PortfolioUserConcurrency: getint("KAFKA_CONCURRENCY_PORTFOLIO", 1),
		KafkaGroupProtocol:       getenv("KAFKA_GROUP_PROTOCOL", "consumer"),
		MaxPollRecords:           getint("KAFKA_MAX_POLL_RECORDS", 100),
		BatchMaxWait:             time.Duration(getint("KAFKA_BATCH_MAX_WAIT_MS", 50)) * time.Millisecond,
		ShutdownTimeout:          time.Duration(getint("SHUTDOWN_TIMEOUT_SECONDS", 20)) * time.Second,
	}
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func getint(k string, d int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return d
}
func getbool(k string, d bool) bool {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return d
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return d
	}
	return parsed
}
func split(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
