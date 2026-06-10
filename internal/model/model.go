package model

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

type Decimal = decimal.Decimal

var Zero = decimal.Zero

type StockPriceUpdatedEvent struct {
	StockID   int64       `json:"stockId"`
	Price     DecimalJSON `json:"price"`
	UpdatedAt LocalTime   `json:"updatedAt"`
}

type PortfolioTradeEvent struct {
	TradeID     int64       `json:"tradeId"`
	UserID      string      `json:"userId"`
	Type        string      `json:"type"`
	Amount      DecimalJSON `json:"amount"`
	Price       int64       `json:"price"`
	PortfolioID int64       `json:"portfolioId"`
	StockID     int64       `json:"stockId"`
	Name        string      `json:"name"`
	Currency    string      `json:"currency"`
}

type PortfolioUserEvent struct {
	EventType         string    `json:"eventType"`
	UserID            string    `json:"userId"`
	PortfolioID       int64     `json:"portfolioId"`
	TargetPortfolioID int64     `json:"targetPortfolioId"`
	OccurredAt        time.Time `json:"occurredAt"`
}

type ProfitCalculationRequest struct {
	StockID   int64
	NewPrice  int64
	Timestamp time.Time
}

type PortfolioTradeType string

const (
	StockBuy  PortfolioTradeType = "STOCK_BUY"
	StockSell PortfolioTradeType = "STOCK_SELL"
)

type PortfolioCacheUpdateRequest struct {
	PortfolioID int64
	StockID     int64
	Type        PortfolioTradeType
	Price       int64
	Quantity    Decimal
}

type UserChangeType string

const (
	PortfolioCreated UserChangeType = "CREATED"
	PortfolioDeleted UserChangeType = "DELETED"
)

type UserCacheUpdateRequest struct {
	UserID      string
	PortfolioID int64
	Type        UserChangeType
}

type PortfolioValuation struct {
	PortfolioID    int64
	PurchasedValue int64
	CurrentValue   int64
	ProfitRate     float64
	AssetCount     int64
}

type UserValuation struct {
	UserID         string
	PurchasedValue int64
	CurrentValue   int64
	ProfitRate     float64
	PortfolioCount int64
}

type StockHoldingKey struct {
	PortfolioID int64
	StockID     int64
}

func (k StockHoldingKey) String() string { return fmt.Sprintf("%d:%d", k.PortfolioID, k.StockID) }

type StockHolding struct {
	Quantity     Decimal
	CurrentValue Decimal
}

type PortfolioMetadata struct {
	PurchasedValue int64
	AssetCount     int64
	UserID         string
	CurrentValue   Decimal
}

type PortfolioStateSnapshot struct {
	CurrentValue Decimal
	Metadata     PortfolioMetadata
}

type UserMetadata struct {
	PurchasedValue int64
	PortfolioCount int64
}

type UserStateSnapshot struct {
	CurrentValue Decimal
	Metadata     UserMetadata
}

type DecimalJSON struct{ decimal.Decimal }

func (d *DecimalJSON) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	if s == "" || s == "null" {
		return fmt.Errorf("decimal is empty")
	}
	v, err := decimal.NewFromString(s)
	if err != nil {
		return err
	}
	d.Decimal = v
	return nil
}

func (d DecimalJSON) Int64Exact() (int64, error) {
	if !d.Decimal.Equal(d.Decimal.Truncate(0)) {
		return 0, fmt.Errorf("decimal %s is not an integer", d.String())
	}
	return d.Decimal.IntPart(), nil
}

type LocalTime struct{ time.Time }

func (lt *LocalTime) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	if s == "" || s == "null" {
		return fmt.Errorf("time is empty")
	}
	layouts := []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05"}
	var last error
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			lt.Time = t
			return nil
		} else {
			last = err
		}
	}
	return last
}

func Decode[T any](payload []byte) (T, error) {
	var out T
	err := json.Unmarshal(payload, &out)
	return out, err
}

func RoundToInt64(v Decimal) int64 {
	f, _ := v.Round(0).Float64()
	if f > float64(math.MaxInt64) {
		return math.MaxInt64
	}
	if f < float64(math.MinInt64) {
		return math.MinInt64
	}
	return int64(math.Round(f))
}

func ProfitRate(purchased int64, current Decimal) float64 {
	if purchased == 0 {
		return 0
	}
	pv := decimal.NewFromInt(purchased)
	rate := current.Sub(pv).DivRound(pv, 8).Mul(decimal.NewFromInt(100))
	f, _ := rate.Float64()
	return f
}

func ParseInt(s string) int64 { v, _ := strconv.ParseInt(s, 10, 64); return v }
