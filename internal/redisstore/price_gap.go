package redisstore

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

// 아래 키는 모놀리식이 관리하며 워커와 같은 Redis Cluster에 있다.
// 보유 종목 집합은 HoldingStockRedisRepository, 버전·현재가는 CurrentPriceRepositoryImpl이 쓴다.
const holdingStockIDsKey = "market:holding:stock-ids"

func publishedVersionKey(stockID int64) string {
	return fmt.Sprintf("market:current-price-version:{stock:%d}", stockID)
}

func publishedPriceKey(stockID int64) string {
	return fmt.Sprintf("market:current-price:{stock:%d}", stockID)
}

// PriceVersionGapInput은 모놀리식이 마지막으로 발행한 시세와 워커가 마지막으로 반영한 시세다.
type PriceVersionGapInput struct {
	StockID          int64
	PublishedVersion int64
	PublishedPrice   decimal.Decimal
	// Applied가 nil이면 워커가 이 종목의 가격을 한 번도 반영하지 않았다.
	Applied *AppliedStockPrice
}

// FetchPriceVersionGapInputs는 보유 종목 중 모놀리식의 버전과 현재가가 모두 남아 있는 종목만 돌려준다.
// 현재가 키(TTL 5분)가 없으면 판정 근거가 없으므로 제외한다.
func (s *Store) FetchPriceVersionGapInputs(ctx context.Context) ([]PriceVersionGapInput, error) {
	members, err := s.rdb.SMembers(ctx, holdingStockIDsKey).Result()
	if err != nil {
		return nil, err
	}
	stockIDs := make([]int64, 0, len(members))
	for _, member := range members {
		stockID, err := strconv.ParseInt(member, 10, 64)
		if err != nil {
			continue
		}
		stockIDs = append(stockIDs, stockID)
	}
	if len(stockIDs) == 0 {
		return nil, nil
	}

	pipe := s.rdb.Pipeline()
	versionCmds := make([]*redis.StringCmd, len(stockIDs))
	priceCmds := make([]*redis.StringCmd, len(stockIDs))
	for i, stockID := range stockIDs {
		versionCmds[i] = pipe.Get(ctx, publishedVersionKey(stockID))
		priceCmds[i] = pipe.Get(ctx, publishedPriceKey(stockID))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	inputs := make([]PriceVersionGapInput, 0, len(stockIDs))
	published := make([]int64, 0, len(stockIDs))
	for i, stockID := range stockIDs {
		rawVersion, versionErr := versionCmds[i].Result()
		rawPrice, priceErr := priceCmds[i].Result()
		if versionErr != nil || priceErr != nil {
			continue
		}
		version, err := strconv.ParseInt(rawVersion, 10, 64)
		if err != nil {
			continue
		}
		price, ok := publishedClose(rawPrice)
		if !ok {
			continue
		}
		inputs = append(inputs, PriceVersionGapInput{StockID: stockID, PublishedVersion: version, PublishedPrice: price})
		published = append(published, stockID)
	}
	if len(inputs) == 0 {
		return nil, nil
	}

	applied, err := s.BulkFetchAppliedStockPrices(ctx, published)
	if err != nil {
		return nil, err
	}
	for i := range inputs {
		if state, ok := applied[inputs[i].StockID]; ok {
			inputs[i].Applied = &state
		}
	}
	return inputs, nil
}

func publishedClose(raw string) (decimal.Decimal, bool) {
	var snapshot struct {
		Close json.Number `json:"close"`
	}
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil || snapshot.Close == "" {
		return decimal.Zero, false
	}
	price, err := decimal.NewFromString(snapshot.Close.String())
	if err != nil {
		return decimal.Zero, false
	}
	return price, true
}
