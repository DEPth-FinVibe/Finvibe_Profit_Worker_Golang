package redisstore

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

// 매매·포트폴리오 이벤트의 캐시 쓰기는 증감분 누적이라, 같은 이벤트를 두 번 반영하면 값이 영구히 어긋난다.
// 쓰기마다 "마커 확인 → 반영 → 마커 기록"을 Lua 하나로 실행해, 이벤트가 어디서 끊겨 재시도되더라도
// 끝난 쓰기는 건너뛰고 남은 쓰기만 한 번 반영되게 한다.
//
// 마커 키는 applied:{<데이터 키>}:<이벤트 키>다. Redis Cluster는 {} 안의 문자열로 slot을 정하고,
// 괄호가 없는 데이터 키는 키 전체로 slot을 정하므로 마커는 키 이름을 바꾸지 않고도 항상 데이터 키와 같은 slot에 놓인다.

// DLT 재처리까지 중복을 막아야 하므로 재처리 가능 기간보다 길게 유지한다.
const appliedMarkerTTL = 7 * 24 * time.Hour

const maxCompareAndSetAttempts = 5

func appliedMarkerKey(dataKey, eventKey string) (string, error) {
	if eventKey == "" {
		// 이벤트 키가 없으면 매번 다른 마커를 써서 중복 검사 없이 반영한다.
		token, err := randomToken()
		if err != nil {
			return "", err
		}
		eventKey = "once:" + token
	}
	return "applied:{" + dataKey + "}:" + eventKey, nil
}

// KEYS[1]=데이터, KEYS[2]=마커 / ARGV[1]=기대값(빈 문자열 = 없음), ARGV[2]=새 값(빈 문자열 = 삭제), ARGV[3]=결과, ARGV[4]=마커 TTL(초)
// 반환: {상태(applied|conflict|ok), 결과, 반영 후 값}
const compareAndSetScript = `
local applied = redis.call('GET', KEYS[2])
if applied then
    return {'applied', applied, redis.call('GET', KEYS[1]) or ''}
end
local current = redis.call('GET', KEYS[1]) or ''
if current ~= ARGV[1] then
    return {'conflict', '', current}
end
if ARGV[2] == '' then
    redis.call('DEL', KEYS[1])
else
    redis.call('SET', KEYS[1], ARGV[2])
end
redis.call('SET', KEYS[2], ARGV[3], 'EX', ARGV[4])
return {'ok', ARGV[3], ARGV[2]}
`

// compareAndSet은 문자열 키를 현재 값 기준으로 한 번만 갱신한다. 다른 쓰기와 겹치면 새 값으로 다시 계산한다.
// update는 현재 값(빈 문자열 = 없음)을 받아 새 값(빈 문자열 = 삭제)과 마커에 저장할 결과를 돌려준다.
// 반환값은 이번 또는 이전 시도에서 반영한 결과와 반영 후 값이다.
func (s *Store) compareAndSet(ctx context.Context, key, eventKey string, update func(current string) (next, result string)) (string, string, error) {
	marker, err := appliedMarkerKey(key, eventKey)
	if err != nil {
		return "", "", err
	}
	current, err := s.rdb.Get(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return "", "", err
	}
	for attempt := 0; attempt < maxCompareAndSetAttempts; attempt++ {
		next, result := update(current)
		start := time.Now()
		reply, err := s.rdb.Eval(ctx, compareAndSetScript, []string{key, marker},
			current, next, result, int64(appliedMarkerTTL.Seconds())).StringSlice()
		s.observe("lua_compare_and_set", err, start)
		if err != nil {
			return "", "", err
		}
		if reply[0] != "conflict" {
			return reply[1], reply[2], nil
		}
		current = reply[2]
	}
	return "", "", fmt.Errorf("too many concurrent updates on %s", key)
}

func decimalOrZero(value string) decimal.Decimal {
	if value == "" {
		return decimal.Zero
	}
	parsed, err := decimal.NewFromString(value)
	if err != nil {
		return decimal.Zero
	}
	return parsed
}

// IncreaseStockQuantity는 보유 수량을 한 번만 늘리고, 새로 보유하게 됐는지를 돌려준다.
func (s *Store) IncreaseStockQuantity(ctx context.Context, eventKey string, stockID, portfolioID int64, qty decimal.Decimal) (bool, error) {
	result, after, err := s.compareAndSet(ctx, quantityKey(portfolioID, stockID), eventKey, func(current string) (string, string) {
		previous := decimalOrZero(current)
		return previous.Add(qty).String(), strconv.FormatBool(previous.IsZero())
	})
	if err != nil {
		return false, err
	}
	if err := s.syncHoldingIndexes(ctx, stockID, portfolioID, after); err != nil {
		return false, err
	}
	return result == "true", nil
}

// DecreaseStockQuantity는 보유 수량을 한 번만 줄이고, 수량이 0 이하가 되어 보유가 없어졌는지를 돌려준다.
func (s *Store) DecreaseStockQuantity(ctx context.Context, eventKey string, stockID, portfolioID int64, qty decimal.Decimal) (bool, error) {
	result, after, err := s.compareAndSet(ctx, quantityKey(portfolioID, stockID), eventKey, func(current string) (string, string) {
		next := decimalOrZero(current).Sub(qty)
		if next.Sign() <= 0 {
			return "", "true"
		}
		return next.String(), "false"
	})
	if err != nil {
		return false, err
	}
	if err := s.syncHoldingIndexes(ctx, stockID, portfolioID, after); err != nil {
		return false, err
	}
	return result == "true", nil
}

// syncHoldingIndexes는 보유 인덱스를 이벤트가 아니라 반영 후 수량으로 맞춘다.
// 재시도해도 멱등하고, 늦게 재처리된 매도가 그 사이 다시 산 종목을 인덱스에서 빼지 않는다.
func (s *Store) syncHoldingIndexes(ctx context.Context, stockID, portfolioID int64, quantityAfter string) error {
	portfolio := strconv.FormatInt(portfolioID, 10)
	stock := strconv.FormatInt(stockID, 10)
	pipe := s.rdb.Pipeline()
	if decimalOrZero(quantityAfter).Sign() > 0 {
		pipe.SAdd(ctx, stockPortfoliosKey(stockID), portfolio)
		pipe.SAdd(ctx, portfolioStocksKey(portfolioID), stock)
	} else {
		pipe.SRem(ctx, stockPortfoliosKey(stockID), portfolio)
		pipe.SRem(ctx, portfolioStocksKey(portfolioID), stock)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// PortfolioTradeTotals는 매매 1건이 포트폴리오 해시에 주는 증감분이다.
type PortfolioTradeTotals struct {
	PurchasedValue    int64
	CurrentValue      decimal.Decimal
	StockCurrentValue decimal.Decimal
	// RemoveStockCurrentValue면 종목 평가액(scv)을 지운다. 전량 매도로 보유가 없어졌을 때 쓴다.
	RemoveStockCurrentValue bool
	// DeleteNonPositiveStockValue면 반영 후 종목 평가액이 0 이하일 때 지운다. 매도에서 쓴다.
	DeleteNonPositiveStockValue bool
	AssetCount                  int64
}

// KEYS[1]=pf 해시, KEYS[2]=마커
// ARGV: 1=마커 TTL, 2=pv 증감, 3=cvp 증감, 4=scv 필드, 5=scv 증감, 6=scv 기준값(호환 key), 7=scv 삭제, 8=0 이하면 scv 삭제, 9=ac 증감
// 반환: 반영 후 scv(빈 문자열 = 없음)
const applyPortfolioTradeTotalsScript = `
if redis.call('EXISTS', KEYS[2]) == 1 then
    return redis.call('HGET', KEYS[1], ARGV[4]) or ''
end
if ARGV[2] ~= '0' then
    redis.call('HINCRBY', KEYS[1], 'pv', ARGV[2])
end
if ARGV[3] ~= '0' then
    redis.call('HINCRBYFLOAT', KEYS[1], 'cvp', ARGV[3])
end
local stock_value = ''
if ARGV[7] == '1' then
    redis.call('HDEL', KEYS[1], ARGV[4])
else
    if not redis.call('HGET', KEYS[1], ARGV[4]) then
        redis.call('HSET', KEYS[1], ARGV[4], ARGV[6])
    end
    stock_value = redis.call('HINCRBYFLOAT', KEYS[1], ARGV[4], ARGV[5])
    if ARGV[8] == '1' and tonumber(stock_value) <= 0 then
        redis.call('HDEL', KEYS[1], ARGV[4])
        stock_value = ''
    end
end
if ARGV[9] ~= '0' then
    redis.call('HINCRBY', KEYS[1], 'ac', ARGV[9])
end
redis.call('SET', KEYS[2], '1', 'EX', ARGV[1])
return stock_value
`

// ApplyPortfolioTradeTotals는 포트폴리오 해시의 구매액·평가액·종목 평가액·보유 종목 수를 한 번에, 한 번만 반영한다.
// cvp와 scv가 같은 스크립트에서 바뀌므로 둘이 어긋나지 않는다. 호환 projection은 반영 후 scv로 맞춘다.
func (s *Store) ApplyPortfolioTradeTotals(ctx context.Context, eventKey string, portfolioID, stockID int64, totals PortfolioTradeTotals) error {
	key := pfKey(portfolioID)
	marker, err := appliedMarkerKey(key, eventKey)
	if err != nil {
		return err
	}
	legacyCurrent := s.getDec(ctx, currentValueKey(portfolioID, stockID))
	start := time.Now()
	stockValue, err := s.rdb.Eval(ctx, applyPortfolioTradeTotalsScript, []string{key, marker},
		int64(appliedMarkerTTL.Seconds()),
		totals.PurchasedValue,
		totals.CurrentValue.String(),
		stockCurrentValueField(stockID),
		totals.StockCurrentValue.String(),
		legacyCurrent.String(),
		boolFlag(totals.RemoveStockCurrentValue),
		boolFlag(totals.DeleteNonPositiveStockValue),
		totals.AssetCount,
	).Text()
	s.observe("lua_apply_portfolio_trade_totals", err, start)
	if err != nil {
		return err
	}
	if stockValue == "" {
		return s.rdb.Del(ctx, currentValueKey(portfolioID, stockID)).Err()
	}
	return s.rdb.Set(ctx, currentValueKey(portfolioID, stockID), stockValue, 0).Err()
}

// KEYS[1]=usr 해시, KEYS[2]=마커 / ARGV: 1=마커 TTL, 2=pv 증감, 3=cvp 증감, 4=pc 증감
const applyUserTotalsScript = `
if redis.call('EXISTS', KEYS[2]) == 1 then
    return 0
end
if ARGV[2] ~= '0' then
    redis.call('HINCRBY', KEYS[1], 'pv', ARGV[2])
end
if ARGV[3] ~= '0' then
    redis.call('HINCRBYFLOAT', KEYS[1], 'cvp', ARGV[3])
end
if ARGV[4] ~= '0' then
    redis.call('HINCRBY', KEYS[1], 'pc', ARGV[4])
end
redis.call('SET', KEYS[2], '1', 'EX', ARGV[1])
return 1
`

// ApplyUserTotals는 유저 해시의 구매액·평가액·포트폴리오 수를 한 번에, 한 번만 반영한다.
func (s *Store) ApplyUserTotals(ctx context.Context, eventKey, userID string, purchasedValue int64, currentValue decimal.Decimal, portfolioCount int64) error {
	key := usrKey(userID)
	marker, err := appliedMarkerKey(key, eventKey)
	if err != nil {
		return err
	}
	start := time.Now()
	err = s.rdb.Eval(ctx, applyUserTotalsScript, []string{key, marker},
		int64(appliedMarkerTTL.Seconds()), purchasedValue, currentValue.String(), portfolioCount).Err()
	s.observe("lua_apply_user_totals", err, start)
	return err
}

func boolFlag(value bool) string {
	if value {
		return "1"
	}
	return "0"
}
