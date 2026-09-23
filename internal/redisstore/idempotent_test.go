package redisstore

import (
	"strings"
	"testing"
)

// 마커가 Lua에서 데이터 키와 함께 쓰이려면 Redis Cluster에서 같은 slot이어야 한다.
func TestAppliedMarkerKeySharesSlotWithDataKey(t *testing.T) {
	// CLUSTER KEYSLOT foo = 12182 (Redis 문서 예시)로 계산식을 먼저 확인한다.
	if got := clusterSlot("foo"); got != 12182 {
		t.Fatalf("clusterSlot(foo) got %d want 12182", got)
	}
	for _, dataKey := range []string{"pf:100", "usr:7a22103f-1d1c-4ab4-9c47-4040c3a46964", quantityKey(100, 10)} {
		marker, err := appliedMarkerKey(dataKey, "trade:1")
		if err != nil {
			t.Fatal(err)
		}
		if got, want := clusterSlot(marker), clusterSlot(dataKey); got != want {
			t.Fatalf("%s: marker slot %d, data slot %d", dataKey, got, want)
		}
	}
}

// clusterSlot은 Redis Cluster의 key slot 계산(CRC16/XMODEM, hash tag 규칙)이다.
func clusterSlot(key string) uint16 {
	if start := strings.IndexByte(key, '{'); start >= 0 {
		if end := strings.IndexByte(key[start+1:], '}'); end > 0 {
			key = key[start+1 : start+1+end]
		}
	}
	var crc uint16
	for i := 0; i < len(key); i++ {
		crc ^= uint16(key[i]) << 8
		for bit := 0; bit < 8; bit++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc % 16384
}
