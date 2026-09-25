package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestLocalTimeUnmarshalJSON_ParsesTimezoneLessAsUTC(t *testing.T) {
	var parsed LocalTime

	if err := json.Unmarshal([]byte(`"2026-06-12T03:17:44"`), &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if got, want := parsed.Location().String(), "UTC"; got != want {
		t.Fatalf("location got %s want %s", got, want)
	}
	if got, want := parsed.Format(time.RFC3339), "2026-06-12T03:17:44Z"; got != want {
		t.Fatalf("formatted got %s want %s", got, want)
	}
}

func TestLocalTimeUnmarshalJSON_PreservesExplicitOffset(t *testing.T) {
	var parsed LocalTime

	if err := json.Unmarshal([]byte(`"2026-06-12T03:17:44Z"`), &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if got, want := parsed.UTC().Format(time.RFC3339), "2026-06-12T03:17:44Z"; got != want {
		t.Fatalf("utc got %s want %s", got, want)
	}
}

func TestVersionFromWallClock_ReadsWallClockAsKST(t *testing.T) {
	// 모놀리식은 KST 10:00:01을 오프셋 없이 보내고, LocalTime은 이를 UTC 10:00:01로 읽는다.
	var parsed LocalTime
	if err := json.Unmarshal([]byte(`"2026-09-25T10:00:01"`), &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	// 모놀리식이 부여하는 버전의 순번 0 값: KST 10:00:01의 epoch 초 × 10^6
	want := time.Date(2026, 9, 25, 1, 0, 1, 0, time.UTC).Unix() * 1_000_000
	if got := VersionFromWallClock(parsed.Time); got != want {
		t.Fatalf("version got %d want %d", got, want)
	}
}

func TestStockPriceUpdatedEventVersion_PrefersAssignedVersion(t *testing.T) {
	var withVersion, withoutVersion StockPriceUpdatedEvent
	if err := json.Unmarshal([]byte(`{"stockId":10,"price":120,"updatedAt":"2026-09-25T10:00:01","priceVersion":1790298001000002}`), &withVersion); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(`{"stockId":10,"price":120,"updatedAt":"2026-09-25T10:00:01"}`), &withoutVersion); err != nil {
		t.Fatal(err)
	}

	if got := withVersion.Version(); got != 1790298001000002 {
		t.Fatalf("assigned version got %d", got)
	}
	if got, want := withoutVersion.Version(), VersionFromWallClock(withoutVersion.UpdatedAt.Time); got != want {
		t.Fatalf("derived version got %d want %d", got, want)
	}
	// 같은 초의 구 형식 이벤트는 순번 0으로 유도되어, 새 형식의 순번 1 이상보다 항상 작다.
	if withoutVersion.Version() >= withVersion.Version() {
		t.Fatalf("derived %d should be below assigned %d", withoutVersion.Version(), withVersion.Version())
	}
}
