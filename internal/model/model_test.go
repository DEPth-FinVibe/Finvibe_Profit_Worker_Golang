package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestLocalTimeUnmarshalJSON_ParsesTimezoneLessAsAsiaSeoul(t *testing.T) {
	var parsed LocalTime

	if err := json.Unmarshal([]byte(`"2026-06-12T03:17:44"`), &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if got, want := parsed.Location().String(), "Asia/Seoul"; got != want {
		t.Fatalf("location got %s want %s", got, want)
	}
	if got, want := parsed.Format(time.RFC3339), "2026-06-12T03:17:44+09:00"; got != want {
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
