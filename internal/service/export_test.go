package service

import "time"

// SetClock은 테스트에서 격차 점검 기준 시각을 고정한다.
func (c *PriceGapChecker) SetClock(now func() time.Time) { c.now = now }
