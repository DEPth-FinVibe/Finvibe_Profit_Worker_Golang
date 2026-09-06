# 가격 이벤트 정합성 복구 Result

## 문서 정보

- 완료일: 2026-09-06
- 대상 저장소: `Finvibe_Profit_Worker_Golang`
- 작업 브랜치: `fix/price-event-reconciliation`
- 상태: 구현 및 로컬 검증 완료
- GitHub Issue·PR: 사용자 승인에 따라 생략
- 리뷰 상태: 리뷰 필요

## 결과 요약

Go 수익률 워커에 가격 DLT 빠른 복구, 재시도 안전한 평가액 갱신과 `updatedAt` 기반 최신성 보호를 추가했다. 시스템 전체의 주기적 가격·평가액 검증은 병렬 Batch reconciliation이 담당하며, 두 작업이 공유할 Redis 계약을 확정했다.

## 구현 결과

### Kafka DLT 처리

- `market.stock-price-updated.v1.DLT`를 별도 consumer group으로 소비한다.
- 최초 offset이 없으면 `latest`에서 시작해 배포 이후 DLT 이벤트를 처리한다.
- 기본 가격 이벤트와 같은 수익률 계산 경로를 사용한다.
- DLT worker도 readiness의 필수 worker 수에 포함한다.
- DLT listener·처리량·지연·skip 지표를 기본 topic과 구분한다.

### 원자적 평가액 갱신

- `pf:<portfolioId>` hash에 종목별 `scv:<stockId>` 평가액을 저장한다.
- 한 포트폴리오의 `scv` 교체와 `cvp` delta 반영을 Lua 하나로 처리한다.
- 같은 요청을 재처리하면 delta가 0이 되어 중복 증분하지 않는다.
- 서로 다른 종목의 동시 delta는 포트폴리오 합계에 모두 보존된다.
- 기존 종목 current-value key는 호환용 projection으로 계속 갱신한다.
- 매수·매도·포트폴리오 삭제도 `scv`를 함께 변경한다.

### 최신성 및 동시 실행 보호

- 저장 시각보다 최신인 `updatedAt`만 적용한다.
- 이전 시각은 `stale_price_event`로 skip한다.
- 동일 시각·동일 가격은 `duplicate_price_event`로 skip한다.
- 동일 시각·다른 가격은 `price_timestamp_conflict`로 기록하고 기존 값을 유지한다.
- 기본 topic과 DLT가 종목별 공용 lock 및 적용 상태를 사용한다.
- lock TTL을 처리 중 갱신하고, fan-out 성공 후 상태 저장과 lock 해제를 원자 처리한다.
- DLT batch 안에서도 메시지 배열의 마지막 값이 아니라 가장 최신 `updatedAt`을 선택한다.

## 설정

| 환경변수 | 기본값 | 용도 |
|---|---:|---|
| `KAFKA_TOPIC_STOCK_PRICE_UPDATED_DLT` | `market.stock-price-updated.v1.DLT` | 가격 DLT topic |
| `KAFKA_GROUP_STOCK_PRICE_DLT` | `profit-worker-price-dlt` | DLT consumer group |
| `KAFKA_CONCURRENCY_STOCK_PRICE_DLT` | `1` | DLT worker 수 |
| `KAFKA_STOCK_PRICE_DLT_ENABLED` | `true` | DLT 소비 활성화 |
| `PRICE_APPLICATION_LOCK_TTL_SECONDS` | `30` | 종목 가격 적용 lock TTL |

긴 fan-out이 30초를 넘더라도 lock은 TTL의 약 1/3 주기로 갱신된다. 긴급하게 DLT 소비만 중지할 때는 `KAFKA_STOCK_PRICE_DLT_ENABLED=false`를 사용한다.

## Redis 연동 계약

| key 또는 field | 의미 |
|---|---|
| `pf:<portfolioId>`의 `scv:<stockId>` | 원자 갱신 기준 종목 평가액 |
| `pf:<portfolioId>`의 `cvp` | 포트폴리오 평가액 합계 |
| `portfolio:<portfolioId>:stock:<stockId>:current-value` | 기존 consumer 호환용 projection |
| `stock:{<stockId>}:price-application`의 `at` | 마지막 적용 시각, UTC RFC3339Nano |
| `stock:{<stockId>}:price-application`의 `price` | 마지막 적용 정수 가격 |
| `stock:{<stockId>}:price-application-lock` | 가격 fan-out 직렬화 lock |

`price-application`과 lock key의 `{<stockId>}`는 Redis Cluster에서 같은 hash slot을 사용하기 위한 tag다. Batch reconciliation은 authoritative 가격을 적용할 때 같은 lock과 적용 상태 계약을 사용하고, `scv`, `cvp`, 호환 projection을 함께 보정한다.

## 관측 지표

- `profit_worker_events_consumed_total{event_type="stock_price_updated_dlt",...}`
- `profit_worker_events_skipped_total{event_type,reason}`
- `profit_worker_event_age_seconds{event_type="stock_price_updated_dlt"}`
- `profit_worker_listener_duration_seconds{event_type="stock_price_updated_dlt",...}`
- `profit_worker_redis_command_duration_seconds{command="pipeline_eval_replace_stock_cv",...}`

종목 ID는 Prometheus label에 사용하지 않는다. 동일 시각 가격 충돌은 집계 metric과 stock ID가 포함된 경고 로그로 확인한다.

## 검증 결과

- `go test ./...`: 통과
- `go test -race ./...`: 통과
- `go vet ./...`: 통과
- `git diff --check`: 통과
- `gofmt -l .`: 출력 없음
- 작업 문서 4종: 각 200줄 이하

검증한 주요 상황:

- DLT 이벤트의 수익률 계산 경로
- 기존 `scv` 없는 데이터의 최초 이관
- Lua 완료 후 호환 projection 전 중단을 가정한 재처리
- 같은 포트폴리오의 서로 다른 종목 동시 갱신
- 오래된 이벤트, 동일 이벤트와 동일 시각 가격 충돌
- DLT batch 내부 역순 메시지
- lock 경합, TTL 갱신과 소유권 상실 뒤 상태 확정 차단
- 거래 이벤트의 `scv` 호환 갱신

## 배포 및 롤백

1. Batch 병렬 작업이 이 문서의 Redis 계약을 사용하는지 확인한다.
2. staging Redis Cluster에서 Lua key slot과 배포 이후 DLT 이벤트 smoke test를 수행한다.
3. DLT lag, stale·duplicate·conflict 지표와 lock 경합 로그를 확인한다.
4. 이상 시 DLT consumer를 환경변수로 먼저 비활성화한다.
5. 전체 변경 롤백이 필요하면 이전 worker image를 배포한다. 추가된 Redis hash field와 state key는 이전 worker 동작을 방해하지 않는다.

## 커밋

- `e9d6f2a` 가격 이벤트 DLT 복구 소비
- `cc30c64` 가격 평가액 갱신 원자화
- `5d42163` 병렬 정합성 복구 책임 반영
- `29a6dd7` 가격 이벤트 최신성 보호
- `72fcd42` 가격 이벤트 복구 결과 정리
- `70777e2` DLT 최초 소비 위치를 `latest`로 변경

최종 사람 리뷰와 staging Redis Cluster 검증은 배포 전에 필요하다.
