# 가격 이벤트 정합성 복구 Decisions

## 문서 정보

- 작성일: 2026-09-06
- 기준 문서: `01-overview.md`, `02-plan.md`
- 상태: 진행 중
- 리뷰 상태: 리뷰 필요

## D1. Go 워커의 복구 데이터 원본

### 검토한 선택지

1. Redis current-price를 Go 워커가 주기적으로 대조
2. Kafka DLT를 별도 consumer group으로 소비
3. stale metric만 추가하고 자동 복구하지 않음

### 추가 조사와 계획 정정

초기 Plan은 Redis current-price 대조를 추천했다. 사용자 규모는 사용자 10만, 포트폴리오 20만, 보유 포지션 60만이며, Redis 전체 keyspace를 짧은 주기로 SCAN하는 방식은 정상 트래픽과 무관한 지속 부하를 만든다.

Batch 서버에는 DB 기준으로 포트폴리오 소유자, 보유 자산 snapshot과 종목-포트폴리오 역색인을 10분마다 점검하는 `RedisIndexReconciliationScheduler`가 있다. 시스템 전체의 silent loss 검증은 이 기존 Batch reconciliation을 확장하는 병렬 작업에서 담당하기로 했다.

따라서 Go 워커가 같은 전수 대조 책임을 중복해서 갖지 않고, Kafka에서 명시적으로 격리된 가격 이벤트를 빠르게 복구하는 역할에 집중하도록 선택지를 재평가했다.

### 결정

- 사용자 선택: **2번 Kafka DLT 소비**
- Go 워커는 `market.stock-price-updated.v1.DLT`를 별도 consumer group으로 소비한다.
- 신규 DLT group은 기존 backlog도 처리할 수 있도록 최초 offset reset을 `earliest`로 설정한다.
- DLT consumer는 기본 가격 consumer와 같은 수익률 계산 진입점을 사용한다.
- DLT 처리량과 성공·실패는 기본 가격 이벤트와 구분해 metric에 기록한다.
- DLT 소비 활성화와 concurrency는 환경변수로 조정할 수 있게 한다.

### 근거

- Go 워커의 책임을 실시간 이벤트 처리와 빠른 실패 복구로 유지한다.
- 정상 상태에서 Redis keyspace 또는 대규모 보유 상태를 추가 순회하지 않는다.
- 기존 Producer가 이미 생성하는 DLT를 활용하므로 Kafka event schema 변경이 없다.
- 별도 group을 사용해 기본 consumer offset과 DLT 복구 offset을 독립적으로 관리한다.

### 감수한 트레이드오프와 연동 전제

- DLT backlog에는 현재 상태보다 오래된 가격이 포함될 수 있으므로 D4의 최신성 판정이 적용되기 전에는 배포하지 않는다.
- 중복 또는 중간 실패 재처리는 D2의 원자적 평가액 갱신으로 보호한다.
- 시스템 전체의 주기적 가격·평가액 검증은 병렬로 진행하는 Batch reconciliation이 담당한다.
- 두 작업의 Redis key와 최신성 계약은 최종 통합 검증에서 함께 확인해야 한다.

## D2. 재시도 안전한 평가액 갱신 방식

### 검토한 선택지

1. 기존 delta 갱신에 종목 lock과 마지막 적용 상태만 추가
2. 포트폴리오 hash의 종목별 평가액을 Lua로 교체하면서 delta 반영
3. 포트폴리오 전체 보유 종목 평가액을 매번 재합산

### 결정

- 사용자 선택: **2번 Lua 원자적 교체**
- `pf:<portfolioId>` hash에 `scv:<stockId>` 필드를 추가한다.
- 한 포트폴리오의 종목별 평가액 교체와 `cvp` 증분은 하나의 Lua 실행으로 처리한다.
- 여러 종목이 한 포트폴리오에 함께 반영되면 delta를 합산해 `cvp`를 한 번만 변경한다.
- 기존 `portfolio:<portfolioId>:stock:<stockId>:current-value` key는 호환용 projection으로 유지한다.
- `scv`가 없는 기존 데이터는 호환 key에서 읽은 평가액을 최초 기준값으로 사용한다.

### 근거

- 동일 이벤트를 다시 처리하면 저장된 종목 평가액과 새 평가액이 같아 delta가 0이 된다.
- Lua 완료 후 호환 key 갱신 전에 중단되어도 재처리 시 포트폴리오 평가액이 중복 증가하지 않는다.
- 다른 종목이 같은 포트폴리오를 동시에 변경해도 `cvp` 전체를 덮어쓰지 않고 각 delta를 보존한다.
- Lua가 `pf:<portfolioId>` 한 key만 다루므로 Redis Cluster의 동일 hash slot 제약을 충족한다.

### 감수한 트레이드오프와 연동 전제

- 포트폴리오 hash에 보유 종목 수만큼 `scv` field가 추가된다.
- 호환 key는 Lua와 같은 원자성 경계에 넣을 수 없어 projection으로 취급한다.
- Batch reconciliation은 `scv`와 `cvp`를 정합성 기준으로 사용하고 호환 key도 함께 보정해야 한다.
- 이벤트 최신성 및 동시 가격 적용 순서는 D4에서 별도로 보호한다.

## D3. 주기적 전수 검증 실행 주체

### 결정

- 사용자 선택: **기존 Batch reconciliation을 확장하는 병렬 작업에서 담당**
- Go 워커에는 Redis 전체 SCAN, 주기 scheduler와 전역 실행 lease를 추가하지 않는다.
- Go 워커는 기본 가격 이벤트와 DLT의 실시간 처리, 원자적 평가액 갱신과 최신성 보호를 담당한다.
- Batch 작업은 시스템 전체의 주기적 가격·평가액 검증과 보정을 담당한다.

### 근거

- 사용자 10만, 포트폴리오 20만, 보유 포지션 60만 규모에서 두 서비스가 같은 전수 탐색을 중복 수행하지 않는다.
- 기존 Batch의 10분 주기 reconciliation, chunk 처리와 단일 실행 제어를 확장할 수 있다.
- Go 워커의 정상 이벤트 처리 경로에는 전수 탐색 부하가 추가되지 않는다.

### 연동 계약

- 원자 갱신 기준은 `pf:<portfolioId>`의 `scv:<stockId>`와 `cvp`이다.
- 기존 종목별 current-value key는 호환용 projection으로 함께 보정한다.
- 가격 최신성 비교에 사용하는 Redis field와 동일 시각 규칙은 D4 결정 후 확정한다.

## D4. 최신성 판정 규칙

### 검토한 선택지

1. `updatedAt`을 우선 비교하고 동일 시각 충돌은 기존 확정 값을 유지
2. 가격이 다르면 수신 순서대로 항상 적용
3. Kafka partition과 offset으로 비교

### 결정

- 사용자 선택: **1번 `updatedAt` 기준**
- 저장된 시각보다 최신인 이벤트만 평가액 fan-out을 수행한다.
- 동일 시각·동일 가격은 정상 중복으로 skip한다.
- 이전 시각 이벤트는 stale로 skip한다.
- 동일 시각·다른 가격은 충돌 metric과 경고 로그를 남기고 평가액을 변경하지 않는다.
- Batch reconciliation은 authoritative 가격을 기준으로 동일 시각 충돌을 확정할 수 있다.

### Redis 계약

- 적용 상태 key: `stock:{<stockId>}:price-application`
- 적용 상태 field: `at`은 UTC RFC3339Nano, `price`는 정수 가격
- 적용 lock key: `stock:{<stockId>}:price-application-lock`
- 두 key는 같은 Redis Cluster hash tag를 사용한다.
- lock TTL 기본값은 30초이며 `PRICE_APPLICATION_LOCK_TTL_SECONDS`로 조정한다.

### 실패 및 동시성 규칙

- 기본 topic과 DLT는 같은 종목 lock과 적용 상태를 사용한다.
- 여러 종목의 lock은 stock ID 오름차순으로 획득하고 경합 시 전체 Kafka batch를 재시도한다.
- 처리 중 lock TTL을 주기적으로 갱신한다.
- 전체 평가액 fan-out 성공 후 적용 상태 저장과 lock 해제를 Lua로 원자 처리한다.
- fan-out 중 실패하면 적용 상태를 확정하지 않아 Kafka 재처리가 다시 수행된다.
- lock 소유권을 잃으면 적용 상태를 확정하지 않는다.

### 근거와 트레이드오프

- 기본 topic과 DLT에 공통으로 있는 `updatedAt`을 사용해 오래된 backlog의 롤백을 막는다.
- offset에 의존하지 않아 Batch reconciliation과 동일한 최신성 계약을 사용할 수 있다.
- lock 경합 시 해당 Kafka batch 처리 지연이 생길 수 있지만 상태 순서를 우선한다.
- 같은 시각의 서로 다른 가격은 Go 워커가 임의로 선택하지 않아 결정 결과가 수신 순서에 좌우되지 않는다.
