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
