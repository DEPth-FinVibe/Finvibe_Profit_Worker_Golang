# 가격 이벤트 정합성 복구 Plan

## 문서 정보

- 작성일: 2026-09-05
- 기준 Overview: `01-overview.md`
- 대상: `Finvibe_Profit_Worker_Golang`
- 상태: 사용자 확인 필요
- 리뷰 상태: 리뷰 필요

## 완료 조건

1. Kafka의 마지막 가격 이벤트를 누락시킨 뒤 authoritative current-price만 갱신해도 워커가 불일치를 찾아 복구한다.
2. 동일 이벤트 재처리와 reconciliation 재실행은 평가액을 중복 변경하지 않는다.
3. 최신 가격 적용 후 오래된 Kafka 이벤트를 처리해도 평가액이 롤백되지 않는다.
4. 두 worker replica가 reconciliation을 시작해도 하나의 실행만 전체 탐색을 수행한다.
5. Kafka 처리와 reconciliation이 같은 종목을 동시에 갱신해도 결과가 최신 절대가격에 수렴한다.
6. current-price 누락·만료·파싱 실패는 평가액을 변경하지 않고 metric과 로그에 남는다.
7. 기능을 설정으로 비활성화하면 기존 Kafka 소비 흐름만 실행된다.
8. `go test ./...`, `go vet ./...`, `git diff --check`가 통과한다.

## 구현 단계

### S1. 기준 동작 고정

- 현재 `main` 전체 테스트를 실행해 기준 결과를 기록한다.
- 가격 이벤트의 중복 처리, offset rewind와 절대가격 delta 계산 테스트를 확인한다.

검증:

- `go test ./...`
- 기존 가격 계산 통합 테스트 통과

### S2. 최신 가격과 적용 상태 모델링

- Backend Monolith의 current-price JSON 중 `stockId`, `price`, `at`만 읽는 Go 모델을 추가한다.
- 종목별 마지막 적용 가격·시각을 표현하는 모델과 비교 규칙을 추가한다.
- 기존 Kafka event schema는 변경하지 않는다.

검증:

- 실제 JSON 형식 역직렬화 테스트
- 이전·동일·최신 시각 비교 table test
- timezone 없는 `LocalDateTime` 호환 테스트

### S3. Redis 정합성 primitive 구현

- 보유 종목 역색인을 탐색한다.
- authoritative current-price와 마지막 적용 상태를 bulk 조회한다.
- 전체 reconciliation 실행 lease와 종목별 적용 lock을 구현한다.
- 종목 가격 적용과 포트폴리오 평가액 delta를 재시도 안전하게 갱신한다.
- 기존 current-value key는 호환성을 위해 유지한다.

검증:

- standalone Redis 탐색 테스트
- lock 획득·경합·소유자 검증·만료 테스트
- 중간 실패 후 재실행 시 중복 증분 방지 테스트

### S4. Kafka 가격 처리에 최신성 보호 적용

- Kafka handler와 reconciliation이 동일한 가격 적용 진입점을 사용하게 한다.
- 종목 lock 안에서 마지막 적용 상태를 확인한다.
- 오래된 이벤트는 성공적으로 skip하고 offset은 정상 커밋할 수 있게 한다.
- 전체 fan-out 성공 후에만 마지막 적용 상태를 확정한다.

검증:

- 최신 이벤트 뒤 과거 이벤트 처리 테스트
- 동일 이벤트 반복 처리 테스트
- 적용 실패 뒤 재처리 테스트

### S5. 주기적 reconciliation과 관측성 추가

- context 종료를 따르는 reconciliation loop를 실행 조립에 추가한다.
- 보유 종목과 current-price를 비교해 불일치 종목만 가격 적용 서비스로 전달한다.
- 실행·검사·일치·복구·skip·실패 수와 실행 시간을 집계 metric으로 제공한다.
- stock ID는 metric label로 사용하지 않는다.

검증:

- 마지막 Kafka 이벤트 누락 복구 통합 테스트
- 기능 비활성화와 graceful shutdown 테스트
- metric 등록 및 결과별 증가 테스트

### S6. 전체 검증과 운영 문서

- 결정별 테스트 후 전체 테스트와 정적 검증을 수행한다.
- 실제 동작, 설정, metric, 위험, 롤백과 남은 한계를 `04-result.md`에 기록한다.
- 문서별 200줄 제한과 Markdown 형식을 확인한다.

검증:

- `go test ./...`
- `go test -race ./...`
- `go vet ./...`
- `git diff --check`
- 작업 문서별 `wc -l`

## 결정 포인트

### D1. 복구 데이터 원본

1. Redis current-price를 주기적으로 대조한다.
   - 장점: 기본 토픽과 DLT 모두 실패한 마지막 이벤트도 TTL 안에 복구할 수 있다.
   - 단점: Redis 탐색·조회 부하와 Monolith Redis key 계약 의존이 생긴다.
2. Kafka DLT를 추가 소비한다.
   - 장점: 구현과 운영 흐름이 단순하고 Redis 탐색이 없다.
   - 단점: DLT에도 기록되지 않은 유실과 조용한 불일치를 발견하지 못한다.
3. stale metric만 추가하고 자동 복구하지 않는다.
   - 장점: 상태 변경 위험과 추가 I/O가 가장 작다.
   - 단점: 완료 조건의 자동 복구를 충족하지 못한다.

AI 추천: **1번**. Go 워커만 변경한다는 범위 안에서 마지막 이벤트 유실을 실제로 발견하고 복구할 수 있는 유일한 선택이다.

### D2. 재시도 안전한 평가액 갱신 방식

1. 기존 delta 갱신에 종목 lock과 마지막 적용 상태만 추가한다.
   - 장점: 변경량과 Redis 명령 수가 작다.
   - 단점: 포트폴리오 증분과 종목 current-value 저장 사이의 실패는 재처리 시 중복 또는 누락을 만들 수 있다.
2. 포트폴리오 hash에 종목별 current-value 필드를 두고 Lua로 종목 값 교체와 포트폴리오 delta 반영을 원자화한다.
   - 장점: 동일·중복·중간 실패 재처리가 delta 0으로 수렴하고 다른 종목의 동시 증분도 보존한다.
   - 단점: Redis 내부 상태 필드가 추가되고 Lua 및 호환용 이중 쓰기가 필요하다.
3. 불일치가 있을 때 포트폴리오 전체 종목 평가액을 다시 합산해 절대값으로 저장한다.
   - 장점: 기존 drift까지 바로 교정할 수 있다.
   - 단점: 보유 종목 수만큼 읽기가 늘고 실시간 Kafka 갱신과 절대값 쓰기가 경합할 수 있다.

AI 추천: **2번**. 기존 fan-out 성능 특성을 유지하면서 이번 작업에서 드러난 재처리 경계를 원자화할 수 있다.

### D3. 보유 종목 탐색과 다중 replica 제어

1. Redis Cluster의 각 master에서 `stock:<id>:portfolios`를 SCAN하고, 전역 lease를 획득한 replica 하나만 실행한다.
   - 장점: 기존 보유 종목을 빠짐없이 찾고 중복 전체 탐색을 막는다.
   - 단점: Redis Cluster master 순회 코드와 SCAN 부하 제어가 필요하다.
2. 각 replica가 실행 중 관측한 종목만 메모리에 보관한다.
   - 장점: Redis SCAN이 없다.
   - 단점: 재시작 직후와 마지막 이벤트 자체가 오지 않은 종목을 놓친다.
3. 복구 대상 종목 목록을 설정으로 주입한다.
   - 장점: 동작 범위가 명확하다.
   - 단점: 보유 종목 변경과 설정이 쉽게 어긋난다.

AI 추천: **1번**. 정확성을 우선하되 cursor와 scan count로 한 번에 처리하는 양을 제한한다.

### D4. 최신성 판정 규칙

1. `updatedAt`을 우선 비교하고, 같은 시각의 가격 충돌에서는 current-price를 authoritative 값으로 인정한다.
   - 장점: 늦은 Kafka 이벤트의 롤백을 막고 reconciliation 결과를 확정할 수 있다.
   - 단점: current-price와 Kafka의 시각 계약이 동일하다는 전제가 필요하다.
2. 가격이 다르면 수신 순서대로 항상 적용한다.
   - 장점: 구현이 단순하다.
   - 단점: DLT나 재처리된 과거 이벤트가 최신 상태를 되돌릴 수 있다.
3. Kafka offset만 비교한다.
   - 장점: 같은 partition 안의 순서를 명확히 판단한다.
   - 단점: Redis current-price에는 Kafka offset이 없어 reconciliation과 비교할 수 없다.

AI 추천: **1번**. 두 입력에 공통으로 존재하는 `updatedAt`을 사용하고 충돌 규칙을 테스트로 고정한다.

### D5. 기본 실행 정책

1. 기본 활성화, 5초 주기, 실행 lease 30초, 종목 lock 10초로 시작한다.
   - 장점: current-price의 5분 TTL보다 충분히 빠르고 별도 Manifest 변경 없이 보호가 적용된다.
   - 단점: 배포 즉시 Redis 추가 부하가 발생한다.
2. 기본 비활성화하고 환경변수로 활성화한다.
   - 장점: 운영자가 부하를 확인한 뒤 점진적으로 켤 수 있다.
   - 단점: 이번 범위에서 Manifest를 바꾸지 않으므로 배포 직후 문제는 그대로다.
3. 기본 활성화, 30초 주기로 시작한다.
   - 장점: Redis 부하가 더 작다.
   - 단점: 잘못된 수익률 노출 시간이 길어진다.

AI 추천: **1번**. 모든 값은 환경변수로 조정하고 `PRICE_RECONCILIATION_ENABLED=false`로 즉시 비활성화할 수 있게 한다.

## 적용 순서

```text
D1 복구 원본
  → D2 원자적 갱신
    → D3 탐색·lease
      → D4 최신성 규칙
        → D5 실행 정책
          → 전체 통합 검증과 Result
```

각 결정은 사용자 확정 직후 `03-decisions.md`에 근거와 트레이드오프를 기록하고, 해당 범위의 구현·테스트·커밋까지 마친 뒤 다음 결정로 이동한다.

