# 가격 이벤트 정합성 복구 Plan

## 문서 정보

- 작성일: 2026-09-05
- 최근 개정일: 2026-09-06
- 기준 Overview: `01-overview.md`
- 대상: `Finvibe_Profit_Worker_Golang`
- 상태: 진행 중
- 리뷰 상태: 리뷰 필요

## 완료 조건

1. DLT 가격 이벤트가 별도 group에서 기본 가격 계산 경로로 처리된다.
2. 동일 이벤트 재처리와 처리 단계 중단 뒤 재실행은 평가액을 중복 변경하지 않는다.
3. 최신 가격 적용 후 오래된 Kafka 이벤트를 처리해도 평가액이 롤백되지 않는다.
4. 같은 포트폴리오의 서로 다른 종목 동시 갱신이 각 delta를 보존한다.
5. DLT 기능을 설정으로 비활성화하면 기본 consumer 수만 readiness에 반영된다.
6. DLT 처리량·성공·실패·지연을 기본 가격 이벤트와 구분한다.
7. `go test ./...`, `go test -race ./...`, `go vet ./...`, `git diff --check`가 통과한다.

## 구현 단계

### S1. 기준 동작 고정 — 완료

- 선행 PR을 병합하고 해당 merge commit에서 작업 브랜치를 생성했다.
- 기존 가격 계산·Kafka 재처리 테스트를 기준선으로 확인했다.

### S2. Kafka DLT 빠른 복구 — 완료

- DLT topic을 별도 consumer group으로 소비한다.
- 최초 backlog 처리를 위해 offset reset을 `earliest`로 설정한다.
- 기본 가격 handler와 수익률 계산 진입점을 공유한다.
- DLT 전용 event type으로 처리 지표를 분리한다.
- 활성화 여부와 concurrency를 환경변수로 제어한다.

검증:

- DLT 설정 기본값·override 테스트
- readiness worker 수 테스트
- DLT에서 수익률 계산까지의 통합 테스트

### S3. Redis 평가액 갱신 원자화 — 완료

- `pf:<portfolioId>`에 `scv:<stockId>` field를 추가한다.
- 한 포트폴리오의 종목 평가액 교체와 `cvp` delta 반영을 Lua 하나로 처리한다.
- 기존 종목 평가액 key는 호환용 projection으로 유지한다.
- 거래 및 삭제 경로도 `scv`를 함께 갱신한다.

검증:

- 기존 데이터 최초 이관 테스트
- Lua 반영 직후 중단을 가정한 재처리 테스트
- 서로 다른 종목의 동시 delta 보존 테스트
- 가격·거래 경로의 `scv` 호환 테스트

### S4. Kafka 가격 최신성 보호 — 완료

- 종목별 마지막 적용 가격·시각을 Redis에 저장한다.
- 기본 가격과 DLT handler가 같은 비교 규칙을 사용한다.
- 과거 이벤트는 성공적으로 skip해 offset을 커밋할 수 있게 한다.
- 평가액 fan-out이 완료된 경우에만 적용 상태를 확정한다.

검증:

- 최신 이벤트 뒤 과거 이벤트 처리 테스트
- 동일 시각·동일 가격 반복 테스트
- 적용 실패 뒤 재처리 테스트

### S5. 전체 검증과 운영 문서

- 설정, metric, Redis field 계약, 위험과 롤백 방법을 `04-result.md`에 기록한다.
- Batch 병렬 작업과 공유하는 Redis 계약을 확인한다.
- 문서별 200줄 제한과 Markdown 형식을 확인한다.

검증:

- `go test ./...`
- `go test -race ./...`
- `go vet ./...`
- `git diff --check`
- 작업 문서별 `wc -l`

## 결정 상태

| 결정 | 상태 | 결과 |
|---|---|---|
| D1 복구 데이터 원본 | 완료 | Go 워커는 Kafka DLT를 별도 group으로 소비 |
| D2 재시도 안전한 갱신 | 완료 | 포트폴리오 hash와 Lua로 원자적 교체 |
| D3 주기적 전수 검증 주체 | 완료 | 병렬 Batch reconciliation이 담당 |
| D4 최신성 판정 규칙 | 완료 | `updatedAt` 우선, 동일 시각 충돌은 기존 값 유지 |

세부 근거와 트레이드오프는 `03-decisions.md`에서 관리한다.

## 적용 순서

```text
D1 DLT 소비 (완료)
  → D2 원자적 갱신 (완료)
    → D3 Batch 책임 분리 (완료)
      → D4 최신성 규칙
        → 전체 검증과 Result
```
