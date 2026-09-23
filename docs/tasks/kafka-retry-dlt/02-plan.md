# Kafka 재시도·DLT와 캐시 쓰기 원자화 Plan

## 문서 정보

- 작성일: 2026-09-22
- 기준 Overview: `01-overview.md`
- 대상: `Finvibe_Profit_Worker_Golang`
- 상태: 완료
- 리뷰 상태: 리뷰 필요

## 완료 조건

1. 깨진 메시지가 있어도 앞뒤 레코드가 처리된다.
2. 가격 배치는 200ms × 2회 재시도 후 버려진다.
3. 매매·포트폴리오 레코드는 5분 재시도 후 그 레코드만 DLT로 가고, 뒤 레코드 처리가 이어진다.
4. 재시도 대기 중 파티션이 pause되고 Poll이 계속 호출된다.
5. 매매·포트폴리오 이벤트를 중간 실패 후 재처리해도 캐시에 한 번만 반영된다.
6. `go test ./...`, `go test -race ./...`, `go vet ./...`, `git diff --check`가 통과한다.

## 구현 단계

### S1. 기준 동작 고정

- `fix/price-event-reconciliation`(`394280d`)에서 `feat/kafka-retry-dlt`를 분기한다.
- Docker `golang:1.25`에서 `go test ./...`가 통과하는 것을 확인한다.

### S2. DLT producer와 토픽 생성 (D1, D5)

- confluent producer로 원본 key·value·header와 원본 위치·에러 header를 DLT에 동기 발행한다.
- 기동 시 매매·포트폴리오 DLT 토픽을 만든다.

### S3. 가격 consumer 에러 처리 (D1, D2)

- decode·가격 검증 실패 레코드는 skip하고 나머지를 처리한다.
- 일시 장애는 200ms 간격 2회 재시도 후 배치를 버린다.

### S4. 매매·포트폴리오 레코드 단위 재시도 (D1–D4)

- 레코드를 순서대로 처리한다. 깨진 레코드는 곧바로 DLT로 보낸다.
- 일시 장애는 지수 backoff로 5분까지 재시도한다. 대기 전에 앞 레코드 offset을 커밋하고, 대기 중에는 pause + Poll한다.
- 소진되면 그 레코드를 DLT로 보내고 다음으로 넘어간다.

검증: 가짜 consumer로 재시도·pause·회수·DLT 경로 테스트

### S5. 매매 쓰기 원자화 (D6)

- 수량 compare-and-set, `pf` 해시 Lua, `usr` 해시 Lua에 마커를 둔다.
- 보유 인덱스와 호환 projection을 반영 후 상태로 맞춘다.

검증: miniredis로 중간 실패 후 재처리, 같은 이벤트 반복, 마커 slot 테스트

### S6. 포트폴리오 생성·삭제 원자화 (D6)

- 이벤트 단위 마커와 `usr` 해시 Lua로 구매액·평가액·포트폴리오 수를 한 번만 반영한다.

### S7. 결과 정리

- `04-result.md`에 결과와 검증을 남긴다.
