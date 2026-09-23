# Kafka 재시도·DLT와 캐시 쓰기 원자화 Result

## 문서 정보

- 완료일: 2026-09-23
- 대상 저장소: `Finvibe_Profit_Worker_Golang`
- 작업 브랜치: `feat/kafka-retry-dlt`
- 상태: 구현 완료, 실제 Kafka·Redis Cluster 로컬 검증 완료, 미배포
- 리뷰 상태: 리뷰 필요

## 결과 요약

깨진 메시지가 더 이상 파티션을 멈추지 않는다. 일시 장애는 간격을 두고 한도 안에서 재시도하고, 재시도를 포기한 매매·포트폴리오 이벤트는 그 레코드만 DLT로 간다. 매매·포트폴리오 캐시 쓰기는 해시 단위 Lua와 마커로 원자적이고 멱등해져서, 어디서 끊겨 재처리하더라도 한 번만 반영된다.

## 구현 결과

### Kafka 에러 처리 (`internal/kafka`)

| 토픽 | 깨진 메시지 | 일시 장애 |
|---|---|---|
| 가격, 가격 DLT | 그 메시지만 버림 (`invalid_payload`) | 200ms 간격 2회 재시도 후 배치를 버림 |
| 매매, 포트폴리오 | 곧바로 `<topic>.DLT` | 레코드 단위로 1s부터 2배·최대 30s 간격, 5분 재시도 후 그 레코드만 `<topic>.DLT` |

- 매매·포트폴리오는 레코드를 순서대로 처리한다. 대기에 들어가기 전에 앞 레코드 offset을 커밋한다.
- 대기 중에는 할당된 파티션을 pause하고 Poll을 계속 불러 `max.poll.interval.ms`를 지킨다. 대기 중 새로 할당된 파티션도 pause하고, Poll이 돌려준 메시지는 그 offset으로 seek해 잃지 않는다.
- 대기 중 레코드의 파티션이 회수되면 재시도를 멈추고, 처리하지 않은 레코드를 되감는다.
- 가격 필수값(`stockId`, `updatedAt`) 검증을 decode 단계에 추가했다. 이전에는 서비스에서 에러가 나 배치 전체가 재시도됐다.

### DLT 발행 (`internal/kafka/dlt.go`)

- confluent producer(`acks=all`, idempotence)로 원본 key·value·header를 보존해 동기 발행한다.
- 추가 header: `dlt-original-topic`, `dlt-original-partition`, `dlt-original-offset`, `dlt-exception-type`, `dlt-exception-message`
- 기동 시 매매·포트폴리오 DLT 토픽을 없으면 만든다(파티션 1, 복제 수 브로커 기본값). 실패해도 기동은 계속한다.

### 매매 쓰기 원자화 (`internal/redisstore/idempotent.go`)

| 쓰기 | 방식 |
|---|---|
| 보유 수량 | compare-and-set Lua + 마커. 신규 보유·보유 해제 결과를 마커에 저장해 재시도 때 그대로 돌려준다 |
| `pf:<p>`의 `pv`·`cvp`·`scv:<s>`·`ac` | Lua 하나 + 마커. **`cvp`와 `scv`가 같은 스크립트에서 바뀐다** |
| `usr:<u>`의 `pv`·`cvp`(·`pc`) | Lua 하나 + 마커 |
| 보유 인덱스 | 반영 후 수량 기준으로 SADD/SREM |
| 호환 projection(`current-value` 키) | 반영 후 `scv`로 SET/DEL |

- 마커 키: `applied:{<데이터 키>}:<이벤트 키>`, TTL 7일. 로컬 Redis Cluster에서 `pf:100`과 마커의 slot이 모두 5332인 것을 확인했다.
- 기존 이벤트 단위 마커(`processed:trade:<id>`)는 그대로 둔다.

### 포트폴리오 생성·삭제 (`internal/service/cache.go`)

- 생성: 유저 합계를 멱등하게 먼저 반영하고 매핑을 만든다. 이전 순서(매핑 → 합계)에서는 매핑 직후 끊긴 재시도가 "이미 생성됨"으로 빠져 **유저 합계가 반영되지 않았다**.
- 삭제: 유저 합계를 멱등하게 뺀다. 매핑이 이미 없는 재시도에서도 유저 목록 제거·삭제 표시·상태 삭제를 다시 실행한다. 이전에는 매핑 필드 삭제와 유저 목록 제거 사이에서 끊기면 **삭제된 포트폴리오가 유저 목록에 남았다**.
- 이미 반영된 재처리에서도 유저 snapshot을 다시 저장한다.

## 지표

- `profit_worker_events_recovered_total{event_type, action="dead_lettered"|"dropped"}` — 재시도를 포기한 이벤트
- `profit_worker_events_skipped_total{reason="invalid_payload"}` — 버린 깨진 가격 메시지
- `profit_worker_redis_command_duration_seconds{command="lua_compare_and_set"|"lua_apply_portfolio_trade_totals"|"lua_apply_user_totals"}`

## 검증 결과

실행 환경: Docker `golang:1.25`(go1.25.14, linux/arm64), Redis는 miniredis.

- `go vet ./...`, `gofmt -l .`, `git diff --check`: 통과
- `go test -race -count=1 ./...`: 통과

추가 테스트:

| 테스트 | 검증 |
|---|---|
| `TestRecordRetriesThenDeadLettersOnlyTheFailingRecord` | 실패 레코드만 DLT, 앞뒤 레코드는 한 번씩 처리, 대기 중 pause·poll·resume |
| `TestInvalidRecordGoesToDLTWithoutRetry` | 깨진 레코드는 재시도 없이 DLT |
| `TestRecordRetrySucceedsAfterTransientFailure` | 일시 장애 후 성공, 대기 전 앞 레코드 커밋 |
| `TestRecordRetryStopsWhenPartitionIsRevoked` | 파티션 회수 시 재시도 중단, 미처리 레코드 되감기 |
| `TestMessagePolledDuringWaitIsSeekedBack` | 대기 중 받은 메시지를 되감아 유실 방지 |
| `TestPriceBatchIsDroppedAfterShortRetries` | 가격 배치 3회 시도 후 버림 |
| `TestInvalidStockPriceEventDoesNotBlockOthers` | 깨진·필수값 누락 가격 메시지가 있어도 나머지 반영 |
| `TestTradeRetryAfterPartialFailureAppliesOnce` | 포트폴리오 쓰기 후 끊긴 매수 재처리 시 한 번만 반영 |
| `TestLateSellReplayKeepsHoldingBoughtAgain` | 늦은 매도 재처리가 다시 산 종목을 지우지 않음 |
| `TestPortfolioCreateRetryAfterTotalsAppliedOnce` | 생성 재처리 시 유저 합계 한 번만 |
| `TestPortfolioDeleteRetryFinishesCleanup` | 삭제 재처리 시 유저 목록·상태 정리 완료 |
| `TestAppliedMarkerKeySharesSlotWithDataKey` | 마커와 데이터 키 slot 일치 (CRC16 계산식을 `foo`=12182로 먼저 검증) |

마커를 무력화(항상 새 마커)하면 멱등성 테스트 3개가 실패하는 것을 확인했다. 삭제 재처리 테스트는 "매핑이 없으면 합계를 빼지 않는" 순서가 따로 막아 통과한다.

### 실제 의존성 검증

로컬 Docker에 실제 Kafka(3.9.0, KRaft 단일 노드)와 3노드 Redis Cluster(7.2)를 띄워 확인했다. 환경변수가 없으면 자동으로 skip된다.

```bash
KAFKA_TEST_BROKERS=verify-kafka:9092 \
REDIS_TEST_CLUSTER_NODES=<node1>:6379,<node2>:6379,<node3>:6379 \
  go test ./...
```

| 테스트 | 검증 | 결과 |
|---|---|---|
| `TestEnsureTopicsCreatesDeadLetterTopics` | DLT 토픽 생성, 재호출 시 에러 없음 | 통과 |
| `TestFailingRecordIsDeadLetteredOnRealBroker` | 실제 브로커에서 실패 레코드 재시도(pause·poll) → 그 레코드만 DLT, 원본 위치 header, 배치 전체 offset 커밋(3) | 통과 |
| `TestIdempotentWritesRunOnRealCluster` | Cluster에서 멱등 쓰기 Lua 3회 반복 → 수량·구매액·평가액·종목 평가액 모두 한 번만 | 통과 |
| `TestSellRemovesStockValueOnRealCluster` | 전량 매도 시 수량 키·`scv` 필드·역인덱스 정리 | 통과 |
| `TestTradesSurviveRebalanceAcrossPartitions` | 3파티션·컨슈머 2개, 처리 중 참여·이탈로 리밸런스 2회, 이후 전체 재전달 → 포트폴리오마다 수량 40·금액 4,000원 그대로 | 통과 |

마커 키를 같은 slot에 두지 않도록(`applied:<이벤트 키>:<데이터 키>`) 바꾸면 두 Cluster 테스트가 **`CROSSSLOT Keys in request don't hash to the same slot`**으로 실패한다. hash tag로 마커를 데이터 키에 붙이는 것이 이 설계의 전제라는 것을 실측으로 확인했다.

### 여러 파티션·리밸런스 검증

3파티션 토픽에 매수 60건(포트폴리오 3개 × 20건)을 넣고, 컨슈머 하나로 처리를 시작한 뒤 두 번째 컨슈머를 붙이고(리밸런스 1회) 처리 중 첫 컨슈머를 내렸다(리밸런스 2회). 그다음 토픽 전체를 처음부터 다시 처리해 **커밋 전에 파티션을 잃은 컨슈머의 재전달**을 재현했다. 최종 값은 포트폴리오마다 수량 40, 구매액·평가액·종목 평가액 4,000원, 유저 합계 4,000원으로 정확히 한 번만 반영됐고 DLT는 비어 있었다.

처음 작성한 버전은 리밸런스만 일으키고 재전달을 재현하지 않아, 마커를 모두 무력화해도 통과했다. 처리 루프가 배치 끝에서 커밋하고 ctx 취소도 배치를 마친 뒤 멈추기 때문에 커밋되지 않은 구간이 생기지 않았다. 재전달 단계를 넣은 뒤에는 마커를 무력화하면 수량이 40에서 **80**으로 두 배가 되어 실패한다.

### 여러 pod 검증 (`verify-multi-pod.sh`)

워커 이미지를 독립 컨테이너 두 개로 띄우고(운영과 같은 배선·환경변수), 처리 중 하나를 `SIGKILL`로 죽였다. 1파동으로 모든 파티션에 커밋된 offset을 만든 뒤, 2파동(포트폴리오당 50건)을 처리하는 동안 Redis를 잠깐 멈추고 죽였다.

결과는 포트폴리오마다 수량 102, 금액 10,200원으로 **유실도 중복도 없었다**. DLT도 비어 있었다.

브로커는 Kafka 4.0을 쓴다. 운영과 같은 `KAFKA_GROUP_PROTOCOL=consumer`(KIP-848)가 기본 지원되기 때문이다. Kafka 3.9로 돌리면 컨슈머가 그룹에 붙지 못하고 **에러 로그도 없이 아무것도 처리하지 않는다**. 검증 환경을 만들 때 주의해야 한다.

검증 중 확인한 두 가지:

- 워커의 `auto.offset.reset`은 `latest`다. 어떤 파티션에 커밋된 offset이 한 번도 없는 상태에서 소유자가 죽으면, 새 소유자는 log end부터 읽어 **그 사이 메시지를 건너뛴다**. 새 consumer group의 첫 배포 구간에만 해당한다.
- 이 스크립트로는 **재전달 자체를 만들지 못했다**. 마커를 모두 무력화해도 결과가 같았다. 처리 루프가 배치마다 곧바로 커밋해서, `SIGKILL` 시점에 적용됐지만 커밋되지 않은 구간이 남지 않았다. 중복 방지 경로는 재전달을 강제하는 in-process 테스트(`TestTradesSurviveRebalanceAcrossPartitions`)가 증명한다.

### 재시도 중 파티션 회수 검증 (`TestLongRetrySurvivesAnotherConsumerJoining`)

2파티션 토픽에서 레코드 하나를 계속 실패시켜 재시도에 들어간 뒤, 같은 그룹에 두 번째 컨슈머를 붙였다. 실제 브로커 로그에서 회수 경로가 그대로 실행됐다.

```
WARN record failed, retrying wait=500ms
WARN record failed, retrying wait=1s
WARN kafka rewind unprocessed records err="Local: Erroneous state"   ← 파티션 회수로 재시도 중단
WARN record failed, retrying wait=500ms                              ← 두 번째 컨슈머가 이어받음
```

장애를 해소하자 레코드는 **정확히 1회** 반영됐고, DLT는 비었고 offset은 1로 커밋됐다. 회수된 파티션을 되감으려다 실패하는 `Local: Erroneous state` 경고는 정상 흐름이지만 회수마다 남는다.

## 검증하지 못한 것

- 스테이징·운영 배포 후의 지표 관찰
- 여러 pod 환경에서 재전달이 실제로 발생하는 상황 (위 참고)

## 남은 위험

- 매매 재시도가 5분을 넘기면 같은 포트폴리오의 뒤 이벤트가 먼저 반영된다.
- 마커 TTL(7일)이 지난 뒤 재처리하면 중복 검사가 되지 않는다.
- 매니페스트 배포 이미지는 `d6288fc`로, 이 브랜치와 선행 브랜치(`fix/price-event-reconciliation`)의 변경은 아직 배포되지 않았다.
