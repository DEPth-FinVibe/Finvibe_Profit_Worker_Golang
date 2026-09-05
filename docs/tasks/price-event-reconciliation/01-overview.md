# 가격 이벤트 정합성 복구 Overview

## 문서 정보

- 작성일: 2026-09-05
- 대상 독자: 수익률 워커 개발자와 운영 담당자
- 대상 저장소: `Finvibe_Profit_Worker_Golang`
- 작업 브랜치: `fix/price-event-reconciliation`
- 기준 커밋: `73fac13`
- GitHub Issue·PR: 사용자 승인에 따라 이번 작업에서 생략
- 리뷰 상태: 리뷰 필요

## 배경

수익률 워커는 `market.stock-price-updated.v1` 이벤트의 절대가격을 받아 Redis의 종목·포트폴리오·사용자 평가액을 갱신한다. 중간 가격 이벤트가 누락돼도 다음 절대가격 이벤트를 처리하면 최신 상태로 수렴한다.

하지만 마지막 가격 이벤트가 Kafka 기본 토픽과 워커 사이에서 누락되면 후속 이벤트가 없으므로 평가액이 이전 가격에 머문다. 현재 워커는 이벤트 순번이나 종목별 마지막 적용 상태를 관리하지 않으며, 수신한 이벤트의 지연 시간만 관측한다.

## 확인된 현재 동작

- 이벤트 계약은 `stockId`, `price`, `updatedAt`만 제공한다.
- 동일 배치의 같은 종목 이벤트는 마지막 이벤트 하나로 합쳐 처리한다.
- 평가액은 새 절대 평가액과 Redis에 저장된 기존 평가액의 차이를 포트폴리오에 더한다.
- 처리 성공 후 수동으로 Kafka offset을 저장하고 커밋한다.
- 처리·offset 저장·커밋 실패 시 배치를 되감아 재처리한다.
- 워커와 Backend Monolith는 같은 Redis Cluster를 사용한다.
- Backend Monolith는 최신 시세를 `market:current-price:{stock:<id>}` 키에 JSON으로 저장하며 TTL은 5분이다.
- 보유 종목 역색인은 `stock:<id>:portfolios` Redis Set으로 유지된다.
- 종목별 마지막 적용 가격·시각, 누락 감지, 최신 시세 대조 작업은 없다.

## 해결할 문제

1. Kafka에 마지막 가격 이벤트가 도착하지 않아도 워커가 Redis의 최신 시세와 평가 상태 불일치를 발견해야 한다.
2. 발견한 최신 가격으로 포트폴리오와 사용자 수익률을 자동 복구해야 한다.
3. 늦게 도착하거나 재처리된 과거 이벤트가 복구된 최신 상태를 되돌리지 않아야 한다.
4. 복구 성공·실패와 오래된 평가 상태를 운영 지표로 확인할 수 있어야 한다.
5. Kafka 재처리와 복구 작업이 동시에 실행돼도 평가액이 중복 반영되지 않아야 한다.

## 목표

- 보유 종목의 authoritative current-price와 워커의 마지막 적용 상태를 주기적으로 대조한다.
- 불일치를 발견하면 절대가격을 기준으로 평가 상태를 최신 값에 수렴시킨다.
- 종목별 최신성 규칙과 동시 실행 제어를 명시한다.
- 장애 주입 테스트로 마지막 이벤트 누락, 중복, 역순 도착과 재실행을 검증한다.
- 복구 지연 한도와 결과를 설정 및 Prometheus metric으로 노출한다.

## 변경 범위

- Go 워커의 설정, 모델, Redis adapter, 수익률 서비스와 실행 조립
- Kafka 가격 처리와 reconciliation 사이의 정합성 제어
- 보유 종목 탐색 및 current-price 조회
- reconciliation 주기 실행과 graceful shutdown
- 관련 단위·통합 테스트
- 작업 문서 4종과 README 또는 운영 설명 중 필요한 최소 문서

## 제외 범위

- Backend Monolith, Batch, Manifest 저장소 변경
- Java·WebFlux 수익률 워커 변경
- Kafka 이벤트 schema 변경 또는 sequence 필드 추가
- 모든 가격 틱의 영구 보존과 누락된 중간 틱 복원
- Kafka·Redis를 포함한 end-to-end exactly-once 보장
- GitHub Issue와 PR 생성

## 완료 조건

- 마지막 Kafka 가격 이벤트를 누락시킨 테스트에서 설정된 시간 안에 authoritative current-price로 평가액이 복구된다.
- 같은 최신 가격을 반복 적용해도 평가액이 변하지 않는다.
- 최신 가격 적용 후 오래된 Kafka 이벤트가 도착해도 상태가 롤백되지 않는다.
- Kafka 처리와 reconciliation의 동시 실행 테스트에서 중복 증분이 발생하지 않는다.
- current-price 키가 없거나 만료된 종목은 기존 평가액을 임의로 변경하지 않고 관측 가능한 결과로 남긴다.
- reconciliation의 검사·불일치·복구·실패·skip 결과를 metric과 로그로 확인할 수 있다.
- 기능 비활성화 설정으로 기존 Kafka 전용 동작으로 되돌릴 수 있다.
- `go test ./...`와 필요한 정적 검증이 통과한다.

## 위험과 제약

- Redis Cluster 전체 key scan은 노드 부하와 구현 복잡도를 높일 수 있다.
- 두 worker replica가 동시에 복구하면 같은 종목 평가액 갱신이 경합할 수 있다.
- Kafka 이벤트와 current-price JSON의 `updatedAt`은 시간대와 정밀도가 일치해야 한다.
- current-price TTL 만료 후에는 외부 최신가를 워커만으로 복원할 수 없다.
- 절대 합계 재계산은 delta 갱신보다 Redis 읽기 비용이 크다.
- 종목 ID를 label로 노출하면 metric cardinality가 증가하므로 집계 지표를 우선한다.

## 의존성과 적용 조건

- 선행 PR `#1 feat: 사용자 수익률 Redis write-behind 갱신`은 2026-09-05에 `main`으로 병합됐다.
- 구현은 병합 커밋 `73fac13`을 기준으로 시작한다.
- 구체적인 복구 방식, 동시성 제어와 기본 주기는 Plan의 사용자 결정 후 확정한다.

