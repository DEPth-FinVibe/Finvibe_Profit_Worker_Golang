# 가격 이벤트 정합성 복구 Overview

## 문서 정보

- 작성일: 2026-09-05
- 최근 개정일: 2026-09-06
- 대상 독자: 수익률 워커 개발자와 운영 담당자
- 대상 저장소: `Finvibe_Profit_Worker_Golang`
- 작업 브랜치: `fix/price-event-reconciliation`
- 기준 커밋: `73fac13`
- GitHub Issue·PR: 사용자 승인에 따라 이번 작업에서 생략
- 리뷰 상태: 리뷰 필요

## 배경

수익률 워커는 `market.stock-price-updated.v1`의 절대가격을 받아 Redis의 종목·포트폴리오·사용자 평가액을 갱신한다. 중간 가격 이벤트가 누락돼도 다음 절대가격 이벤트를 처리하면 최신 상태로 수렴한다.

Producer는 처리에 실패한 가격 이벤트를 `market.stock-price-updated.v1.DLT`에 격리한다. 시스템 전체의 주기적 가격·평가액 검증은 기존 Batch reconciliation을 확장하는 병렬 작업에서 담당한다.

## 확인된 기존 동작

- 이벤트 계약은 `stockId`, `price`, `updatedAt`을 제공한다.
- 같은 배치의 동일 종목 이벤트는 마지막 이벤트 하나로 합쳐 처리한다.
- 평가액은 새 절대 평가액과 저장된 종목 평가액의 차이를 포트폴리오에 더한다.
- 성공 후 Kafka offset을 수동 저장·커밋하며 실패 시 배치를 되감는다.
- Go 워커와 Backend Monolith는 같은 Redis Cluster를 사용한다.
- 보유 종목 역색인은 `stock:<id>:portfolios` Redis Set으로 유지된다.

## 해결할 문제

1. DLT로 격리된 가격 이벤트를 Go 워커가 빠르게 다시 처리해야 한다.
2. 중복 처리나 처리 단계 중단 뒤 재시도에도 평가액이 중복 반영되지 않아야 한다.
3. 늦게 도착한 과거 이벤트가 최신 평가 상태를 되돌리지 않아야 한다.
4. 기본 가격 이벤트와 DLT 처리 결과를 운영 지표에서 구분해야 한다.
5. Batch reconciliation과 Go 워커가 공유할 Redis 평가 상태 계약이 명확해야 한다.

## 목표

- DLT를 기본 가격 이벤트와 독립된 consumer group으로 처리한다.
- 종목 평가액 교체와 포트폴리오 delta 반영을 Redis에서 원자화한다.
- Kafka 기본 토픽과 DLT에 동일한 최신성 판정 규칙을 적용한다.
- 기존 종목 평가액 key를 호환용 projection으로 유지한다.
- 시스템 전체 검증을 담당하는 Batch 작업과 `scv`, `cvp`, 적용 시각 계약을 맞춘다.

## 변경 범위

- Go 워커의 Kafka DLT consumer와 설정
- 가격 이벤트 공통 처리 경로와 지표
- Redis의 종목별 평가액 및 포트폴리오 평가액 원자 갱신
- 거래 이벤트의 종목별 평가액 field 호환 갱신
- 가격 이벤트 최신성 판정
- 관련 단위·통합·race 테스트와 작업 문서

## 제외 범위

- Backend Monolith, Batch, Manifest 저장소 변경
- Go 워커의 Redis 전체 SCAN, reconciliation scheduler와 실행 lease
- Java·WebFlux 수익률 워커 변경
- Kafka 이벤트 schema 변경 또는 sequence 필드 추가
- 모든 가격 틱의 영구 보존과 누락된 중간 틱 복원
- Kafka·Redis를 포함한 end-to-end exactly-once 보장
- GitHub Issue와 PR 생성

## 완료 조건

- DLT 이벤트가 기본 가격 이벤트와 같은 수익률 계산 경로에서 처리된다.
- 같은 가격을 반복 적용해도 평가액이 변하지 않는다.
- Lua 반영 후 호환 key 갱신 전에 중단된 상황을 재처리해도 중복 증분이 없다.
- 한 포트폴리오의 서로 다른 종목을 동시에 갱신해도 각 delta가 보존된다.
- 최신 가격 적용 후 과거 이벤트가 도착해도 상태가 롤백되지 않는다.
- DLT 처리 성공·실패·지연을 기본 가격 이벤트와 구분해 확인할 수 있다.
- `go test ./...`, `go test -race ./...`, `go vet ./...`가 통과한다.

## 위험과 제약

- DLT consumer group 최초 생성 전의 기존 DLT 이벤트는 재생하지 않는다.
- Redis Lua의 숫자 계산은 기존 `HINCRBYFLOAT`과 동일하게 부동소수점 정밀도 제약이 있다.
- 호환용 종목 평가액 key는 Redis Cluster의 slot이 달라 Lua 원자성 경계 밖에서 갱신된다.
- 가격 이벤트와 거래 이벤트가 같은 보유 종목을 동시에 변경하는 순서는 별도 최신성 제어가 필요하다.
- 종목 ID는 metric label로 노출하지 않고 집계 지표를 사용한다.

## 의존성과 적용 조건

- 선행 PR `#1 feat: 사용자 수익률 Redis write-behind 갱신`은 2026-09-05에 `main`으로 병합됐다.
- 구현은 병합 커밋 `73fac13`을 기준으로 시작했다.
- Batch 병렬 작업은 `pf:<portfolioId>`의 `scv:<stockId>`, `cvp`와 가격 적용 시각 계약을 사용한다.
- DLT consumer는 최초 offset이 없을 때 `latest`에서 시작하고 배포 이후 격리 이벤트부터 처리한다.
