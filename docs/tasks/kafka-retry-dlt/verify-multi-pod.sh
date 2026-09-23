#!/usr/bin/env bash
# 워커를 독립 프로세스 두 개로 띄우고, 처리 중 하나를 SIGKILL로 죽여 운영과 같은 갑작스러운 이탈을 만든다.
# 커밋 전에 죽은 파티션은 남은 워커에 재전달되므로, 마커가 없으면 값이 두 번 반영된다.
# 브로커는 Kafka 4.0을 쓴다. 운영과 같은 consumer 그룹 프로토콜(KIP-848)이 기본 지원되기 때문이다.
# Kafka 3.9로 돌리면 컨슈머가 그룹에 붙지 못하고 조용히 아무것도 처리하지 않는다.
# 사용법: docs/tasks/kafka-retry-dlt/verify-multi-pod.sh
set -euo pipefail

NETWORK=verify-multipod
TOPIC=trade.verify-multipod.v1
PORTFOLIOS=3
TRADES_EACH=50
QTY=2
PRICE=100
# 1파동: 포트폴리오마다 1건. 모든 파티션에 커밋된 offset을 만들어 둔다.
# 2파동: 포트폴리오마다 TRADES_EACH건을 한 배치로 처리하는 중간에 워커를 죽인다.
#        적용됐지만 커밋되지 않은 구간이 남아 남은 워커에 재전달된다.
EXPECTED_QTY=$(((TRADES_EACH + 1) * QTY))
EXPECTED_AMOUNT=$((EXPECTED_QTY * PRICE))
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

cleanup() {
  docker rm -f mp-kafka mp-redis-a mp-redis-b mp-redis-c mp-worker-1 mp-worker-2 >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

echo "== 의존성 기동"
docker network create "$NETWORK" >/dev/null
for node in a b c; do
  docker run -d --rm --name "mp-redis-$node" --network "$NETWORK" redis:7.2 \
    redis-server --port 6379 --cluster-enabled yes --cluster-config-file nodes.conf --save '' --appendonly no >/dev/null
done
docker run -d --rm --name mp-kafka --network "$NETWORK" \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://mp-kafka:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@mp-kafka:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e CLUSTER_ID=5L6g3nShT-eMCtK--X86sw apache/kafka:4.0.0 >/dev/null
sleep 5
NODES=$(for node in a b c; do docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}:6379' "mp-redis-$node"; done | paste -sd' ' -)
docker run --rm --network "$NETWORK" redis:7.2 sh -c "redis-cli --cluster create $NODES --cluster-replicas 0 --cluster-yes" >/dev/null
CLUSTER_NODES=$(echo "$NODES" | tr ' ' ',')
until docker run --rm --network "$NETWORK" apache/kafka:4.0.0 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server mp-kafka:9092 --list >/dev/null 2>&1; do sleep 2; done
docker run --rm --network "$NETWORK" apache/kafka:4.0.0 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server mp-kafka:9092 --create --topic "$TOPIC" --partitions 3 --replication-factor 1 >/dev/null

echo "== 워커 이미지 빌드"
docker build -q -t finvibe-profit-worker-verify "$REPO_ROOT" >/dev/null

run_worker() {
  docker run -d --rm --name "$1" --network "$NETWORK" \
    -e KAFKA_BOOTSTRAP_SERVERS=mp-kafka:9092 \
    -e KAFKA_TOPIC_PORTFOLIO_TRADE="$TOPIC" \
    -e KAFKA_TOPIC_STOCK_PRICE_UPDATED=market.verify-unused.v1 \
    -e KAFKA_TOPIC_PORTFOLIO_USER=asset.verify-unused.v1 \
    -e KAFKA_STOCK_PRICE_DLT_ENABLED=false \
    -e KAFKA_MAX_POLL_RECORDS=100 \
    -e REDIS_MODE=cluster \
    -e REDIS_CLUSTER_NODES="$CLUSTER_NODES" \
    finvibe-profit-worker-verify >/dev/null
}

echo "== 워커 2개 기동"
run_worker mp-worker-1
run_worker mp-worker-2

redis_get() { docker run --rm --network "$NETWORK" redis:7.2 redis-cli -c -h "${CLUSTER_NODES%%:*}" "$@"; }

# 워커의 auto.offset.reset은 운영과 같은 latest다. 그래서 컨슈머가 붙은 뒤에 이벤트를 발행해야 한다.
echo "== 워커 준비 대기"
for worker in mp-worker-1 mp-worker-2; do
  for _ in $(seq 1 60); do
    if docker run --rm --network "$NETWORK" curlimages/curl:latest -sf "http://$worker:8080/actuator/health/readiness" >/dev/null 2>&1; then break; fi
    sleep 1
  done
done
sleep 3 # 파티션 할당이 끝날 시간

publish() { # publish <시작 tradeId> <포트폴리오당 건수>
  local trade=$1 each=$2
  {
    for portfolio in $(seq 1 $PORTFOLIOS); do
      for _ in $(seq 1 "$each"); do
        printf '%s:{"tradeId":%d,"userId":"user-%d","type":"BUY","amount":%d,"price":%d,"stockId":10,"portfolioId":%d}\n' \
          "$portfolio" "$trade" "$portfolio" "$QTY" "$PRICE" "$portfolio"
        trade=$((trade + 1))
      done
    done
  } | docker run --rm -i --network "$NETWORK" apache/kafka:4.0.0 \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server mp-kafka:9092 --topic "$TOPIC" \
    --property parse.key=true --property key.separator=:
}

quantity_of() { redis_get get "portfolio:$1:stock:10:quantity" || true; }

echo "== 1파동 발행 (포트폴리오당 1건)"
publish 1 1

echo "== 모든 파티션 커밋 대기"
for _ in $(seq 1 90); do
  started=0
  for portfolio in $(seq 1 $PORTFOLIOS); do
    [ "$(quantity_of "$portfolio")" = "$QTY" ] && started=$((started + 1))
  done
  [ "$started" = "$PORTFOLIOS" ] && break
  sleep 1
done
sleep 2

echo "== 2파동 발행 (포트폴리오당 ${TRADES_EACH}건)"
publish 1000 "$TRADES_EACH"

# 배치는 수백 ms 안에 끝나고 곧바로 커밋되므로, 그대로 죽이면 커밋되지 않은 구간이 남지 않는다.
# Redis를 잠깐 멈춰 워커를 배치 중간에 세운 뒤 죽여야, 적용됐지만 커밋되지 않은 레코드가 재전달된다.
echo "== 2파동 처리 중 Redis 일시 정지 후 worker-1 SIGKILL"
for _ in $(seq 1 300); do
  [ "$(quantity_of 1)" != "$QTY" ] && break
  sleep 0.05
done
docker pause mp-redis-a mp-redis-b mp-redis-c >/dev/null
sleep 0.3
docker kill -s KILL mp-worker-1 >/dev/null
docker unpause mp-redis-a mp-redis-b mp-redis-c >/dev/null

echo "== 전체 반영 대기"
for _ in $(seq 1 120); do
  done_count=0
  for portfolio in $(seq 1 $PORTFOLIOS); do
    value=$(redis_get hget "pf:$portfolio" pv || true)
    [ "$value" = "$EXPECTED_AMOUNT" ] && done_count=$((done_count + 1))
  done
  [ "$done_count" = "$PORTFOLIOS" ] && break
  sleep 1
done
sleep 5 # 재전달된 레코드가 더 반영되지 않는지 확인

echo "== 결과"
status=0
for portfolio in $(seq 1 $PORTFOLIOS); do
  quantity=$(redis_get get "portfolio:$portfolio:stock:10:quantity" || true)
  purchased=$(redis_get hget "pf:$portfolio" pv || true)
  current=$(redis_get hget "pf:$portfolio" cvp || true)
  user_purchased=$(redis_get hget "usr:user-$portfolio" pv || true)
  printf 'portfolio %s: quantity=%s pv=%s cvp=%s user_pv=%s\n' "$portfolio" "$quantity" "$purchased" "$current" "$user_purchased"
  if [ "$quantity" != "$EXPECTED_QTY" ] || [ "$purchased" != "$EXPECTED_AMOUNT" ] || [ "$current" != "$EXPECTED_AMOUNT" ] || [ "$user_purchased" != "$EXPECTED_AMOUNT" ]; then
    echo "  기대값과 다름 (quantity=$EXPECTED_QTY, 금액=$EXPECTED_AMOUNT)"
    status=1
  fi
done
# DLT 토픽이 없으면(발행이 없었으면) 조회가 실패하므로 0으로 본다.
dlt_count=$(docker run --rm --network "$NETWORK" apache/kafka:4.0.0 \
  /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server mp-kafka:9092 --topic "$TOPIC.DLT" 2>/dev/null \
  | awk -F: '{sum += $3} END {print sum + 0}' || echo 0)
echo "DLT 메시지 수: $dlt_count"
[ "$dlt_count" = "0" ] || status=1

[ "$status" = "0" ] && echo "PASS" || echo "FAIL"
exit "$status"
