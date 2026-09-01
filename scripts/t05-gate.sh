#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
ROOT_COMPOSE=$ROOT
if command -v cygpath >/dev/null 2>&1; then ROOT_COMPOSE=$(cygpath -w "$ROOT"); fi
REPORT="$ROOT/reports/t05-gate.json"
KAFKA_NETWORK_NAME=${T05_KAFKA_NETWORK_NAME:-kafkanuts-kafka}
mkdir -p "$ROOT/reports"
compose() {
  docker compose --project-directory "$ROOT_COMPOSE" -f "$ROOT_COMPOSE/compose.yaml" --profile kafka "$@"
}
run_kafka() {
  MSYS_NO_PATHCONV=1 compose run --rm --no-deps --entrypoint /bin/bash kafka-init -ec "$1"
}
cleanup() {
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
}
verify_cleanup() {
  test -z "$(compose ps -q)"
  ! docker network inspect "$KAFKA_NETWORK_NAME" >/dev/null 2>&1
}
trap cleanup EXIT HUP INT TERM
compose up -d kafka schema-registry ksqldb

i=1
ready=no
while test "$i" -le 60; do
  if run_kafka 'curl -fsS http://ksqldb:8088/info >/dev/null'; then ready=yes; break; fi
  sleep 2
  i=$((i + 1))
done
test "$ready" = yes
compose run --rm kafka-init

run_kafka "printf 'm0-event-1\\n' | kafka-console-producer --bootstrap-server kafka:9092 --topic orders"
m0_output=$(run_kafka "kafka-console-consumer --bootstrap-server kafka:9092 --topic orders --from-beginning --max-messages 1 --timeout-ms 10000")
test "$m0_output" = m0-event-1

run_kafka "curl -fsS -X PUT -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data '{\"compatibility\":\"BACKWARD_TRANSITIVE\"}' http://schema-registry:8081/config/orders-value"
run_kafka 'schema=$(python3 -c "import json,sys; print(json.dumps(dict(schemaType=\"AVRO\",schema=json.dumps(json.load(open(sys.argv[1])),separators=(\",\",\":\")))))" /workspace/modules/event-contracts/src/main/avro/EventEnvelope.avsc); curl -fsS -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "$schema" http://schema-registry:8081/subjects/orders-value/versions'
run_kafka 'schema=$(python3 -c "import json,sys; print(json.dumps(dict(schemaType=\"AVRO\",schema=json.dumps(json.load(open(sys.argv[1])),separators=(\",\",\":\")))))" /workspace/scripts/fixtures/EventEnvelope-compatible.avsc); curl -fsS -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "$schema" http://schema-registry:8081/subjects/orders-value/versions'
run_kafka 'schema=$(python3 -c "import json,sys; print(json.dumps(dict(schemaType=\"AVRO\",schema=json.dumps(json.load(open(sys.argv[1])),separators=(\",\",\":\")))))" /workspace/scripts/fixtures/EventEnvelope-incompatible.avsc); code=$(curl -sS -o /tmp/incompatible.json -w "%{http_code}" -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "$schema" http://schema-registry:8081/subjects/orders-value/versions); test "$code" = 409'
compat=$(run_kafka "curl -fsS http://schema-registry:8081/config/orders-value")
echo "$compat" | grep -q BACKWARD_TRANSITIVE
run_kafka "curl -fsS -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data '{\"schemaType\":\"AVRO\",\"schema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"PaymentEvent\\\",\\\"fields\\\":[{\\\"name\\\":\\\"event_id\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"payment_status\\\",\\\"type\\\":\\\"string\\\"}]}\"}' http://schema-registry:8081/subjects/payments-value/versions"

run_kafka 'python3 /workspace/scripts/ksql-apply.py /workspace/modules/kafka-baseline/src/main/resources/ksqldb/payment-summary.sql'
run_kafka "curl -fsS -X POST -H 'Content-Type: application/vnd.ksql.v1+json' --data \"{\\\"ksql\\\":\\\"DESCRIBE payment_events;\\\",\\\"streamsProperties\\\":{}}\" http://ksqldb:8088/ksql | grep -q PAYMENT_EVENTS"
run_kafka "curl -fsS -X POST -H 'Content-Type: application/vnd.ksql.v1+json' --data \"{\\\"ksql\\\":\\\"DESCRIBE payment_status_summary;\\\",\\\"streamsProperties\\\":{}}\" http://ksqldb:8088/ksql | grep -q PAYMENT_STATUS_SUMMARY"

compose restart kafka >/dev/null
restart_ready=no
i=1
while test "$i" -le 60; do
  if run_kafka 'kafka-broker-api-versions --bootstrap-server kafka:9092 >/dev/null'; then restart_ready=yes; break; fi
  sleep 2
  i=$((i + 1))
done
test "$restart_ready" = yes
run_kafka "printf 'restart-event-1\\n' | kafka-console-producer --bootstrap-server kafka:9092 --topic orders"
restart_output=$(run_kafka "kafka-console-consumer --bootstrap-server kafka:9092 --topic orders --from-beginning --max-messages 2 --timeout-ms 10000")
echo "$restart_output" | grep -q m0-event-1
echo "$restart_output" | grep -q restart-event-1

cleanup
trap - EXIT HUP INT TERM
verify_cleanup
cat > "$REPORT" <<EOF
{
  "task": "T05",
  "result": "PASS",
  "m0": {"status": "PASS", "evidence": "orders produced and consumed through Kafka"},
  "schema_compatibility": {"status": "PASS", "mode": "BACKWARD_TRANSITIVE", "evidence": "Registry exercised modules/event-contracts/src/main/avro/EventEnvelope.avsc plus separate compatible/incompatible fixture artifacts"},
  "ksqldb": {"status": "PASS", "artifact": "modules/kafka-baseline/src/main/resources/ksqldb/payment-summary.sql", "evidence": "versioned production SQL applied and objects described by real ksqlDB"},
  "restart": {"status": "PASS", "evidence": "broker became queryable after restart; retained m0-event-1 and accepted restart-event-1"},
  "cleanup": {"status": "PASS", "network": "$KAFKA_NETWORK_NAME", "evidence": "compose ps empty and effective Kafka network absent after down --volumes"},
  "limitations": ["No Flink/NATS processing claim; Streams logic is verified by TopologyTestDriver"]
}
EOF
cat "$REPORT"
