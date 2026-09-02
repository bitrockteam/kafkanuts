#!/usr/bin/env sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
ROOT_COMPOSE=$ROOT
if command -v cygpath >/dev/null 2>&1; then ROOT_COMPOSE=$(cygpath -w "$ROOT"); fi
NETWORK=${T06_NETWORK_NAME:-kafkanuts-flink-kafka}
REPORT="$ROOT/reports/t06-gate.json"
mkdir -p "$ROOT/reports"
compose() { docker compose --project-directory "$ROOT_COMPOSE" -f "$ROOT_COMPOSE/compose.yaml" --profile kafka --profile t06 "$@"; }
cleanup() { compose down --volumes --remove-orphans >/dev/null 2>&1 || true; }
verify_cleanup() { test -z "$(compose ps -q)"; ! docker network inspect "$NETWORK" >/dev/null 2>&1; }
trap cleanup EXIT HUP INT TERM
compose build flink-kafka-submit
compose up -d kafka schema-registry flink-kafka-jobmanager flink-kafka-taskmanager
compose run --rm kafka-init
compose run --rm flink-kafka-submit
compose run --rm t06-gate pre
watcher=$(compose run -d t06-gate watch)
sleep 3
compose restart flink-kafka-taskmanager >/dev/null
watch_status=$(docker wait "$watcher")
test "$watch_status" -eq 0
compose run --rm t06-gate recovery
cleanup
trap - EXIT HUP INT TERM
verify_cleanup
cat > "$REPORT" <<EOF
{
  "task": "T06",
  "result": "PASS",
  "processing": {"status": "PASS", "evidence": "Flink Kafka job consumed t06-events and emitted t06-parity-output"},
  "checkpoint": {"status": "PASS", "evidence": "state-under-test checkpoint completed before restart and strictly newer checkpoint completed after observed job modification transition"},
  "duplicate_recovery": {"status": "PASS", "evidence": "complete bounded Avro output set contains each eventId exactly once before and after TaskManager restart"},
  "cluster": {"jobmanager": "flink-kafka-jobmanager", "taskmanager": "flink-kafka-taskmanager", "network": "$NETWORK", "checkpoint_volume": "flink-kafka-checkpoints"},
  "cleanup": {"status": "PASS", "evidence": "Compose resources empty and effective T06 network absent"},
  "avro": {"status": "PASS", "evidence": "canonical EventEnvelope via Confluent framing and Schema Registry"},
  "limitations": ["Kafka delivery is at-least-once; deduplication is application state; no Flink/NATS processing claim"]
}
EOF
cat "$REPORT"
