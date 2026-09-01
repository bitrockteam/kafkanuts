#!/usr/bin/env bash
set -euo pipefail

phase=${1:?phase required}
job_id=''
wait_running() {
  for _ in $(seq 1 60); do
    job_id=$(curl -fsS http://flink-kafka-jobmanager:8081/jobs/overview | python3 -c 'import json,sys; jobs=json.load(sys.stdin)["jobs"]; print(jobs[0]["jid"] if jobs else "")')
    if test -n "$job_id" && test "$(curl -fsS "http://flink-kafka-jobmanager:8081/jobs/$job_id" | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])')" = RUNNING; then return 0; fi
    sleep 2
  done
  return 1
}
wait_checkpoint() {
  for _ in $(seq 1 60); do
    completed=$(curl -fsS "http://flink-kafka-jobmanager:8081/jobs/$job_id/checkpoints" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("counts",{}).get("completed",0))')
    test "$completed" -gt 0 && return 0
    sleep 2
  done
  return 1
}
produce() {
  printf '%s\n' "$1" | kafka-console-producer --bootstrap-server kafka:9092 --topic t06-events >/dev/null
}
consume() {
  kafka-console-consumer --bootstrap-server kafka:9092 --topic t06-parity-output --from-beginning --max-messages "$1" --timeout-ms 15000
}
wait_running
wait_checkpoint
if test "$phase" = pre; then
  produce 'event-1|alpha'
  output=$(consume 1)
  printf '%s\n' "$output" | grep -Fqx 'event-1|alpha'
  produce 'event-duplicate|payload'
  produce 'event-duplicate|payload'
  output=$(consume 2)
  count=$(printf '%s\n' "$output" | grep -Fc 'event-duplicate|payload' || true)
  test "$count" -eq 1
  printf '{"phase":"pre","status":"PASS","checkpoint":"completed","duplicate":"deduplicated"}\n'
elif test "$phase" = recovery; then
  produce 'event-recovery|payload'
  produce 'event-recovery|payload'
  output=$(consume 3)
  count=$(printf '%s\n' "$output" | grep -Fc 'event-recovery|payload' || true)
  test "$count" -eq 1
  printf '{"phase":"recovery","status":"PASS","checkpoint":"completed","duplicate":"deduplicated-after-restart"}\n'
else
  echo "unknown phase: $phase" >&2
  exit 2
fi
