#!/usr/bin/env bash
set -euo pipefail

phase=${1:?phase required}
state=/state/t06-state.json
job_id=''
rest() { curl -fsS "http://flink-kafka-jobmanager:8081$1"; }
job_state() { rest "/jobs/$job_id" | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])'; }
snapshot() { rest "/jobs/$job_id/checkpoints" | python3 -c 'import json,sys; d=json.load(sys.stdin); c=d.get("counts",{}); x=d.get("latest",{}).get("completed") or {}; print(c.get("completed",0), x.get("id",0))'; }
job_last_mod() {
  rest /jobs/overview | python3 -c 'import json,sys; jid=sys.argv[1]; jobs=json.load(sys.stdin).get("jobs",[]); print(next((j.get("last-modification",0) for j in jobs if j["jid"]==jid),0))' "$job_id"
}
wait_running() {
  for _ in $(seq 1 60); do
    job_id=$(rest /jobs/overview | python3 -c 'import json,sys; j=json.load(sys.stdin)["jobs"]; print(j[0]["jid"] if j else "")')
    if test -n "$job_id" && test "$(job_state)" = RUNNING; then return 0; fi
    sleep 2
  done
  return 1
}
wait_checkpoint_after() {
  local old_count=$1 old_id=$2 count id
  for _ in $(seq 1 60); do
    read -r count id <<< "$(snapshot)"
    if test "$count" -gt "$old_count" && test "$id" -gt "$old_id"; then return 0; fi
    sleep 2
  done
  return 1
}
produce() {
  local id=$1 payload=$2
  printf '{"eventId":"%s","eventType":"T06.Test","eventVersion":1,"aggregateId":"%s","occurredAt":1700000000000,"producer":"t06-gate","correlationId":{"string":"t06-correlation"},"causationId":null,"schemaFingerprint":"t03-event-envelope-v1","payload":"%s"}\n' "$id" "$id" "$payload" |
    kafka-avro-console-producer --bootstrap-server kafka:9092 --topic t06-events --property schema.registry.url=http://schema-registry:8081 --property value.schema.file=/workspace/modules/event-contracts/src/main/avro/EventEnvelope.avsc >/dev/null
}
consume_all() {
  kafka-avro-console-consumer --bootstrap-server kafka:9092 --topic t06-parity-output --from-beginning --timeout-ms 8000 --property schema.registry.url=http://schema-registry:8081 2>/dev/null || true
}
assert_exact_ids() {
  local output=$1; shift
  local expected=($*) lines
  lines=$(printf '%s\n' "$output" | grep -E '"eventId"' || true)
  test "$(printf '%s\n' "$lines" | sed '/^$/d' | wc -l)" -eq "${#expected[@]}"
  for id in "${expected[@]}"; do test "$(printf '%s\n' "$lines" | grep -Fc "\"eventId\":\"$id\"")" -eq 1; done
}
wait_running
read -r base_count base_id <<< "$(snapshot)"
if test "$phase" = pre; then
  produce event-1 YWxo
  produce event-duplicate cGF5bG9hZA==
  produce event-duplicate cGF5bG9hZA==
  output=$(consume_all)
  assert_exact_ids "$output" event-1 event-duplicate
  read -r pre_count pre_id <<< "$(snapshot)"
  wait_checkpoint_after "$pre_count" "$pre_id"
  read -r state_count state_id <<< "$(snapshot)"
  last_mod=$(job_last_mod)
  python3 - "$state" "$job_id" "$state_count" "$state_id" "$last_mod" <<'PY'
import json,sys
p,j,c,i,m=sys.argv[1:]
json.dump({'job_id':j,'checkpoint_count':int(c),'checkpoint_id':int(i),'last_modification':int(m),'pre_ids':['event-1','event-duplicate'],'restart_seen':False},open(p,'w'))
PY
  printf '{"phase":"pre","status":"PASS","checkpoint":"state-under-test-completed","checkpoint_id":%s,"duplicate":"deduplicated","output":"complete-bounded-set"}\n' "$state_id"
elif test "$phase" = recovery; then
  test -s "$state"
  saved_job=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["job_id"])' "$state")
  saved_count=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["checkpoint_count"])' "$state")
  saved_id=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["checkpoint_id"])' "$state")
  test "$job_id" = "$saved_job"
  test -s /state/restart-seen
  saved_mod=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["last_modification"])' "$state")
  test "$(job_last_mod)" -gt "$saved_mod"
  produce event-recovery cGF5bG9hZA==
  produce event-recovery cGF5bG9hZA==
  output=$(consume_all)
  assert_exact_ids "$output" event-1 event-duplicate event-recovery
  wait_checkpoint_after "$saved_count" "$saved_id"
  read -r new_count new_id <<< "$(snapshot)"
  python3 - "$state" "$new_count" "$new_id" <<'PY'
import json,sys
p,c,i=sys.argv[1:]; d=json.load(open(p)); d.update(recovery_checkpoint_count=int(c),recovery_checkpoint_id=int(i),restart_seen=True); json.dump(d,open(p,'w'))
PY
  printf '{"phase":"recovery","status":"PASS","checkpoint":"strictly-newer-after-recovery","checkpoint_id":%s,"restart":"transition-observed","duplicate":"deduplicated-after-restart","output":"complete-bounded-set"}\n' "$new_id"
elif test "$phase" = watch; then
  saved_job=$(python3 -c 'import json,sys; print(json.load(open("/state/t06-state.json"))["job_id"])')
  saved_mod=$(python3 -c 'import json,sys; print(json.load(open("/state/t06-state.json"))["last_modification"])')
  job_id=$saved_job
  for _ in $(seq 1 90); do
    if test "$(job_last_mod)" -gt "$saved_mod"; then printf '%s\n' job-modification-advanced > /state/restart-seen; exit 0; fi
    sleep 1
  done
  exit 1
else
  echo "unknown phase: $phase" >&2; exit 2
fi
