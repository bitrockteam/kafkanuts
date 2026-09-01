# T10 — Flink/NATS gate

## Decision: C

**C — processing Flink/NATS is excluded from the initial commercial displacement.** The spike preserves reproducible packaging and NATS/Flink cluster connectivity evidence, but it does not claim a supported processing path. Decision B is forbidden unless every mandatory runtime gate has status `PASS`.

## Reproduction

```sh
docker compose --profile t10 config
docker compose --profile t10 build t10-spike
docker compose --profile t10 run --rm t10-spike
```

Machine-readable evidence: `modules/flink-nats-spike/src/test/resources/t10-spike-report.json`.

## Gate matrix

| Criterion | Status | Evidence / reason |
|---|---|---|
| Packaging | PASS | Maven module builds in Docker; confined probe only |
| Versions/licences/maintenance | PASS | Flink 1.20.2, NATS 2.11.8, jnats 2.20.5, Avro 1.12.0; Apache-2.0; maintenance bounded to probe |
| Avro/registry | NOT_TESTED | no registry service or wire-compatibility test |
| Event time/watermark | NOT_TESTED | API is constructed but no job is submitted |
| Window/join | NOT_TESTED | no runtime window/join job |
| Checkpoint/recovery | NOT_TESTED | no checkpoint/recovery observed |
| Redelivery/dedup | NOT_TESTED | connectivity probe has no consumer or dedup assertion |
| Parallelism/backpressure | NOT_TESTED | no submitted job or load threshold |
| Budget | PASS | explicit limits 5.25 GiB; idle observation 315.5 MiB |

The live test is `NatsConnectivityProbeTest`: it only connects to NATS and inspects server info. It does not publish, consume, redeliver, or deduplicate messages. The isolated Compose network and healthchecks prove packaging/connectivity, not Flink processing semantics.

## Downstream consequences

T04 must not claim Flink/NATS processing support or depend on a NATS Flink connector. Transport, schema and migration work may continue. Reopening B requires a new scoped spike (or ADR) with a submitted Flink job and PASS evidence for every mandatory runtime criterion, including registry, event-time/window-join, checkpoint/recovery, redelivery/dedup and backpressure.
