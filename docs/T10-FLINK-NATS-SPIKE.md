# T10 — Flink/NATS gate

## Decision

**B — DataStream API only.** The spike proves the confined JVM/DataStream surface and a dedicated `flink-nats` Compose cluster. It does not claim a Flink SQL/Table Factory and does not add a general-purpose connector.

## Reproduction

```sh
docker compose --profile t10 config
docker compose --profile t10 build t10-spike
docker compose --profile t10 run --rm t10-spike
```

The report is `modules/flink-nats-spike/src/test/resources/t10-spike-report.json` and is intentionally machine-readable.

## Evidence matrix

| Area | Probe/evidence | Boundary |
|---|---|---|
| Packaging/version/licence | Maven module, Flink 1.20.2, jnats 2.20.5, NATS 2.11.8, pinned Maven base image | no general connector artifact |
| Avro/registry | shared `event-contracts` dependency and fingerprint identity | remote registry compatibility is follow-up |
| Event time/watermark | `WatermarkStrategy` in DataStream probe | monotonous fixture only |
| Window/join | DataStream API is the selected integration surface | representative live scenario is follow-up |
| Checkpoint/recovery | checkpointing configured at 1 second | NATS ack/checkpoint coordination is not exactly-once |
| Redelivery/duplicates | contract requires `eventId` deduplication and at-least-once semantics | live failure scenario is follow-up |
| Parallelism/backpressure | one TaskManager/one slot baseline and bounded resource limits | load threshold is follow-up |
| Budget | JobManager 2 GiB, TaskManager 2 GiB, NATS 256 MiB, spike 1 GiB; configured maximum 5.25 GiB | reduced-profile guardrail |

The cluster is isolated on `kafkanuts-flink-nats`; its services have healthchecks and explicit CPU/memory limits. Any future adapter must remain confined to this spike unless a new architectural decision approves a general connector.
