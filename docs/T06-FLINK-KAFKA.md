# T06 — Flink Kafka cluster

T06 adds a dedicated `flink-kafka` cluster using Flink 1.20.2 and Kafka connector 3.3.0-1.20. It is isolated from the existing `flink-nats` profile on `flink-kafka-net`, while JobManager/TaskManager also join `kafka-net` to consume and publish Kafka records. The runtime image carries the connector and the job is compiled for the Java 17 runtime supplied by the pinned Flink image.

## Job and parity

`FlinkKafkaParityJob` consumes and emits the canonical T03 `EventEnvelope` Avro record using Confluent wire framing and the existing Schema Registry (`schema-registry:8081`). It keys state by stable `eventId`, writes one parity envelope per event ID, checkpoints every two seconds to the named `flink-kafka-checkpoints` volume, and uses a fixed-delay restart strategy. Kafka delivery is explicitly at-least-once; the gate verifies application-level deduplication rather than claiming exactly-once.

## Docker-only gate

```sh
./scripts/t06-gate.sh
# PowerShell equivalent:
./scripts/t06-gate.ps1
```

The gate builds the pinned runtime image, starts Kafka, Schema Registry and the dedicated JobManager/TaskManager, initializes topics, submits the job, verifies processing and a completed checkpoint through the Flink REST API, publishes canonical Avro duplicates and checks the complete bounded output set by `eventId`. It records a checkpoint after the state-under-test is processed, starts a restart probe, restarts the TaskManager, requires a strictly advanced job modification/recovery transition and newer completed checkpoint, then repeats the complete duplicate check. It then removes services, networks, checkpoint and gate-state volumes before writing `reports/t06-gate.json`.

The checkpoint init container exists only to grant the Flink user access to the named volume and has explicit resource limits. TaskManager readiness is guarded by a process healthcheck and consumed through `service_healthy` dependencies. Both POSIX and PowerShell gates use the same Compose scenario; PowerShell suppresses the expected negative `docker network inspect` probe safely under Windows PowerShell 5.1. The report is written only after cleanup succeeds. T10 outcome C remains authoritative for Flink/NATS: this task makes no NATS processing or connector claim.
