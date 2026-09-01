# T06 — Flink Kafka cluster

T06 adds a dedicated `flink-kafka` cluster using Flink 1.20.2 and Kafka connector 3.3.0-1.20. It is isolated from the existing `flink-nats` profile on `flink-kafka-net`, while JobManager/TaskManager also join `kafka-net` to consume and publish Kafka records. The runtime image carries the connector and the job is compiled for the Java 17 runtime supplied by the pinned Flink image.

## Job and parity

`FlinkKafkaParityJob` consumes `t06-events` and emits one record per event ID to `t06-parity-output`. It uses keyed state for duplicate suppression, checkpointing every two seconds with the named `flink-kafka-checkpoints` volume, and a fixed-delay restart strategy. Kafka delivery is explicitly at-least-once; the gate verifies application-level deduplication rather than claiming exactly-once.

## Docker-only gate

```sh
./scripts/t06-gate.sh
# PowerShell equivalent:
./scripts/t06-gate.ps1
```

The gate builds the pinned runtime image, starts Kafka and the dedicated JobManager/TaskManager, initializes topics, submits the job, verifies processing and a completed checkpoint through the Flink REST API, publishes a duplicate and checks one parity output, restarts the TaskManager, waits for the job/checkpoint recovery, and repeats the duplicate check. It then removes services, networks and checkpoint volume before writing `reports/t06-gate.json`.

The checkpoint init container exists only to grant the Flink user access to the named volume and has explicit resource limits. The report is written only after cleanup succeeds. T10 outcome C remains authoritative for Flink/NATS: this task makes no NATS processing or connector claim.
