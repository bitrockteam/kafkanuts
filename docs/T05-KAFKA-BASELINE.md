# T05 — Kafka baseline

T05 adds a single-node Kafka OSS KRaft baseline, Confluent Schema Registry, versioned Kafka Streams topology and a versioned ksqlDB query. The Kafka profile is isolated on `kafkanuts-kafka`; all services have explicit resource limits and healthchecks.

## Gate

```sh
./scripts/t05-gate.sh
# PowerShell equivalent:
./scripts/t05-gate.ps1
```

The gate starts Kafka, Schema Registry and ksqlDB, runs idempotent topic provisioning, produces/consumes an M0 record, registers an Avro subject under `BACKWARD`, restarts the broker, verifies retained data, emits `reports/t05-gate.json`, and removes services/volumes in a trap/finally cleanup.

`modules/kafka-baseline` tests the canonical T03 envelope with Confluent wire framing using `MockSchemaRegistryClient`, builds the versioned Kafka Streams topology without starting a data plane, and verifies the versioned ksqlDB query. No Flink/NATS connector or processing claim is introduced; T10 outcome C remains authoritative.
