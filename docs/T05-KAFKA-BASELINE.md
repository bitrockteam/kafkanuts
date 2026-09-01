# T05 — Kafka baseline

T05 provides a single-node Kafka OSS KRaft baseline, Confluent Schema Registry, a versioned Kafka Streams topology and a versioned ksqlDB query. The `kafka` profile is isolated on `kafkanuts-kafka`; runtime limits are documented in [`RESOURCE-BUDGET.md`](RESOURCE-BUDGET.md).

## Gate

```sh
./scripts/t05-gate.sh
# PowerShell equivalent:
./scripts/t05-gate.ps1
```

Both gates use Docker Compose only. They start Kafka, Schema Registry and ksqlDB, wait for real readiness, create topics idempotently, produce/consume an M0 event, configure `BACKWARD_TRANSITIVE`, register `modules/event-contracts/src/main/avro/EventEnvelope.avsc`, then register the separate `scripts/fixtures/EventEnvelope-compatible.avsc` and `scripts/fixtures/EventEnvelope-incompatible.avsc` artifacts. The incompatible fixture must receive HTTP 409. The gates use `scripts/ksql-apply.py` to apply the actual `modules/kafka-baseline/src/main/resources/ksqldb/payment-summary.sql` file to the real server, then verify both objects with `DESCRIBE`.

The broker is restarted and the gate waits until the broker API is usable again, verifies the retained M0 event, publishes a new event and consumes both. Cleanup runs before the report is written; the report is marked PASS only when `compose ps -q` is empty and the effective network selected by `T05_KAFKA_NETWORK_NAME` (default `kafkanuts-kafka`) is absent. The output is `reports/t05-gate.json` (ignored runtime evidence, regenerated on every run). The report names the exact `EventEnvelope.avsc`, separate evolution fixtures and `payment-summary.sql` artifacts exercised.

## Java verification

`modules/kafka-baseline` tests the canonical T03 envelope with Confluent wire framing using `MockSchemaRegistryClient`. `KafkaBaselineTest` also runs `PaymentStreamsTopology` in `TopologyTestDriver`, pipes two records with the same key and asserts output counts `1` and `2`. The versioned ksqlDB query is kept under `src/main/resources/ksqldb/payment-summary.sql` and is executed by the Compose gates.

No Flink/NATS connector or processing claim is introduced; T10 outcome C remains authoritative.
