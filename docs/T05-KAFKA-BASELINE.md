# T05 — Kafka baseline

T05 provides a single-node Kafka OSS KRaft baseline, Confluent Schema Registry, a versioned Kafka Streams topology and a versioned ksqlDB query. The `kafka` profile is isolated on `kafkanuts-kafka`; runtime limits are documented in [`RESOURCE-BUDGET.md`](RESOURCE-BUDGET.md).

## Gate

```sh
./scripts/t05-gate.sh
# PowerShell equivalent:
./scripts/t05-gate.ps1
```

Both gates use Docker Compose only. They start Kafka, Schema Registry and ksqlDB, wait for real readiness, create topics idempotently, produce/consume an M0 event, configure `BACKWARD_TRANSITIVE`, register the canonical envelope v1 and a compatible v2 fixture, and assert that an incompatible v3 receives HTTP 409. They then register the payment fixture, apply both statements from the versioned ksqlDB query to the real server, and verify both objects with `DESCRIBE`.

The broker is restarted and the gate waits until the broker API is usable again, verifies the retained M0 event, publishes a new event and consumes both. Cleanup runs before the report is written; the report is marked PASS only when `compose ps -q` is empty and the named Kafka network is absent. The output is `reports/t05-gate.json` (ignored runtime evidence, regenerated on every run).

## Java verification

`modules/kafka-baseline` tests the canonical T03 envelope with Confluent wire framing using `MockSchemaRegistryClient`. `KafkaBaselineTest` also runs `PaymentStreamsTopology` in `TopologyTestDriver`, pipes two records with the same key and asserts output counts `1` and `2`. The versioned ksqlDB query is kept under `src/main/resources/ksqldb/payment-summary.sql` and is executed by the Compose gates.

No Flink/NATS connector or processing claim is introduced; T10 outcome C remains authoritative.
