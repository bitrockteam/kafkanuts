-- T05 M0 query, versioned with the Kafka baseline.
CREATE STREAM payment_events (
  event_id VARCHAR KEY,
  payment_status VARCHAR
) WITH (
  KAFKA_TOPIC='payments',
  VALUE_FORMAT='AVRO'
);

CREATE TABLE payment_status_summary AS
  SELECT payment_status, COUNT(*) AS payment_count
  FROM payment_events
  GROUP BY payment_status
  EMIT CHANGES;
