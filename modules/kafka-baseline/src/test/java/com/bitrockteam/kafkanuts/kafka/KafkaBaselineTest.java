package com.bitrockteam.kafkanuts.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bitrockteam.kafkanuts.contracts.EventEnvelope;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Properties;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.Test;

class KafkaBaselineTest {
  @Test
  void roundTripsCanonicalEnvelopeWithConfluentWireFormat() {
    KafkaAvroCodec codec = new KafkaAvroCodec(new MockSchemaRegistryClient());
    EventEnvelope event = event();
    byte[] encoded = codec.serialize("orders", event);
    EventEnvelope decoded = codec.deserialize("orders", encoded);
    assertEquals(event.getEventId(), decoded.getEventId());
    assertEquals(event.getSchemaFingerprint(), decoded.getSchemaFingerprint());
    assertArrayEquals(event.getPayload().array(), decoded.getPayload().array());
    assertEquals(0, encoded[0]);
  }

  @Test
  void buildsVersionedPaymentStreamsTopologyWithoutStartingDataPlane() {
    Properties properties = new Properties();
    Topology topology = PaymentStreamsTopology.build(properties);
    assertEquals("kafkanuts-payment-streams-v1", properties.getProperty("application.id"));
    assertTrue(topology.describe().toString().contains("payment-counts"));
  }

  @Test
  void shipsVersionedKsqlQuery() throws Exception {
    String query =
        new String(
            getClass().getResourceAsStream("/ksqldb/payment-summary.sql").readAllBytes(),
            java.nio.charset.StandardCharsets.UTF_8);
    assertTrue(query.contains("VALUE_FORMAT='AVRO'"));
    assertTrue(query.contains("payment_status_summary"));
  }

  private EventEnvelope event() {
    return EventEnvelope.newBuilder()
        .setEventId("event-1")
        .setEventType("OrderCreated")
        .setEventVersion(1)
        .setAggregateId("order-1")
        .setOccurredAt(Instant.EPOCH)
        .setProducer("order-simulator")
        .setCorrelationId("trace-1")
        .setCausationId("span-1")
        .setSchemaFingerprint("d5f3919955102019")
        .setPayload(ByteBuffer.wrap(new byte[] {1, 2, 3}))
        .build();
  }
}
