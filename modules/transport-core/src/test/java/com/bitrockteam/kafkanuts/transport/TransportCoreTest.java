package com.bitrockteam.kafkanuts.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class TransportCoreTest {
  @Test
  void routesKafkaNatsAndDual() {
    EventEnvelope event = event("evt-1");
    TransportAdapter kafka = new InMemoryTransportAdapter("kafka", new IdempotencyStore());
    TransportAdapter nats = new InMemoryTransportAdapter("nats", new IdempotencyStore());
    assertEquals(
        1, new TransportRouter(TransportMode.KAFKA, kafka, nats).destinations(event).size());
    assertEquals(
        1, new TransportRouter(TransportMode.NATS, kafka, nats).destinations(event).size());
    assertEquals(
        2, new TransportRouter(TransportMode.DUAL, kafka, nats).destinations(event).size());
  }

  @Test
  void idempotencyRejectsDuplicateLogicalEvent() {
    IdempotencyStore store = new IdempotencyStore();
    TransportAdapter adapter = new InMemoryTransportAdapter("nats", store);
    assertTrue(adapter.publish(event("evt-1")));
    assertFalse(adapter.publish(event("evt-1")));
    assertEquals(1, store.size());
  }

  @Test
  void dualPublishesOnceToBothAdaptersAndRejectsDuplicate() {
    IdempotencyStore kafkaStore = new IdempotencyStore();
    IdempotencyStore natsStore = new IdempotencyStore();
    TransportRouter router =
        new TransportRouter(
            TransportMode.DUAL,
            new InMemoryTransportAdapter("kafka", kafkaStore),
            new InMemoryTransportAdapter("nats", natsStore));
    EventEnvelope event = event("evt-dual");
    assertTrue(router.publish(event));
    assertFalse(router.publish(event));
    assertEquals(1, kafkaStore.size());
    assertEquals(1, natsStore.size());
  }

  @Test
  void telemetryAndPayloadAreImmutable() {
    byte[] payload = {1, 2};
    EventEnvelope event =
        new EventEnvelope(
            "evt-1",
            "OrderCreated",
            "order-1",
            Instant.EPOCH,
            new TelemetryContext("trace", "span"),
            payload);
    payload[0] = 9;
    assertEquals(1, event.payload()[0]);
    assertEquals("trace", event.telemetryContext().traceId());
    com.bitrockteam.kafkanuts.contracts.EventEnvelope canonical =
        CanonicalEventMapper.toCanonical(event, "order-simulator", 1, "fingerprint");
    assertEquals(event.eventId(), canonical.getEventId().toString());
    assertEquals("trace", canonical.getCorrelationId().toString());
    assertEquals(ByteBuffer.wrap(new byte[] {1, 2}), canonical.getPayload());
  }

  private EventEnvelope event(String id) {
    return new EventEnvelope(
        id,
        "OrderCreated",
        "order-1",
        Instant.EPOCH,
        new TelemetryContext("trace", "span"),
        new byte[0]);
  }
}
