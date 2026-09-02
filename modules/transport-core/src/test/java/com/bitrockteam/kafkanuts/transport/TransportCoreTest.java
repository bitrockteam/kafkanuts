package com.bitrockteam.kafkanuts.transport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class TransportCoreTest {
  @Test
  void idempotencyRejectsDuplicateLogicalEvent() {
    IdempotencyStore store = new IdempotencyStore();
    TransportAdapter adapter = new InMemoryTransportAdapter("nats", store);
    assertTrue(adapter.publish(event("evt-1")));
    assertFalse(adapter.publish(event("evt-1")));
    assertEquals(1, store.size());
  }

  @Test
  void seenDoesNotRecordTheEvent() {
    IdempotencyStore store = new IdempotencyStore();
    assertFalse(store.seen("evt-1"));
    assertEquals(0, store.size());
    assertTrue(store.firstSeen("evt-1"));
    assertTrue(store.seen("evt-1"));
  }

  @Test
  void wireFormatRoundTripsSchemaIdAndBody() {
    byte[] body = {7, 8, 9};
    byte[] framed = ConfluentWireFormat.encode(42, body);
    assertEquals(ConfluentWireFormat.HEADER_SIZE + body.length, framed.length);
    assertEquals(ConfluentWireFormat.MAGIC_BYTE, framed[0]);
    assertEquals(42, ConfluentWireFormat.schemaId(framed));
    assertArrayEquals(body, ConfluentWireFormat.body(framed));
  }

  @Test
  void wireFormatRejectsForeignPayloads() {
    assertThrows(
        IllegalArgumentException.class, () -> ConfluentWireFormat.schemaId(new byte[] {1}));
    byte[] wrongMagic = {9, 0, 0, 0, 1, 5};
    assertThrows(IllegalArgumentException.class, () -> ConfluentWireFormat.schemaId(wrongMagic));
  }

  @Test
  void deadLetterSubjectMirrorsTheEventSubject() {
    assertEquals(
        "kafkanuts.dlq.order",
        JetStreamTopology.deadLetterSubject(JetStreamTopology.ORDER_SUBJECT));
  }

  @Test
  void failureSelectionIsDeterministic() {
    String aggregate = "order-abc";
    assertEquals(LifecycleSupport.alwaysFails(aggregate), LifecycleSupport.alwaysFails(aggregate));
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
