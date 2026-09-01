package com.bitrockteam.kafkanuts.transport;

import com.bitrockteam.kafkanuts.contracts.EventEnvelope;
import java.nio.ByteBuffer;
import java.time.Instant;

/** Explicit boundary mapper from application metadata to the canonical T03 Avro envelope. */
public final class CanonicalEventMapper {
  private CanonicalEventMapper() {}

  public static EventEnvelope toCanonical(
      com.bitrockteam.kafkanuts.transport.EventEnvelope event,
      String producer,
      int version,
      String fingerprint) {
    return EventEnvelope.newBuilder()
        .setEventId(event.eventId())
        .setEventType(event.eventType())
        .setEventVersion(version)
        .setAggregateId(event.aggregateId())
        .setOccurredAt(Instant.ofEpochMilli(event.occurredAt().toEpochMilli()))
        .setProducer(producer)
        .setCorrelationId(event.telemetryContext().traceId())
        .setCausationId(event.telemetryContext().spanId())
        .setSchemaFingerprint(fingerprint)
        .setPayload(ByteBuffer.wrap(event.payload()))
        .build();
  }
}
