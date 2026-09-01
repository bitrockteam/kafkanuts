package com.bitrockteam.kafkanuts.transport;

import java.time.Instant;

/**
 * Immutable transport-neutral event metadata.
 *
 * @param eventId stable logical event identifier
 * @param eventType logical event type
 * @param aggregateId aggregate identifier
 * @param occurredAt event timestamp
 * @param telemetryContext propagated trace context
 * @param payload serialized payload
 */
public record EventEnvelope(
    String eventId,
    String eventType,
    String aggregateId,
    Instant occurredAt,
    TelemetryContext telemetryContext,
    byte[] payload) {
  public EventEnvelope {
    if (eventId == null || eventId.isBlank() || eventType == null || eventType.isBlank()) {
      throw new IllegalArgumentException("event identity is required");
    }
    if (aggregateId == null
        || aggregateId.isBlank()
        || occurredAt == null
        || telemetryContext == null) {
      throw new IllegalArgumentException("aggregate and telemetry metadata are required");
    }
    payload = payload == null ? new byte[0] : payload.clone();
  }

  @Override
  public byte[] payload() {
    return payload.clone();
  }
}
