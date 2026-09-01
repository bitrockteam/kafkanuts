package com.bitrockteam.kafkanuts.transport;

/**
 * Trace context propagated with every logical event.
 *
 * @param traceId trace identifier
 * @param spanId span identifier
 */
public record TelemetryContext(String traceId, String spanId) {
  public TelemetryContext {
    if (traceId == null || traceId.isBlank() || spanId == null || spanId.isBlank()) {
      throw new IllegalArgumentException("trace and span identifiers are required");
    }
  }
}
