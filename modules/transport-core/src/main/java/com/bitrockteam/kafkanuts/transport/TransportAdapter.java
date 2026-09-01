package com.bitrockteam.kafkanuts.transport;

/** Minimal application-facing transport port; implementations are test doubles in T04. */
public interface TransportAdapter {
  String transportName();

  boolean publish(EventEnvelope event);
}
