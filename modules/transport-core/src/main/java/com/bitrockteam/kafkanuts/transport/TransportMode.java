package com.bitrockteam.kafkanuts.transport;

/** Supported producer routing modes. */
public enum TransportMode {
  /** Kafka application route. */
  KAFKA,
  /** NATS application route. */
  NATS,
  /** Kafka and NATS application routes. */
  DUAL;

  public static TransportMode parse(String value) {
    return value == null ? DUAL : valueOf(value.trim().toUpperCase());
  }
}
