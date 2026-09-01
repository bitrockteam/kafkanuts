package com.bitrockteam.kafkanuts.transport;

import java.util.List;

/** Routes one application event according to the configured mode. */
public final class TransportRouter {
  /** Configured routing mode. */
  private final TransportMode mode;

  /** Kafka application adapter. */
  private final TransportAdapter kafka;

  /** NATS application adapter. */
  private final TransportAdapter nats;

  public TransportRouter(TransportMode mode, TransportAdapter kafka, TransportAdapter nats) {
    this.mode = mode;
    this.kafka = kafka;
    this.nats = nats;
  }

  public TransportMode mode() {
    return mode;
  }

  public List<String> destinations(EventEnvelope event) {
    return switch (mode) {
      case KAFKA -> List.of(kafka.transportName());
      case NATS -> List.of(nats.transportName());
      case DUAL -> List.of(kafka.transportName(), nats.transportName());
    };
  }

  public boolean publish(EventEnvelope event) {
    return switch (mode) {
      case KAFKA -> kafka.publish(event);
      case NATS -> nats.publish(event);
      case DUAL -> {
        boolean kafkaPublished = kafka.publish(event);
        boolean natsPublished = nats.publish(event);
        yield kafkaPublished && natsPublished;
      }
    };
  }
}
