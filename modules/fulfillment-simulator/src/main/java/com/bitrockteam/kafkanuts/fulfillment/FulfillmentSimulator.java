package com.bitrockteam.kafkanuts.fulfillment;

import com.bitrockteam.kafkanuts.transport.EventEnvelope;
import com.bitrockteam.kafkanuts.transport.IdempotencyStore;
import com.bitrockteam.kafkanuts.transport.InMemoryTransportAdapter;
import com.bitrockteam.kafkanuts.transport.TelemetryContext;
import com.bitrockteam.kafkanuts.transport.TransportMode;
import com.bitrockteam.kafkanuts.transport.TransportRouter;
import java.time.Instant;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Spring Boot fulfillment producer with configurable application transport routing. */
@SpringBootApplication
public final class FulfillmentSimulator {
  private FulfillmentSimulator() {}

  public static void main(String[] args) {
    SpringApplication.run(FulfillmentSimulator.class, args);
  }

  public static String name() {
    return "fulfillment-simulator";
  }

  public static TransportRouter router(String mode) {
    return new TransportRouter(
        TransportMode.parse(mode),
        new InMemoryTransportAdapter("kafka", new IdempotencyStore()),
        new InMemoryTransportAdapter("nats", new IdempotencyStore()));
  }

  public static EventEnvelope event(String eventId, String traceId) {
    return new EventEnvelope(
        eventId,
        "FulfillmentCompleted",
        "fulfillment-" + eventId,
        Instant.now(),
        new TelemetryContext(traceId, eventId),
        new byte[0]);
  }
}
