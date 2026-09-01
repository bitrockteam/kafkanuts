package com.bitrockteam.kafkanuts.order;

import com.bitrockteam.kafkanuts.transport.EventEnvelope;
import com.bitrockteam.kafkanuts.transport.IdempotencyStore;
import com.bitrockteam.kafkanuts.transport.InMemoryTransportAdapter;
import com.bitrockteam.kafkanuts.transport.TelemetryContext;
import com.bitrockteam.kafkanuts.transport.TransportMode;
import com.bitrockteam.kafkanuts.transport.TransportRouter;
import java.time.Instant;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Spring Boot order producer with configurable application transport routing. */
@SpringBootApplication
public final class OrderSimulator {
  private OrderSimulator() {}

  public static void main(String[] args) {
    SpringApplication.run(OrderSimulator.class, args);
  }

  public static String name() {
    return "order-simulator";
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
        "OrderCreated",
        "order-" + eventId,
        Instant.now(),
        new TelemetryContext(traceId, eventId),
        new byte[0]);
  }
}
