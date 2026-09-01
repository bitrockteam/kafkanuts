package com.bitrockteam.kafkanuts.fulfillment;

import com.bitrockteam.kafkanuts.transport.EventEnvelope;
import com.bitrockteam.kafkanuts.transport.IdempotencyStore;
import com.bitrockteam.kafkanuts.transport.InMemoryTransportAdapter;
import com.bitrockteam.kafkanuts.transport.TelemetryContext;
import com.bitrockteam.kafkanuts.transport.TransportMode;
import com.bitrockteam.kafkanuts.transport.TransportRouter;
import java.time.Instant;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Spring Boot fulfillment producer with configurable application transport routing. */
@SpringBootApplication(proxyBeanMethods = false)
@RestController
public final class FulfillmentSimulator {
  /** Singleton application router. */
  @Autowired private TransportRouter configuredRouter;

  private FulfillmentSimulator() {}

  public static void main(String[] args) {
    SpringApplication.run(FulfillmentSimulator.class, args);
  }

  @Bean
  public static TransportRouter transportRouter(@Value("${transport.mode:dual}") String mode) {
    return router(mode);
  }

  @GetMapping("/health")
  public Map<String, String> health() {
    return Map.of(
        "status",
        "UP",
        "service",
        name(),
        "transportMode",
        configuredRouter.mode().name().toLowerCase());
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
