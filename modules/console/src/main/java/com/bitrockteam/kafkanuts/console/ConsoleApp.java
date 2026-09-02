package com.bitrockteam.kafkanuts.console;

import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/** Spring Boot service streaming the live event flow to the dashboard. */
@SpringBootApplication(proxyBeanMethods = false)
@RestController
public final class ConsoleApp {
  /** Timeout of one SSE connection, effectively unlimited for a demo. */
  private static final long STREAM_TIMEOUT_MILLIS = 24L * 60L * 60L * 1000L;

  /** Passive observer of the JetStream flow. */
  @Autowired private EventTail tail;

  private ConsoleApp() {}

  /**
   * Boots the service.
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    SpringApplication.run(ConsoleApp.class, args);
  }

  /**
   * Creates the passive observer from the environment.
   *
   * @param natsUrl NATS server URL
   * @param registryUrl ccompat base URL
   * @return attached observer
   */
  @Bean(destroyMethod = "close")
  public static EventTail eventTail(
      @Value("${NATS_URL:}") String natsUrl, @Value("${REGISTRY_URL:}") String registryUrl) {
    return new EventTail(natsUrl, registryUrl);
  }

  /**
   * Streams observed events, oldest first.
   *
   * @return emitter bound to the HTTP response
   */
  @GetMapping(value = "/api/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public SseEmitter events() {
    SseEmitter emitter = new SseEmitter(STREAM_TIMEOUT_MILLIS);
    tail.register(emitter);
    return emitter;
  }

  /**
   * Reports service health and observation counters.
   *
   * @return health attributes
   */
  @GetMapping({"/health", "/api/health"})
  public Map<String, Object> health() {
    return tail.health();
  }
}
