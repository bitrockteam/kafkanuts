package com.bitrockteam.kafkanuts.payment;

import com.bitrockteam.kafkanuts.transport.LifecycleRole;
import com.bitrockteam.kafkanuts.transport.SimulatorNode;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Spring Boot simulator playing the {@code PAYMENT_WORKER} role on JetStream. */
@SpringBootApplication(proxyBeanMethods = false)
@RestController
public final class PaymentSimulator {
  /** Background lifecycle node. */
  @Autowired private SimulatorNode node;

  private PaymentSimulator() {}

  /**
   * Boots the service.
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    SpringApplication.run(PaymentSimulator.class, args);
  }

  /**
   * Creates the lifecycle node from the environment.
   *
   * @param natsUrl NATS server URL
   * @param registryUrl ccompat base URL
   * @return started node
   */
  @Bean(destroyMethod = "close")
  public static SimulatorNode simulatorNode(
      @Value("${NATS_URL:}") String natsUrl, @Value("${REGISTRY_URL:}") String registryUrl) {
    return new SimulatorNode(name(), LifecycleRole.PAYMENT_WORKER, natsUrl, registryUrl);
  }

  /**
   * Reports service health and lifecycle counters.
   *
   * @return health attributes
   */
  @GetMapping("/health")
  public Map<String, Object> health() {
    return node.health();
  }

  /**
   * Returns the module name.
   *
   * @return service name
   */
  public static String name() {
    return "payment-simulator";
  }
}
