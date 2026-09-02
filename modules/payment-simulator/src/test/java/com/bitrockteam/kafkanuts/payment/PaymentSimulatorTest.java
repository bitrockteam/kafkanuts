package com.bitrockteam.kafkanuts.payment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bitrockteam.kafkanuts.transport.LifecycleRole;
import com.bitrockteam.kafkanuts.transport.SimulatorNode;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PaymentSimulatorTest {
  @Test
  void exposesModuleName() {
    assertEquals("payment-simulator", PaymentSimulator.name());
  }

  @Test
  void staysInactiveAndSaysSoWithoutADataPlane() {
    try (SimulatorNode node =
        new SimulatorNode(PaymentSimulator.name(), LifecycleRole.PAYMENT_WORKER, "", "")) {
      Map<String, Object> health = node.health();
      assertEquals("UP", health.get("status"));
      assertEquals("nats", health.get("transport"));
      assertEquals("inactive", health.get("lifecycle"));
      assertEquals("data plane not configured", health.get("reason"));
    }
  }
}
