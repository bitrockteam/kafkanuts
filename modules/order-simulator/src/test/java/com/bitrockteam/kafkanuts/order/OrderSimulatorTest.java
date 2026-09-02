package com.bitrockteam.kafkanuts.order;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bitrockteam.kafkanuts.transport.LifecycleRole;
import com.bitrockteam.kafkanuts.transport.SimulatorNode;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OrderSimulatorTest {
  @Test
  void exposesModuleName() {
    assertEquals("order-simulator", OrderSimulator.name());
  }

  @Test
  void staysInactiveAndSaysSoWithoutADataPlane() {
    try (SimulatorNode node =
        new SimulatorNode(OrderSimulator.name(), LifecycleRole.ORDER_PRODUCER, "", "")) {
      Map<String, Object> health = node.health();
      assertEquals("UP", health.get("status"));
      assertEquals("nats", health.get("transport"));
      assertEquals("inactive", health.get("lifecycle"));
      assertEquals("data plane not configured", health.get("reason"));
    }
  }
}
