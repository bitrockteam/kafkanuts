package com.bitrockteam.kafkanuts.fulfillment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bitrockteam.kafkanuts.transport.LifecycleRole;
import com.bitrockteam.kafkanuts.transport.SimulatorNode;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FulfillmentSimulatorTest {
  @Test
  void exposesModuleName() {
    assertEquals("fulfillment-simulator", FulfillmentSimulator.name());
  }

  @Test
  void staysInactiveAndSaysSoWithoutADataPlane() {
    try (SimulatorNode node =
        new SimulatorNode(FulfillmentSimulator.name(), LifecycleRole.FULFILLMENT_WORKER, "", "")) {
      Map<String, Object> health = node.health();
      assertEquals("UP", health.get("status"));
      assertEquals("nats", health.get("transport"));
      assertEquals("inactive", health.get("lifecycle"));
      assertEquals("data plane not configured", health.get("reason"));
    }
  }
}
