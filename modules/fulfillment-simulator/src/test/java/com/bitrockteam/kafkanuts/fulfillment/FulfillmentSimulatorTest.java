package com.bitrockteam.kafkanuts.fulfillment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class FulfillmentSimulatorTest {
  @Test
  void routesConfiguredApplicationTransports() {
    assertEquals("fulfillment-simulator", FulfillmentSimulator.name());
    assertEquals(
        1,
        FulfillmentSimulator.router("kafka")
            .destinations(FulfillmentSimulator.event("e1", "t1"))
            .size());
    assertEquals(
        1,
        FulfillmentSimulator.router("nats")
            .destinations(FulfillmentSimulator.event("e2", "t2"))
            .size());
    assertEquals(
        2,
        FulfillmentSimulator.router("dual")
            .destinations(FulfillmentSimulator.event("e3", "t3"))
            .size());
  }
}
