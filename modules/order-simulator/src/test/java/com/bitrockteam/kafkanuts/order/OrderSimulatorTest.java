package com.bitrockteam.kafkanuts.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class OrderSimulatorTest {
  @Test
  void exposesModuleNameAndAllTransportModes() {
    assertEquals("order-simulator", OrderSimulator.name());
    assertEquals(
        1, OrderSimulator.router("kafka").destinations(OrderSimulator.event("e1", "t1")).size());
    assertEquals(
        1, OrderSimulator.router("nats").destinations(OrderSimulator.event("e2", "t2")).size());
    assertEquals(
        2, OrderSimulator.router("dual").destinations(OrderSimulator.event("e3", "t3")).size());
    assertTrue(OrderSimulator.router("nats").publish(OrderSimulator.event("e4", "t4")));
  }
}
