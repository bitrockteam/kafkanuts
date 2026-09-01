package com.bitrockteam.kafkanuts.payment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class PaymentSimulatorTest {
  @Test
  void routesConfiguredApplicationTransports() {
    assertEquals("payment-simulator", PaymentSimulator.name());
    assertEquals(
        1,
        PaymentSimulator.router("kafka").destinations(PaymentSimulator.event("e1", "t1")).size());
    assertEquals(
        1, PaymentSimulator.router("nats").destinations(PaymentSimulator.event("e2", "t2")).size());
    assertEquals(
        2, PaymentSimulator.router("dual").destinations(PaymentSimulator.event("e3", "t3")).size());
  }
}
