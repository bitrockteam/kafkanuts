package com.bitrockteam.kafkanuts.order;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class OrderSimulatorTest {
  @Test
  void exposesModuleName() {
    assertEquals("order-simulator", OrderSimulator.name());
  }
}
