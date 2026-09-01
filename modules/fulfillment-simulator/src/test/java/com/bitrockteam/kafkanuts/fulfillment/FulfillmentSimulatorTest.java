package com.bitrockteam.kafkanuts.fulfillment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class FulfillmentSimulatorTest {
  @Test
  void exposesModuleName() {
    assertEquals("fulfillment-simulator", FulfillmentSimulator.name());
  }
}
