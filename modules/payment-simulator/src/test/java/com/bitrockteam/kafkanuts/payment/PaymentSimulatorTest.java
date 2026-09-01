package com.bitrockteam.kafkanuts.payment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class PaymentSimulatorTest {
  @Test
  void exposesModuleName() {
    assertEquals("payment-simulator", PaymentSimulator.name());
  }
}
