package com.bitrockteam.kafkanuts.transport;

/** Role played by a simulator inside the order lifecycle. */
public enum LifecycleRole {
  /** Provisions the topology and produces orders. */
  ORDER_PRODUCER,
  /** Consumes orders and produces payment outcomes. */
  PAYMENT_WORKER,
  /** Consumes payment outcomes and produces fulfillment outcomes. */
  FULFILLMENT_WORKER
}
