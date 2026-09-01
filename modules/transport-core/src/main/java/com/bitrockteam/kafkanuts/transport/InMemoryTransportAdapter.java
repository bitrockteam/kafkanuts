package com.bitrockteam.kafkanuts.transport;

/** Deterministic adapter double for T04 smoke tests; no data plane dependency. */
public final class InMemoryTransportAdapter implements TransportAdapter {
  /** Adapter destination label. */
  private final String name;

  /** Logical-event guard. */
  private final IdempotencyStore idempotencyStore;

  public InMemoryTransportAdapter(String name, IdempotencyStore idempotencyStore) {
    this.name = name;
    this.idempotencyStore = idempotencyStore;
  }

  @Override
  public String transportName() {
    return name;
  }

  @Override
  public boolean publish(EventEnvelope event) {
    return idempotencyStore.firstSeen(event.eventId());
  }
}
