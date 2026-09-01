package com.bitrockteam.kafkanuts.transport;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Thread-safe event-id guard shared by transport adapters. */
public final class IdempotencyStore {
  /** Processed logical event identifiers. */
  private final Set<String> processed = ConcurrentHashMap.newKeySet();

  public boolean firstSeen(String eventId) {
    return processed.add(eventId);
  }

  public int size() {
    return processed.size();
  }
}
