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

  /**
   * Reports whether a logical event has already been processed, without recording it.
   *
   * @param eventId logical event identifier
   * @return true when the event was already processed
   */
  public boolean seen(String eventId) {
    return processed.contains(eventId);
  }

  public int size() {
    return processed.size();
  }
}
