package com.bitrockteam.kafkanuts.transport;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Owns the background thread of one simulator role.
 *
 * <p>Se il data plane non è configurato il nodo resta inattivo e lo dichiara: nessun test unitario
 * richiede un broker in piedi.
 */
public final class SimulatorNode implements AutoCloseable {
  /** Service name reported by the health endpoint. */
  private final String service;

  /** Active runner, null when the data plane is not configured. */
  private final LifecycleRunner runner;

  /** Reason why the node is inactive, null when running. */
  private final String inactiveReason;

  /**
   * Starts a node, or records why it could not start.
   *
   * @param service service name
   * @param role role to play
   * @param natsUrl NATS server URL, may be blank
   * @param registryUrl ccompat base URL, may be blank
   */
  public SimulatorNode(String service, LifecycleRole role, String natsUrl, String registryUrl) {
    this.service = service;
    if (natsUrl == null || natsUrl.isBlank() || registryUrl == null || registryUrl.isBlank()) {
      this.runner = null;
      this.inactiveReason = "data plane not configured";
      return;
    }
    LifecycleRunner started = null;
    String failure = null;
    try {
      started = new LifecycleRunner(role, natsUrl, registryUrl);
      Thread thread = new Thread(started, service + "-lifecycle");
      thread.setDaemon(true);
      thread.start();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      failure = "interrupted while connecting";
    } catch (Exception cause) {
      failure = describe(cause);
      cause.printStackTrace();
    }
    this.runner = started;
    this.inactiveReason = failure;
  }

  /**
   * Builds the payload of the health endpoint.
   *
   * @return ordered health attributes
   */
  public Map<String, Object> health() {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("status", "UP");
    payload.put("service", service);
    payload.put("transport", "nats");
    if (runner == null) {
      payload.put("lifecycle", "inactive");
      payload.put("reason", inactiveReason);
      return payload;
    }
    LifecycleStats stats = runner.stats();
    payload.put("lifecycle", "active");
    payload.put("published", stats.published());
    payload.put("duplicateAcks", stats.duplicateAcks());
    payload.put("deliveries", stats.deliveries());
    payload.put("duplicateDeliveries", stats.duplicateDeliveries());
    payload.put("uniqueEvents", stats.uniqueEvents());
    payload.put("deadLettered", stats.deadLettered());
    return payload;
  }

  private static String describe(Throwable cause) {
    StringBuilder text = new StringBuilder(cause.getClass().getSimpleName());
    if (cause.getMessage() != null) {
      text.append(": ").append(cause.getMessage());
    }
    Throwable root = cause.getCause();
    if (root != null) {
      text.append(" <- ").append(root.getClass().getSimpleName());
      if (root.getMessage() != null) {
        text.append(": ").append(root.getMessage());
      }
    }
    return text.toString();
  }

  @Override
  public void close() {
    if (runner != null) {
      runner.close();
    }
  }
}
