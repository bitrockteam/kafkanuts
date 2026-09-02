package com.bitrockteam.kafkanuts.transport;

/** Names of streams, subjects and consumers used by the laboratory. */
public final class JetStreamTopology {
  /** Stream holding every lifecycle event. */
  public static final String EVENT_STREAM = "KAFKANUTS_EVENTS";

  /** Stream holding events that exhausted their delivery attempts. */
  public static final String DLQ_STREAM = "KAFKANUTS_DLQ";

  /** Wildcard covering every lifecycle subject. */
  public static final String EVENT_SUBJECT_WILDCARD = "kafkanuts.events.>";

  /** Wildcard covering every dead letter subject. */
  public static final String DLQ_SUBJECT_WILDCARD = "kafkanuts.dlq.>";

  /** Subject carrying created orders. */
  public static final String ORDER_SUBJECT = "kafkanuts.events.order";

  /** Subject carrying payment outcomes. */
  public static final String PAYMENT_SUBJECT = "kafkanuts.events.payment";

  /** Subject carrying fulfillment outcomes. */
  public static final String FULFILLMENT_SUBJECT = "kafkanuts.events.fulfillment";

  /** Durable consumer feeding the payment simulator. */
  public static final String PAYMENT_CONSUMER = "payment-worker";

  /** Durable consumer feeding the fulfillment simulator. */
  public static final String FULFILLMENT_CONSUMER = "fulfillment-worker";

  /** ccompat subject holding the canonical envelope schema. */
  public static final String REGISTRY_SUBJECT = "kafkanuts.events-value";

  private JetStreamTopology() {}

  /**
   * Returns the dead letter subject matching a lifecycle subject.
   *
   * @param eventSubject lifecycle subject
   * @return dead letter subject
   */
  public static String deadLetterSubject(String eventSubject) {
    return eventSubject.replace("kafkanuts.events.", "kafkanuts.dlq.");
  }
}
