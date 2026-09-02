package com.bitrockteam.kafkanuts.transport;

import com.bitrockteam.kafkanuts.contracts.EventEnvelope;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Runs one simulator role against a live JetStream stack.
 *
 * <p>Ogni ruolo crea la topologia, in modo idempotente. I due worker consumano con ack esplicito;
 * il worker dei pagamenti fallisce deterministicamente una quota di ordini per esercitare
 * redelivery, backoff e instradamento in dead letter.
 */
public final class LifecycleRunner implements AutoCloseable, Runnable {
  /** Role played by this runner. */
  private final LifecycleRole role;

  /** Live NATS connection. */
  private final Connection connection;

  /** Codec bound to the canonical envelope subject. */
  private final AvroEventCodec codec;

  /** Adapter used to publish this role's output. */
  private final JetStreamAdapter publisher;

  /** Worker consuming this role's input, absent for the producer. */
  private final JetStreamWorker worker;

  /** Dead letter router, present only for the payment worker. */
  private final DeadLetterRouter deadLetterRouter;

  /** Count of events accepted by JetStream. */
  private final AtomicLong published = new AtomicLong();

  /** Stop flag honoured by the run loop. */
  private final AtomicBoolean running = new AtomicBoolean(true);

  /**
   * Wires a runner for one role.
   *
   * @param role role to play
   * @param natsUrl NATS server URL
   * @param registryUrl ccompat base URL
   * @throws IOException on transport failure
   * @throws InterruptedException when the connection wait is interrupted
   * @throws JetStreamApiException when provisioning or binding is rejected
   */
  public LifecycleRunner(LifecycleRole role, String natsUrl, String registryUrl)
      throws IOException, InterruptedException, JetStreamApiException {
    this.role = role;
    this.connection = LifecycleSupport.connect(natsUrl);
    this.codec = LifecycleSupport.codec(registryUrl);
    // Provisiona ogni ruolo, non solo il produttore: l'operazione e' idempotente e cosi' un
    // worker che parte per primo non dipende dall'ordine di avvio degli altri container.
    LifecycleSupport.provision(connection);
    this.publisher =
        new JetStreamAdapter(connection, codec, outputSubject(role), producerName(role));
    this.worker =
        role == LifecycleRole.ORDER_PRODUCER
            ? null
            : new JetStreamWorker(connection, codec, inputConsumer(role));
    this.deadLetterRouter =
        role == LifecycleRole.PAYMENT_WORKER
            ? new DeadLetterRouter(connection, JetStreamTopology.PAYMENT_CONSUMER)
            : null;
  }

  /**
   * Returns the current counters of this runner.
   *
   * @return immutable snapshot
   */
  public LifecycleStats stats() {
    return new LifecycleStats(
        published.get(),
        publisher.duplicateAcks(),
        worker == null ? 0 : worker.deliveries(),
        worker == null ? 0 : worker.duplicateDeliveries(),
        worker == null ? 0 : worker.uniqueEvents(),
        deadLetterRouter == null ? 0 : deadLetterRouter.routed());
  }

  /** Runs until {@link #close()} is called. */
  @Override
  public void run() {
    while (running.get()) {
      try {
        if (role == LifecycleRole.ORDER_PRODUCER) {
          produceOrder();
          Thread.sleep(Duration.ofSeconds(2).toMillis());
        } else {
          worker.poll(10, Duration.ofSeconds(2), this::handle);
        }
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        return;
      } catch (RuntimeException failure) {
        return;
      }
    }
  }

  private void produceOrder() {
    String eventId = UUID.randomUUID().toString();
    com.bitrockteam.kafkanuts.transport.EventEnvelope event =
        new com.bitrockteam.kafkanuts.transport.EventEnvelope(
            eventId,
            "OrderCreated",
            "order-" + eventId,
            Instant.now(),
            new TelemetryContext(eventId, eventId),
            ("{\"orderId\":\"" + eventId + "\"}").getBytes(StandardCharsets.UTF_8));
    if (publisher.publish(event)) {
      published.incrementAndGet();
    }
  }

  private boolean handle(EventEnvelope incoming) {
    String aggregateId = incoming.getAggregateId().toString();
    if (role == LifecycleRole.PAYMENT_WORKER && LifecycleSupport.alwaysFails(aggregateId)) {
      return false;
    }
    String eventId = UUID.randomUUID().toString();
    com.bitrockteam.kafkanuts.transport.EventEnvelope outgoing =
        new com.bitrockteam.kafkanuts.transport.EventEnvelope(
            eventId,
            role == LifecycleRole.PAYMENT_WORKER ? "PaymentAuthorized" : "FulfillmentCompleted",
            aggregateId,
            Instant.now(),
            new TelemetryContext(
                incoming.getCorrelationId().toString(), incoming.getEventId().toString()),
            incoming.getPayload().array());
    if (publisher.publish(outgoing)) {
      published.incrementAndGet();
    }
    return true;
  }

  private static String outputSubject(LifecycleRole role) {
    return switch (role) {
      case ORDER_PRODUCER -> JetStreamTopology.ORDER_SUBJECT;
      case PAYMENT_WORKER -> JetStreamTopology.PAYMENT_SUBJECT;
      case FULFILLMENT_WORKER -> JetStreamTopology.FULFILLMENT_SUBJECT;
    };
  }

  private static String inputConsumer(LifecycleRole role) {
    return role == LifecycleRole.PAYMENT_WORKER
        ? JetStreamTopology.PAYMENT_CONSUMER
        : JetStreamTopology.FULFILLMENT_CONSUMER;
  }

  private static String producerName(LifecycleRole role) {
    return switch (role) {
      case ORDER_PRODUCER -> "order-simulator";
      case PAYMENT_WORKER -> "payment-simulator";
      case FULFILLMENT_WORKER -> "fulfillment-simulator";
    };
  }

  @Override
  public void close() {
    running.set(false);
    if (worker != null) {
      worker.close();
    }
    if (deadLetterRouter != null) {
      deadLetterRouter.close();
    }
    try {
      connection.close();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }
}
