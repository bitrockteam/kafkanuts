package com.bitrockteam.kafkanuts.transport;

import com.bitrockteam.kafkanuts.contracts.EventEnvelope;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PullSubscribeOptions;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Pull consumer that decodes canonical Avro events and applies explicit acknowledgement.
 *
 * <p>Un handler che restituisce {@code false} produce un {@code nak}: il messaggio viene
 * riconsegnato fino a {@code MaxDeliver}, poi il server emette l'advisory che il router di dead
 * letter intercetta.
 */
public final class JetStreamWorker implements AutoCloseable {
  /** Bound pull subscription. */
  private final JetStreamSubscription subscription;

  /** Codec resolving the writer schema from the registry. */
  private final AvroEventCodec codec;

  /** Guard against processing the same logical event twice. */
  private final IdempotencyStore idempotencyStore = new IdempotencyStore();

  /** Count of deliveries received, redeliveries included. */
  private final AtomicLong deliveries = new AtomicLong();

  /** Count of deliveries suppressed because the event was already processed. */
  private final AtomicLong duplicateDeliveries = new AtomicLong();

  /**
   * Binds a worker to a durable consumer.
   *
   * @param connection connected NATS client
   * @param codec codec resolving the writer schema
   * @param durable durable consumer name
   * @throws IOException on transport failure
   * @throws JetStreamApiException when the consumer cannot be bound
   */
  public JetStreamWorker(Connection connection, AvroEventCodec codec, String durable)
      throws IOException, JetStreamApiException {
    this.codec = codec;
    this.subscription =
        connection
            .jetStream()
            .subscribe(null, PullSubscribeOptions.bind(JetStreamTopology.EVENT_STREAM, durable));
  }

  /**
   * Returns how many deliveries the worker received, redeliveries included.
   *
   * @return delivery count
   */
  public long deliveries() {
    return deliveries.get();
  }

  /**
   * Returns how many deliveries carried an already processed logical event.
   *
   * @return duplicate delivery count
   */
  public long duplicateDeliveries() {
    return duplicateDeliveries.get();
  }

  /**
   * Returns how many distinct logical events the worker processed.
   *
   * @return unique logical event count
   */
  public int uniqueEvents() {
    return idempotencyStore.size();
  }

  /**
   * Fetches a batch and applies the handler to every decoded envelope.
   *
   * @param batchSize maximum number of messages to fetch
   * @param wait how long to wait for the batch
   * @param handler returns true to acknowledge, false to negatively acknowledge
   * @return number of messages fetched
   */
  public int poll(int batchSize, Duration wait, Function<EventEnvelope, Boolean> handler) {
    List<Message> messages = subscription.fetch(batchSize, wait);
    for (Message message : messages) {
      deliveries.incrementAndGet();
      EventEnvelope envelope = codec.decode(message.getData());
      if (idempotencyStore.seen(envelope.getEventId())) {
        duplicateDeliveries.incrementAndGet();
        message.ack();
        continue;
      }
      if (Boolean.TRUE.equals(handler.apply(envelope))) {
        idempotencyStore.firstSeen(envelope.getEventId());
        message.ack();
      } else {
        message.nak();
      }
    }
    return messages.size();
  }

  @Override
  public void close() {
    subscription.unsubscribe();
  }
}
