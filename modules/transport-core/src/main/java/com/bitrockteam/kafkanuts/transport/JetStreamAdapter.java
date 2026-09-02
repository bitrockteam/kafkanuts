package com.bitrockteam.kafkanuts.transport;

import com.bitrockteam.kafkanuts.contracts.EventEnvelope;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publishes canonical Avro events on JetStream with server side deduplication.
 *
 * <p>La deduplica usa l'header {@code Nats-Msg-Id} valutato dal server dentro la finestra {@code
 * duplicate_window} dello stream. Non è una garanzia exactly once end to end: è una finestra di
 * deduplica lato server, e come tale viene dichiarata.
 */
public final class JetStreamAdapter implements TransportAdapter {
  /** Header evaluated by the server for deduplication. */
  public static final String MESSAGE_ID_HEADER = "Nats-Msg-Id";

  /** JetStream publish context. */
  private final JetStream jetStream;

  /** Codec producing framed Avro payloads. */
  private final AvroEventCodec codec;

  /** Subject this adapter publishes to. */
  private final String subject;

  /** Producer name recorded in the canonical envelope. */
  private final String producer;

  /** Count of publishes the server flagged as duplicates. */
  private final AtomicLong duplicateAcks = new AtomicLong();

  /**
   * Binds an adapter to a subject on a live connection.
   *
   * @param connection connected NATS client
   * @param codec codec producing framed Avro payloads
   * @param subject subject to publish on
   * @param producer producer name recorded in the envelope
   * @throws IOException when the JetStream context cannot be created
   */
  public JetStreamAdapter(
      Connection connection, AvroEventCodec codec, String subject, String producer)
      throws IOException {
    this.jetStream = connection.jetStream();
    this.codec = codec;
    this.subject = subject;
    this.producer = producer;
  }

  @Override
  public String transportName() {
    return "nats";
  }

  /**
   * Returns how many publishes the server answered as duplicates.
   *
   * @return duplicate acknowledgement count
   */
  public long duplicateAcks() {
    return duplicateAcks.get();
  }

  @Override
  public boolean publish(com.bitrockteam.kafkanuts.transport.EventEnvelope event) {
    EventEnvelope canonical =
        CanonicalEventMapper.toCanonical(
            event, producer, 1, EventEnvelope.getClassSchema().getFullName());
    Headers headers = new Headers();
    headers.add(MESSAGE_ID_HEADER, event.eventId());
    NatsMessage message =
        NatsMessage.builder()
            .subject(subject)
            .headers(headers)
            .data(codec.encode(canonical))
            .build();
    try {
      PublishAck ack = jetStream.publish(message);
      if (ack.isDuplicate()) {
        duplicateAcks.incrementAndGet();
        return false;
      }
      return true;
    } catch (IOException cause) {
      throw new UncheckedIOException("cannot publish on " + subject, cause);
    } catch (JetStreamApiException cause) {
      throw new IllegalStateException("JetStream rejected publish on " + subject, cause);
    }
  }
}
