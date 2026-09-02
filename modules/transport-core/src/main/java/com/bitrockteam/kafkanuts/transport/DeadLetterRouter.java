package com.bitrockteam.kafkanuts.transport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.MessageInfo;
import io.nats.client.impl.NatsMessage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Routes exhausted messages to the dead letter stream.
 *
 * <p>JetStream non ha una dead letter queue nativa. Il server pubblica un advisory quando un
 * consumer supera {@code MaxDeliver}; questo router lo intercetta, recupera il messaggio originale
 * per sequenza e lo ripubblica sullo stream di dead letter. È un pattern applicativo, non una
 * funzionalità del broker, e come tale va dichiarato.
 */
public final class DeadLetterRouter implements AutoCloseable {
  /** Advisory subject prefix emitted when a consumer exhausts its delivery budget. */
  public static final String MAX_DELIVERIES_ADVISORY =
      "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.";

  /** Management handle used to fetch the original message. */
  private final JetStreamManagement management;

  /** Publish context for the dead letter stream. */
  private final JetStream jetStream;

  /** Advisory subscription dispatcher. */
  private final Dispatcher dispatcher;

  /** JSON mapper for advisory payloads. */
  private final ObjectMapper objectMapper = new ObjectMapper();

  /** Count of messages routed to the dead letter stream. */
  private final AtomicLong routed = new AtomicLong();

  /**
   * Subscribes to the exhaustion advisories of one durable consumer.
   *
   * @param connection connected NATS client
   * @param durable durable consumer to watch
   * @throws IOException when the JetStream contexts cannot be created
   */
  public DeadLetterRouter(Connection connection, String durable) throws IOException {
    this.management = connection.jetStreamManagement();
    this.jetStream = connection.jetStream();
    this.dispatcher = connection.createDispatcher();
    String advisory = MAX_DELIVERIES_ADVISORY + JetStreamTopology.EVENT_STREAM + "." + durable;
    dispatcher.subscribe(advisory, message -> route(message.getData()));
  }

  /**
   * Returns how many messages this router moved to the dead letter stream.
   *
   * @return routed message count
   */
  public long routed() {
    return routed.get();
  }

  private void route(byte[] advisoryPayload) {
    try {
      JsonNode advisory =
          objectMapper.readTree(new String(advisoryPayload, StandardCharsets.UTF_8));
      long sequence = advisory.path("stream_seq").asLong();
      if (sequence <= 0) {
        return;
      }
      MessageInfo original = management.getMessage(JetStreamTopology.EVENT_STREAM, sequence);
      jetStream.publish(
          NatsMessage.builder()
              .subject(JetStreamTopology.deadLetterSubject(original.getSubject()))
              .data(original.getData())
              .build());
      routed.incrementAndGet();
    } catch (IOException | JetStreamApiException cause) {
      throw new IllegalStateException("cannot route message to the dead letter stream", cause);
    }
  }

  @Override
  public void close() {
    dispatcher.unsubscribe(MAX_DELIVERIES_ADVISORY + JetStreamTopology.EVENT_STREAM + ".*");
  }
}
