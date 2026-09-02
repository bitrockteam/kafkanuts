package com.bitrockteam.kafkanuts.features;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.bitrockteam.kafkanuts.contracts.EventEnvelope;
import com.bitrockteam.kafkanuts.transport.ApicurioSchemaRegistry;
import com.bitrockteam.kafkanuts.transport.AvroEventCodec;
import com.bitrockteam.kafkanuts.transport.CanonicalEventMapper;
import com.bitrockteam.kafkanuts.transport.DeadLetterRouter;
import com.bitrockteam.kafkanuts.transport.JetStreamAdapter;
import com.bitrockteam.kafkanuts.transport.JetStreamTopology;
import com.bitrockteam.kafkanuts.transport.LifecycleSupport;
import com.bitrockteam.kafkanuts.transport.TelemetryContext;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Functional suite executed against the running stack.
 *
 * <p>I test girano soltanto quando la variabile T18_LIVE vale true e gli endpoint sono configurati.
 * Senza data plane la suite viene saltata invece di dichiarare un falso PASS.
 */
class JetStreamFeatureTest {
  /** Subject used only by this suite, isolated from the lifecycle subjects. */
  private static final String SUITE_SUBJECT = "kafkanuts.events.suite";

  /** Live connection shared by the suite. */
  private static Connection connection;

  /** Codec bound to the canonical envelope subject. */
  private static AvroEventCodec codec;

  /** Registry client used for compatibility checks. */
  private static ApicurioSchemaRegistry registry;

  @BeforeAll
  static void connectToLiveStack() throws Exception {
    assumeTrue("true".equals(System.getenv("T18_LIVE")), "live stack not requested");
    String natsUrl = System.getenv("NATS_URL");
    String registryUrl = System.getenv("REGISTRY_URL");
    assumeTrue(natsUrl != null && registryUrl != null, "data plane endpoints not configured");
    connection = LifecycleSupport.connect(natsUrl);
    registry = new ApicurioSchemaRegistry(registryUrl);
    codec = new AvroEventCodec(JetStreamTopology.REGISTRY_SUBJECT, registry);
    LifecycleSupport.provision(connection);
  }

  @AfterAll
  static void disconnect() throws Exception {
    if (connection != null) {
      connection.close();
    }
  }

  @Test
  void streamsAreProvisionedIdempotently() throws Exception {
    JetStreamManagement management = connection.jetStreamManagement();
    LifecycleSupport.provision(connection);
    StreamInfo events = management.getStreamInfo(JetStreamTopology.EVENT_STREAM);
    StreamInfo dlq = management.getStreamInfo(JetStreamTopology.DLQ_STREAM);
    assertNotNull(events);
    assertNotNull(dlq);
    assertTrue(management.getStreamNames().contains(JetStreamTopology.EVENT_STREAM));
    assertTrue(management.getStreamNames().contains(JetStreamTopology.DLQ_STREAM));
  }

  @Test
  void avroRoundTripsThroughApicurioCcompat() {
    EventEnvelope canonical = canonical("round-trip");
    byte[] framed = codec.encode(canonical);
    EventEnvelope decoded = codec.decode(framed);
    assertEquals(canonical.getEventId().toString(), decoded.getEventId().toString());
    assertEquals(canonical.getAggregateId().toString(), decoded.getAggregateId().toString());
    assertTrue(codec.writerSchemaId() > 0);
  }

  @Test
  void registryAcceptsCompatibleAndRejectsIncompatibleSchemas() {
    assertEquals(
        AvroEventCodec.DEFAULT_COMPATIBILITY,
        registry.compatibilityLevel(JetStreamTopology.REGISTRY_SUBJECT),
        "il codec deve imporre esplicitamente un livello: Apicurio parte da NONE");
    String canonicalSchema = EventEnvelope.getClassSchema().toString();
    assertTrue(registry.compatible(JetStreamTopology.REGISTRY_SUBJECT, canonicalSchema));
    assertFalse(registry.compatible(JetStreamTopology.REGISTRY_SUBJECT, incompatibleSchema()));
  }

  @Test
  void serverDeduplicatesRepeatedMessageId() throws Exception {
    JetStream jetStream = connection.jetStream();
    String messageId = UUID.randomUUID().toString();
    PublishAck first = jetStream.publish(suiteMessage(messageId, "dedup"));
    PublishAck second = jetStream.publish(suiteMessage(messageId, "dedup"));
    assertFalse(first.isDuplicate());
    assertTrue(second.isDuplicate());
    assertEquals(first.getSeqno(), second.getSeqno());
  }

  @Test
  void exhaustedDeliveriesReachTheDeadLetterStream() throws Exception {
    JetStreamManagement management = connection.jetStreamManagement();
    long before = deadLetterCount(management);
    String isolation = UUID.randomUUID().toString().substring(0, 8);
    String poisonSubject = SUITE_SUBJECT + "-dlq-" + isolation;
    String durable = "suite-dlq-" + isolation;
    management.addOrUpdateConsumer(
        JetStreamTopology.EVENT_STREAM,
        ConsumerConfiguration.builder()
            .durable(durable)
            .filterSubject(poisonSubject)
            .ackWait(Duration.ofSeconds(1))
            .maxDeliver(2)
            .build());
    try (DeadLetterRouter router = new DeadLetterRouter(connection, durable)) {
      connection
          .jetStream()
          .publish(message(poisonSubject, UUID.randomUUID().toString(), "poison"));
      JetStreamSubscription subscription =
          connection
              .jetStream()
              .subscribe(null, PullSubscribeOptions.bind(JetStreamTopology.EVENT_STREAM, durable));
      long after = before;
      for (int attempt = 0; attempt < 12 && after == before; attempt++) {
        for (Message message : subscription.fetch(1, Duration.ofSeconds(1))) {
          message.nak();
        }
        after = deadLetterCount(management);
      }
      assertTrue(after > before, "the exhausted message must reach the dead letter stream");
      assertTrue(router.routed() > 0);
      subscription.unsubscribe();
    } finally {
      management.deleteConsumer(JetStreamTopology.EVENT_STREAM, durable);
    }
  }

  @Test
  void replayBySequenceAndByTimeReturnStoredMessages() throws Exception {
    Instant boundary = Instant.now();
    Thread.sleep(50);
    PublishAck ack =
        connection.jetStream().publish(suiteMessage(UUID.randomUUID().toString(), "replay"));
    List<Message> bySequence =
        drain(
            ConsumerConfiguration.builder()
                .filterSubject(SUITE_SUBJECT)
                .deliverPolicy(DeliverPolicy.ByStartSequence)
                .startSequence(ack.getSeqno())
                .build());
    assertFalse(bySequence.isEmpty(), "replay by sequence must return the stored message");
    List<Message> byTime =
        drain(
            ConsumerConfiguration.builder()
                .filterSubject(SUITE_SUBJECT)
                .deliverPolicy(DeliverPolicy.ByStartTime)
                .startTime(boundary.atZone(ZoneOffset.UTC))
                .build());
    assertFalse(byTime.isEmpty(), "replay by time must return the stored message");
  }

  @Test
  void lifecycleEventsAreCausallyChained() throws Exception {
    Map<String, List<EventEnvelope>> byAggregate = new LinkedHashMap<>();
    for (EventEnvelope event : drainLifecycle()) {
      byAggregate
          .computeIfAbsent(event.getAggregateId().toString(), key -> new ArrayList<>())
          .add(event);
    }
    List<EventEnvelope> chain =
        byAggregate.values().stream().filter(events -> events.size() >= 3).findFirst().orElse(null);
    assertNotNull(chain, "lo stream deve contenere almeno un ciclo di vita completo");

    EventEnvelope order = chain.get(0);
    EventEnvelope payment = chain.get(1);
    EventEnvelope fulfillment = chain.get(2);

    assertEquals("OrderCreated", order.getEventType().toString());
    assertEquals("PaymentAuthorized", payment.getEventType().toString());
    assertEquals("FulfillmentCompleted", fulfillment.getEventType().toString());

    // Stesso aggregato: i tre eventi appartengono allo stesso ordine.
    assertEquals(order.getAggregateId().toString(), payment.getAggregateId().toString());
    assertEquals(order.getAggregateId().toString(), fulfillment.getAggregateId().toString());

    // Stessa correlazione lungo tutta la catena.
    assertEquals(order.getCorrelationId().toString(), payment.getCorrelationId().toString());
    assertEquals(order.getCorrelationId().toString(), fulfillment.getCorrelationId().toString());

    // Causazione: ogni evento punta a quello che lo ha provocato, non a se stesso.
    assertEquals(order.getEventId().toString(), payment.getCausationId().toString());
    assertEquals(payment.getEventId().toString(), fulfillment.getCausationId().toString());

    // Identita' distinte: la catena non e' lo stesso evento ripubblicato.
    assertNotEquals(order.getEventId().toString(), payment.getEventId().toString());
    assertNotEquals(payment.getEventId().toString(), fulfillment.getEventId().toString());

    // Ogni stadio dichiara il proprio produttore.
    assertEquals("order-simulator", order.getProducer().toString());
    assertEquals("payment-simulator", payment.getProducer().toString());
    assertEquals("fulfillment-simulator", fulfillment.getProducer().toString());

    printChain(chain);
  }

  /**
   * Prints the verified chain so the evidence is readable, not only asserted.
   *
   * @param chain the three events of one order, in publication order
   */
  private static void printChain(List<EventEnvelope> chain) {
    System.out.println("catena verificata, aggregato " + chain.get(0).getAggregateId());
    System.out.println("  correlationId comune: " + chain.get(0).getCorrelationId());
    for (EventEnvelope event : chain) {
      System.out.println(
          "  "
              + event.getEventType()
              + "  eventId="
              + event.getEventId()
              + "  causationId="
              + event.getCausationId()
              + "  producer="
              + event.getProducer());
    }
  }

  private List<EventEnvelope> drainLifecycle() throws Exception {
    JetStreamSubscription subscription =
        connection
            .jetStream()
            .subscribe(
                JetStreamTopology.EVENT_SUBJECT_WILDCARD,
                PullSubscribeOptions.builder()
                    .configuration(
                        ConsumerConfiguration.builder()
                            .filterSubject(JetStreamTopology.EVENT_SUBJECT_WILDCARD)
                            .deliverPolicy(DeliverPolicy.All)
                            .build())
                    .build());
    List<EventEnvelope> decoded = new ArrayList<>();
    for (Message message : subscription.fetch(500, Duration.ofSeconds(5))) {
      decoded.add(codec.decode(message.getData()));
      message.ack();
    }
    subscription.unsubscribe();
    return decoded;
  }

  private static long deadLetterCount(JetStreamManagement management) throws Exception {
    return management.getStreamInfo(JetStreamTopology.DLQ_STREAM).getStreamState().getMsgCount();
  }

  private List<Message> drain(ConsumerConfiguration configuration) throws Exception {
    JetStreamSubscription subscription =
        connection
            .jetStream()
            .subscribe(
                SUITE_SUBJECT, PullSubscribeOptions.builder().configuration(configuration).build());
    List<Message> messages = subscription.fetch(5, Duration.ofSeconds(3));
    messages.forEach(Message::ack);
    subscription.unsubscribe();
    return messages;
  }

  private static NatsMessage suiteMessage(String messageId, String marker) {
    return message(SUITE_SUBJECT, messageId, marker);
  }

  private static NatsMessage message(String subject, String messageId, String marker) {
    Headers headers = new Headers();
    headers.add(JetStreamAdapter.MESSAGE_ID_HEADER, messageId);
    return NatsMessage.builder()
        .subject(subject)
        .headers(headers)
        .data(codec.encode(canonical(marker)))
        .build();
  }

  private static String incompatibleSchema() {
    return "{\"type\":\"record\",\"name\":\"EventEnvelope\","
        + "\"namespace\":\"com.bitrockteam.kafkanuts.contracts\","
        + "\"fields\":[{\"name\":\"unrelated\",\"type\":\"string\"}]}";
  }

  private static EventEnvelope canonical(String marker) {
    String eventId = UUID.randomUUID().toString();
    com.bitrockteam.kafkanuts.transport.EventEnvelope event =
        new com.bitrockteam.kafkanuts.transport.EventEnvelope(
            eventId,
            "SuiteEvent",
            "suite-" + marker,
            Instant.now(),
            new TelemetryContext(eventId, eventId),
            marker.getBytes(StandardCharsets.UTF_8));
    return CanonicalEventMapper.toCanonical(event, "feature-suite", 1, "suite");
  }
}
