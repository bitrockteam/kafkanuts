package com.bitrockteam.kafkanuts.console;

import com.bitrockteam.kafkanuts.contracts.EventEnvelope;
import com.bitrockteam.kafkanuts.transport.AvroEventCodec;
import com.bitrockteam.kafkanuts.transport.JetStreamTopology;
import com.bitrockteam.kafkanuts.transport.LifecycleSupport;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Observes the live event flow and republishes it to browsers as Server-Sent Events.
 *
 * <p>Osservatore passivo: si lega a consumer effimeri con ack policy none, mai ai durable
 * payment-worker e fulfillment-worker, quindi non sottrae messaggi ai simulatori e non lascia stato
 * sul server quando il container si ferma.
 */
public final class EventTail implements AutoCloseable {
  /** Attempts allowed while NATS and the registry finish coming up. */
  private static final int BOOTSTRAP_ATTEMPTS = 10;

  /** Events kept in memory, so a browser opened later is not greeted by an empty page. */
  private static final int BUFFER_SIZE = 400;

  /** How far back the ephemeral consumers start, to hydrate the page on connection. */
  private static final int HYDRATION_MINUTES = 3;

  /** Serializer rendering one event as a JSON line. */
  private final ObjectMapper json = new ObjectMapper();

  /** Recently observed events, oldest first. */
  private final Deque<String> buffer = new ArrayDeque<>();

  /** Browsers currently listening. */
  private final List<SseEmitter> listeners = new CopyOnWriteArrayList<>();

  /** Count of events observed since startup. */
  private final AtomicLong observed = new AtomicLong();

  /** Live NATS connection, null when the console could not attach. */
  private final Connection connection;

  /** Reason why the tail is inactive, null when running. */
  private final String inactiveReason;

  /**
   * Attaches to the live stack, or records why it could not.
   *
   * @param natsUrl NATS server URL, may be blank
   * @param registryUrl ccompat base URL, may be blank
   */
  public EventTail(String natsUrl, String registryUrl) {
    if (natsUrl == null || natsUrl.isBlank() || registryUrl == null || registryUrl.isBlank()) {
      this.connection = null;
      this.inactiveReason = "data plane not configured";
      return;
    }
    Connection started = null;
    String failure = null;
    for (int attempt = 1; attempt <= BOOTSTRAP_ATTEMPTS && started == null; attempt++) {
      try {
        started = attach(natsUrl, registryUrl);
        failure = null;
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        failure = "interrupted while connecting";
        break;
      } catch (Exception cause) {
        failure = cause.getClass().getSimpleName() + ": " + cause.getMessage();
        pause(Math.min(500L * attempt, 2000L));
      }
    }
    this.connection = started;
    this.inactiveReason = failure;
  }

  private Connection attach(String natsUrl, String registryUrl) throws Exception {
    Connection live = LifecycleSupport.connect(natsUrl);
    AvroEventCodec codec = LifecycleSupport.codec(registryUrl);
    Dispatcher dispatcher = live.createDispatcher();
    JetStream jetStream = live.jetStream();
    ConsumerConfiguration configuration =
        ConsumerConfiguration.builder()
            .deliverPolicy(DeliverPolicy.ByStartTime)
            .startTime(ZonedDateTime.now().minusMinutes(HYDRATION_MINUTES))
            .ackPolicy(AckPolicy.None)
            .build();
    PushSubscribeOptions options =
        PushSubscribeOptions.builder().configuration(configuration).build();
    jetStream.subscribe(
        JetStreamTopology.EVENT_SUBJECT_WILDCARD,
        dispatcher,
        message -> accept(codec, message, "EVENTS"),
        false,
        options);
    jetStream.subscribe(
        JetStreamTopology.DLQ_SUBJECT_WILDCARD,
        dispatcher,
        message -> accept(codec, message, "DLQ"),
        false,
        options);
    return live;
  }

  private void accept(AvroEventCodec codec, Message message, String stream) {
    String line;
    try {
      line = json.writeValueAsString(describe(codec.decode(message.getData()), message, stream));
    } catch (JsonProcessingException | RuntimeException cause) {
      return;
    }
    observed.incrementAndGet();
    synchronized (buffer) {
      buffer.addLast(line);
      while (buffer.size() > BUFFER_SIZE) {
        buffer.removeFirst();
      }
    }
    for (SseEmitter listener : listeners) {
      send(listener, line);
    }
  }

  private static Map<String, Object> describe(
      EventEnvelope envelope, Message message, String stream) {
    Map<String, Object> event = new LinkedHashMap<>();
    event.put("seq", message.metaData().streamSequence());
    event.put("stream", stream);
    event.put("subject", message.getSubject());
    event.put("eventType", text(envelope.getEventType()));
    event.put("aggregateId", text(envelope.getAggregateId()));
    event.put("eventId", text(envelope.getEventId()));
    event.put("correlationId", text(envelope.getCorrelationId()));
    event.put("causationId", text(envelope.getCausationId()));
    event.put("producer", text(envelope.getProducer()));
    event.put("occurredAt", envelope.getOccurredAt().toEpochMilli());
    event.put("payload", new String(envelope.getPayload().array(), StandardCharsets.UTF_8));
    return event;
  }

  private static String text(CharSequence value) {
    return value == null ? null : value.toString();
  }

  /**
   * Registers a browser, replaying the buffer before the live flow.
   *
   * @param emitter emitter bound to the HTTP response
   */
  public void register(SseEmitter emitter) {
    List<String> replay;
    synchronized (buffer) {
      replay = new ArrayList<>(buffer);
    }
    for (String line : replay) {
      send(emitter, line);
    }
    emitter.onCompletion(() -> listeners.remove(emitter));
    emitter.onTimeout(() -> listeners.remove(emitter));
    emitter.onError(error -> listeners.remove(emitter));
    listeners.add(emitter);
  }

  private void send(SseEmitter emitter, String line) {
    try {
      emitter.send(SseEmitter.event().data(line));
    } catch (Exception cause) {
      listeners.remove(emitter);
      emitter.completeWithError(cause);
    }
  }

  /**
   * Builds the payload of the health endpoint.
   *
   * @return ordered health attributes
   */
  public Map<String, Object> health() {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("status", "UP");
    payload.put("service", "console");
    if (connection == null) {
      payload.put("tail", "inactive");
      payload.put("reason", inactiveReason);
      return payload;
    }
    payload.put("tail", "active");
    payload.put("observed", observed.get());
    payload.put("listeners", listeners.size());
    synchronized (buffer) {
      payload.put("buffered", buffer.size());
    }
    return payload;
  }

  private static void pause(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void close() {
    for (SseEmitter listener : listeners) {
      listener.complete();
    }
    listeners.clear();
    if (connection != null) {
      try {
        connection.close();
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
