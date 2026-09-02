package com.bitrockteam.kafkanuts.transport;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.Nats;
import io.nats.client.Options;
import java.io.IOException;
import java.time.Duration;

/** Shared wiring used by the three simulators to join the JetStream lifecycle. */
public final class LifecycleSupport {
  /** Delivery attempts allowed before a message is routed to the dead letter stream. */
  public static final int MAX_DELIVER = 3;

  private LifecycleSupport() {}

  /**
   * Opens a resilient connection to NATS.
   *
   * @param url NATS server URL
   * @return connected client
   * @throws IOException when the server cannot be reached
   * @throws InterruptedException when the connection wait is interrupted
   */
  public static Connection connect(String url) throws IOException, InterruptedException {
    Options options =
        Options.builder()
            .server(url)
            .connectionTimeout(Duration.ofSeconds(10))
            .maxReconnects(-1)
            .reconnectWait(Duration.ofSeconds(2))
            .build();
    return Nats.connect(options);
  }

  /**
   * Registers the canonical envelope schema and returns a codec bound to it.
   *
   * @param registryUrl ccompat base URL
   * @return codec for the canonical envelope
   */
  public static AvroEventCodec codec(String registryUrl) {
    return new AvroEventCodec(
        JetStreamTopology.REGISTRY_SUBJECT, new ApicurioSchemaRegistry(registryUrl));
  }

  /**
   * Creates streams and the two durable consumers idempotently.
   *
   * @param connection connected NATS client
   * @throws IOException on transport failure
   * @throws JetStreamApiException when the server rejects the configuration
   */
  public static void provision(Connection connection) throws IOException, JetStreamApiException {
    JetStreamProvisioner provisioner = new JetStreamProvisioner(connection);
    provisioner.provisionStreams();
    provisioner.provisionConsumer(
        JetStreamTopology.PAYMENT_CONSUMER, JetStreamTopology.ORDER_SUBJECT, MAX_DELIVER);
    provisioner.provisionConsumer(
        JetStreamTopology.FULFILLMENT_CONSUMER, JetStreamTopology.PAYMENT_SUBJECT, MAX_DELIVER);
  }

  /**
   * Decides deterministically whether a payment must fail every delivery attempt.
   *
   * <p>Serve a esercitare redelivery, backoff e instradamento in dead letter senza introdurre
   * casualità: la stessa fixture produce sempre lo stesso esito.
   *
   * @param aggregateId aggregate identifier of the order
   * @return true when the payment must be rejected on every attempt
   */
  public static boolean alwaysFails(String aggregateId) {
    return Math.floorMod(aggregateId.hashCode(), 10) == 0;
  }
}
