package com.bitrockteam.kafkanuts.transport;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import java.io.IOException;
import java.time.Duration;

/**
 * Creates streams and durable consumers idempotently.
 *
 * <p>Ogni chiamata è ripetibile: un provisioning già presente viene aggiornato, non duplicato.
 */
public final class JetStreamProvisioner {
  /** Management handle on the connected server. */
  private final JetStreamManagement management;

  /**
   * Binds the provisioner to a live connection.
   *
   * @param connection connected NATS client
   * @throws IOException when the management context cannot be created
   */
  public JetStreamProvisioner(Connection connection) throws IOException {
    this.management = connection.jetStreamManagement();
  }

  /**
   * Creates or updates the two streams used by the laboratory.
   *
   * @throws IOException on transport failure
   * @throws JetStreamApiException when the server rejects the configuration
   */
  public void provisionStreams() throws IOException, JetStreamApiException {
    upsertStream(
        StreamConfiguration.builder()
            .name(JetStreamTopology.EVENT_STREAM)
            .subjects(JetStreamTopology.EVENT_SUBJECT_WILDCARD)
            .storageType(StorageType.File)
            .retentionPolicy(RetentionPolicy.Limits)
            .duplicateWindow(Duration.ofMinutes(2))
            .maxAge(Duration.ofHours(24))
            .build());
    upsertStream(
        StreamConfiguration.builder()
            .name(JetStreamTopology.DLQ_STREAM)
            .subjects(JetStreamTopology.DLQ_SUBJECT_WILDCARD)
            .storageType(StorageType.File)
            .retentionPolicy(RetentionPolicy.Limits)
            .maxAge(Duration.ofHours(24))
            .build());
  }

  /**
   * Creates or updates a durable pull consumer with an explicit redelivery budget.
   *
   * @param durable durable consumer name
   * @param filterSubject subject the consumer is bound to
   * @param maxDeliver maximum delivery attempts before the message is abandoned
   * @throws IOException on transport failure
   * @throws JetStreamApiException when the server rejects the configuration
   */
  public void provisionConsumer(String durable, String filterSubject, int maxDeliver)
      throws IOException, JetStreamApiException {
    management.addOrUpdateConsumer(
        JetStreamTopology.EVENT_STREAM,
        ConsumerConfiguration.builder()
            .durable(durable)
            .filterSubject(filterSubject)
            .ackPolicy(AckPolicy.Explicit)
            .ackWait(Duration.ofSeconds(5))
            .maxDeliver(maxDeliver)
            .backoff(Duration.ofSeconds(1), Duration.ofSeconds(2))
            .build());
  }

  private void upsertStream(StreamConfiguration configuration)
      throws IOException, JetStreamApiException {
    try {
      management.addStream(configuration);
    } catch (JetStreamApiException alreadyPresent) {
      management.updateStream(configuration);
    }
  }
}
