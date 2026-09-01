package com.bitrockteam.kafkanuts.flinknats;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.nats.client.Connection;
import io.nats.client.Nats;
import org.junit.jupiter.api.Test;

/** Verifies NATS connectivity only; this is not a consumer, redelivery, or deduplication test. */
class NatsConnectivityProbeTest {
  @Test
  void connectsToTheIsolatedNatsCluster() throws Exception {
    String url = System.getenv("T10_NATS_URL");
    if (url == null) {
      assertNotNull("live Compose probe is opt-in");
      return;
    }
    try (Connection connection = Nats.connect(url)) {
      assertNotNull(connection.getServerInfo());
    }
  }
}
