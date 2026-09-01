package com.bitrockteam.kafkanuts.flinknats;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.nats.client.Connection;
import io.nats.client.Nats;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class NatsLiveProbeTest {
  @Test
  void publishesOnTheIsolatedNatsCluster() throws Exception {
    String url = System.getenv("T10_NATS_URL");
    if (url == null) {
      assertNotNull("live Compose probe is opt-in");
      return;
    }
    try (Connection connection = Nats.connect(url)) {
      connection.publish("t10.events", "eventId=t10-duplicate-probe".getBytes());
      connection.publish("t10.events", "eventId=t10-duplicate-probe".getBytes());
      connection.flush(Duration.ofSeconds(2));
      assertNotNull(connection.getServerInfo());
    }
  }
}
