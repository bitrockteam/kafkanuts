package com.bitrockteam.kafkanuts.flinknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class FlinkNatsSpikeTest {
  @Test
  void selectsDataStreamOnlyAndConfiguresCheckpointing() {
    StreamExecutionEnvironment environment = FlinkNatsSpike.createDataStreamProbe();
    assertNotNull(environment.getCheckpointConfig());
    assertEquals("B", FlinkNatsSpike.DECISION);
  }

  @Test
  void reportIsMachineReadableAndCoversEveryGate() throws IOException {
    try (InputStream input = getClass().getResourceAsStream("/t10-spike-report.json")) {
      assertNotNull(input);
      String report = new String(input.readAllBytes(), StandardCharsets.UTF_8);
      for (String required :
          new String[] {
            "\"decision\": \"B\"",
            "packaging",
            "licenses",
            "avro_registry",
            "event_time_watermark",
            "window_join",
            "checkpoint_recovery",
            "redelivery_duplicates",
            "parallelism_backpressure",
            "budget"
          }) {
        assertTrue(report.contains(required), "missing report field: " + required);
      }
    }
  }
}
