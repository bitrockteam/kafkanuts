package com.bitrockteam.kafkanuts.flinknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class FlinkNatsSpikeTest {
  @Test
  void selectsOutcomeCWhenMandatoryRuntimeGatesAreNotAllPassing() throws IOException {
    JsonNode report = readReport();
    boolean allRuntimeGatesPass =
        Stream.of(
                "avro_registry",
                "event_time_watermark",
                "window_join",
                "checkpoint_recovery",
                "redelivery_duplicates",
                "parallelism_backpressure")
            .map(name -> report.path("criteria").path(name).path("status").asText())
            .allMatch("PASS"::equals);
    assertFalse(allRuntimeGatesPass);
    assertEquals("C", report.path("decision").asText());
    assertEquals("C", FlinkNatsSpike.DECISION);
    assertEquals("C", report.path("decision_rule").path("derived").asText().substring(0, 1));
  }

  @Test
  void constructsOnlyTheConfinedDataStreamProbe() {
    StreamExecutionEnvironment environment = FlinkNatsSpike.createDataStreamProbe();
    assertNotNull(environment.getCheckpointConfig());
  }

  private JsonNode readReport() throws IOException {
    try (InputStream input = getClass().getResourceAsStream("/t10-spike-report.json")) {
      assertNotNull(input);
      return new ObjectMapper().readTree(new String(input.readAllBytes(), StandardCharsets.UTF_8));
    }
  }
}
