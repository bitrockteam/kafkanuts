package com.bitrockteam.kafkanuts.flinknats;

import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** The deliberately confined DataStream-only T10 probe; it is not a general-purpose connector. */
public final class FlinkNatsSpike {
  /** Selected gate outcome: DataStream API only. */
  public static final String DECISION = "B";

  /** Flink version exercised by the probe. */
  public static final String FLINK_VERSION = "1.20.2";

  /** jnats version packaged by the probe. */
  public static final String NATS_CLIENT_VERSION = "2.20.5";

  private FlinkNatsSpike() {}

  public static StreamExecutionEnvironment createDataStreamProbe() {
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    environment.enableCheckpointing(1000L);
    DataStreamSource<Long> source = environment.fromData(List.of(1L, 2L, 3L));
    source.assignTimestampsAndWatermarks(
        WatermarkStrategy.<Long>forMonotonousTimestamps()
            .withTimestampAssigner((value, ts) -> value));
    return environment;
  }
}
