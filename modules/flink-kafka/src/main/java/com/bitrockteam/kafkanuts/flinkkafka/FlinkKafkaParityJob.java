package com.bitrockteam.kafkanuts.flinkkafka;

import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Flink Kafka job that emits one parity record per unique event id. */
public final class FlinkKafkaParityJob {
  /** Job input topic. */
  public static final String INPUT_TOPIC = "t06-events";

  /** Parity output topic. */
  public static final String OUTPUT_TOPIC = "t06-parity-output";

  /** Stable job name. */
  public static final String JOB_NAME = "t06-flink-kafka-parity-v1";

  private FlinkKafkaParityJob() {}

  /**
   * Builds the checkpointed Kafka-to-Kafka topology.
   *
   * @param properties Kafka client properties
   * @return configured streaming environment
   */
  public static StreamExecutionEnvironment build(Properties properties) {
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    environment.enableCheckpointing(2000L);
    environment.getCheckpointConfig().setCheckpointStorage("file:///opt/flink/checkpoints");
    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(2)));
    KafkaSource<String> source =
        KafkaSource.<String>builder()
            .setBootstrapServers(properties.getProperty("bootstrap.servers", "kafka:9092"))
            .setTopics(INPUT_TOPIC)
            .setGroupId("t06-flink-kafka-parity")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
    DataStreamSource<String> events =
        environment.fromSource(source, WatermarkStrategy.noWatermarks(), "t06-kafka-source");
    KafkaSink<String> sink =
        KafkaSink.<String>builder()
            .setBootstrapServers(properties.getProperty("bootstrap.servers", "kafka:9092"))
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(OUTPUT_TOPIC)
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
    events
        .keyBy(value -> value.split("\\|", 2)[0])
        .process(new UniqueEventFunction())
        .sinkTo(sink)
        .name("t06-parity-sink");
    return environment;
  }

  /**
   * Starts the job using environment properties.
   *
   * @param args command-line arguments
   * @throws Exception if job submission fails
   */
  public static void main(String[] args) throws Exception {
    Properties properties = new Properties();
    properties.put(
        "bootstrap.servers", System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"));
    build(properties).execute(JOB_NAME);
  }

  private static final class UniqueEventFunction
      extends KeyedProcessFunction<String, String, String> {
    /** State marking the keyed event as emitted. */
    private transient ValueState<Boolean> seen;

    @Override
    public void open(org.apache.flink.configuration.Configuration configuration) {
      seen = getRuntimeContext().getState(new ValueStateDescriptor<>("seen", Types.BOOLEAN));
    }

    @Override
    public void processElement(String value, Context context, Collector<String> out)
        throws Exception {
      if (!Boolean.TRUE.equals(seen.value())) {
        seen.update(true);
        out.collect(value);
      }
    }
  }
}
