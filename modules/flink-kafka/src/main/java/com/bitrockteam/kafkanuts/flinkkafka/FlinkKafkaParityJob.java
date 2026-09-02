package com.bitrockteam.kafkanuts.flinkkafka;

import com.bitrockteam.kafkanuts.contracts.EventEnvelope;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Flink Kafka job that emits one canonical Avro envelope per unique event id. */
public final class FlinkKafkaParityJob {
  /** Job input topic. */
  public static final String INPUT_TOPIC = "t06-events";

  /** Parity output topic. */
  public static final String OUTPUT_TOPIC = "t06-parity-output";

  /** Stable job name. */
  public static final String JOB_NAME = "t06-flink-kafka-parity-v1";

  private FlinkKafkaParityJob() {}

  /**
   * Builds the checkpointed canonical Avro Kafka topology.
   *
   * @param properties Kafka and registry client properties
   * @return configured streaming environment
   */
  public static StreamExecutionEnvironment build(Properties properties) {
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    environment.enableCheckpointing(2000L);
    environment.getCheckpointConfig().setCheckpointStorage("file:///opt/flink/checkpoints");
    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(2)));
    String registry = properties.getProperty("schema.registry.url", "http://schema-registry:8081");
    KafkaSource<byte[]> source =
        KafkaSource.<byte[]>builder()
            .setBootstrapServers(properties.getProperty("bootstrap.servers", "kafka:9092"))
            .setTopics(INPUT_TOPIC)
            .setGroupId("t06-flink-kafka-parity")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new RawBytesSchema())
            .build();
    DataStreamSource<byte[]> rawEvents =
        environment.fromSource(source, WatermarkStrategy.noWatermarks(), "t06-kafka-avro-source");
    var events =
        rawEvents.map(
            new EnvelopeMapFunction(registry),
            org.apache.flink.api.common.typeinfo.Types.GENERIC(EventEnvelope.class));
    KafkaSink<EventEnvelope> sink =
        KafkaSink.<EventEnvelope>builder()
            .setBootstrapServers(properties.getProperty("bootstrap.servers", "kafka:9092"))
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(OUTPUT_TOPIC)
                    .setValueSerializationSchema(new EnvelopeSerializer(registry))
                    .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
    events
        .keyBy(EventEnvelope::getEventId)
        .process(
            new UniqueEventFunction(),
            org.apache.flink.api.common.typeinfo.Types.GENERIC(EventEnvelope.class))
        .sinkTo(sink)
        .name("t06-parity-avro-sink");
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
    properties.put(
        "schema.registry.url",
        System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"));
    build(properties).execute(JOB_NAME);
  }

  private static final class RawBytesSchema implements DeserializationSchema<byte[]> {
    @Override
    public byte[] deserialize(byte[] message) {
      return message;
    }

    @Override
    public boolean isEndOfStream(byte[] nextElement) {
      return false;
    }

    @Override
    public TypeInformation<byte[]> getProducedType() {
      return TypeInformation.of(byte[].class);
    }
  }

  private static final class EnvelopeMapFunction extends RichMapFunction<byte[], EventEnvelope> {
    /** Registry endpoint. */
    private final String registryUrl;

    /** Confluent deserializer initialized on the task manager. */
    private transient KafkaAvroDeserializer delegate;

    private EnvelopeMapFunction(String registryUrl) {
      this.registryUrl = registryUrl;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration configuration) {
      delegate = new KafkaAvroDeserializer();
      delegate.configure(
          Map.of("schema.registry.url", registryUrl, "specific.avro.reader", true), false);
    }

    @Override
    public EventEnvelope map(byte[] message) {
      return (EventEnvelope) delegate.deserialize(INPUT_TOPIC, message);
    }
  }

  private static final class EnvelopeSerializer implements SerializationSchema<EventEnvelope> {
    /** Registry endpoint. */
    private final String registryUrl;

    /** Confluent serializer initialized on the task manager. */
    private transient KafkaAvroSerializer delegate;

    private EnvelopeSerializer(String registryUrl) {
      this.registryUrl = registryUrl;
    }

    @Override
    public void open(InitializationContext context) {
      delegate = new KafkaAvroSerializer();
      delegate.configure(Map.of("schema.registry.url", registryUrl), false);
    }

    @Override
    public byte[] serialize(EventEnvelope element) {
      return delegate.serialize(OUTPUT_TOPIC, element);
    }
  }

  private static final class UniqueEventFunction
      extends KeyedProcessFunction<String, EventEnvelope, EventEnvelope> {
    /** State marking the keyed event as emitted. */
    private transient ValueState<Boolean> seen;

    @Override
    public void open(org.apache.flink.configuration.Configuration configuration) {
      seen = getRuntimeContext().getState(new ValueStateDescriptor<>("seen", Boolean.class));
    }

    @Override
    public void processElement(EventEnvelope value, Context context, Collector<EventEnvelope> out)
        throws Exception {
      if (!Boolean.TRUE.equals(seen.value())) {
        seen.update(true);
        out.collect(value);
      }
    }
  }
}
