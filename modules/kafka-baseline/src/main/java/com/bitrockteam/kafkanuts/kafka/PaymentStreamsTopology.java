package com.bitrockteam.kafkanuts.kafka;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;

/** Versioned, bounded Kafka Streams topology for the T05 M0 baseline. */
public final class PaymentStreamsTopology {
  private PaymentStreamsTopology() {}

  public static Topology build(Properties properties) {
    properties.putIfAbsent("application.id", "kafkanuts-payment-streams-v1");
    properties.putIfAbsent("bootstrap.servers", "kafka:9092");
    properties.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    properties.putIfAbsent(
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> payments = builder.stream("payments");
    payments
        .filter((key, value) -> key != null && value != null)
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .count()
        .toStream()
        .to("payment-counts");
    return builder.build();
  }
}
