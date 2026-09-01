package com.bitrockteam.kafkanuts.kafka;

import com.bitrockteam.kafkanuts.contracts.EventEnvelope;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Map;

/** Confluent wire-format codec for the canonical T03 Avro envelope. */
public final class KafkaAvroCodec {
  /** Confluent serializer. */
  private final KafkaAvroSerializer serializer;

  /** Confluent deserializer. */
  private final KafkaAvroDeserializer deserializer;

  public KafkaAvroCodec(SchemaRegistryClient registry) {
    serializer = new KafkaAvroSerializer(registry);
    deserializer = new KafkaAvroDeserializer(registry);
    serializer.configure(Map.of("schema.registry.url", "mock://t05"), false);
    deserializer.configure(
        Map.of("schema.registry.url", "mock://t05", "specific.avro.reader", true), false);
  }

  public byte[] serialize(String topic, EventEnvelope event) {
    return serializer.serialize(topic, event);
  }

  public EventEnvelope deserialize(String topic, byte[] bytes) {
    return (EventEnvelope) deserializer.deserialize(topic, bytes);
  }
}
