package com.bitrockteam.kafkanuts.transport;

import com.bitrockteam.kafkanuts.contracts.EventEnvelope;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * Serializes the canonical Avro envelope with Confluent framing against an Apicurio registry.
 *
 * <p>Il writer schema viene risolto dal registry a ogni decodifica: il codec non assume che
 * l'identificatore numerico sia stabile fra registry diversi.
 */
public final class AvroEventCodec {
  /** Compatibility level enforced on the subject, since Apicurio defaults to NONE. */
  public static final String DEFAULT_COMPATIBILITY = "BACKWARD";

  /** ccompat subject holding the canonical envelope schema. */
  private final String subject;

  /** Registry client used to register and resolve schemas. */
  private final ApicurioSchemaRegistry registry;

  /** Identifier of the writer schema in the registry. */
  private final int writerSchemaId;

  /**
   * Registers the canonical envelope schema and prepares the codec.
   *
   * @param subject ccompat subject name
   * @param registry registry client
   */
  public AvroEventCodec(String subject, ApicurioSchemaRegistry registry) {
    this.subject = subject;
    this.registry = registry;
    registry.enforceCompatibility(subject, DEFAULT_COMPATIBILITY);
    this.writerSchemaId = registry.register(subject, EventEnvelope.getClassSchema().toString());
  }

  /**
   * Returns the registry identifier of the writer schema.
   *
   * @return registry identifier
   */
  public int writerSchemaId() {
    return writerSchemaId;
  }

  /**
   * Returns the ccompat subject backing this codec.
   *
   * @return subject name
   */
  public String subject() {
    return subject;
  }

  /**
   * Encodes a canonical envelope into a framed payload.
   *
   * @param envelope canonical Avro envelope
   * @return framed payload
   */
  public byte[] encode(EventEnvelope envelope) {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(buffer, null);
    SpecificDatumWriter<EventEnvelope> writer =
        new SpecificDatumWriter<>(EventEnvelope.getClassSchema());
    try {
      writer.write(envelope, encoder);
      encoder.flush();
    } catch (IOException cause) {
      throw new UncheckedIOException("cannot encode canonical envelope", cause);
    }
    return ConfluentWireFormat.encode(writerSchemaId, buffer.toByteArray());
  }

  /**
   * Decodes a framed payload, resolving the writer schema from the registry.
   *
   * @param framed payload produced by {@link #encode(EventEnvelope)}
   * @return canonical Avro envelope
   */
  public EventEnvelope decode(byte[] framed) {
    int id = ConfluentWireFormat.schemaId(framed);
    Schema writerSchema = new Schema.Parser().parse(registry.schema(id));
    SpecificDatumReader<EventEnvelope> reader =
        new SpecificDatumReader<>(writerSchema, EventEnvelope.getClassSchema());
    BinaryDecoder decoder =
        DecoderFactory.get().binaryDecoder(ConfluentWireFormat.body(framed), null);
    try {
      return reader.read(null, decoder);
    } catch (IOException cause) {
      throw new UncheckedIOException("cannot decode canonical envelope", cause);
    }
  }
}
