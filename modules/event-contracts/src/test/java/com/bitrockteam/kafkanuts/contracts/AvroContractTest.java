package com.bitrockteam.kafkanuts.contracts;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.Test;

class AvroContractTest {
  @Test
  void generatesAllCanonicalSchemas() {
    List<String> names = List.of("EventEnvelope", "Order", "Payment", "Fulfillment");
    for (String name : names) {
      Schema schema = load("/" + name + ".avsc");
      assertEquals(name, schema.getName());
      assertFalse(SchemaFingerprint.of(schema).isBlank());
      assertDoesNotThrow(() -> Class.forName("com.bitrockteam.kafkanuts.contracts." + name));
    }
  }

  @Test
  void fingerprintIsStableAndSchemaSpecific() {
    Schema order = load("/Order.avsc");
    Schema payment = load("/Payment.avsc");
    assertEquals("d5f3919955102019", SchemaFingerprint.of(load("/EventEnvelope.avsc")));
    assertEquals("e324caad1046e0bd", SchemaFingerprint.of(order));
    assertEquals("0c54bc735a308a6a", SchemaFingerprint.of(payment));
    assertEquals("78e6108986eb091d", SchemaFingerprint.of(load("/Fulfillment.avsc")));
    assertNotEquals(SchemaFingerprint.of(order), SchemaFingerprint.of(payment));
  }

  @Test
  void fixturesParseAgainstTheirSchemas() throws IOException {
    for (String name : List.of("order", "payment", "fulfillment")) {
      Schema schema = load("/" + capitalize(name) + ".avsc");
      try (InputStream fixture = resource("/fixtures/" + name + ".json")) {
        GenericRecord record =
            new GenericDatumReader<GenericRecord>(schema)
                .read(null, DecoderFactory.get().jsonDecoder(schema, fixture));
        assertEquals(schema.getName(), record.getSchema().getName());
      }
    }
  }

  @Test
  void incompatibleSchemaIsRejected() {
    Schema writer = load("/Payment.avsc");
    Schema compatibleReader = load("/schemas/PaymentCompatible.avsc");
    Schema incompatibleReader = load("/schemas/PaymentIncompatible.avsc");
    SchemaCompatibility.SchemaCompatibilityType result =
        SchemaCompatibility.checkReaderWriterCompatibility(incompatibleReader, writer).getType();
    assertEquals(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE, result);
    assertEquals(
        SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE,
        SchemaCompatibility.checkReaderWriterCompatibility(compatibleReader, writer).getType());
  }

  private static Schema load(String path) {
    try (InputStream input = resource(path)) {
      return new Schema.Parser().parse(input);
    } catch (IOException exception) {
      throw new IllegalStateException("Unable to load " + path, exception);
    }
  }

  private static InputStream resource(String path) {
    InputStream input = AvroContractTest.class.getResourceAsStream(path);
    if (input == null) {
      throw new IllegalArgumentException("Missing resource " + path);
    }
    return input;
  }

  private static String capitalize(String value) {
    return value.substring(0, 1).toUpperCase() + value.substring(1);
  }
}
