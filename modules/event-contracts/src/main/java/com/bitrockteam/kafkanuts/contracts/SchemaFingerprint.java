package com.bitrockteam.kafkanuts.contracts;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;

/** Computes the canonical Avro parsing fingerprint used as portable schema identity. */
public final class SchemaFingerprint {
  private SchemaFingerprint() {}

  public static String of(Schema schema) {
    long fingerprint = SchemaNormalization.parsingFingerprint64(schema);
    return String.format("%016x", fingerprint);
  }

  public static String of(String schemaJson) {
    return of(new Schema.Parser().parse(schemaJson));
  }
}
