package com.bitrockteam.kafkanuts.transport;

import java.nio.ByteBuffer;

/**
 * Confluent wire format framing: one magic byte, a four byte big endian schema identifier, then the
 * Avro binary body.
 *
 * <p>Apicurio expone la stessa codifica attraverso l'API {@code ccompat}. Il formato è documentato,
 * non misurato: qui viene implementato esplicitamente per rendere visibile il contratto invece di
 * nasconderlo dentro un serializer di terze parti.
 */
public final class ConfluentWireFormat {
  /** Leading byte of every framed payload. */
  public static final byte MAGIC_BYTE = 0;

  /** Size in bytes of magic byte plus schema identifier. */
  public static final int HEADER_SIZE = 5;

  private ConfluentWireFormat() {}

  /**
   * Frames an Avro body with the registry schema identifier.
   *
   * @param schemaId registry identifier of the writer schema
   * @param body Avro binary encoding without schema
   * @return framed payload
   */
  public static byte[] encode(int schemaId, byte[] body) {
    return ByteBuffer.allocate(HEADER_SIZE + body.length)
        .put(MAGIC_BYTE)
        .putInt(schemaId)
        .put(body)
        .array();
  }

  /**
   * Reads the schema identifier from a framed payload.
   *
   * @param framed payload produced by {@link #encode(int, byte[])}
   * @return registry identifier of the writer schema
   */
  public static int schemaId(byte[] framed) {
    requireFramed(framed);
    return ByteBuffer.wrap(framed, 1, 4).getInt();
  }

  /**
   * Extracts the Avro body from a framed payload.
   *
   * @param framed payload produced by {@link #encode(int, byte[])}
   * @return Avro binary encoding without schema
   */
  public static byte[] body(byte[] framed) {
    requireFramed(framed);
    byte[] body = new byte[framed.length - HEADER_SIZE];
    System.arraycopy(framed, HEADER_SIZE, body, 0, body.length);
    return body;
  }

  private static void requireFramed(byte[] framed) {
    if (framed == null || framed.length < HEADER_SIZE) {
      throw new IllegalArgumentException("payload is shorter than the wire format header");
    }
    if (framed[0] != MAGIC_BYTE) {
      throw new IllegalArgumentException("unexpected magic byte: " + framed[0]);
    }
  }
}
