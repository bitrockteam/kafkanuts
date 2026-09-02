package com.bitrockteam.kafkanuts.transport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Minimal client for the Apicurio {@code ccompat} API, the Confluent compatible surface of Apicurio
 * Registry.
 *
 * <p>Only the three calls the laboratory actually needs are implemented: register a subject
 * version, resolve a schema by identifier and check compatibility. Gli identificatori numerici
 * restituiti dal registry non sono identità portabile e non vengono usati come tali.
 */
public final class ApicurioSchemaRegistry {
  /** Base URL of the ccompat API, without trailing slash. */
  private final String baseUrl;

  /** Shared HTTP client. */
  private final HttpClient httpClient;

  /** JSON mapper for registry payloads. */
  private final ObjectMapper objectMapper = new ObjectMapper();

  /**
   * Attempts allowed when two processes create the same subject at the same time.
   *
   * <p>La creazione concorrente dello stesso subject fa rispondere Apicurio con 409. Una volta che
   * il subject esiste, la registrazione dello stesso schema è idempotente e restituisce lo stesso
   * identificatore, quindi basta ritentare.
   */
  private static final int CONFLICT_ATTEMPTS = 5;

  /** Cache from schema text to registry identifier. */
  private final Map<String, Integer> idBySchema = new ConcurrentHashMap<>();

  /** Cache from registry identifier to schema text. */
  private final Map<Integer, String> schemaById = new ConcurrentHashMap<>();

  /**
   * Creates a client bound to a ccompat base URL.
   *
   * @param baseUrl for example {@code http://registry:8080/apis/ccompat/v7}
   */
  public ApicurioSchemaRegistry(String baseUrl) {
    if (baseUrl == null || baseUrl.isBlank()) {
      throw new IllegalArgumentException("registry base url is required");
    }
    this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
  }

  /**
   * Registers a schema under a subject and returns its registry identifier.
   *
   * @param subject ccompat subject name
   * @param schema Avro schema text
   * @return registry identifier of the schema
   */
  public int register(String subject, String schema) {
    Integer cached = idBySchema.get(schema);
    if (cached != null) {
      return cached;
    }
    ObjectNode body = objectMapper.createObjectNode();
    body.put("schemaType", "AVRO");
    body.put("schema", schema);
    ConflictException lastConflict = null;
    for (int attempt = 1; attempt <= CONFLICT_ATTEMPTS; attempt++) {
      try {
        JsonNode response = send("POST", "/subjects/" + subject + "/versions", body.toString());
        int id = response.path("id").asInt();
        if (id <= 0) {
          throw new IllegalStateException("registry did not return a schema id for " + subject);
        }
        idBySchema.put(schema, id);
        schemaById.put(id, schema);
        return id;
      } catch (ConflictException conflict) {
        lastConflict = conflict;
        pause(200L * attempt);
      }
    }
    throw lastConflict;
  }

  private static void pause(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("registry retry interrupted", interrupted);
    }
  }

  /**
   * Resolves the schema text behind a registry identifier.
   *
   * @param id registry identifier
   * @return Avro schema text
   */
  public String schema(int id) {
    String cached = schemaById.get(id);
    if (cached != null) {
      return cached;
    }
    JsonNode response = send("GET", "/schemas/ids/" + id, null);
    String schema = response.path("schema").asText();
    if (schema.isEmpty()) {
      throw new IllegalStateException("registry returned no schema for id " + id);
    }
    schemaById.put(id, schema);
    return schema;
  }

  /**
   * Reads the compatibility level currently enforced on a subject.
   *
   * @param subject ccompat subject name
   * @return compatibility level name, for example NONE or BACKWARD
   */
  public String compatibilityLevel(String subject) {
    return send("GET", "/config/" + subject, null).path("compatibilityLevel").asText();
  }

  /**
   * Sets the compatibility level enforced on a subject.
   *
   * <p>Apicurio via {@code ccompat} non applica alcun livello per default: il valore iniziale è
   * {@code NONE}, mentre Confluent Schema Registry adotta {@code BACKWARD}. Senza questa chiamata
   * il registry accetterebbe anche uno schema incompatibile.
   *
   * @param subject ccompat subject name
   * @param level compatibility level to enforce
   */
  public void enforceCompatibility(String subject, String level) {
    ObjectNode body = objectMapper.createObjectNode();
    body.put("compatibility", level);
    send("PUT", "/config/" + subject, body.toString());
  }

  /**
   * Checks whether a candidate schema is compatible with the latest version of a subject.
   *
   * @param subject ccompat subject name
   * @param schema candidate Avro schema text
   * @return true when the registry accepts the candidate
   */
  public boolean compatible(String subject, String schema) {
    ObjectNode body = objectMapper.createObjectNode();
    body.put("schemaType", "AVRO");
    body.put("schema", schema);
    JsonNode response =
        send("POST", "/compatibility/subjects/" + subject + "/versions/latest", body.toString());
    return response.path("is_compatible").asBoolean(false);
  }

  private JsonNode send(String method, String path, String body) {
    HttpRequest.BodyPublisher publisher =
        body == null
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(body);
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(baseUrl + path))
            .timeout(Duration.ofSeconds(20))
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .header("Accept", "application/json")
            .method(method, publisher)
            .build();
    try {
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() == 409) {
        throw new ConflictException("registry call " + method + " " + path + " conflicted");
      }
      if (response.statusCode() >= 400) {
        throw new IllegalStateException(
            "registry call " + method + " " + path + " failed: " + response.statusCode());
      }
      return objectMapper.readTree(response.body());
    } catch (IOException cause) {
      throw new IllegalStateException("registry call " + method + " " + path + " failed", cause);
    } catch (InterruptedException cause) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("registry call interrupted", cause);
    }
  }

  /** Raised when the registry answers 409, typically on concurrent creation of a subject. */
  private static final class ConflictException extends IllegalStateException {
    /** Serialization identifier. */
    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param message description of the conflicting call
     */
    ConflictException(String message) {
      super(message);
    }
  }
}
