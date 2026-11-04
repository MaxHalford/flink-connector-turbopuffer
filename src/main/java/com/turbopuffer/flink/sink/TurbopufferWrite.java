package com.turbopuffer.flink.sink;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.annotation.PublicEvolving;

/** A document write produced from one Flink stream element. */
@PublicEvolving
public final class TurbopufferWrite {

  /** Supported namespace write operations. */
  public enum Operation {
    /** Insert a new document or replace an existing document. */
    UPSERT,

    /** Update only the supplied attributes on an existing document. */
    PATCH,

    /** Remove a document. */
    DELETE
  }

  private final Operation operation;
  private final Object id;
  private final Map<String, Object> attributes;

  private TurbopufferWrite(Operation operation, Object id, Map<String, ?> attributes) {
    this.operation = Objects.requireNonNull(operation, "operation must not be null");
    this.id = Objects.requireNonNull(id, "id must not be null");
    this.attributes =
        new LinkedHashMap<>(Objects.requireNonNull(attributes, "attributes must not be null"));
  }

  /** Creates an upsert with a string ID. */
  public static TurbopufferWrite upsert(String id, Map<String, ?> attributes) {
    return new TurbopufferWrite(Operation.UPSERT, id, attributes);
  }

  /** Creates an upsert with an integer ID. */
  public static TurbopufferWrite upsert(long id, Map<String, ?> attributes) {
    return new TurbopufferWrite(Operation.UPSERT, id, attributes);
  }

  /** Creates a partial update with a string ID. */
  public static TurbopufferWrite patch(String id, Map<String, ?> attributes) {
    return new TurbopufferWrite(Operation.PATCH, id, attributes);
  }

  /** Creates a partial update with an integer ID. */
  public static TurbopufferWrite patch(long id, Map<String, ?> attributes) {
    return new TurbopufferWrite(Operation.PATCH, id, attributes);
  }

  /** Creates a delete with a string ID. */
  public static TurbopufferWrite delete(String id) {
    return new TurbopufferWrite(Operation.DELETE, id, Map.of());
  }

  /** Creates a delete with an integer ID. */
  public static TurbopufferWrite delete(long id) {
    return new TurbopufferWrite(Operation.DELETE, id, Map.of());
  }

  /** Returns the operation represented by this write. */
  public Operation operation() {
    return operation;
  }

  /** Returns the string or 64-bit integer document ID. */
  public Object id() {
    return id;
  }

  /** Returns an unmodifiable view of the document attributes. */
  public Map<String, Object> attributes() {
    return Collections.unmodifiableMap(attributes);
  }
}
