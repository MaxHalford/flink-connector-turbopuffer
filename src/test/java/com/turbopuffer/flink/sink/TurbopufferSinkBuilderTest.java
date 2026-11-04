package com.turbopuffer.flink.sink;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

class TurbopufferSinkBuilderTest {

  @Test
  void requiresNamespace() {
    assertThatThrownBy(
            () ->
                TurbopufferSink.<String>builder()
                    .setSerializationSchema(
                        (element, context) ->
                            TurbopufferWrite.upsert(element, Map.of("text", element)))
                    .build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("namespace");
  }

  @Test
  void requiresSerializationSchema() {
    assertThatThrownBy(
            () -> TurbopufferSink.<String>builder().setNamespace("wikipedia-edits").build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("serialization schema");
  }

  @Test
  void rejectsInvalidNamespace() {
    assertThatThrownBy(
            () ->
                TurbopufferSink.<String>builder()
                    .setNamespace("not/a/namespace")
                    .setSerializationSchema(
                        (element, context) ->
                            TurbopufferWrite.upsert(element, Map.of("text", element)))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("namespace");
  }

  @Test
  void rejectsSerializationSchemaWithNonSerializableCapture() {
    Object nonSerializable = new Object();

    assertThatThrownBy(
            () ->
                TurbopufferSink.<String>builder()
                    .setNamespace("wikipedia-edits")
                    .setSerializationSchema(
                        (element, context) -> {
                          nonSerializable.toString();
                          return TurbopufferWrite.upsert(element, Map.of("text", element));
                        })
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("serializable");
  }
}
