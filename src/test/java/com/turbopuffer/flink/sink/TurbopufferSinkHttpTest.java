package com.turbopuffer.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.turbopuffer.models.namespaces.AttributeSchemaConfig;
import com.turbopuffer.models.namespaces.DistanceMetric;
import com.turbopuffer.models.namespaces.Schema;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class TurbopufferSinkHttpTest {

  private final MockWebServer server = new MockWebServer();
  private final ObjectMapper objectMapper = new ObjectMapper();

  @AfterEach
  void closeServer() throws Exception {
    server.close();
  }

  @Test
  @Timeout(10)
  void flushWritesOneBatchThroughThePublicSink() throws Exception {
    server.enqueue(
        new MockResponse()
            .setHeader("Content-Type", "application/json")
            .setBody(
                "{\"billing\":{\"billable_logical_bytes_written\":0},"
                    + "\"message\":\"ok\",\"rows_affected\":2}"));

    TurbopufferSink<String> sink =
        TurbopufferSink.<String>builder()
            .setNamespace("wikipedia-edits")
            .setApiKey("tpuf_test")
            .setBaseUrl(server.url("/").toString())
            .setMaxBatchSize(100)
            .setSerializationSchema(
                (element, context) ->
                    TurbopufferWrite.upsert(element, Map.of("text", "article " + element)))
            .build();

    try (SinkWriter<String> writer = sink.createWriter(new TestSinkInitContext())) {
      writer.write("one", null);
      writer.write("two", null);
      writer.flush(true);
    }

    RecordedRequest request = server.takeRequest(5, TimeUnit.SECONDS);
    assertThat(request).isNotNull();
    assertThat(request.getPath()).isEqualTo("/v2/namespaces/wikipedia-edits");
    JsonNode body = objectMapper.readTree(request.getBody().readUtf8());
    assertThat(body.get("upsert_rows")).hasSize(2);
  }

  @Test
  @Timeout(10)
  void includesNamespaceSchemaAndDistanceMetric() throws Exception {
    server.enqueue(
        new MockResponse()
            .setHeader("Content-Type", "application/json")
            .setBody(
                "{\"billing\":{\"billable_logical_bytes_written\":0},"
                    + "\"message\":\"ok\",\"rows_affected\":1}"));
    Schema schema =
        Schema.builder()
            .put(
                "text", AttributeSchemaConfig.builder().type("string").fullTextSearch(true).build())
            .build();

    TurbopufferSink<String> sink =
        TurbopufferSink.<String>builder()
            .setNamespace("wikipedia-edits")
            .setApiKey("tpuf_test")
            .setBaseUrl(server.url("/").toString())
            .setSchema(schema)
            .setDistanceMetric(DistanceMetric.COSINE_DISTANCE)
            .setSerializationSchema(
                (element, context) -> TurbopufferWrite.upsert(element, Map.of("text", element)))
            .build();

    try (SinkWriter<String> writer = sink.createWriter(new TestSinkInitContext())) {
      writer.write("one", null);
      writer.flush(true);
    }

    RecordedRequest request = server.takeRequest(5, TimeUnit.SECONDS);
    JsonNode body = objectMapper.readTree(request.getBody().readUtf8());
    assertThat(body.at("/schema/text/full_text_search").booleanValue()).isTrue();
    assertThat(body.get("distance_metric").textValue()).isEqualTo("cosine_distance");
  }

  @Test
  void opensTheSerializationSchemaWhenTheWriterStarts() throws Exception {
    OpeningSchema schema = new OpeningSchema();
    TurbopufferSink<String> sink =
        TurbopufferSink.<String>builder()
            .setNamespace("wikipedia-edits")
            .setApiKey("tpuf_test")
            .setBaseUrl(server.url("/").toString())
            .setSerializationSchema(schema)
            .build();

    try (SinkWriter<String> ignored = sink.createWriter(new TestSinkInitContext())) {
      assertThat(schema.opened).isTrue();
    }
  }

  @Test
  @Timeout(10)
  void retriesRateLimitedBatchesWithoutLosingRecords() throws Exception {
    server.enqueue(
        new MockResponse()
            .setResponseCode(429)
            .setHeader("Content-Type", "application/json")
            .setBody("{\"message\":\"slow down\"}"));
    server.enqueue(
        new MockResponse()
            .setHeader("Content-Type", "application/json")
            .setBody(
                "{\"billing\":{\"billable_logical_bytes_written\":0},"
                    + "\"message\":\"ok\",\"rows_affected\":1}"));
    TurbopufferSink<String> sink =
        TurbopufferSink.<String>builder()
            .setNamespace("wikipedia-edits")
            .setApiKey("tpuf_test")
            .setBaseUrl(server.url("/").toString())
            .setClientMaxRetries(0)
            .setMaxBatchSize(1)
            .setSerializationSchema(
                (element, context) -> TurbopufferWrite.upsert(element, Map.of("text", element)))
            .build();

    try (SinkWriter<String> writer = sink.createWriter(new TestSinkInitContext())) {
      writer.write("one", null);
      writer.flush(true);
    }

    assertThat(server.getRequestCount()).isEqualTo(2);
  }

  @Test
  @Timeout(10)
  void restoredWriterFlushesBufferedCheckpointState() throws Exception {
    server.enqueue(
        new MockResponse()
            .setHeader("Content-Type", "application/json")
            .setBody(
                "{\"billing\":{\"billable_logical_bytes_written\":0},"
                    + "\"message\":\"ok\",\"rows_affected\":1}"));
    TurbopufferSink<String> sink =
        TurbopufferSink.<String>builder()
            .setNamespace("wikipedia-edits")
            .setApiKey("tpuf_test")
            .setBaseUrl(server.url("/").toString())
            .setMaxBatchSize(100)
            .setMaxTimeInBufferMS(60_000)
            .setSerializationSchema(
                (element, context) -> TurbopufferWrite.upsert(element, Map.of("text", element)))
            .build();
    TestSinkInitContext context = new TestSinkInitContext();
    List<BufferedRequestState<TurbopufferRequestEntry>> state;

    try (StatefulSinkWriter<String, BufferedRequestState<TurbopufferRequestEntry>> writer =
        sink.createWriter(context)) {
      writer.write("one", null);
      state = writer.snapshotState(1L);
    }
    assertThat(server.getRequestCount()).isZero();

    try (StatefulSinkWriter<String, BufferedRequestState<TurbopufferRequestEntry>> writer =
        sink.restoreWriter(new TestSinkInitContext(), state)) {
      writer.flush(true);
    }

    RecordedRequest request = server.takeRequest(5, TimeUnit.SECONDS);
    JsonNode body = objectMapper.readTree(request.getBody().readUtf8());
    assertThat(body.at("/upsert_rows/0/id").textValue()).isEqualTo("one");
  }

  @Test
  @Timeout(10)
  void rejectsMalformedRowsWithoutRetrying() throws Exception {
    server.enqueue(
        new MockResponse()
            .setResponseCode(400)
            .setHeader("Content-Type", "application/json")
            .setBody("{\"message\":\"bad row\"}"));
    TurbopufferSink<String> sink =
        TurbopufferSink.<String>builder()
            .setNamespace("wikipedia-edits")
            .setApiKey("tpuf_test")
            .setBaseUrl(server.url("/").toString())
            .setClientMaxRetries(0)
            .setMaxBatchSize(1)
            .setSerializationSchema(
                (element, context) -> TurbopufferWrite.upsert(element, Map.of("text", element)))
            .build();

    try (SinkWriter<String> writer = sink.createWriter(new TestSinkInitContext())) {
      writer.write("one", null);
      assertThatThrownBy(() -> writer.flush(true)).isInstanceOf(Exception.class);
    }

    assertThat(server.getRequestCount()).isEqualTo(1);
  }

  @Test
  @Timeout(10)
  void sendsUpsertsPatchesAndDeletesInOneWriteRequest() throws Exception {
    server.enqueue(
        new MockResponse()
            .setHeader("Content-Type", "application/json")
            .setBody(
                "{\"billing\":{\"billable_logical_bytes_written\":0},"
                    + "\"message\":\"ok\",\"rows_affected\":3}"));
    TurbopufferSink<TurbopufferWrite> sink =
        TurbopufferSink.<TurbopufferWrite>builder()
            .setNamespace("wikipedia-edits")
            .setApiKey("tpuf_test")
            .setBaseUrl(server.url("/").toString())
            .setSerializationSchema((write, context) -> write)
            .build();

    try (SinkWriter<TurbopufferWrite> writer = sink.createWriter(new TestSinkInitContext())) {
      writer.write(TurbopufferWrite.upsert("one", Map.of("text", "new")), null);
      writer.write(TurbopufferWrite.patch("two", Map.of("text", "changed")), null);
      writer.write(TurbopufferWrite.delete(3L), null);
      writer.flush(true);
    }

    RecordedRequest request = server.takeRequest(5, TimeUnit.SECONDS);
    JsonNode body = objectMapper.readTree(request.getBody().readUtf8());
    assertThat(body.get("upsert_rows")).hasSize(1);
    assertThat(body.get("patch_rows")).hasSize(1);
    assertThat(body.get("deletes")).hasSize(1);
  }

  private static final class OpeningSchema implements TurbopufferSerializationSchema<String> {
    private boolean opened;

    @Override
    public void open(WriterInitContext context) {
      opened = true;
    }

    @Override
    public TurbopufferWrite serialize(String element, SinkWriter.Context context) {
      return TurbopufferWrite.upsert(element, Map.of());
    }
  }
}
