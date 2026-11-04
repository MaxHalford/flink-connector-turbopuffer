package com.turbopuffer.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.turbopuffer.client.TurbopufferClient;
import com.turbopuffer.client.okhttp.TurbopufferOkHttpClient;
import com.turbopuffer.errors.NotFoundException;
import com.turbopuffer.models.namespaces.Filter;
import com.turbopuffer.models.namespaces.NamespaceQueryParams;
import com.turbopuffer.models.namespaces.Row;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class TurbopufferSinkIT {

  @Test
  @Timeout(60)
  void writesDocumentsToTurbopuffer() throws Exception {
    String apiKey = setting("TURBOPUFFER_API_KEY");
    Assumptions.assumeTrue(apiKey != null, "TURBOPUFFER_API_KEY is not configured");
    String region = setting("TURBOPUFFER_REGION");
    if (region == null) {
      region = "gcp-europe-west3";
    }
    String namespace = "flink-connector-it-" + UUID.randomUUID().toString().replace("-", "");

    TurbopufferClient verificationClient =
        TurbopufferOkHttpClient.builder()
            .apiKey(apiKey)
            .region(region)
            .timeout(Duration.ofSeconds(30))
            .build();
    try {
      TurbopufferSink<String> sink =
          TurbopufferSink.<String>builder()
              .setNamespace(namespace)
              .setApiKey(apiKey)
              .setRegion(region)
              .setMaxBatchSize(2)
              .setSerializationSchema(
                  (element, context) ->
                      TurbopufferWrite.upsert(element, Map.of("text", "article " + element)))
              .build();

      try (SinkWriter<String> writer = sink.createWriter(new TestSinkInitContext())) {
        writer.write("one", null);
        writer.write("two", null);
        writer.flush(true);
      }

      List<Row> rows =
          verificationClient
              .namespace(namespace)
              .query(
                  NamespaceQueryParams.builder()
                      .consistencyStrong()
                      .filters(Filter.in("id", List.of("one", "two")))
                      .includeAttributes("text")
                      .topK(10)
                      .build())
              .rows()
              .orElseThrow();

      assertThat(rows)
          .extracting(row -> row.get("text").convert(String.class))
          .containsExactlyInAnyOrder("article one", "article two");
    } finally {
      try {
        try {
          verificationClient.namespace(namespace).deleteAll();
        } catch (NotFoundException ignored) {
          // The test can fail before the first write creates the namespace.
        }
      } finally {
        verificationClient.close();
      }
    }
  }

  private static String setting(String name) throws Exception {
    String environmentValue = System.getenv(name);
    if (environmentValue != null && !environmentValue.isBlank()) {
      return environmentValue;
    }

    Path dotenv = Path.of(".env");
    if (!Files.exists(dotenv)) {
      return null;
    }
    for (String line : Files.readAllLines(dotenv)) {
      String prefix = name + "=";
      if (line.startsWith(prefix)) {
        String value = line.substring(prefix.length()).trim();
        if ((value.startsWith("\"") && value.endsWith("\""))
            || (value.startsWith("'") && value.endsWith("'"))) {
          value = value.substring(1, value.length() - 1);
        }
        return value.isBlank() ? null : value;
      }
    }
    return null;
  }
}
