# Flink connector for turbopuffer

An asynchronous Apache Flink DataStream sink for writing documents to
[turbopuffer](https://turbopuffer.com). It batches upserts, patches, and deletes, applies
backpressure, retries transient failures, and preserves buffered writes in Flink checkpoints.

This is an MVP targeting Flink 2.3 and Java 17. It currently provides a fixed-namespace
DataStream sink; it does not yet provide a Table/SQL connector or a source.

## Quick start

The connector is not published to Maven Central yet. Install the snapshot locally:

```shell
./mvnw install
```

Then add it to your Flink application:

```xml
<dependency>
  <groupId>com.turbopuffer</groupId>
  <artifactId>flink-connector-turbopuffer</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Set `TURBOPUFFER_API_KEY` and, if needed, `TURBOPUFFER_REGION` in every Flink TaskManager.
Using environment variables keeps the API key out of the serialized job graph.

```java
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.turbopuffer.flink.sink.TurbopufferSink;
import com.turbopuffer.flink.sink.TurbopufferWrite;

import java.util.Map;

record Article(String id, String title, String text) {}

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStream<Article> articles =
        env.fromElements(new Article("42", "Apache Flink", "A stream processor"));

TurbopufferSink<Article> sink =
        TurbopufferSink.<Article>builder()
                .setNamespace("wikipedia-edits")
                .setSerializationSchema(
                        (article, context) ->
                                TurbopufferWrite.upsert(
                                        article.id(),
                                        Map.of(
                                                "title", article.title(),
                                                "text", article.text())))
                .build();

articles.sinkTo(sink).name("turbopuffer");
env.execute("Wikipedia edits to turbopuffer");
```

Use stable document IDs, such as the Wikipedia page ID, so replaying a checkpoint converges on
the same documents.

## Operations

The serialization schema produces one operation for each input element:

```java
TurbopufferWrite.upsert("page-42", Map.of("title", "Flink"));
TurbopufferWrite.patch("page-42", Map.of("title", "Apache Flink"));
TurbopufferWrite.delete("page-42");
```

Document IDs can be strings or 64-bit integers. The connector rejects an `id` entry in the
attribute map because the factory method owns that field. A patch can use `null` values when the
turbopuffer API operation needs them.

You can apply a turbopuffer schema and distance metric with every batch:

```java
import com.turbopuffer.models.namespaces.AttributeSchemaConfig;
import com.turbopuffer.models.namespaces.DistanceMetric;
import com.turbopuffer.models.namespaces.Schema;

Schema schema =
        Schema.builder()
                .put(
                        "text",
                        AttributeSchemaConfig.builder()
                                .type("string")
                                .fullTextSearch(true)
                                .build())
                .build();

TurbopufferSink<Article> sink =
        TurbopufferSink.<Article>builder()
                .setNamespace("wikipedia-edits")
                .setSchema(schema)
                .setDistanceMetric(DistanceMetric.COSINE_DISTANCE)
                .setSerializationSchema(/* ... */)
                .build();
```

## Delivery behavior

The connector provides at-least-once delivery. Flink checkpoints retain buffered requests, and
the sink flushes in-flight work at checkpoint boundaries. A failure after turbopuffer accepts a
request but before Flink records completion can replay that request.

Upserts with deterministic IDs are replay-safe for final document state. Patches and deletes
should also be designed to tolerate replays. Concurrent writes to the same ID can arrive out of
order; key or otherwise serialize those records upstream when order matters.

HTTP 408, 409, 429, and 5xx responses, client I/O failures, and retryable SDK failures are retried.
Other 4xx responses fail the Flink job so malformed data is not retried forever.

## Configuration

| Builder method | Default | Purpose |
| --- | ---: | --- |
| `setNamespace` | required | Fixed destination namespace |
| `setSerializationSchema` | required | Maps each stream element to one write |
| `setApiKey` | `TURBOPUFFER_API_KEY` | Explicit API key; environment is safer |
| `setRegion` | SDK/environment default | turbopuffer region |
| `setBaseUrl` | SDK default | Custom API endpoint |
| `setSchema` | unset | Schema updates sent with each batch |
| `setDistanceMetric` | unset | Namespace distance metric |
| `setMaxBatchSize` | 1,000 | Maximum records per API request |
| `setMaxBatchSizeInBytes` | 16 MiB | Maximum serialized bytes per request |
| `setMaxInFlightRequests` | 4 | Concurrent API requests per sink subtask |
| `setMaxBufferedRequests` | 10,000 | Records buffered per sink subtask |
| `setMaxTimeInBufferMS` | 1,000 ms | Maximum batching delay |
| `setMaxRecordSizeInBytes` | 4 MiB | Maximum serialized record size |
| `setClientMaxRetries` | 4 | Retries inside the turbopuffer SDK |
| `setRequestTimeout` | 2 minutes | Per-request timeout |

The sink registers `numRequestsSubmitted`, `numRecordsRetried`, and `numRowsAffected` counters.
Fatal records increment Flink's standard `numRecordsOutErrors` counter.

## Development

Copy `.env.example` to `.env` and add a turbopuffer API key to run the live integration test.
The test creates a unique namespace and removes it afterward. It defaults to `gcp-europe-west3`;
set `TURBOPUFFER_REGION` to use another region.

```shell
./mvnw test    # unit and local HTTP integration tests
./mvnw verify  # also runs the live API test when a key is available
```

The CI workflow runs `verify` without credentials, so the live test is skipped there. The local
HTTP tests cover batching, all write operations, checkpoint restoration, schema updates, retries,
and fatal error handling.

## License

Apache License 2.0.
