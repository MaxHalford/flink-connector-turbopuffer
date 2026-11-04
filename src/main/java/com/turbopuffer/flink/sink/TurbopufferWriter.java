package com.turbopuffer.flink.sink;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.turbopuffer.client.TurbopufferClientAsync;
import com.turbopuffer.client.okhttp.TurbopufferOkHttpClientAsync;
import com.turbopuffer.errors.TurbopufferIoException;
import com.turbopuffer.errors.TurbopufferRetryableException;
import com.turbopuffer.errors.TurbopufferServiceException;
import com.turbopuffer.models.namespaces.DistanceMetric;
import com.turbopuffer.models.namespaces.NamespaceWriteParams;
import com.turbopuffer.models.namespaces.Row;
import com.turbopuffer.models.namespaces.Schema;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.DequeRequestBuffer;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.SimpleBatchCreator;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.metrics.Counter;

final class TurbopufferWriter<IN> extends AsyncSinkWriter<IN, TurbopufferRequestEntry> {

  private static final TypeReference<LinkedHashMap<String, Object>> ROW_TYPE =
      new TypeReference<>() {};

  private final TurbopufferClientAsync client;
  private final String namespace;
  private final JsonMapper jsonMapper;
  private final Counter requestsSubmitted;
  private final Counter recordsRetried;
  private final Counter recordsFailed;
  private final Counter rowsAffected;
  private final Schema schema;
  private final DistanceMetric distanceMetric;

  TurbopufferWriter(
      ElementConverter<IN, TurbopufferRequestEntry> elementConverter,
      WriterInitContext context,
      int maxBatchSize,
      int maxInFlightRequests,
      int maxBufferedRequests,
      long maxBatchSizeInBytes,
      long maxTimeInBufferMs,
      long maxRecordSizeInBytes,
      long requestTimeoutMs,
      String namespace,
      TurbopufferClientOptions clientOptions,
      TurbopufferWriteOptions writeOptions,
      Collection<BufferedRequestState<TurbopufferRequestEntry>> recoveredState) {
    super(
        elementConverter,
        context,
        AsyncSinkWriterConfiguration.builder()
            .setMaxBatchSize(maxBatchSize)
            .setMaxBatchSizeInBytes(maxBatchSizeInBytes)
            .setMaxInFlightRequests(maxInFlightRequests)
            .setMaxBufferedRequests(maxBufferedRequests)
            .setMaxTimeInBufferMS(maxTimeInBufferMs)
            .setMaxRecordSizeInBytes(maxRecordSizeInBytes)
            .setRequestTimeoutMS(requestTimeoutMs)
            .setFailOnTimeout(true)
            .build(),
        recoveredState,
        new SimpleBatchCreator<>(maxBatchSizeInBytes),
        new DequeRequestBuffer<>());

    this.namespace = namespace;
    this.jsonMapper = JsonMapper.builder().findAndAddModules().build();
    this.client = createClient(namespace, clientOptions);
    this.requestsSubmitted = context.metricGroup().counter("numRequestsSubmitted");
    this.recordsRetried = context.metricGroup().counter("numRecordsRetried");
    this.recordsFailed = context.metricGroup().getNumRecordsOutErrorsCounter();
    this.rowsAffected = context.metricGroup().counter("numRowsAffected");
    this.schema = readSchema(writeOptions.schemaJson());
    this.distanceMetric =
        writeOptions.distanceMetric() == null
            ? null
            : DistanceMetric.of(writeOptions.distanceMetric());
  }

  @Override
  protected void submitRequestEntries(
      List<TurbopufferRequestEntry> requestEntries,
      ResultHandler<TurbopufferRequestEntry> resultHandler) {
    requestsSubmitted.inc();

    final NamespaceWriteParams params;
    try {
      params = createWriteParams(requestEntries);
    } catch (Exception e) {
      recordsFailed.inc(requestEntries.size());
      resultHandler.completeExceptionally(e);
      return;
    }

    client
        .namespace(namespace)
        .write(params)
        .whenComplete(
            (response, error) -> {
              if (error != null) {
                handleFailure(requestEntries, resultHandler, error);
                return;
              }
              try {
                rowsAffected.inc(response.rowsAffected());
                resultHandler.complete();
              } catch (Exception e) {
                recordsFailed.inc(requestEntries.size());
                resultHandler.completeExceptionally(e);
              }
            });
  }

  @Override
  protected long getSizeInBytes(TurbopufferRequestEntry requestEntry) {
    return requestEntry.sizeInBytes();
  }

  @Override
  public void close() {
    client.close();
  }

  private NamespaceWriteParams createWriteParams(List<TurbopufferRequestEntry> entries)
      throws IOException {
    NamespaceWriteParams.Builder builder = NamespaceWriteParams.builder();
    if (schema != null) {
      builder.schema(schema);
    }
    if (distanceMetric != null) {
      builder.distanceMetric(distanceMetric);
    }
    for (TurbopufferRequestEntry entry : entries) {
      switch (entry.operation()) {
        case UPSERT -> builder.addUpsertRow(readRow(entry));
        case PATCH -> builder.addPatchRow(readRow(entry));
        case DELETE -> addDelete(builder, entry);
      }
    }
    return builder.build();
  }

  private Schema readSchema(byte[] schemaJson) {
    if (schemaJson == null) {
      return null;
    }
    try {
      return jsonMapper.readValue(schemaJson, Schema.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to deserialize namespace schema", e);
    }
  }

  private Row readRow(TurbopufferRequestEntry entry) throws IOException {
    Map<String, Object> values = jsonMapper.readValue(entry.json(), ROW_TYPE);
    return Row.builder().putAll(values).build();
  }

  private void addDelete(NamespaceWriteParams.Builder builder, TurbopufferRequestEntry entry)
      throws IOException {
    JsonNode id = jsonMapper.readTree(entry.json());
    if (id.isTextual()) {
      builder.addDelete(id.textValue());
    } else if (id.isIntegralNumber() && id.canConvertToLong()) {
      builder.addDelete(id.longValue());
    } else {
      throw new IOException("turbopuffer document ID must be a string or a 64-bit integer");
    }
  }

  private void handleFailure(
      List<TurbopufferRequestEntry> entries,
      ResultHandler<TurbopufferRequestEntry> resultHandler,
      Throwable error) {
    Throwable cause = unwrap(error);
    if (isRetryable(cause)) {
      recordsRetried.inc(entries.size());
      resultHandler.retryForEntries(entries);
      return;
    }

    recordsFailed.inc(entries.size());
    resultHandler.completeExceptionally(asException(cause));
  }

  private static boolean isRetryable(Throwable error) {
    if (error instanceof TurbopufferIoException || error instanceof TurbopufferRetryableException) {
      return true;
    }
    if (error instanceof TurbopufferServiceException serviceException) {
      int status = serviceException.statusCode();
      return status == 408 || status == 409 || status == 429 || status >= 500;
    }
    return false;
  }

  private static Throwable unwrap(Throwable error) {
    Throwable current = error;
    while ((current instanceof CompletionException || current instanceof ExecutionException)
        && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  private static Exception asException(Throwable error) {
    return error instanceof Exception exception ? exception : new RuntimeException(error);
  }

  private static TurbopufferClientAsync createClient(
      String namespace, TurbopufferClientOptions options) {
    TurbopufferOkHttpClientAsync.Builder builder =
        TurbopufferOkHttpClientAsync.builder()
            .fromEnv()
            .defaultNamespace(namespace)
            .maxRetries(options.maxRetries())
            .timeout(options.requestTimeout());

    if (options.apiKey() != null) {
      builder.apiKey(options.apiKey());
    }
    if (options.region() != null) {
      builder.region(options.region());
    }
    if (options.baseUrl() != null) {
      builder.baseUrl(options.baseUrl());
    }
    return builder.build();
  }
}
