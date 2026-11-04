package com.turbopuffer.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;
import org.junit.jupiter.api.Test;

class TurbopufferRequestStateSerializerTest {

  @Test
  void restoresBufferedWritesFromVersionOneState() throws Exception {
    byte[] json = "{\"id\":\"page-1\",\"text\":\"hello\"}".getBytes(StandardCharsets.UTF_8);
    TurbopufferRequestEntry entry =
        new TurbopufferRequestEntry(TurbopufferWrite.Operation.UPSERT, json);
    BufferedRequestState<TurbopufferRequestEntry> state =
        new BufferedRequestState<>(List.of(new RequestEntryWrapper<>(entry, entry.sizeInBytes())));
    TurbopufferRequestStateSerializer serializer = new TurbopufferRequestStateSerializer();

    byte[] checkpoint = serializer.serialize(state);
    BufferedRequestState<TurbopufferRequestEntry> restored = serializer.deserialize(1, checkpoint);

    assertThat(restored.getBufferedRequestEntries().get(0).getRequestEntry()).isEqualTo(entry);
  }
}
