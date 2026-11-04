package com.turbopuffer.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TurbopufferWriteTest {

  @Test
  void upsertOwnsItsAttributes() {
    Map<String, Object> attributes = new LinkedHashMap<>();
    attributes.put("text", "before");

    TurbopufferWrite write = TurbopufferWrite.upsert("page-1", attributes);
    attributes.put("text", "after");

    assertThat(write.attributes()).containsEntry("text", "before");
  }

  @Test
  void createsPatchOperation() {
    TurbopufferWrite write = TurbopufferWrite.patch(42L, Map.of("last_editor", "Grace Hopper"));

    assertThat(write.operation()).isEqualTo(TurbopufferWrite.Operation.PATCH);
  }

  @Test
  void createsDeleteOperationWithoutAttributes() {
    TurbopufferWrite write = TurbopufferWrite.delete("page-1");

    assertThat(write.attributes()).isEmpty();
  }

  @Test
  void allowsNullAttributeValues() {
    Map<String, Object> attributes = new LinkedHashMap<>();
    attributes.put("category", null);

    TurbopufferWrite write = TurbopufferWrite.upsert("page-1", attributes);

    assertThat(write.attributes()).containsEntry("category", null);
  }
}
