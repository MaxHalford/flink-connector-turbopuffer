package com.turbopuffer.flink.sink;

import java.io.Serializable;

final class TurbopufferWriteOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  private final byte[] schemaJson;
  private final String distanceMetric;

  TurbopufferWriteOptions(byte[] schemaJson, String distanceMetric) {
    this.schemaJson = schemaJson == null ? null : schemaJson.clone();
    this.distanceMetric = distanceMetric;
  }

  byte[] schemaJson() {
    return schemaJson == null ? null : schemaJson.clone();
  }

  String distanceMetric() {
    return distanceMetric;
  }
}
