package com.turbopuffer.flink.sink;

import java.io.Serializable;
import java.time.Duration;

record TurbopufferClientOptions(
    String apiKey, String region, String baseUrl, int maxRetries, Duration requestTimeout)
    implements Serializable {

  private static final long serialVersionUID = 1L;
}
