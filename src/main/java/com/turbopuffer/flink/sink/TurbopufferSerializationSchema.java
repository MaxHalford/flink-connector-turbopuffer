package com.turbopuffer.flink.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

/** Converts an input element into one turbopuffer write operation. */
@FunctionalInterface
@PublicEvolving
public interface TurbopufferSerializationSchema<IN> extends Function {

  /** Initializes this schema when its sink writer starts. */
  default void open(WriterInitContext context) throws Exception {}

  /** Converts one stream element into an upsert, patch, or delete. */
  TurbopufferWrite serialize(IN element, SinkWriter.Context context);
}
