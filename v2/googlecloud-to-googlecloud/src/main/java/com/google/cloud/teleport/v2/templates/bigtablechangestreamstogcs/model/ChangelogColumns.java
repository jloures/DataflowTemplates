package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public enum ChangelogColumns {
  ROW_KEY("row_key"),
  MOD_TYPE("mod_type"),
  IS_GC("is_gc"),
  TIEBREAKER("tiebreaker"),
  COMMIT_TIMESTAMP("commit_timestamp"),
  COLUMN_FAMILY("column_family"),
  LOW_WATERMARK("low_watermark"),
  COLUMN("column"),
  TIMESTAMP("timestamp"),
  TIMESTAMP_FROM("timestamp_from"),
  TIMESTAMP_TO("timestamp_to"),
  VALUE("value");

  ChangelogColumns(String columnName) {
    this.columnName = columnName;
  }

  private String columnName;

  public String getColumnName() {
    return this.columnName;
  }

  public ByteBuffer getColumnNameAsByteBuffer() {
    return ByteBuffer.wrap(this.columnName.getBytes(StandardCharsets.UTF_8));
  }
}
