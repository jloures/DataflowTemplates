package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import com.google.cloud.teleport.bigtable.BigtableCell;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.BigtableUtils;

/**
 * {@link ModType} represents the type of Modification that CDC
 * {@link com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation} entries represent.
 */
public enum ModType {
  SET_CELL("SET_CELL"),
  DELETE_FAMILY("DELETE_FAMILY"),
  DELETE_CELLS("DELETE_CELLS"),
  UNKNOWN("UNKNOWN");

  ModType(String propertyName) {
    this.propertyName = propertyName;
  }

  private String propertyName;

  public String getPropertyName() {
    return this.propertyName;
  }

  public ByteBuffer getPropertyNameAsByteBuffer(Charset charset) {
    return BigtableUtils.copyByteBuffer(ByteBuffer.wrap(this.propertyName.getBytes(charset)));
  }
}
