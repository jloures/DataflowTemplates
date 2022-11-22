package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

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

  public ByteBuffer getPropertyNameAsByteBuffer() {
    return ByteBuffer.wrap(this.propertyName.getBytes(StandardCharsets.UTF_8));
  }
}
