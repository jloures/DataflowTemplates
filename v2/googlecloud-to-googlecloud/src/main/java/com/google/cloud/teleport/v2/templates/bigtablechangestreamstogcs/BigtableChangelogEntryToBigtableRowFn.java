package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.transforms.SimpleFunction;

/**
 * A {@link org.apache.beam.sdk.transforms.PTransform} which converts items in a {@link org.apache.beam.sdk.values.PCollection}
 * from {@link com.google.cloud.teleport.bigtable.ChangelogEntry} to {@link com.google.cloud.teleport.bigtable.BigtableRow}
 */
public class BigtableChangelogEntryToBigtableRowFn extends
    SimpleFunction<com.google.cloud.teleport.bigtable.ChangelogEntry, com.google.cloud.teleport.bigtable.BigtableRow> {

  private final String workerId;
  private final AtomicLong counter;
  private final Charset charset;

  public BigtableChangelogEntryToBigtableRowFn(String workerId, AtomicLong counter, Charset charset) {
    this.workerId = workerId;
    this.counter = counter;
    this.charset = charset;
  }

  @Override
  public com.google.cloud.teleport.bigtable.BigtableRow apply(
      com.google.cloud.teleport.bigtable.ChangelogEntry entry
  ) {
    return BigtableUtils.createBigtableRow(entry, workerId, counter.incrementAndGet(), charset);
  }
}
