package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.BigtableUtils;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.transforms.SimpleFunction;

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