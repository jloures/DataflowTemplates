package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.teleport.v2.utils.BigtableUtils;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.transforms.SimpleFunction;

public class BigtableChangelogEntryToBigtableRowFn extends
    SimpleFunction<com.google.cloud.teleport.bigtable.ChangelogEntry, com.google.cloud.teleport.bigtable.BigtableRow> implements
    com.google.cloud.teleport.v2.utils.ChangelogEntryToBigtableRowFn {

  private final String workerId;
  private final AtomicLong counter;

  public BigtableChangelogEntryToBigtableRowFn(String workerId, AtomicLong counter) {
    this.workerId = workerId;
    this.counter = counter;
  }

  @Override
  public com.google.cloud.teleport.bigtable.BigtableRow apply(
      com.google.cloud.teleport.bigtable.ChangelogEntry entry
  ) {
    return BigtableUtils.createBigtableRow(entry, workerId, counter);
  }
}