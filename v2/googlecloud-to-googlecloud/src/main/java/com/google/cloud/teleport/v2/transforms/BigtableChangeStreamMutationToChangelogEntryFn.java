package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.teleport.v2.utils.BigtableUtils;
import java.util.HashSet;
import java.util.List;
import org.apache.beam.sdk.transforms.SimpleFunction;

public class BigtableChangeStreamMutationToChangelogEntryFn extends
    SimpleFunction<ChangeStreamMutation, List<com.google.cloud.teleport.bigtable.ChangelogEntry>> {

  private final HashSet<String> ignoreColumns;
  private final HashSet<String> ignoreColumnFamilies;

  public BigtableChangeStreamMutationToChangelogEntryFn(HashSet<String> ignoreColumns,
      HashSet<String> ignoreColumnFamilies) {
    this.ignoreColumns = ignoreColumns;
    this.ignoreColumnFamilies = ignoreColumnFamilies;
  }

  @Override
  public List<com.google.cloud.teleport.bigtable.ChangelogEntry> apply(
      ChangeStreamMutation mutation) {
    return BigtableUtils.getValidEntries(
        mutation,
        ignoreColumns,
        ignoreColumnFamilies
    );
  }
}