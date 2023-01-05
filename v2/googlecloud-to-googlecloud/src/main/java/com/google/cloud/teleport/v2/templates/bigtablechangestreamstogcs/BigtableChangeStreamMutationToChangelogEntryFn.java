package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import java.util.List;
import org.apache.beam.sdk.transforms.SimpleFunction;

/**
 * Class used as a {@link org.apache.beam.sdk.transforms.PTransform} to get
 * valid {@link com.google.cloud.teleport.bigtable.ChangelogEntry} objects based on the parameters
 * provided by the pipeline.
 */
public class BigtableChangeStreamMutationToChangelogEntryFn extends
    SimpleFunction<ChangeStreamMutation, List<com.google.cloud.teleport.bigtable.ChangelogEntry>> {

  private final BigtableUtils bigtableUtils;

  public BigtableChangeStreamMutationToChangelogEntryFn(BigtableUtils bigtableUtils) {
    this.bigtableUtils = bigtableUtils;
  }

  @Override
  public List<com.google.cloud.teleport.bigtable.ChangelogEntry> apply(
      ChangeStreamMutation mutation) {
    return this.bigtableUtils.getValidEntries(mutation);
  }
}