/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.transforms;

import static com.google.cloud.teleport.v2.utils.WriteToGCSUtility.BigtableSchemaFormat.BIGTABLEROW;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.teleport.bigtable.BigtableRow;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.v2.options.BigtableChangeStreamsToGcsFilterOptions;
import com.google.cloud.teleport.v2.utils.BigtableUtils;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility.BigtableSchemaFormat;
import com.google.gson.Gson;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link WriteChangeStreamMutationsToGcsText} class is a {@link PTransform} that takes in {@link
 * PCollection} of Bigtable change stream mutations. The transform converts and writes these records to
 * GCS in JSON text file format.
 */
@AutoValue
public abstract class WriteChangeStreamMutationsToGcsText
    extends PTransform<PCollection<ChangeStreamMutation>, PDone> {
  private static final Gson gson = new Gson();
  
  private static final AtomicLong counter = new AtomicLong(0);
  
  private static final String workerId = UUID.randomUUID().toString();

  @VisibleForTesting
  protected static final String DEFAULT_OUTPUT_FILE_PREFIX = "output";

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(WriteChangeStreamMutationsToGcsText.class);

  public static WriteToGcsBuilder newBuilder() {
    return new AutoValue_WriteChangeStreamMutationsToGcsText.Builder();
  }

  public abstract String gcsOutputDirectory();

  public abstract String outputFilenamePrefix();

  public abstract String tempLocation();

  public abstract Integer numShards();

  public abstract HashSet<String> ignoreColumnFamilies();

  public abstract HashSet<String> ignoreColumns();

  public abstract BigtableSchemaFormat schemaOutputFormat();

  public abstract Charset charset();

  @Override
  public PDone expand(PCollection<ChangeStreamMutation> mutations) {
    PCollection<com.google.cloud.teleport.bigtable.ChangelogEntry> changelogEntry = mutations
        .apply("ChangeStreamMutation to ChangelogEntry",
            FlatMapElements.via(new BigtableChangeStreamMutationToChangelogEntryFn(ignoreColumns(),
                ignoreColumnFamilies(), charset())));
    /*
     * Writing as text file using {@link TextIO}.
     *
     * The {@link WindowedFilenamePolicy} class specifies the file path for writing the file.
     * The {@link withNumShards} option specifies the number of shards passed by the user.
     * The {@link withTempDirectory} option sets the base directory used to generate temporary files.
     */
    if (schemaOutputFormat() == BIGTABLEROW) {
      return changelogEntry
          .apply("ChangelogEntry to BigtableRow",
              MapElements.via(new BigtableChangelogEntryToBigtableRowFn(workerId, counter)))
          .apply(MapElements.into(TypeDescriptors.strings())
              .via((com.google.cloud.teleport.bigtable.BigtableRow row) -> gson.toJson(row,
                  com.google.cloud.teleport.bigtable.BigtableRow.class)))
          .apply(
              "Writing as Text",
              TextIO.write()
                  .to(
                      WindowedFilenamePolicy.writeWindowedFiles()
                          .withOutputDirectory(gcsOutputDirectory())
                          .withOutputFilenamePrefix(outputFilenamePrefix())
                          .withShardTemplate(WriteToGCSUtility.SHARD_TEMPLATE)
                          .withSuffix(
                              WriteToGCSUtility.FILE_SUFFIX_MAP.get(
                                  WriteToGCSUtility.FileFormat.TEXT)))
                  .withTempDirectory(
                      FileBasedSink.convertToFileResourceIfPossible(tempLocation())
                          .getCurrentDirectory())
                  .withWindowedWrites()
                  .withNumShards(numShards()));
    }

    return changelogEntry
        .apply(MapElements.into(TypeDescriptors.strings())
            .via((com.google.cloud.teleport.bigtable.ChangelogEntry row) -> gson.toJson(row,
                com.google.cloud.teleport.bigtable.ChangelogEntry.class)))
        .apply(
            "Writing as Text",
            TextIO.write()
                .to(
                    WindowedFilenamePolicy.writeWindowedFiles()
                        .withOutputDirectory(gcsOutputDirectory())
                        .withOutputFilenamePrefix(outputFilenamePrefix())
                        .withShardTemplate(WriteToGCSUtility.SHARD_TEMPLATE)
                        .withSuffix(
                            WriteToGCSUtility.FILE_SUFFIX_MAP.get(
                                WriteToGCSUtility.FileFormat.TEXT)))
                .withTempDirectory(
                    FileBasedSink.convertToFileResourceIfPossible(tempLocation())
                        .getCurrentDirectory())
                .withWindowedWrites()
                .withNumShards(numShards()));
  }

  private class BigtableChangelogEntryToJsonTextFn extends
      SimpleFunction<com.google.cloud.teleport.bigtable.ChangelogEntry, String> {

    @Override
    public String apply(com.google.cloud.teleport.bigtable.ChangelogEntry entry) {
      return gson.toJson(
          BigtableUtils.createBigtableRow(entry, workerId, counter, charset()),
          BigtableRow.class
      );
    }
  }

  /** Builder for {@link WriteChangeStreamMutationsToGcsText}. */
  @AutoValue.Builder
  public abstract static class WriteToGcsBuilder {

    abstract WriteToGcsBuilder setGcsOutputDirectory(String gcsOutputDirectory);

    abstract String gcsOutputDirectory();

    abstract WriteToGcsBuilder setTempLocation(String tempLocation);

    abstract String tempLocation();

    abstract WriteToGcsBuilder setOutputFilenamePrefix(String outputFilenamePrefix);

    abstract WriteToGcsBuilder setNumShards(Integer numShards);

    abstract WriteToGcsBuilder setSchemaOutputFormat(BigtableSchemaFormat schema);

    abstract WriteToGcsBuilder setCharset(Charset charset);

    abstract WriteChangeStreamMutationsToGcsText autoBuild();

    public WriteToGcsBuilder withCharset(Charset charset) {
      return setCharset(charset);
    }

    public WriteToGcsBuilder withGcsOutputDirectory(String gcsOutputDirectory) {
      checkArgument(
          gcsOutputDirectory != null,
          "withGcsOutputDirectory(gcsOutputDirectory) called with null input.");
      return setGcsOutputDirectory(gcsOutputDirectory);
    }

    public WriteToGcsBuilder withTempLocation(String tempLocation) {
      checkArgument(tempLocation != null, "withTempLocation(tempLocation) called with null input.");
      return setTempLocation(tempLocation);
    }

    public WriteToGcsBuilder withOutputFilenamePrefix(String outputFilenamePrefix) {
      if (outputFilenamePrefix == null) {
        LOG.info("Defaulting output filename prefix to: {}", DEFAULT_OUTPUT_FILE_PREFIX);
        outputFilenamePrefix = DEFAULT_OUTPUT_FILE_PREFIX;
      }
      return setOutputFilenamePrefix(outputFilenamePrefix);
    }

    public WriteToGcsBuilder withSchemaOutputFormat(BigtableSchemaFormat schema) {
      return setSchemaOutputFormat(schema);
    }

    public WriteChangeStreamMutationsToGcsText build() {
      checkNotNull(gcsOutputDirectory(), "Provide output directory to write to.");
      checkNotNull(tempLocation(), "Temporary directory needs to be provided.");
      return autoBuild();
    }

    abstract WriteToGcsBuilder setIgnoreColumnFamilies(
        HashSet<String> ignoreColumnFamilies);

    abstract WriteToGcsBuilder setIgnoreColumns(HashSet<String> ignoreColumns);

    public WriteToGcsBuilder withIgnoreColumnFamilies(String ignoreColumnFamilies) {
      checkArgument(ignoreColumnFamilies != null, "withIgnoreColumnFamilies(ignoreColumnFamilies) called with null input.");
      HashSet<String> parsedColumnFamilies = new HashSet<>();
      for (String word : ignoreColumnFamilies.split(",")) {
        String columnFamily = word.trim();
        if (columnFamily.length() == 0) {
          continue;
        }
        parsedColumnFamilies.add(columnFamily);
      }
      return setIgnoreColumnFamilies(parsedColumnFamilies);
    }

    public WriteToGcsBuilder withIgnoreColumns(String ignoreColumns) {
      checkArgument(ignoreColumns != null, "withIgnoreColumns(ignoreColumns) called with null input.");
      HashSet<String> parsedColumns = new HashSet<>();
      for (String word : ignoreColumns.split(",")) {
        String trimmedColumns = word.trim();
        if (trimmedColumns.length() == 0) {
          continue;
        }
        checkArgument(trimmedColumns.matches(BigtableUtils.columnPattern), "The Column specified does not follow the required format of 'cf1:c1, cf2:c2 ...'");
        parsedColumns.add(trimmedColumns);
      }
      return setIgnoreColumns(parsedColumns);
    }
  }
}
