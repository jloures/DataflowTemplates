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
import static com.google.cloud.teleport.v2.utils.WriteToGCSUtility.BigtableSchemaFormat.SIMPLE;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.transforms.FlatMapElements;
import com.google.auto.value.AutoValue;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.teleport.bigtable.BigtableRow;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model.ChangelogEntry;
import com.google.cloud.teleport.v2.utils.BigtableUtils;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility.BigtableSchemaFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link WriteChangeStreamMutationToGcsAvro} class is a {@link PTransform} that takes in {@link
 * PCollection} of Bigtable Change Stream Mutations. The transform converts and writes these records to
 * GCS in avro file format.
 */
@AutoValue
public abstract class WriteChangeStreamMutationToGcsAvro
    extends PTransform<PCollection<ChangeStreamMutation>, PDone> {
  @VisibleForTesting protected static final String DEFAULT_OUTPUT_FILE_PREFIX = "output";
  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(WriteChangeStreamMutationToGcsAvro.class);
  private static final long serialVersionUID = 825905520835363852L;

  private static final AtomicLong counter = new AtomicLong(0);

  private static final String workerId = UUID.randomUUID().toString();

  public static WriteToGcsBuilder newBuilder() {
    return new AutoValue_WriteChangeStreamMutationToGcsAvro.Builder();
  }

  public abstract String gcsOutputDirectory();

  public abstract String outputFilenamePrefix();

  public abstract String tempLocation();

  public abstract Integer numShards();

  public abstract HashSet<String> ignoreColumnFamilies();

  public abstract HashSet<String> ignoreColumns();

  public abstract BigtableSchemaFormat schemaOutputFormat();

  @Override
  public PDone expand(PCollection<ChangeStreamMutation> mutations) {
    if (schemaOutputFormat() == BIGTABLEROW) {
      return mutations
          /*
           * Writing as avro file using {@link AvroIO}.
           *
           * The {@link WindowedFilenamePolicy} class specifies the file path for writing the file.
           * The {@link withNumShards} option specifies the number of shards passed by the user.
           * The {@link withTempDirectory} option sets the base directory used to generate temporary files.
           */
          .apply("Transform to Avro BigtableRow",
              FlatMapElements.via(new ProcessChangeStreamMutationToGcsAvroBigtableRowSchemaFormat()))
          .apply(
              "Writing as Avro",
              AvroIO.write(com.google.cloud.teleport.bigtable.BigtableRow.class)
                  .to(
                      WindowedFilenamePolicy.writeWindowedFiles()
                          .withOutputDirectory(gcsOutputDirectory())
                          .withOutputFilenamePrefix(outputFilenamePrefix())
                          .withShardTemplate(WriteToGCSUtility.SHARD_TEMPLATE)
                          .withSuffix(
                              WriteToGCSUtility.FILE_SUFFIX_MAP.get(
                                  WriteToGCSUtility.FileFormat.AVRO)))
                  .withTempDirectory(
                      FileBasedSink.convertToFileResourceIfPossible(tempLocation())
                          .getCurrentDirectory())
                  .withWindowedWrites()
                  .withNumShards(numShards()));
    } else {
      return mutations
          /*
           * Writing as avro file using {@link AvroIO}.
           *
           * The {@link WindowedFilenamePolicy} class specifies the file path for writing the file.
           * The {@link withNumShards} option specifies the number of shards passed by the user.
           * The {@link withTempDirectory} option sets the base directory used to generate temporary files.
           */
          //filtering will return a common Class of PCollection ChangelogEntry
          .apply("Transform to Avro Simple",
              FlatMapElements.via(new ProcessChangeStreamMutationToGcsAvroSimpleSchemaFormat()))
          // Do i have to format
          .apply(
              "Writing as Avro",
              AvroIO.write(ChangelogEntry.class)
                  .to(
                      WindowedFilenamePolicy.writeWindowedFiles()
                          .withOutputDirectory(gcsOutputDirectory())
                          .withOutputFilenamePrefix(outputFilenamePrefix())
                          .withShardTemplate(WriteToGCSUtility.SHARD_TEMPLATE)
                          .withSuffix(
                              WriteToGCSUtility.FILE_SUFFIX_MAP.get(
                                  WriteToGCSUtility.FileFormat.AVRO)))
                  .withTempDirectory(
                      FileBasedSink.convertToFileResourceIfPossible(tempLocation())
                          .getCurrentDirectory())
                  .withWindowedWrites()
                  .withNumShards(numShards()));
    }
  }

  private class ProcessChangeStreamMutationToGcsAvroSimpleSchemaFormat extends
      SimpleFunction<ChangeStreamMutation, List<ChangelogEntry>> {

    @Override
    public List<ChangelogEntry> apply(ChangeStreamMutation mutation) {
      return BigtableUtils.getValidEntries(
          mutation,
          ignoreColumns(),
          ignoreColumnFamilies()
      );
    }
  }

  private class ProcessChangeStreamMutationToGcsAvroBigtableRowSchemaFormat extends
      SimpleFunction<ChangeStreamMutation, List<com.google.cloud.teleport.bigtable.BigtableRow>> {

    @Override
    public List<com.google.cloud.teleport.bigtable.BigtableRow> apply(
        ChangeStreamMutation mutation) {
      List<ChangelogEntry> validEntries = BigtableUtils.getValidEntries(
          mutation,
          ignoreColumns(),
          ignoreColumnFamilies()
      );
      List<com.google.cloud.teleport.bigtable.BigtableRow> rowEntries = new ArrayList<>();
      for (ChangelogEntry entry : validEntries) {
        rowEntries.add(
            BigtableUtils.createBigtableRow(mutation, entry, workerId, counter)
        );
      }
      return rowEntries;
    }
  }
  /**
   * The {@link WriteToGcsAvroOptions} interface provides the custom execution options passed by the
   * executor at the command-line.
   */
  public interface WriteToGcsAvroOptions extends PipelineOptions {
    @TemplateParameter.GcsWriteFolder(
        order = 1,
        description = "Output file directory in Cloud Storage",
        helpText =
            "The path and filename prefix for writing output files. Must end with a slash. DateTime formatting is used to parse directory path for date & time formatters.",
        example = "gs://your-bucket/your-path")
    String getGcsOutputDirectory();

    //void setGcsOutputDirectory(String gcsOutputDirectory);

    @TemplateParameter.Text(
        order = 2,
        optional = true,
        description = "Output filename prefix of the files to write",
        helpText = "The prefix to place on each windowed file.",
        example = "output-")
    @Default.String("output")
    String getOutputFilenamePrefix();

    void setOutputFilenamePrefix(String outputFilenamePrefix);

    @TemplateParameter.Integer(
        order = 3,
        optional = true,
        description = "Maximum output shards",
        helpText =
            "The maximum number of output shards produced when writing. A higher number of "
                + "shards means higher throughput for writing to Cloud Storage, but potentially higher "
                + "data aggregation cost across shards when processing output Cloud Storage files.")
    @Default.Integer(20)
    Integer getNumShards();

    void setNumShards(Integer numShards);
  }

  /** Builder for {@link WriteChangeStreamMutationToGcsAvro}. */
  @AutoValue.Builder
  public abstract static class WriteToGcsBuilder {
    abstract WriteToGcsBuilder setGcsOutputDirectory(String gcsOutputDirectory);

    abstract String gcsOutputDirectory();

    abstract WriteToGcsBuilder setTempLocation(String tempLocation);

    abstract String tempLocation();

    abstract WriteToGcsBuilder setOutputFilenamePrefix(String outputFilenamePrefix);

    abstract WriteToGcsBuilder setNumShards(Integer numShards);

    abstract WriteChangeStreamMutationToGcsAvro autoBuild();

    abstract WriteToGcsBuilder setIgnoreColumnFamilies(
        HashSet<String> ignoreColumnFamilies);

    abstract WriteToGcsBuilder setIgnoreColumns(HashSet<String> ignoreColumns);

    abstract WriteToGcsBuilder setSchemaOutputFormat(BigtableSchemaFormat schema);

    public WriteToGcsBuilder withSchemaOutputFormat(BigtableSchemaFormat schema) {
      return setSchemaOutputFormat(schema);
    }

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

    public WriteChangeStreamMutationToGcsAvro build() {
      checkNotNull(gcsOutputDirectory(), "Provide output directory to write to. ");
      checkNotNull(tempLocation(), "Temporary directory needs to be provided. ");
      return autoBuild();
    }
  }
}
