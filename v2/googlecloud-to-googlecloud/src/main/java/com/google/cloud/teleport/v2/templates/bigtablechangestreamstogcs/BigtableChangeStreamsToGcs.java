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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs;

import static com.google.cloud.teleport.v2.utils.WriteToGCSUtility.BigtableSchemaFormat.BIGTABLEROW;
import static com.google.cloud.teleport.v2.utils.WriteToGCSUtility.BigtableSchemaFormat.SIMPLE;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.Timestamp;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.teleport.bigtable.BigtableRow;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.v2.options.BigtableChangeStreamsToGcsOptions;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model.ChangelogEntry;
import com.google.cloud.teleport.v2.utils.BigtableUtils;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility.FileFormat;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link BigtableChangeStreamsToGcs} pipeline streams change stream record(s) and stores to
 * Google Cloud Storage bucket in user specified format. The sink data can be stored in a Text or
 * Avro file format.
 */
@Template(
        name = "Bigtable_Change_Streams_to_Google_Cloud_Storage",
        category = TemplateCategory.STREAMING,
        displayName = "Cloud Bigtable change streams to Cloud Storage",
        description =
                "Streaming pipeline. Streams Bigtable change stream data records and writes them into a Cloud Storage bucket using Dataflow Runner V2.",
        flexContainerName = "bigtable-changestreams-to-gcs",
        contactInformation = "https://cloud.google.com/support",
        optionsClass = BigtableChangeStreamsToGcsOptions.class)
public class BigtableChangeStreamsToGcs {
    private static final Logger LOG = LoggerFactory.getLogger(BigtableChangeStreamsToGcs.class);
    private static final String USE_RUNNER_V2_EXPERIMENT = "use_runner_v2";
    private static Gson gson = new Gson();

    private static long counter = 0;

    private static UUID workerId = UUID.randomUUID();

    @VisibleForTesting
    protected static final String DEFAULT_OUTPUT_FILE_PREFIX = "output";
    public static void main(String[] args) {
        LOG.info("Starting Input Files to GCS");

        BigtableChangeStreamsToGcsOptions options =
                PipelineOptionsFactory.fromArgs(args).as(BigtableChangeStreamsToGcsOptions.class);

        run(options);
    }

    private static String getProjectId(BigtableChangeStreamsToGcsOptions options) {
        return StringUtils.isEmpty(options.getBigtableProjectId())
                ? options.getProject()
                : options.getBigtableProjectId();
    }

    private static HashSet<String> getIgnoreColumnFamilies(String ignoreColumnFamilies) {
        HashSet<String> parsedColumnFamilies = new HashSet<>();
        if (ignoreColumnFamilies.isEmpty()) {
            return parsedColumnFamilies;
        }
        for (String word : ignoreColumnFamilies.split(",")) {
            String columnFamily = word.trim();
            checkArgument(columnFamily.length() > 0, "Column Family provided: " + columnFamily + " is an empty string and is invalid");
            parsedColumnFamilies.add(columnFamily);
        }
        return parsedColumnFamilies;
    }

    private static HashSet<String> getIgnoreColumns(String ignoreColumns) {
        checkArgument(ignoreColumns != null, "withIgnoreColumns(ignoreColumns) called with null input.");
        HashSet<String> parsedColumns = new HashSet<>();
        if (ignoreColumns.isEmpty()) {
            return parsedColumns;
        }
        for (String word : ignoreColumns.split(",")) {
            String trimmedColumns = word.trim();
            checkArgument(trimmedColumns.matches(BigtableUtils.columnPattern), "The Column specified does not follow the required format of 'cf1:c1, cf2:c2 ...'");
            parsedColumns.add(trimmedColumns);
        }
        return parsedColumns;
    }

    public static PipelineResult run(BigtableChangeStreamsToGcsOptions options) {
        LOG.info("Requested File Format is " + options.getOutputFileFormat());
        options.setStreaming(true);
        options.setEnableStreamingEngine(true);

        final Pipeline pipeline = Pipeline.create(options);

        // Get the Bigtable project, instance, database, and change stream parameters.
        String projectId = getProjectId(options);
        String instanceId = options.getBigtableInstanceId();

        // Retrieve and parse the start / end timestamps.
        Timestamp startTimestamp =
            options.getStartTimestamp().isEmpty()
                ? Timestamp.now()
                : Timestamp.parseTimestamp(options.getStartTimestamp());
        Timestamp endTimestamp =
            options.getEndTimestamp().isEmpty()
                ? Timestamp.MAX_VALUE
                : Timestamp.parseTimestamp(options.getEndTimestamp());

        // Add use_runner_v2 to the experiments option, since Change Streams connector is only supported
        // on Dataflow runner v2.
        List<String> experiments = options.getExperiments();
        if (experiments == null) {
            experiments = new ArrayList<>();
        }
        boolean hasUseRunnerV2 = false;
        for (String experiment : experiments) {
            if (experiment.toLowerCase().equals(USE_RUNNER_V2_EXPERIMENT)) {
                hasUseRunnerV2 = true;
                break;
            }
        }
        if (!hasUseRunnerV2) {
            experiments.add(USE_RUNNER_V2_EXPERIMENT);
        }
        options.setExperiments(experiments);

        String metadataTableName =
            options.getBigtableMetadataTableId() == null
                ? null
                : options.getBigtableMetadataTableId();

        // Parse IgnoreColumns and IgnoreColumnFamilies
        HashSet<String> ignoreColumnFamilies = getIgnoreColumnFamilies(
            options.getIgnoreColumnFamilies());
        HashSet<String> ignoreColumns = getIgnoreColumns(options.getIgnoreColumns());

        BigtableIO.ReadChangeStream readChangeStream =
            BigtableIO.readChangeStream()
                .withProjectId(projectId)
                .withAppProfileId(options.getBigtableAppProfileId())
                .withInstanceId(instanceId)
                .withTableId(options.getBigtableTableId())
                .withStartTime(startTimestamp)
                .withEndTime(endTimestamp)
                .withMetadataTableTableId(metadataTableName);

        /**
         * Output a PCollection of {@link ChangeStreamMutation} based on processing time.
         */
        PCollection<ChangeStreamMutation> changeStreamMutation =
            pipeline
                .apply(readChangeStream)
                .apply(ParDo.of(
                    new DoFn<KV<ByteString, ChangeStreamMutation>, ChangeStreamMutation>() {
                        @ProcessElement
                        public void processElement(
                            @Element KV<ByteString, ChangeStreamMutation> element,
                            OutputReceiver<ChangeStreamMutation> out) {
                            Instant processingTime = Instant.now();
                            out.outputWithTimestamp(element.getValue(), processingTime);
                        }
                    }));

        /**
         * Group all {@link ChangeStreamMutation} into windows to be written into GCS files.
         */
        if (options.getOutputFileFormat() == FileFormat.AVRO) {
            if (options.getSchemaOutputFormat() == BIGTABLEROW) {
                changeStreamMutation
                    .apply(
                        "Creating " + options.getWindowDuration() + " Window",
                        Window.into(
                            FixedWindows.of(
                                DurationUtils.parseDuration(options.getWindowDuration()))))
                    /*
                     * Writing as avro file using {@link AvroIO}.
                     *
                     * The {@link WindowedFilenamePolicy} class specifies the file path for writing the file.
                     * The {@link withNumShards} option specifies the number of shards passed by the user.
                     * The {@link withTempDirectory} option sets the base directory used to generate temporary files.
                     */
                    .apply("Transform to Avro BigtableRow",
                        FlatMapElements.via(
                            new SimpleFunction<ChangeStreamMutation, List<BigtableRow>>() {
                                @Override
                                public List<BigtableRow> apply(ChangeStreamMutation mutation) {
                                    List<ChangelogEntry> validEntries = BigtableUtils.getValidEntries(
                                        mutation,
                                        ignoreColumns,
                                        ignoreColumnFamilies
                                    );
                                    List<com.google.cloud.teleport.bigtable.BigtableRow> rowEntries = new ArrayList<>();
                                    for (ChangelogEntry entry : validEntries) {
                                        rowEntries.add(
                                            BigtableUtils.createBigtableRow(mutation, entry, workerId, counter)
                                        );
                                    }
                                    return rowEntries;
                                }
                            }))
                    .apply(
                        "Writing as Avro",
                        AvroIO.write(com.google.cloud.teleport.bigtable.BigtableRow.class)
                            .to(
                                WindowedFilenamePolicy.writeWindowedFiles()
                                    .withOutputDirectory(options.getGcsOutputDirectory())
                                    .withOutputFilenamePrefix(options.getOutputFilenamePrefix())
                                    .withShardTemplate(WriteToGCSUtility.SHARD_TEMPLATE)
                                    .withSuffix(
                                        WriteToGCSUtility.FILE_SUFFIX_MAP.get(
                                            WriteToGCSUtility.FileFormat.AVRO)))
                            .withTempDirectory(
                                FileBasedSink.convertToFileResourceIfPossible(options.getTempLocation())
                                    .getCurrentDirectory())
                            .withWindowedWrites()
                            .withNumShards(options.getNumShards()));
            } else {

                /*
                 * Writing as avro file using {@link AvroIO}.
                 *
                 * The {@link WindowedFilenamePolicy} class specifies the file path for writing the file.
                 * The {@link withNumShards} option specifies the number of shards passed by the user.
                 * The {@link withTempDirectory} option sets the base directory used to generate temporary files.
                 */
                changeStreamMutation
                    .apply(
                        "Creating " + options.getWindowDuration() + " Window",
                        Window.into(
                            FixedWindows.of(
                                DurationUtils.parseDuration(options.getWindowDuration()))))
                    .apply("Transform to Avro Simple",
                        FlatMapElements.via(
                            new SimpleFunction<ChangeStreamMutation, List<ChangelogEntry>>() {
                                @Override
                                public List<ChangelogEntry> apply(ChangeStreamMutation mutation) {
                                    return BigtableUtils.getValidEntries(
                                        mutation,
                                        ignoreColumns,
                                        ignoreColumnFamilies
                                    );
                                }
                            }))
                    .apply(
                        "Writing as Avro",
                        AvroIO.write(ChangelogEntry.class)
                            .to(
                                WindowedFilenamePolicy.writeWindowedFiles()
                                    .withOutputDirectory(options.getGcsOutputDirectory())
                                    .withOutputFilenamePrefix(options.getOutputFilenamePrefix())
                                    .withShardTemplate(WriteToGCSUtility.SHARD_TEMPLATE)
                                    .withSuffix(
                                        WriteToGCSUtility.FILE_SUFFIX_MAP.get(
                                            WriteToGCSUtility.FileFormat.AVRO)))
                            .withTempDirectory(
                                FileBasedSink.convertToFileResourceIfPossible(
                                        options.getTempLocation())
                                    .getCurrentDirectory())
                            .withWindowedWrites()
                            .withNumShards(options.getNumShards()));
            }
        } else if (options.getOutputFileFormat() == FileFormat.TEXT) {
            /*
             * Writing as text file using {@link TextIO}.
             *
             * The {@link WindowedFilenamePolicy} class specifies the file path for writing the file.
             * The {@link withNumShards} option specifies the number of shards passed by the user.
             * The {@link withTempDirectory} option sets the base directory used to generate temporary files.
             */
            changeStreamMutation
                .apply(
                    "Creating " + options.getWindowDuration() + " Window",
                    Window.into(
                        FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))
                .apply(
                    FlatMapElements.via(new SimpleFunction<ChangeStreamMutation, List<String>>() {
                        @Override
                        public List<String> apply(ChangeStreamMutation mutation) {
                            List<String> jsonEntries = new ArrayList<>();
                            List<ChangelogEntry> validEntries = BigtableUtils.getValidEntries(
                                mutation,
                                ignoreColumns,
                                ignoreColumnFamilies
                            );
                            for (ChangelogEntry entry : validEntries) {
                                String jsonEntry = null;
                                switch (options.getSchemaOutputFormat()) {
                                    case BIGTABLEROW:
                                        jsonEntry = gson.toJson(
                                            BigtableUtils.createBigtableRow(mutation, entry,
                                                workerId, counter),
                                            BigtableRow.class
                                        );
                                        break;
                                    case SIMPLE:
                                        jsonEntry = gson.toJson(entry, ChangelogEntry.class);
                                        break;
                                }
                                if (jsonEntry == null || jsonEntry.length() > 0) {
                                    jsonEntries.add(jsonEntry);
                                }
                            }
                            return jsonEntries;
                        }
                    }))
                .apply(
                    "Writing as Text",
                    TextIO.write()
                        .to(
                            WindowedFilenamePolicy.writeWindowedFiles()
                                .withOutputDirectory(options.getGcsOutputDirectory())
                                .withOutputFilenamePrefix(options.getOutputFilenamePrefix())
                                .withShardTemplate(WriteToGCSUtility.SHARD_TEMPLATE)
                                .withSuffix(
                                    WriteToGCSUtility.FILE_SUFFIX_MAP.get(
                                        WriteToGCSUtility.FileFormat.TEXT)))
                        .withTempDirectory(
                            FileBasedSink.convertToFileResourceIfPossible((options.getTempLocation()))
                                .getCurrentDirectory())
                        .withWindowedWrites()
                        .withNumShards(options.getNumShards()));
        } else {
            LOG.error("Unknown OutputFileFormat chosen.");
        }
        return pipeline.run();
    }
}
