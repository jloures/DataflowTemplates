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

import com.google.bigtable.repackaged.org.apache.commons.lang3.StringUtils;
import com.google.cloud.Timestamp;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.options.BigtableChangeStreamsToGcsOptions;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model.BigtableSource;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
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

    private static String getBigtableCharset(BigtableChangeStreamsToGcsOptions options) {
        return StringUtils.isEmpty(options.getBigtableCharset())
            ? "UTF-8"
            : options.getBigtableCharset();
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

        BigtableSource sourceInfo = new BigtableSource(
            options.getBigtableInstanceId(),
            options.getBigtableTableId(),
            getBigtableCharset(options),
            options.getIgnoreColumnFamilies(),
            options.getIgnoreColumns()
        );

        BigtableUtils bigtableUtils = new BigtableUtils(sourceInfo);

        // Add use_runner_v2 to the experiments option, since Change Streams connector is only supported
        // on Dataflow runner v2.
        List<String> experiments = options.getExperiments();
        if (experiments == null) {
            experiments = new ArrayList<>();
        }
        boolean hasUseRunnerV2 = false;
        for (String experiment : experiments) {
            if (experiment.equalsIgnoreCase(USE_RUNNER_V2_EXPERIMENT)) {
                hasUseRunnerV2 = true;
                break;
            }
        }
        if (!hasUseRunnerV2) {
            experiments.add(USE_RUNNER_V2_EXPERIMENT);
        }
        options.setExperiments(experiments);

        pipeline
            .apply(
                BigtableIO.readChangeStream()
                    .withProjectId(projectId)
                    .withAppProfileId(options.getBigtableAppProfileId())
                    .withInstanceId(instanceId)
                    .withTableId(options.getBigtableTableId())
                    .withStartTime(startTimestamp)
                    .withEndTime(endTimestamp)
                    .withMetadataTableInstanceId(options.getBigtableMetadataInstanceId())
                    .withMetadataTableTableId(options.getBigtableMetadataTableTableId()))
            .apply(
                Window.<KV<ByteString, ChangeStreamMutation>>into(new GlobalWindows())
                    .triggering(
                        Repeatedly.forever(
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(DurationUtils.parseDuration(options.getWindowDuration()))))
                    .discardingFiredPanes())
            .apply(Values.create())
            .apply(
                "Write To GCS",
                FileFormatFactoryBigtableChangeStreams.newBuilder().setOptions(options)
                    .setBigtableUtils(bigtableUtils).build());

        return pipeline.run();
    }
}
