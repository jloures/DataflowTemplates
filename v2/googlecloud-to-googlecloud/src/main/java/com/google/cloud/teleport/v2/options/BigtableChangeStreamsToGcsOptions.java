/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.v2.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model.BigtableSchemaFormat;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility.FileFormat;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link BigtableChangeStreamsToGcsOptions} interface provides the custom execution options
 * passed by the executor at the command-line.
 */
public interface BigtableChangeStreamsToGcsOptions extends DataflowPipelineOptions {
    @TemplateParameter.ProjectId(
        order = 1,
        optional = true,
        description = "Bigtable Project ID",
        helpText =
            "Project to read change streams from. The default for this parameter is the project "
                + "where the Dataflow pipeline is running.")
    @Default.String("")
    String getBigtableProjectId();

    void setBigtableProjectId(String projectId);

    @TemplateParameter.Text(
            order = 2,
            description = "Bigtable Instance ID",
            helpText = "The Bigtable Instance to read change streams from.")
    @Validation.Required
    String getBigtableInstanceId();

    void setBigtableInstanceId(String bigtableInstanceId);

    @TemplateParameter.Text(
            order = 3,
            description = "Bigtable Table ID",
            helpText = "The Bigtable table ID to read change streams from.")
    @Validation.Required
    String getBigtableTableId();

    void setBigtableTableId(String bigtableTableId);

    @TemplateParameter.Text(
        order = 4,
        description = "Cloud Bigtable application profile id",
        helpText = "The application profile is used to distinguish workload in Cloud Bigtable")
    @Validation.Required
    String getBigtableAppProfileId();

    void setBigtableAppProfileId(String bigtableAppProfileId);

    @TemplateParameter.DateTime(
        order = 5,
        optional = true,
        description = "The timestamp to read change streams from",
        helpText =
            "The starting DateTime, inclusive, to use for reading change streams "
                + "(https://tools.ietf.org/html/rfc3339). For example, 2022-05-05T07:59:59Z. Defaults to the "
                + "timestamp when the pipeline starts.")
    @Default.String("")
    String getStartTimestamp();

    void setStartTimestamp(String startTimestamp);

    @TemplateParameter.DateTime(
        order = 6,
        optional = true,
        description = "The timestamp to read change streams to",
        helpText =
            "The ending DateTime, inclusive, to use for reading change streams "
                + "(https://tools.ietf.org/html/rfc3339). Ex-2022-05-05T07:59:59Z. Defaults to an infinite "
                + "time in the future.")
    @Default.String("")
    String getEndTimestamp();

    void setEndTimestamp(String startTimestamp);

    @TemplateParameter.Enum(
        order = 7,
        enumOptions = {"TEXT", "AVRO"},
        optional = true,
        description = "Output file format",
        helpText =
            "The format of the output Cloud Storage file. Allowed formats are TEXT, AVRO. Default is AVRO.")
    @Default.Enum("AVRO")
    FileFormat getOutputFileFormat();

    void setOutputFileFormat(FileFormat outputFileFormat);

    @TemplateParameter.Duration(
        order = 8,
        optional = true,
        description = "Window duration",
        helpText =
            "The window duration/size in which data will be written to Cloud Storage. Allowed formats are: Ns (for "
                + "seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h).",
        example = "5m")
    @Default.String("5m")
    String getWindowDuration();

    void setWindowDuration(String windowDuration);

    @TemplateParameter.Text(
        order = 11,
        optional = true,
        description = "Bigtable Metadata Table Id",
        helpText = "Table ID used for creating the metadata table.",
        example = "__change_stream_md_table")
    @Default.String("__change_stream_md_table")
    String getBigtableMetadataTableTableId();

    void setBigtableMetadataTableTableId(String bigtableMetadataTableTableId);

    @TemplateParameter.Enum(
        order = 12,
        enumOptions = {"SIMPLE", "BIGTABLEROW"},
        optional = true,
        description = "Output schema format",
        helpText = "Schema chosen for outputting data to GCS.")
    @Default.Enum("SIMPLE")
    BigtableSchemaFormat getSchemaOutputFormat();

    void setSchemaOutputFormat(BigtableSchemaFormat outputSchemaFormat);

    @TemplateParameter.Text(
        order = 13,
        optional = true,
        description = "Bigtable charset name when reading values and column qualifiers.",
        helpText = "Bigtable charset name when reading values and column qualifiers.")
    @Default.String("UTF-8")
    String getBigtableCharset();

    void setBigtableCharset(String bigtableCharset);

    @TemplateParameter.GcsWriteFolder(
        order = 14,
        description = "Output file directory in Cloud Storage",
        helpText =
            "The path and filename prefix for writing output files. Must end with a slash. "
                + "DateTime formatting is used to parse directory path for date & time formatters.",
        example = "gs://your-bucket/your-path")
    String getGcsOutputDirectory();

    void setGcsOutputDirectory(String gcsOutputDirectory);

    @TemplateParameter.Text(
        order = 15,
        description = "Output filename prefix of the files to write",
        helpText = "The prefix to place on each windowed file.",
        example = "output-")
    @Default.String("output")
    String getOutputFilenamePrefix();

    void setOutputFilenamePrefix(String outputFilenamePrefix);

    @TemplateParameter.Integer(
        order = 16,
        optional = true,
        description = "Maximum output shards",
        helpText =
            "The maximum number of output shards produced when writing. A higher number of "
                + "shards means higher throughput for writing to Cloud Storage, but potentially higher "
                + "data aggregation cost across shards when processing output Cloud Storage files.")
    @Default.Integer(20)
    Integer getNumShards();

    void setNumShards(Integer numShards);

    @TemplateParameter.Text(
        order = 17,
        optional = true,
        description = "Ignore Column Families",
        helpText = "A comma-separated list of column families for which changes are to be skipped.",
        example = "cf1,cf2,cf3")
    @Default.String("")
    String getIgnoreColumnFamilies();

    void setIgnoreColumnFamilies(String ignoreColumnFamilies);

    @TemplateParameter.Text(
        order = 18,
        optional = true,
        description = "Ignore Columns",
        helpText = "A comma-separated list of columnFamily:columnName values for which changes are to be skipped.",
        example = "cf1:c1,cf2:c2,cf3:c3")
    @Default.String("")
    String getIgnoreColumns();

    void setIgnoreColumns(String ignoreColumns);

    @TemplateParameter.Text(
        order = 19,
        optional = true,
        description = "Cloud Bigtable metadata instance ID",
        helpText = "The Cloud Bigtable instance to use for the change streams connector metadata table.")
    @Default.String("")
    String getBigtableMetadataInstanceId();

    void setBigtableMetadataInstanceId(String value);
}
