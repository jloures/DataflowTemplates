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
package com.google.cloud.teleport.v2.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.teleport.v2.options.BigtableChangeStreamsToGcsOptions;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility.FileFormat;
import java.nio.charset.Charset;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link FileFormatFactoryBigtableChangeStreams} class is a {@link PTransform} that takes in
 * {@link PCollection} of ChangeStreamMutations. The transform writes these records to GCS file(s) in
 * user specified format.
 */
@AutoValue
public abstract class FileFormatFactoryBigtableChangeStreams
    extends PTransform<PCollection<ChangeStreamMutation>, POutput> {

  /** Logger for class. */
  private static final Logger LOG =
      LoggerFactory.getLogger(FileFormatFactoryBigtableChangeStreams.class);

  public static WriteToGcsBuilder newBuilder() {
    return new AutoValue_FileFormatFactoryBigtableChangeStreams.Builder();
  }

  public abstract BigtableChangeStreamsToGcsOptions options();

  public abstract FileFormat outputFileFormat();

  @Override
  public POutput expand(PCollection<ChangeStreamMutation> mutations) {
    POutput output;

    final String errorMessage =
        "Invalid output format:" + outputFileFormat() + ". Supported output formats: TEXT, AVRO";

    // Get the desired charset
    Charset charset = Charset.forName(options().getBigtableCharset());

    /*
     * Calls appropriate class Builder to performs PTransform based on user provided File Format.
     */
    switch (outputFileFormat()) {
      case AVRO:
        output =
            mutations.apply(
                "Write Avro File(s)",
                WriteChangeStreamMutationToGcsAvro.newBuilder()
                    .withGcsOutputDirectory(options().getGcsOutputDirectory())
                    .withOutputFilenamePrefix(options().getOutputFilenamePrefix())
                    .setNumShards(options().getNumShards())
                    .withTempLocation(options().getTempLocation())
                    .withIgnoreColumnFamilies(options().getIgnoreColumnFamilies())
                    .withIgnoreColumns(options().getIgnoreColumns())
                    .withSchemaOutputFormat(options().getSchemaOutputFormat())
                    .withCharset(charset)
                    .build());
        break;
      case TEXT:
        output =
            mutations.apply(
                "Write Text File(s)",
                WriteChangeStreamMutationsToGcsText.newBuilder()
                    .withGcsOutputDirectory(options().getGcsOutputDirectory())
                    .withOutputFilenamePrefix(options().getOutputFilenamePrefix())
                    .setNumShards(options().getNumShards())
                    .withTempLocation(options().getTempLocation())
                    .withIgnoreColumnFamilies(options().getIgnoreColumnFamilies())
                    .withIgnoreColumns(options().getIgnoreColumns())
                    .withSchemaOutputFormat(options().getSchemaOutputFormat())
                    .withCharset(charset)
                    .build());
        break;

      default:
        LOG.info(errorMessage);
        throw new IllegalArgumentException(errorMessage);
    }
    return output;
  }

  /** Builder for {@link FileFormatFactoryBigtableChangeStreams}. */
  @AutoValue.Builder
  public abstract static class WriteToGcsBuilder {

    public abstract WriteToGcsBuilder setOptions(BigtableChangeStreamsToGcsOptions options);

    public abstract WriteToGcsBuilder setOutputFileFormat(FileFormat outputFileFormat);

    abstract BigtableChangeStreamsToGcsOptions options();

    abstract FileFormatFactoryBigtableChangeStreams autoBuild();

    public FileFormatFactoryBigtableChangeStreams build() {
      setOutputFileFormat(options().getOutputFileFormat());
      return autoBuild();
    }
  }
}
