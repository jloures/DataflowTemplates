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

import com.google.cloud.Timestamp;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.teleport.v2.options.BigtableChangeStreamsToGcsOptions;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility.FileFormat;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BigtableChangeStreamsToGcs} */
@RunWith(JUnit4.class)
public class BigtableChangeStreamsToGcsTest {
    /** Rule for exception testing. */
    @Rule public ExpectedException exception = ExpectedException.none();

    @Rule public final TemporaryFolder tmpDir = new TemporaryFolder();

    /** Rule for pipeline testing. */
    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();
    private static final Integer NUM_SHARDS = 1;

    private static String fakeDir;
    private static String fakeTempLocation;
    private static final String FILENAME_PREFIX = "filenamePrefix";

    @Before
    public void setup() throws Exception {
        fakeDir = tmpDir.newFolder("output").getAbsolutePath();
        fakeTempLocation = tmpDir.newFolder("temporaryLocation").getAbsolutePath();
    }

    @After
    public void tearDown() throws NoSuchFieldException, IllegalAccessException {
        // do any necessary cleanup here
    }

    /**
     * Test whether {@link FileFormatFactoryBigtableChangeStreams} maps the output file format to the transform to be
     * carried out. And throws illegal argument exception if invalid file format is passed.
     */
    @Test
    public void testFileFormatFactoryInvalid() {
        exception.expect(IllegalArgumentException.class);
        //exception.expectMessage("Invalid output format:PARQUET. Supported output formats: TEXT, AVRO");

        BigtableChangeStreamsToGcsOptions options =
            PipelineOptionsFactory.create().as(BigtableChangeStreamsToGcsOptions.class);
        options.setOutputFileFormat(FileFormat.PARQUET);
        options.setGcsOutputDirectory(fakeDir);
        options.setOutputFilenamePrefix(FILENAME_PREFIX);
        options.setNumShards(NUM_SHARDS);
        options.setTempLocation(fakeTempLocation);

        Pipeline p = Pipeline.create(options);

        Timestamp startTimestamp = Timestamp.now();
        Timestamp endTimestamp = Timestamp.now();

        p
            // Reads from the change stream
            .apply(
                BigtableIO.readChangeStream()
                    .withStartTime(startTimestamp)
                    .withEndTime(endTimestamp))
            .apply(
                Window.<KV<ByteString, ChangeStreamMutation>>into(new GlobalWindows())
                    .triggering(
                        Repeatedly.forever(
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(DurationUtils.parseDuration(options.getWindowDuration()))))
                    .discardingFiredPanes())
            .apply(Values.create())
            .apply(
                "Creating " + options.getWindowDuration() + " Window",
                Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))
            .apply(
                "Write To GCS",
                FileFormatFactoryBigtableChangeStreams.newBuilder().setOptions(options).build());

        p.run();
    }

    @Test
    public void testInvalidWindowDuration() {
        exception.expect(IllegalArgumentException.class);
        // exception.expectMessage("The window duration must be greater than 0!");
        BigtableChangeStreamsToGcsOptions options =
            PipelineOptionsFactory.create().as(BigtableChangeStreamsToGcsOptions.class);
        options.setOutputFileFormat(FileFormat.AVRO);
        options.setGcsOutputDirectory(fakeDir);
        options.setOutputFilenamePrefix(FILENAME_PREFIX);
        options.setNumShards(NUM_SHARDS);
        options.setTempLocation(fakeTempLocation);
        options.setWindowDuration("invalidWindowDuration");

        Pipeline p = Pipeline.create(options);

        Timestamp startTimestamp = Timestamp.now();
        Timestamp endTimestamp = Timestamp.now();

        p
            // Reads from the change stream
            .apply(
                BigtableIO.readChangeStream()
                    .withStartTime(startTimestamp)
                    .withEndTime(endTimestamp))
            .apply(
                Window.<KV<ByteString, ChangeStreamMutation>>into(new GlobalWindows())
                    .triggering(
                        Repeatedly.forever(
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(DurationUtils.parseDuration(options.getWindowDuration()))))
                    .discardingFiredPanes())
            .apply(Values.create())
            .apply(
                "Creating " + options.getWindowDuration() + " Window",
                Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))
            .apply(
                "Write To GCS",
                FileFormatFactoryBigtableChangeStreams.newBuilder().setOptions(options).build());

        p.run();
    }
}
