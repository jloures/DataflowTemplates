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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.it.dataflow.DataflowUtils.createJobName;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.artifacts.Artifact;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.dataflow.DataflowClient.JobInfo;
import com.google.cloud.teleport.it.dataflow.DataflowClient.JobState;
import com.google.cloud.teleport.it.dataflow.DataflowClient.LaunchConfig;
import com.google.cloud.teleport.it.dataflow.DataflowOperator;
import com.google.cloud.teleport.it.dataflow.DataflowOperator.Result;
import com.google.cloud.teleport.it.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.it.spanner.DefaultSpannerResourceManager;
import com.google.cloud.teleport.it.spanner.SpannerResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator.SchemaTemplate;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator.SinkType;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.util.List;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link StreamingDataGenerator}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(StreamingDataGenerator.class)
@RunWith(JUnit4.class)
public final class StreamingDataGeneratorIT extends TemplateTestBase {

  private static final String SCHEMA_FILE = "gameevent.json";
  private static final String LOCAL_SCHEMA_PATH = Resources.getResource(SCHEMA_FILE).getPath();

  private static final String NUM_SHARDS_KEY = "numShards";
  private static final String OUTPUT_DIRECTORY_KEY = "outputDirectory";
  private static final String QPS_KEY = "qps";
  private static final String SCHEMA_LOCATION_KEY = "schemaLocation";
  private static final String SCHEMA_TEMPLATE_KEY = "schemaTemplate";
  private static final String SINK_TYPE_KEY = "sinkType";
  private static final String WINDOW_DURATION_KEY = "windowDuration";
  private static final String TOPIC_KEY = "topic";

  private static final String DEFAULT_QPS = "15";
  private static final String DEFAULT_WINDOW_DURATION = "60s";
  private static PubsubResourceManager pubsubResourceManager;
  private static BigQueryResourceManager bigQueryResourceManager;
  private static SpannerResourceManager spannerResourceManager;

  @After
  public void tearDown() {
    // clean up resources
    if (pubsubResourceManager != null) {
      pubsubResourceManager.cleanupAll();
      pubsubResourceManager = null;
    }
    if (artifactClient != null) {
      artifactClient.cleanupRun();
      artifactClient = null;
    }
    if (bigQueryResourceManager != null) {
      bigQueryResourceManager.cleanupAll();
      bigQueryResourceManager = null;
    }
    if (spannerResourceManager != null) {
      spannerResourceManager.cleanupAll();
      spannerResourceManager = null;
    }
  }

  @Test
  public void testFakeMessagesToGcs() throws IOException {
    // Arrange
    artifactClient.uploadArtifact(SCHEMA_FILE, LOCAL_SCHEMA_PATH);
    String name = testName.getMethodName();
    String jobName = createJobName(name);

    LaunchConfig.Builder options =
        LaunchConfig.builder(jobName, specPath)
            // TODO(zhoufek): See if it is possible to use the properties interface and generate
            // the map from the set values.
            .addParameter(SCHEMA_LOCATION_KEY, getGcsPath(SCHEMA_FILE))
            .addParameter(QPS_KEY, DEFAULT_QPS)
            .addParameter(SINK_TYPE_KEY, SinkType.GCS.name())
            .addParameter(WINDOW_DURATION_KEY, DEFAULT_WINDOW_DURATION)
            .addParameter(OUTPUT_DIRECTORY_KEY, getGcsPath(name))
            .addParameter(NUM_SHARDS_KEY, "1");
    // Act
    JobInfo info = launchTemplate(options);
    assertThat(info.state()).isIn(JobState.ACTIVE_STATES);

    Result result =
        new DataflowOperator(getDataflowClient())
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  List<Artifact> outputFiles =
                      artifactClient.listArtifacts(name, Pattern.compile(".*output-.*"));
                  return !outputFiles.isEmpty();
                });

    // Assert
    assertThat(result).isEqualTo(Result.CONDITION_MET);
  }

  @Test
  public void testFakeMessagesToGcsWithSchemaTemplate() throws IOException {
    // Arrange
    String name = testName.getMethodName();
    String jobName = createJobName(name);

    LaunchConfig.Builder options =
        LaunchConfig.builder(jobName, specPath)
            .addParameter(SCHEMA_TEMPLATE_KEY, SchemaTemplate.GAME_EVENT.name())
            .addParameter(QPS_KEY, DEFAULT_QPS)
            .addParameter(SINK_TYPE_KEY, SinkType.GCS.name())
            .addParameter(WINDOW_DURATION_KEY, DEFAULT_WINDOW_DURATION)
            .addParameter(OUTPUT_DIRECTORY_KEY, getGcsPath(name))
            .addParameter(NUM_SHARDS_KEY, "1");
    // Act
    JobInfo info = launchTemplate(options);
    assertThat(info.state()).isIn(JobState.ACTIVE_STATES);

    Result result =
        new DataflowOperator(getDataflowClient())
            .waitForConditionAndFinish(
                createConfig(info),
                () ->
                    !artifactClient.listArtifacts(name, Pattern.compile(".*output-.*")).isEmpty());

    // Assert
    assertThat(result).isEqualTo(Result.CONDITION_MET);
  }

  @Test
  public void testFakeMessagesToPubSub() throws IOException {
    // Set up resource manager
    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName.getMethodName(), PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();
    TopicName backlogTopic = pubsubResourceManager.createTopic("output");
    SubscriptionName subscription =
        pubsubResourceManager.createSubscription(backlogTopic, "output-subscription");
    // Arrange
    String name = testName.getMethodName();
    String jobName = createJobName(name);
    LaunchConfig.Builder options =
        LaunchConfig.builder(jobName, specPath)
            .addParameter(SCHEMA_TEMPLATE_KEY, SchemaTemplate.GAME_EVENT.name())
            .addParameter(QPS_KEY, DEFAULT_QPS)
            .addParameter(SINK_TYPE_KEY, SinkType.PUBSUB.name())
            .addParameter(TOPIC_KEY, backlogTopic.toString());

    // Act
    JobInfo info = launchTemplate(options);
    assertThat(info.state()).isIn(JobState.ACTIVE_STATES);
    Result result =
        new DataflowOperator(getDataflowClient())
            .waitForConditionAndFinish(
                createConfig(info),
                () -> pubsubResourceManager.pull(subscription, 5).getReceivedMessagesCount() > 0);
    // Assert
    assertThat(result).isEqualTo(Result.CONDITION_MET);
  }

  @Test
  public void testFakeMessagesToBigQuery() throws IOException {
    // Set up resource manager
    bigQueryResourceManager =
        DefaultBigQueryResourceManager.builder(testName.getMethodName(), PROJECT)
            .setCredentials(credentials)
            .build();
    // schema should match schema supplied to generate fake records.
    Schema schema =
        Schema.of(
            Field.of("eventId", StandardSQLTypeName.STRING),
            Field.of("eventTimestamp", StandardSQLTypeName.INT64),
            Field.of("ipv4", StandardSQLTypeName.STRING),
            Field.of("ipv6", StandardSQLTypeName.STRING),
            Field.of("country", StandardSQLTypeName.STRING),
            Field.of("username", StandardSQLTypeName.STRING),
            Field.of("quest", StandardSQLTypeName.STRING),
            Field.of("score", StandardSQLTypeName.INT64),
            Field.of("completed", StandardSQLTypeName.BOOL));
    // Arrange
    String name = testName.getMethodName();
    String jobName = createJobName(name);
    bigQueryResourceManager.createTable(jobName, schema);
    LaunchConfig.Builder options =
        LaunchConfig.builder(jobName, specPath)
            .addParameter("schemaTemplate", String.valueOf(SchemaTemplate.GAME_EVENT))
            .addParameter("qps", "1000000")
            .addParameter("sinkType", "BIGQUERY")
            .addParameter(
                "outputTableSpec",
                String.format(
                    "%s:%s.%s", PROJECT, bigQueryResourceManager.getDatasetId(), jobName));

    // Act
    JobInfo info = launchTemplate(options);
    assertThat(info.state()).isIn(JobState.ACTIVE_STATES);
    Result result =
        new DataflowOperator(getDataflowClient())
            .waitForConditionAndFinish(
                createConfig(info),
                () -> bigQueryResourceManager.readTable(jobName).getTotalRows() > 0);
    // Assert
    assertThat(result).isEqualTo(Result.CONDITION_MET);
  }

  @Test
  public void testFakeMessagesToSpanner() throws IOException {
    // Arrange
    String name = testName.getMethodName();
    String jobName = createJobName(name);
    spannerResourceManager =
        DefaultSpannerResourceManager.builder(testName.getMethodName(), PROJECT, REGION).build();
    String createTableStatement =
        String.format(
            "CREATE TABLE `%s` (\n"
                + "  eventId STRING(1024) NOT NULL,\n"
                + "  eventTimestamp INT64,\n"
                + "  ipv4 STRING(1024),\n"
                + "  ipv6 STRING(1024),\n"
                + "  country STRING(1024),\n"
                + "  username STRING(1024),\n"
                + "  quest STRING(1024),\n"
                + "  score INT64,\n"
                + "  completed BOOL,\n"
                + ") PRIMARY KEY(eventId)",
            testName.getMethodName());
    ImmutableList<String> columnNames =
        ImmutableList.of(
            "eventId",
            "eventTimestamp",
            "ipv4",
            "ipv6",
            "country",
            "username",
            "quest",
            "score",
            "completed");
    spannerResourceManager.createTable(createTableStatement);

    LaunchConfig.Builder options =
        LaunchConfig.builder(jobName, specPath)
            .addParameter(SCHEMA_TEMPLATE_KEY, SchemaTemplate.GAME_EVENT.name())
            .addParameter(QPS_KEY, DEFAULT_QPS)
            .addParameter(SINK_TYPE_KEY, SinkType.SPANNER.name())
            .addParameter("projectId", PROJECT)
            .addParameter("spannerInstanceName", spannerResourceManager.getInstanceId())
            .addParameter("spannerDatabaseName", spannerResourceManager.getDatabaseId())
            .addParameter("spannerTableName", testName.getMethodName());

    // Act
    JobInfo info = launchTemplate(options);
    assertThat(info.state()).isIn(JobState.ACTIVE_STATES);

    Result result =
        new DataflowOperator(getDataflowClient())
            .waitForConditionAndFinish(
                createConfig(info),
                () ->
                    !spannerResourceManager
                        .readTableRecords(testName.getMethodName(), columnNames)
                        .isEmpty());

    // Assert
    assertThat(result).isEqualTo(Result.CONDITION_MET);
  }
}
