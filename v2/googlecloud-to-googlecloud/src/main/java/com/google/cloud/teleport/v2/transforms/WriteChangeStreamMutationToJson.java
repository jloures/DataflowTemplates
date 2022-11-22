// /*
//  * Copyright (C) 2022 Google LLC
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License"); you may not
//  * use this file except in compliance with the License. You may obtain a copy of
//  * the License at
//  *
//  *   http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//  * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//  * License for the specific language governing permissions and limitations under
//  * the License.
//  */
// package com.google.cloud.teleport.v2.transforms;
//
//
// import static com.google.cloud.teleport.v2.transforms.WriteChangeStreamMutationsToGcsText.DEFAULT_OUTPUT_FILE_PREFIX;
// import static com.google.cloud.teleport.v2.utils.WriteToGCSUtility.BigtableSchemaFormat.BIGTABLEROW;
// import static com.google.cloud.teleport.v2.utils.WriteToGCSUtility.BigtableSchemaFormat.SIMPLE;
// import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
// import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
//
// import com.google.auto.value.AutoValue;
// import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
// import com.google.cloud.teleport.bigtable.BigtableRow;
// import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model.ChangelogEntry;
// import com.google.cloud.teleport.v2.transforms.WriteChangeStreamMutationsToGcsText.WriteToGcsBuilder;
// import com.google.cloud.teleport.v2.utils.BigtableUtils;
// import com.google.cloud.teleport.v2.utils.WriteToGCSUtility.BigtableSchemaFormat;
// import com.google.gson.Gson;
// import java.util.ArrayList;
// import java.util.HashSet;
// import java.util.List;
// import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
// import org.apache.beam.sdk.transforms.PTransform;
// import org.apache.beam.sdk.transforms.SimpleFunction;
// import org.apache.beam.sdk.values.PCollection;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// /**
//  * The {@link ChangeStreamMutationJsonTextFn} class is a {@link PTransform} that takes in {@link
//  * PCollection} of Bigtable change stream mutations. The transform converts and writes these mutations to
//  * GCS in JSON text file format.
//  */
// @AutoValue
// public abstract class WriteChangeStreamMutationToJson {
//   /* Logger for class. */
//   private static final Logger LOG = LoggerFactory.getLogger(WriteChangeStreamMutationToJson.class);
//
//   private static Gson gson = new Gson();
//
//   public abstract HashSet<String> ignoreColumnFamilies();
//
//   public abstract HashSet<String> ignoreColumns();
//
//   public abstract BigtableSchemaFormat schemaOutputFormat();
//
//   public static WriteChangeStreamMutationToJson.WriteToGcsBuilder newBuilder() {
//     return new AutoValue_WriteChangeStreamMutationToJson.Builder();
//   }
//
//   public ChangeS
//
//   @Override
//   public List<String> apply(ChangeStreamMutation mutation) {
//
//   }
//
//   private com.google.cloud.teleport.bigtable.BigtableRow createBigtableRow(
//       ChangeStreamMutation mutation,
//       ChangelogEntry entry
//   ) {
//     java.util.List<com.google.cloud.teleport.bigtable.BigtableCell> cells = new ArrayList<>();
//
//     // TODO create the actual BigtableRow
//     /*
//      *
//      * columnfamily = changelog
//      * qualifiers = properties of the changelog, if it doesn't exist or if it is null, do not include it
//      *
//      * */
//
//     return new BigtableRow(
//         mutation.getRowKey().asReadOnlyByteBuffer(),
//         cells
//     );
//   }
//
//   /** Builder for {@link WriteChangeStreamMutationToJson}. */
//   @AutoValue.Builder
//   public abstract static class WriteToGcsBuilder {
//
//     public static String columnPattern = "^[^:]+:.*$";
//
//     abstract WriteChangeStreamMutationToJson.WriteToGcsBuilder setSchemaOutputFormat(
//         BigtableSchemaFormat schema);
//
//     abstract WriteChangeStreamMutationToJson autoBuild();
//
//     public WriteChangeStreamMutationToJson.WriteToGcsBuilder withSchemaOutputFormat(
//         BigtableSchemaFormat schema) {
//       return setSchemaOutputFormat(schema);
//     }
//
//     public WriteChangeStreamMutationToJson build() {
//       return autoBuild();
//     }
//
//     abstract WriteChangeStreamMutationToJson.WriteToGcsBuilder setIgnoreColumnFamilies(
//         HashSet<String> ignoreColumnFamilies);
//
//     abstract WriteChangeStreamMutationToJson.WriteToGcsBuilder setIgnoreColumns(
//         HashSet<String> ignoreColumns);
//
//     public WriteChangeStreamMutationToJson.WriteToGcsBuilder withIgnoreColumnFamilies(
//         String ignoreColumnFamilies) {
//       checkArgument(ignoreColumnFamilies != null,
//           "withIgnoreColumnFamilies(ignoreColumnFamilies) called with null input.");
//       HashSet<String> parsedColumnFamilies = new HashSet<>();
//       for (String word : ignoreColumnFamilies.split(",")) {
//         String columnFamily = word.trim();
//         checkArgument(columnFamily.length() > 0,
//             "Column Family provided: " + columnFamily + " is an empty string and is invalid");
//         parsedColumnFamilies.add(columnFamily);
//       }
//       return setIgnoreColumnFamilies(parsedColumnFamilies);
//     }
//
//     public WriteChangeStreamMutationToJson.WriteToGcsBuilder withIgnoreColumns(
//         String ignoreColumns) {
//       checkArgument(ignoreColumns != null,
//           "withIgnoreColumns(ignoreColumns) called with null input.");
//       HashSet<String> parsedColumns = new HashSet<>();
//       for (String word : ignoreColumns.split(",")) {
//         String trimmedColumns = word.trim();
//         checkArgument(trimmedColumns.matches(columnPattern),
//             "The Column specified does not follow the required format of 'cf1:c1, cf2:c2 ...'");
//         parsedColumns.add(trimmedColumns);
//       }
//       return setIgnoreColumns(parsedColumns);
//     }
//   }
// }
