package com.google.cloud.teleport.v2.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;

/**
 * The {@link BigtableChangeStreamsToGcsFilterOptions} interface provides the custom execution options
 * passed by the executor at the command-line for {@ChangeStreamMutation} entries to be filtered out.
 */
public interface BigtableChangeStreamsToGcsFilterOptions extends DataflowPipelineOptions {

  @TemplateParameter.Text(
      order = 9,
      optional = true,
      description = "Ignore Column Families",
      helpText = "A comma-separated list of column families for which changes are to be skipped.",
      example = "cf1,cf2,cf3")
  @Default.String("")
  String getIgnoreColumnFamilies();

  void setIgnoreColumnFamilies(String ignoreColumnFamilies);

  @TemplateParameter.Text(
      order = 10,
      optional = true,
      description = "Ignore Columns",
      helpText = "A comma-separated list of columnFamily:columnName values for which changes are to be skipped.",
      example = "cf1:c1,cf2:c2,cf3:c3")
  @Default.String("")
  String getIgnoreColumns();

  void setIgnoreColumns(String ignoreColumns);

}
