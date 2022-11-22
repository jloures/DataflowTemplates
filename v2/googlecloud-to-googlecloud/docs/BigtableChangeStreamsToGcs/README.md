# Bigtable Change Streams To Google Cloud Storage Dataflow Template

The [BigtableChangeStreamsToGCS]
(src/main/java/com/google/cloud/teleport/v2/templates/BigtableChangeStreamsToGCS.java)
pipeline reads messages from Cloud Bigtable Change Streams and stores them in a
Google Cloud Storage bucket using the specified file format.

Data will be bucketed by processing timestamp into different windows. By default, the
window size is 5 minutes.

The data can be stored in a Text or Avro File Format.

NOTE: This template is currently unreleased. If you wish to use it now, you
will need to f follow the steps outlined below to add it to and run it from
your own Google Cloud project. Make sure to be in the /v2 directory.

