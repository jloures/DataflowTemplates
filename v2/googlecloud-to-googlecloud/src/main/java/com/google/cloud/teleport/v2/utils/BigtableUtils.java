package com.google.cloud.teleport.v2.utils;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.teleport.bigtable.BigtableRow;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model.ChangelogColumns;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model.ChangelogEntry;
import com.google.protobuf.Timestamp;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * A set of helper functions and classes for Bigtable.
 */
public class BigtableUtils {

  public static String bigtableRowColumnFamilyName = "changelog";

  public static String bigtableRowKeyDelimiter = "#";

  public static String columnPattern = "^[^:]+:.*$";

  private static Boolean isValidEntry(String familyName, String qualifierName,
      HashSet<String> ignoreColumns, HashSet<String> ignoreColumnFamilies) {
    if (familyName == null) {
      return true;
    }

    if (ignoreColumnFamilies.contains(familyName)) {
      return false;
    }

    String columnFamilyAndQualifier = familyName + ":" + Objects.toString(qualifierName);

    return !ignoreColumns.contains(columnFamilyAndQualifier);
  }

  public static com.google.cloud.teleport.bigtable.BigtableRow createBigtableRow(
      ChangeStreamMutation mutation,
      ChangelogEntry entry,
      UUID workerId,
      long counter
  ) {
    java.util.List<com.google.cloud.teleport.bigtable.BigtableCell> cells = new ArrayList<>();

    // row_key
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        BigtableUtils.bigtableRowColumnFamilyName,
        ChangelogColumns.ROW_KEY.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        mutation.getRowKey().asReadOnlyByteBuffer()
    ));

    // mod_type
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        BigtableUtils.bigtableRowColumnFamilyName,
        ChangelogColumns.MOD_TYPE.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        entry.getModType().getPropertyNameAsByteBuffer()
    ));

    // is_gc
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        BigtableUtils.bigtableRowColumnFamilyName,
        ChangelogColumns.IS_GC.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        getByteBufferFromString(entry.getIsGc().toString())
    ));

    // tiebreaker
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        BigtableUtils.bigtableRowColumnFamilyName,
        ChangelogColumns.TIEBREAKER.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        getByteBufferFromString(String.valueOf(entry.getTieBreaker()))
    ));

    // commit_timestamp
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        BigtableUtils.bigtableRowColumnFamilyName,
        ChangelogColumns.COMMIT_TIMESTAMP.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        getByteBufferFromString(String.valueOf(mutation.getCommitTimestamp()))
    ));

    // column_family
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        BigtableUtils.bigtableRowColumnFamilyName,
        ChangelogColumns.COLUMN_FAMILY.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        getByteBufferFromString(String.valueOf(entry.getTieBreaker()))
    ));

    // low_watermark
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        BigtableUtils.bigtableRowColumnFamilyName,
        ChangelogColumns.LOW_WATERMARK.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        getByteBufferFromString(String.valueOf(entry.getLowWatermark()))
    ));

    if (entry.getColumn() != null) {
      // column
      cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
          BigtableUtils.bigtableRowColumnFamilyName,
          ChangelogColumns.LOW_WATERMARK.getColumnNameAsByteBuffer(),
          entry.getTimestamp(),
          getByteBufferFromString(String.valueOf(entry.getLowWatermark()))
      ));
    }

    if (entry.getTimestamp() != null) {
      // timestamp
      cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
          BigtableUtils.bigtableRowColumnFamilyName,
          ChangelogColumns.TIMESTAMP.getColumnNameAsByteBuffer(),
          entry.getTimestamp(),
          getByteBufferFromString(String.valueOf(entry.getTimestamp()))
      ));
    }

    if (entry.getTimestampFrom() != null) {
      // timestamp_from
      cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
          BigtableUtils.bigtableRowColumnFamilyName,
          ChangelogColumns.TIMESTAMP_FROM.getColumnNameAsByteBuffer(),
          entry.getTimestamp(),
          getByteBufferFromString(String.valueOf(entry.getTimestampFrom()))
      ));
    }

    if (entry.getTimestampTo() != null) {
      // timestamp_to
      cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
          BigtableUtils.bigtableRowColumnFamilyName,
          ChangelogColumns.TIMESTAMP_TO.getColumnNameAsByteBuffer(),
          entry.getTimestamp(),
          getByteBufferFromString(String.valueOf(entry.getTimestampTo()))
      ));
    }

    if (entry.getValue() != null) {
      // value
      cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
          BigtableUtils.bigtableRowColumnFamilyName,
          ChangelogColumns.TIMESTAMP_FROM.getColumnNameAsByteBuffer(),
          entry.getTimestamp(),
          getByteBufferFromString(String.valueOf(entry.getValue()))
      ));
    }

    return new BigtableRow(
        createChangelogRowKey(mutation.getCommitTimestamp(), workerId, counter),
        cells
    );
  }

  private static ByteBuffer getByteBufferFromString(String s) {
    return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
  }

  private static ByteBuffer createChangelogRowKey(Timestamp commitTimestamp, UUID workerId, long counter) {
    String rowKey = (commitTimestamp.toString()
        + BigtableUtils.bigtableRowKeyDelimiter
        + workerId
        + BigtableUtils.bigtableRowKeyDelimiter
        + counter++);

    return ByteBuffer.wrap(rowKey.getBytes(StandardCharsets.UTF_8));
  }

  public static List<ChangelogEntry> getValidEntries(
      ChangeStreamMutation mutation,
      HashSet<String> ignoreColumns,
      HashSet<String> ignoreColumnFamilies
  ) {
    List<ChangelogEntry> validEntries = new ArrayList<>();
    for (Entry entry : mutation.getEntries()) {
      if (entry instanceof SetCell) {
        SetCell setCell = (SetCell) entry;
        String familyName = setCell.getFamilyName();
        String qualifierName = setCell.getQualifier().toStringUtf8();
        if (isValidEntry(familyName, qualifierName, ignoreColumns, ignoreColumnFamilies)) {
          validEntries.add(new ChangelogEntry(mutation, entry));
        }
      } else if (entry instanceof DeleteCells) {
        DeleteCells deleteCells = (DeleteCells) entry;
        String familyName = deleteCells.getFamilyName();
        String qualifierName = deleteCells.getQualifier().toStringUtf8();
        if (isValidEntry(familyName, qualifierName, ignoreColumns, ignoreColumnFamilies)) {
          validEntries.add(new ChangelogEntry(mutation, entry));
        }
      } else if (entry instanceof DeleteFamily) {
        DeleteFamily deleteFamily = (DeleteFamily) entry;
        String familyName = deleteFamily.getFamilyName();
        if (isValidEntry(familyName, null, ignoreColumns, ignoreColumnFamilies)) {
          validEntries.add(new ChangelogEntry(mutation, entry));
        }
      }
    }
    return validEntries;
  }
}
