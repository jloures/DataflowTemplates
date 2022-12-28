package com.google.cloud.teleport.v2.utils;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation.MutationType;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.teleport.bigtable.BigtableRow;
import com.google.cloud.teleport.bigtable.ChangelogEntry;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model.ChangelogColumns;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of helper functions and classes for Bigtable.
 */
public class BigtableUtils {

  public static String bigtableRowColumnFamilyName = "changelog";

  public static String bigtableRowKeyDelimiter = "#";

  public static String columnPattern = "^[^:]+:.*$";

  private static final Logger LOG = LoggerFactory.getLogger(BigtableUtils.class);

  private static Boolean isValidEntry(String familyName, String qualifierName,
      HashSet<String> ignoreColumns, HashSet<String> ignoreColumnFamilies) {
    if (familyName == null) {
      return true;
    }

    if (ignoreColumnFamilies.contains(familyName)) {
      return false;
    }

    String columnFamilyAndQualifier = familyName + ":" + qualifierName;

    return !ignoreColumns.contains(columnFamilyAndQualifier);
  }

  public static com.google.cloud.teleport.bigtable.BigtableRow createBigtableRow(
      com.google.cloud.teleport.bigtable.ChangelogEntry entry,
      String workerId,
      AtomicLong counter,
      Charset charset
  ) {
    java.util.List<com.google.cloud.teleport.bigtable.BigtableCell> cells = new ArrayList<>();

    // row_key
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        BigtableUtils.bigtableRowColumnFamilyName,
        ChangelogColumns.ROW_KEY.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        entry.getRowKey()
    ));

    // mod_type
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        BigtableUtils.bigtableRowColumnFamilyName,
        ChangelogColumns.MOD_TYPE.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        getByteBufferFromString(entry.getModType().toString(), charset)
    ));

    // is_gc
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        BigtableUtils.bigtableRowColumnFamilyName,
        ChangelogColumns.IS_GC.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        getByteBufferFromString(entry.getIsGc().toString(), charset)
    ));

    // tiebreaker
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        BigtableUtils.bigtableRowColumnFamilyName,
        ChangelogColumns.TIEBREAKER.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        getByteBufferFromString(String.valueOf(entry.getTieBreaker()), charset)
    ));

    // commit_timestamp
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        BigtableUtils.bigtableRowColumnFamilyName,
        ChangelogColumns.COMMIT_TIMESTAMP.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        getByteBufferFromString(String.valueOf(entry.getCommitTimestamp()), charset)
    ));

    // column_family
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        BigtableUtils.bigtableRowColumnFamilyName,
        ChangelogColumns.COLUMN_FAMILY.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        getByteBufferFromString(String.valueOf(entry.getTieBreaker()), charset)
    ));

    // low_watermark
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        BigtableUtils.bigtableRowColumnFamilyName,
        ChangelogColumns.LOW_WATERMARK.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        getByteBufferFromString(String.valueOf(entry.getLowWatermark()), charset)
    ));

    if (entry.getColumn() != null) {
      // column
      cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
          BigtableUtils.bigtableRowColumnFamilyName,
          ChangelogColumns.LOW_WATERMARK.getColumnNameAsByteBuffer(),
          entry.getTimestamp(),
          getByteBufferFromString(String.valueOf(entry.getLowWatermark()), charset)
      ));
    }

    if (entry.getTimestamp() != null) {
      // timestamp
      cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
          BigtableUtils.bigtableRowColumnFamilyName,
          ChangelogColumns.TIMESTAMP.getColumnNameAsByteBuffer(),
          entry.getTimestamp(),
          getByteBufferFromString(String.valueOf(entry.getTimestamp()), charset)
      ));
    }

    if (entry.getTimestampFrom() != null) {
      // timestamp_from
      cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
          BigtableUtils.bigtableRowColumnFamilyName,
          ChangelogColumns.TIMESTAMP_FROM.getColumnNameAsByteBuffer(),
          entry.getTimestamp(),
          getByteBufferFromString(String.valueOf(entry.getTimestampFrom()), charset)
      ));
    }

    if (entry.getTimestampTo() != null) {
      // timestamp_to
      cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
          BigtableUtils.bigtableRowColumnFamilyName,
          ChangelogColumns.TIMESTAMP_TO.getColumnNameAsByteBuffer(),
          entry.getTimestamp(),
          getByteBufferFromString(String.valueOf(entry.getTimestampTo()), charset)
      ));
    }

    if (entry.getValue() != null) {
      // value
      cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
          BigtableUtils.bigtableRowColumnFamilyName,
          ChangelogColumns.TIMESTAMP_FROM.getColumnNameAsByteBuffer(),
          entry.getTimestamp(),
          getByteBufferFromString(String.valueOf(entry.getValue()), charset)
      ));
    }

    return new BigtableRow(
        createChangelogRowKey(entry.getCommitTimestamp(), workerId, counter, charset),
        cells
    );
  }

  private static ByteBuffer getByteBufferFromString(String s, Charset charset) {
    return ByteBuffer.wrap(s.getBytes(charset));
  }

  private static ByteBuffer createChangelogRowKey(
      Long commitTimestamp,
      String workerId,
      AtomicLong counter,
      Charset charset) {
    String rowKey = (commitTimestamp.toString()
        + BigtableUtils.bigtableRowKeyDelimiter
        + workerId
        + BigtableUtils.bigtableRowKeyDelimiter
        + counter.incrementAndGet());

    return ByteBuffer.wrap(rowKey.getBytes(charset));
  }

  public static List<ChangelogEntry> getValidEntries(
      ChangeStreamMutation mutation,
      HashSet<String> ignoreColumns,
      HashSet<String> ignoreColumnFamilies,
      Charset charset
  ) {
    // filter first and then format
    List<ChangelogEntry> validEntries = new ArrayList<>();
    for (Entry entry : mutation.getEntries()) {
      if (entry instanceof SetCell) {
        SetCell setCell = (SetCell) entry;
        String familyName = setCell.getFamilyName();
        String qualifierName ;
        qualifierName = setCell.getQualifier().toString(charset);
        if (isValidEntry(familyName, qualifierName, ignoreColumns, ignoreColumnFamilies)) {
          validEntries.add(createChangelogEntry(mutation, entry));
        }
      } else if (entry instanceof DeleteCells) {
        DeleteCells deleteCells = (DeleteCells) entry;
        String familyName = deleteCells.getFamilyName();
        String qualifierName;
        qualifierName = deleteCells.getQualifier().toString(charset);
        if (isValidEntry(familyName, qualifierName, ignoreColumns, ignoreColumnFamilies)) {
          validEntries.add(createChangelogEntry(mutation, entry));
        }
      } else if (entry instanceof DeleteFamily) {
        DeleteFamily deleteFamily = (DeleteFamily) entry;
        String familyName = deleteFamily.getFamilyName();
        if (isValidEntry(familyName, null, ignoreColumns, ignoreColumnFamilies)) {
          validEntries.add(createChangelogEntry(mutation, entry));
        }
      }
    }
    return validEntries;
  }

  public static Charset getCharset(String charset) {
    if (charset == null) {
      return StandardCharsets.UTF_8;
    }

    return StandardCharsets.UTF_8;
  }

  private static com.google.cloud.teleport.bigtable.ChangelogEntry createChangelogEntry(
      ChangeStreamMutation mutation,
      Entry mutationEntry) {
    com.google.cloud.teleport.bigtable.ChangelogEntry.Builder changelogEntry = ChangelogEntry.newBuilder()
        .setRowKey(mutation.getRowKey().asReadOnlyByteBuffer())
        .setModType(getModType(mutationEntry))
        .setIsGc(mutation.getType() == MutationType.GARBAGE_COLLECTION)
        .setTieBreaker(mutation.getTieBreaker())
        .setCommitTimestamp(mutation.getCommitTimestamp().getNanos() / 1000)
        .setLowWatermark(mutation.getLowWatermark().getNanos() / 1000);

    if (mutationEntry instanceof SetCell) {
      setCellEntryProperties(mutationEntry, changelogEntry);
    } else if (mutationEntry instanceof DeleteCells) {
      setDeleteCellEntryProperties(mutationEntry, changelogEntry);
    } else if (mutationEntry instanceof DeleteFamily) {
      setDeleteFamilyEntryProperties(mutationEntry, changelogEntry);
    } else {
      // Unknown ModType, logging a warning
      LOG.warn("Unknown ChangelogEntry ModType, not setting properties in ChangelogEntry.");
    }
    return changelogEntry.build();
  }

  private static void setCellEntryProperties(
      Entry mutationEntry, ChangelogEntry.Builder changelogEntry) {
    SetCell cell = (SetCell) mutationEntry;
    changelogEntry
        .setColumnFamily(cell.getFamilyName())
        .setColumn(cell.getQualifier().asReadOnlyByteBuffer())
        .setTimestamp(cell.getTimestamp())
        .setValue(cell.getValue().asReadOnlyByteBuffer())
        .setTimestampFrom(null)
        .setTimestampTo(null);
  }

  private static void setDeleteCellEntryProperties(
      Entry mutationEntry, ChangelogEntry.Builder changelogEntry) {
    DeleteCells cell = (DeleteCells) mutationEntry;
    changelogEntry
        .setColumnFamily(cell.getFamilyName())
        .setColumn(cell.getQualifier().asReadOnlyByteBuffer())
        .setTimestamp(null)
        .setValue(null)
        .setTimestampFrom(cell.getTimestampRange().getStart())
        .setTimestampTo(cell.getTimestampRange().getEnd());
  }

  private static void setDeleteFamilyEntryProperties(Entry mutationEntry,
      ChangelogEntry.Builder changelogEntry) {
    DeleteFamily cell = (DeleteFamily) mutationEntry;
    changelogEntry
        .setColumnFamily(cell.getFamilyName())
        .setColumn(null)
        .setTimestamp(null)
        .setValue(null)
        .setTimestampFrom(null)
        .setTimestampTo(null);
  }

  private static com.google.cloud.teleport.bigtable.ModType getModType(Entry entry) {
    if (entry instanceof SetCell) {
      return com.google.cloud.teleport.bigtable.ModType.SET_CELL;
    } else if (entry instanceof DeleteCells) {
      return com.google.cloud.teleport.bigtable.ModType.DELETE_CELLS;
    } else if (entry instanceof DeleteFamily) {
      return com.google.cloud.teleport.bigtable.ModType.DELETE_FAMILY;
    }
    // UNKNOWN Entry, making this future-proof
    LOG.warn("Unknown ChangelogEntry ModType, return ModType.Unknown");
    return com.google.cloud.teleport.bigtable.ModType.UNKNOWN;
  }
}
