package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs;

import com.google.bigtable.repackaged.org.apache.commons.lang3.StringUtils;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation.MutationType;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.teleport.bigtable.BigtableRow;
import com.google.cloud.teleport.bigtable.ChangelogEntry;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model.BigtableSource;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model.ChangelogColumns;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link BigtableUtils} provides a set of helper functions and classes for Bigtable.
 */
public class BigtableUtils implements Serializable {

  public static final String ANY_COLUMN_FAMILY = "*";
  private static final Logger LOG = LoggerFactory.getLogger(BigtableUtils.class);

  private final long serialVersionUID = 6360056103639788953L;
  public String bigtableRowColumnFamilyName = "changelog";
  public String bigtableRowKeyDelimiter = "#";
  private final BigtableSource source;
  private final Charset charsetObj;
  private final Map<String, Set<String>> ignoredColumnsMap;

  public BigtableUtils(BigtableSource sourceInfo) {
    this.source = sourceInfo;
    this.charsetObj = Charset.forName(sourceInfo.getCharset());

    ignoredColumnsMap = new HashMap<>();
    for (String columnFamilyAndColumn : sourceInfo.getColumnsToIgnore()) {
      int indexOfColon = columnFamilyAndColumn.indexOf(':');
      String columnFamily = ANY_COLUMN_FAMILY;
      String columnName = columnFamilyAndColumn;
      if (indexOfColon > 0) {
        columnFamily = columnFamilyAndColumn.substring(0, indexOfColon);
        if (StringUtils.isBlank(columnFamily)) {
          columnFamily = ANY_COLUMN_FAMILY;
        }
        columnName = columnFamilyAndColumn.substring(indexOfColon + 1);
      }

      Set<String> appliedToColumnFamilies = ignoredColumnsMap.computeIfAbsent(columnName,
          k -> new HashSet<>());
      appliedToColumnFamilies.add(columnFamily);
    }
  }

  public boolean hasIgnoredColumnFamilies() {
    return this.source.getColumnFamiliesToIgnore().size() > 0;
  }

  public boolean isIgnoredColumnFamily(String columnFamily) {
    return this.source.getColumnFamiliesToIgnore().contains(columnFamily);
  }

  public boolean hasIgnoredColumns() {
    return this.source.getColumnsToIgnore().size() > 0;
  }

  public boolean isIgnoredColumn(String columnFamily, String column) {
    Set<String> columnFamilies = ignoredColumnsMap.get(column);
    if (columnFamilies == null) {
      return false;
    }
    return columnFamilies.contains(columnFamily) || columnFamilies.contains(ANY_COLUMN_FAMILY);
  }

  private Boolean isValidEntry(String familyName, String qualifierName) {
    if (hasIgnoredColumnFamilies() && isIgnoredColumnFamily(familyName)) {
      return false;
    }

    if (hasIgnoredColumns() && !StringUtils.isBlank(qualifierName) && isIgnoredColumn(familyName,
        qualifierName)) {
      return false;
    }

    return true;
  }

  public com.google.cloud.teleport.bigtable.BigtableRow createBigtableRow(
      ChangelogEntry entry,
      String workerId,
      Long counter
  ) {
    java.util.List<com.google.cloud.teleport.bigtable.BigtableCell> cells = new ArrayList<>();

    // row_key
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        this.bigtableRowColumnFamilyName,
        ChangelogColumns.ROW_KEY.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        entry.getRowKey()
    ));

    // mod_type
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        this.bigtableRowColumnFamilyName,
        ChangelogColumns.MOD_TYPE.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        getByteBufferFromString(entry.getModType().toString())
    ));

    // is_gc
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        this.bigtableRowColumnFamilyName,
        ChangelogColumns.IS_GC.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        getByteBufferFromString(entry.getIsGc().toString())
    ));

    // tiebreaker
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        this.bigtableRowColumnFamilyName,
        ChangelogColumns.TIEBREAKER.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        getByteBufferFromString(String.valueOf(entry.getTieBreaker()))
    ));

    // commit_timestamp
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        this.bigtableRowColumnFamilyName,
        ChangelogColumns.COMMIT_TIMESTAMP.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        getByteBufferFromString(String.valueOf(entry.getCommitTimestamp()))
    ));

    // column_family
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        this.bigtableRowColumnFamilyName,
        ChangelogColumns.COLUMN_FAMILY.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        getByteBufferFromString(String.valueOf(entry.getTieBreaker()))
    ));

    // low_watermark
    cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
        this.bigtableRowColumnFamilyName,
        ChangelogColumns.LOW_WATERMARK.getColumnNameAsByteBuffer(),
        entry.getTimestamp(),
        getByteBufferFromString(String.valueOf(entry.getLowWatermark()))
    ));

    if (entry.getColumn() != null) {
      // column
      cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
          this.bigtableRowColumnFamilyName,
          ChangelogColumns.LOW_WATERMARK.getColumnNameAsByteBuffer(),
          entry.getTimestamp(),
          getByteBufferFromString(String.valueOf(entry.getLowWatermark()))
      ));
    }

    if (entry.getTimestamp() != null) {
      // timestamp
      cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
          this.bigtableRowColumnFamilyName,
          ChangelogColumns.TIMESTAMP.getColumnNameAsByteBuffer(),
          entry.getTimestamp(),
          getByteBufferFromString(String.valueOf(entry.getTimestamp()))
      ));
    }

    if (entry.getTimestampFrom() != null) {
      // timestamp_from
      cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
          this.bigtableRowColumnFamilyName,
          ChangelogColumns.TIMESTAMP_FROM.getColumnNameAsByteBuffer(),
          entry.getTimestamp(),
          getByteBufferFromString(String.valueOf(entry.getTimestampFrom()))
      ));
    }

    if (entry.getTimestampTo() != null) {
      // timestamp_to
      cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
          this.bigtableRowColumnFamilyName,
          ChangelogColumns.TIMESTAMP_TO.getColumnNameAsByteBuffer(),
          entry.getTimestamp(),
          getByteBufferFromString(String.valueOf(entry.getTimestampTo()))
      ));
    }

    if (entry.getValue() != null) {
      // value
      cells.add(new com.google.cloud.teleport.bigtable.BigtableCell(
          this.bigtableRowColumnFamilyName,
          ChangelogColumns.TIMESTAMP_FROM.getColumnNameAsByteBuffer(),
          entry.getTimestamp(),
          getByteBufferFromString(String.valueOf(entry.getValue()))
      ));
    }

    return new BigtableRow(
        createChangelogRowKey(entry.getCommitTimestamp(), workerId, counter),
        cells
    );
  }

  private ByteBuffer getByteBufferFromString(String s) {
    return ByteBuffer.wrap(s.getBytes(this.charsetObj));
  }

  private ByteBuffer createChangelogRowKey(
      Long commitTimestamp,
      String workerId,
      Long counter) {
    String rowKey = (commitTimestamp.toString()
        + this.bigtableRowKeyDelimiter
        + workerId
        + this.bigtableRowKeyDelimiter
        + counter);

    return ByteBuffer.wrap(rowKey.getBytes(this.charsetObj));
  }

  /**
   * @param mutation
   * @return {@link ChangelogEntry} with valid entries based on {@param ignoreColumn} and {@param ignoreColumnFamilies}
   */
  public List<ChangelogEntry> getValidEntries(ChangeStreamMutation mutation) {
    // filter first and then format
    List<ChangelogEntry> validEntries = new ArrayList<>();
    for (Entry entry : mutation.getEntries()) {
      if (entry instanceof SetCell) {
        SetCell setCell = (SetCell) entry;
        String familyName = setCell.getFamilyName();
        String qualifierName ;
        qualifierName = setCell.getQualifier().toString(this.charsetObj);
        if (isValidEntry(familyName, qualifierName)) {
          validEntries.add(createChangelogEntry(mutation, entry));
        }
      } else if (entry instanceof DeleteCells) {
        DeleteCells deleteCells = (DeleteCells) entry;
        String familyName = deleteCells.getFamilyName();
        String qualifierName;
        qualifierName = deleteCells.getQualifier().toString(this.charsetObj);
        if (isValidEntry(familyName, qualifierName)) {
          validEntries.add(createChangelogEntry(mutation, entry));
        }
      } else if (entry instanceof DeleteFamily) {
        DeleteFamily deleteFamily = (DeleteFamily) entry;
        String familyName = deleteFamily.getFamilyName();
        if (isValidEntry(familyName, null)) {
          validEntries.add(createChangelogEntry(mutation, entry));
        }
      }
    }
    return validEntries;
  }

  private com.google.cloud.teleport.bigtable.ChangelogEntry createChangelogEntry(
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

  private void setCellEntryProperties(
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

  private void setDeleteCellEntryProperties(
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

  private void setDeleteFamilyEntryProperties(Entry mutationEntry,
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

  private com.google.cloud.teleport.bigtable.ModType getModType(Entry entry) {
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
