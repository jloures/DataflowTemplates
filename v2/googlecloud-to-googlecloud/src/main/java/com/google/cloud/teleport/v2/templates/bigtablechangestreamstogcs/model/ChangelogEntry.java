package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation.MutationType;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.io.Serializable;
import java.util.Objects;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link ChangelogEntry} class is used when writting into GCS.
 * Note that it may be converted into a BigtableRow format if the
 * selection is passed as a parameter.
 */
@DefaultCoder(AvroCoder.class)
public final class ChangelogEntry implements Serializable {

  private static final long serialVersionUID = 3736671476178592416L;

  private static final Logger LOG = LoggerFactory.getLogger(ChangelogEntry.class);

  private ByteString rowKey;
  private ModType modType;
  private Boolean isGc;
  private int tieBreaker;
  private Timestamp commitTimestamp;
  private String columnFamily;
  private Timestamp lowWatermark;
  @Nullable
  private ByteString column;
  @Nullable
  private Long timestamp;
  @Nullable
  private Long timestampFrom;
  @Nullable
  private Long timestampTo;
  @Nullable
  private ByteString value;

  /**
   * Default constructor for serialization only.
   */
  private ChangelogEntry() {
  }

  public ChangelogEntry(
      ByteString rowKey,
      ModType modType,
      Boolean isGc,
      int tieBreaker,
      Timestamp commitTimestamp,
      String columnFamily,
      Timestamp lowWatermark,
      ByteString column,
      Long timestamp,
      Long timestampFrom,
      Long timestampTo,
      ByteString value
  ) {
    this.rowKey = rowKey;
    this.modType = modType;
    this.isGc = isGc;
    this.tieBreaker = tieBreaker;
    this.commitTimestamp = commitTimestamp;
    this.columnFamily = columnFamily;
    this.lowWatermark = lowWatermark;
    this.column = column;
    this.timestamp = timestamp;
    this.timestampFrom = timestampFrom;
    this.timestampTo = timestampTo;
    this.value = value;
  }

  public ChangelogEntry(ChangeStreamMutation mutation, Entry entry) {
    this.rowKey = mutation.getRowKey();
    this.modType = getModType(entry);
    this.isGc = mutation.getType() == MutationType.GARBAGE_COLLECTION;
    this.tieBreaker = mutation.getTieBreaker();
    this.commitTimestamp = mutation.getCommitTimestamp();
    this.lowWatermark = mutation.getLowWatermark();
    setEntryProperties(entry);
  }

  public ByteString getRowKey() {
    return rowKey;
  }

  public ModType getModType() {
    return modType;
  }

  public Boolean getIsGc() {
    return isGc;
  }

  public int getTieBreaker() {
    return tieBreaker;
  }

  public Timestamp getCommitTimestamp() {
    return commitTimestamp;
  }

  public String getColumnFamily() {
    return columnFamily;
  }

  public Timestamp getLowWatermark() {
    return lowWatermark;
  }

  public ByteString getColumn() {
    return column;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public Long getTimestampFrom() {
    return timestampFrom;
  }

  public Long getTimestampTo() {
    return timestampTo;
  }

  public ByteString getValue() {
    return value;
  }

  private void setEntryProperties(Entry entry) {
    if (entry instanceof SetCell) {
      setSetCellEntryProperties(entry);
    } else if (entry instanceof DeleteCells) {
      setDeleteCellEntryProperties(entry);
    } else if (entry instanceof DeleteFamily) {
      setDeleteFamilyEntryProperties(entry);
    } else {
      // Unknown ModType, logging a warning
      LOG.warn("Unknown ChangelogEntry ModType, not setting properties in ChangelogEntry.");
      return;
    }
  }

  private void setSetCellEntryProperties(Entry entry) {
    SetCell cell = (SetCell) entry;
    this.columnFamily = cell.getFamilyName();
    this.column = cell.getQualifier();
    this.timestamp = cell.getTimestamp();
    this.value = cell.getValue();
    this.timestampFrom = null;
    this.timestampTo = null;
  }

  private void setDeleteCellEntryProperties(Entry entry) {
    DeleteCells cell = (DeleteCells) entry;
    this.columnFamily = cell.getFamilyName();
    this.column = cell.getQualifier();
    this.timestamp = null;
    this.value = null;
    this.timestampFrom = cell.getTimestampRange().getStart();
    this.timestampTo = cell.getTimestampRange().getEnd();
  }

  private void setDeleteFamilyEntryProperties(Entry entry) {
    DeleteFamily cell = (DeleteFamily) entry;
    this.columnFamily = cell.getFamilyName();
    this.column = null;
    this.timestamp = null;
    this.value = null;
    this.timestampFrom = null;
    this.timestampTo = null;
  }

  private ModType getModType(Entry entry) {
    if (entry instanceof SetCell) {
      return ModType.SET_CELL;
    } else if (entry instanceof DeleteCells) {
      return ModType.DELETE_CELLS;
    } else if (entry instanceof DeleteFamily) {
      return ModType.DELETE_FAMILY;
    }
    // UNKNOWN Entry, making this future-proof
    LOG.warn("Unknown ChangelogEntry ModType, return ModType.Unknown");
    return ModType.UNKNOWN;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ChangelogEntry)) {
      return false;
    }
    ChangelogEntry that = (ChangelogEntry) o;
    return getTieBreaker() == that.getTieBreaker() && Objects.equals(getRowKey(),
        that.getRowKey()) && getModType() == that.getModType() && Objects.equals(isGc,
        that.isGc) && Objects.equals(getCommitTimestamp(), that.getCommitTimestamp())
        && Objects.equals(getColumnFamily(), that.getColumnFamily())
        && Objects.equals(getLowWatermark(), that.getLowWatermark())
        && Objects.equals(getColumn(), that.getColumn()) && Objects.equals(
        getTimestamp(), that.getTimestamp()) && Objects.equals(getTimestampFrom(),
        that.getTimestampFrom()) && Objects.equals(getTimestampTo(), that.getTimestampTo())
        && Objects.equals(getValue(), that.getValue());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getRowKey(), getModType(), isGc, getTieBreaker(), getCommitTimestamp(),
        getColumnFamily(), getLowWatermark(), getColumn(), getTimestamp(), getTimestampFrom(),
        getTimestampTo(), getValue());
  }

  @Override
  public String toString() {
    return "ChangelogEntry{" +
        "rowKey=" + rowKey +
        ", modType=" + modType +
        ", isGc=" + isGc +
        ", tieBreaker=" + tieBreaker +
        ", commitTimestamp=" + commitTimestamp +
        ", columnFamily='" + columnFamily + '\'' +
        ", lowWatermark=" + lowWatermark +
        ", column=" + column +
        ", timestamp=" + timestamp +
        ", timestampFrom=" + timestampFrom +
        ", timestampTo=" + timestampTo +
        ", value=" + value +
        '}';
  }
}
