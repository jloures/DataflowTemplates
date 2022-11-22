package com.google.cloud.teleport.v2.utils;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model.ChangelogEntry;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * A set of helper functions and classes for Bigtable.
 */
public class BigtableUtils {

  public static String BIGTABLE_ROW_COLUMN_FAMILY = "changelog";

  public static String BIGTABLE_ROW_KEY_DELIMITER = "#";

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
