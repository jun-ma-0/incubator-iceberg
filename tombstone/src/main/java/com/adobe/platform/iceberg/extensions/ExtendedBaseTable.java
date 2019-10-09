package com.adobe.platform.iceberg.extensions;

import com.adobe.platform.iceberg.extensions.tombstone.Entry;
import com.adobe.platform.iceberg.extensions.tombstone.ExtendedEntry;
import com.adobe.platform.iceberg.extensions.tombstone.Namespace;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ExtendedMergeAppend;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.types.Types;

public class ExtendedBaseTable extends BaseTable implements ExtendedTable, HasTableOperations {

  private TombstoneExtension tombstoneExtension;
  private ExtendedTableOperations ops;

  ExtendedBaseTable(ExtendedTableOperations ops, String name, TombstoneExtension tombstoneExtension) {
    super(ops, name);
    this.tombstoneExtension = tombstoneExtension;
    this.ops = ops;
  }

  @Override
  public TableOperations operations() {
    return this.ops;
  }

  @Override
  public ExtendedAppendFiles newAppend() {
    return new ExtendedMergeAppend(ops, tombstoneExtension);
  }

  @Override
  public AppendFiles newFastAppend() {
    throw new RuntimeIOException("Support for fast append is not support in this extension of Iceberg.");
  }

  @Override
  public ExtendedAppendFiles newAppendWithTombstonesAdd(
      Types.NestedField column,
      List<Entry> entries,
      Map<String, String> properties) {
    Preconditions.checkArgument(column != null, "Invalid column name: (null)");
    Preconditions.checkArgument(entries != null, "Invalid tombstone entries: (null)");
    Types.NestedField field = schema().findField(column.fieldId());

    Preconditions.checkArgument(
        field != null,
        "Unable to find column(%s) with id: (%s)",
        column.name(),
        column.fieldId());
    Preconditions.checkArgument(
        field.name().equalsIgnoreCase(column.name()),
        "Column id(%s) does not match with column name: " +
            "(%s)",
        column.fieldId(),
        column.name());

    return new ExtendedMergeAppend(ops, tombstoneExtension)
        .appendTombstones(namespaceOf(field), entries, properties);
  }

  @Override
  public ExtendedAppendFiles newAppendWithTombstonesRemove(Types.NestedField column, List<Entry> entries) {
    Preconditions.checkArgument(column != null, "Invalid column name: (null)");
    Preconditions.checkArgument(entries != null, "Invalid tombstone entries: (null)");
    Types.NestedField field = schema().findField(column.fieldId());
    Preconditions.checkArgument(
        field != null,
        "Unable to find column(%s) with id: (%s)",
        column.name(),
        column.fieldId());
    Preconditions.checkArgument(
        field.name().equals(column.name()),
        "Column id(%s) does not match with column name: (%s)",
        column.fieldId(),
        column.name());

    return new ExtendedMergeAppend(ops, tombstoneExtension)
        .removeTombstones(namespaceOf(field), entries);
  }

  @Override
  public List<ExtendedEntry> getCurrentSnapshotTombstones(Types.NestedField column) {
    Preconditions.checkArgument(column != null, "Invalid column name: (null)");
    Types.NestedField field = schema().findField(column.fieldId());
    Preconditions.checkArgument(
        field != null,
        "Unable to find column(%s) with id: (%s)",
        column.name(),
        column.fieldId());
    Preconditions.checkArgument(
        field.name().equals(column.name()),
        "Column id(%s) does not match with column name: (%s)",
        column.fieldId(),
        column.name());
    return tombstoneExtension.get(currentSnapshot(), namespaceOf(field));
  }

  // Externally we reference a tombstone's namespace by the field name that we apply it to, internally we use the
  // corresponding field id. Tombstones shouldn't apply in case of deleting a column and adding a new column by same
  // name (but potentially different type).
  private Namespace namespaceOf(Types.NestedField field) {
    return () -> String.valueOf(field.fieldId());
  }
}
