package com.adobe.platform.iceberg.extensions;

import com.adobe.platform.iceberg.extensions.tombstone.Entry;
import com.adobe.platform.iceberg.extensions.tombstone.ExtendedEntry;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

public interface ExtendedTable extends Table, Serializable {

  /**
   * Create a new {@link ExtendedAppendFiles append API} to add files and add tombstones to this
   * table and commit as a single operation.
   *
   * @param column a column name that must be part of the schema
   * @param entries tombstone entries that need to be added to the next snapshot
   * @param properties bag of properties to be projected on all entries
   * @return a new {@link ExtendedAppendFiles}
   */
  ExtendedAppendFiles newAppendWithTombstonesAdd(Types.NestedField column, List<Entry> entries,
      Map<String, String> properties);

  /**
   * Create a new {@link ExtendedAppendFiles append API} to add files and remove tombstones to this
   * table and commit as a single operation.
   *
   * @param column a column name that must be part of the schema
   * @param entries tombstone entries that need to be added to the next snapshot
   * @return a new {@link ExtendedAppendFiles}
   */
  ExtendedAppendFiles newAppendWithTombstonesRemove(Types.NestedField column, List<Entry> entries);

  /**
   * Retrieves list of tombstones from this table's specific snapshot for the indicated schema
   * column.
   *
   * @param column a column name that must be part of the schema
   * @param snapshot a specific snapshot
   * @return a new list of tombstones stored for the specific column
   */
  List<ExtendedEntry> getSnapshotTombstones(Types.NestedField column, Snapshot snapshot);

  /**
   * Create a new {@link Vacuum API} to replace files after removing tombstones from this table.
   *
   * @param column a map entry using the full column name as key and the nested field as value
   * @param entries tombstone entries associated for the rows that will be deleted from data files
   * @param readSnapshotId the read snapshot id
   * @return a new {@link Vacuum}
   */
  Vacuum newVacuumTombstones(Map.Entry<String, Types.NestedField> column, List<Entry> entries,
      Long readSnapshotId);
}
