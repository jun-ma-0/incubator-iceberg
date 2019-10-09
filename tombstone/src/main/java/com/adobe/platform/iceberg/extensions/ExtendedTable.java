package com.adobe.platform.iceberg.extensions;

import com.adobe.platform.iceberg.extensions.tombstone.Entry;
import com.adobe.platform.iceberg.extensions.tombstone.ExtendedEntry;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

public interface ExtendedTable extends Table {

  /**
   * Create a new {@link ExtendedAppendFiles append API} to add files and add tombstones to this table and commit as a single
   * operation.
   *
   * @param column a column name that must be part of the schema
   * @param entries tombstone entries that need to be added to the next snapshot
   * @param properties bag of properties to be projected on all entries
   * @return a new {@link ExtendedAppendFiles}
   */
  ExtendedAppendFiles newAppendWithTombstonesAdd(Types.NestedField column, List<Entry> entries, Map<String, String> properties);

  /**
   * Create a new {@link ExtendedAppendFiles append API} to add files and remove tombstones to this table and commit as a
   * single operation.
   *
   * @param column a column name that must be part of the schema
   * @param entries tombstone entries that need to be added to the next snapshot
   * @return a new {@link ExtendedAppendFiles}
   */
  ExtendedAppendFiles newAppendWithTombstonesRemove(Types.NestedField column, List<Entry> entries);

  /**
   * Retrieves list of tombstones from this table's current snapshot for the indicated schema column.
   *
   * @param column a column name that must be part of the schema
   * @return a new list of tombstones stored for the specific column
   */
  List<ExtendedEntry> getCurrentSnapshotTombstones(Types.NestedField column);
}
