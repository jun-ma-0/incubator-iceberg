package com.adobe.platform.iceberg.extensions.tombstone;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.io.OutputFile;

public interface TombstoneExtension {

  String TOMBSTONE_COLUMN = "iceberg.extension.tombstone.col";
  String TOMBSTONE_COLUMN_VALUES_LIST = "iceberg.extension.tombstone.values";
  // This links the file we've used to store the provided tombstones to a snapshot summary property for Iceberg
  String SNAPSHOT_TOMBSTONE_FILE_PROPERTY = "iceberg.extension.tombstone.file";
  String TOMBSTONE_VACUUM = "iceberg.extension.tombstone.vacuum";

  /**
   * Retrieves list of {@link ExtendedEntry} tombstones
   *
   * @param snapshot the table snapshot
   * @param namespace the namespace used to identify the tombstones
   * @return list of tombstones extended entries in the snapshot and namespace
   */
  List<ExtendedEntry> get(Snapshot snapshot, Namespace namespace);

  /**
   * Appends current tombstones entries with the provided tombstones and writes result into new
   * file.
   *
   * @param snapshot the table snapshot
   * @param entries tombstones references
   * @param namespace the namespace used to append the tombstones to
   * @param properties tombstones' properties
   * @param newSnapshotId new snapshot id
   * @return instance of {@link OutputFile} that has the appropriate tombstone entries
   */
  OutputFile append(Snapshot snapshot, List<Entry> entries, Namespace namespace,
      Map<String, String> properties, long newSnapshotId);

  /**
   * Removes all provided tombstones entries by id and namespace and writes result into new file.
   *
   * @param snapshot the table snapshot
   * @param entries tombstones references
   * @param namespace the namespace used to append the tombstones to
   * @return instance of {@link OutputFile} that has the appropriate tombstone entries
   */
  OutputFile remove(Snapshot snapshot, List<Entry> entries, Namespace namespace);

  /**
   * Copies the referenced tombstone file (where available) from the current snapshot to the new
   * snapshot along with appending the referenced files as an atomic commit operation.
   *
   * @param snapshot the table snapshot
   * @return an optional {@link OutputFile} that has the appropriate tombstone entries
   */
  Optional<String> copyReference(Snapshot snapshot);

  /**
   * Removes all existing tombstones entries by namespace and writes result into new file.
   *
   * @param snapshot the table snapshot
   * @param namespace the namespace used to append the tombstones to
   * @return instance of {@link OutputFile} that has the appropriate tombstone entries
   */
  OutputFile remove(Snapshot snapshot, Namespace namespace);

}
