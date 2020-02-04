/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.adobe.platform.iceberg.extensions.tombstone;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.io.OutputFile;

public interface TombstoneExtension {

  String TOMBSTONE_COLUMN = "iceberg.extension.tombstone.col";
  String TOMBSTONE_COLUMN_VALUES_LIST = "iceberg.extension.tombstone.values";
  String TOMBSTONE_COLUMN_EVICT_TS = "iceberg.extension.tombstone.evict.epochTime";
  // This links the file we've used to store the provided tombstones to a snapshot summary property for Iceberg
  String SNAPSHOT_TOMBSTONE_FILE_PROPERTY = "iceberg.extension.tombstone.file";
  String TOMBSTONE_VACUUM = "iceberg.extension.tombstone.vacuum";
  String TOMBSTONE_NOOP = "iceberg.extension.tombstone.noop";

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
  OutputFile append(
      Snapshot snapshot, List<EvictEntry> entries, Namespace namespace, Map<String, String> properties,
      long newSnapshotId);

  /**
   * Removes all provided tombstones entries by id and namespace and writes result into new file.
   *
   * @param snapshot the table snapshot
   * @param entries tombstone entry with associated eviction timestamps
   * @param namespace the namespace used to append the tombstones to
   * @return instance of {@link OutputFile} that has the appropriate tombstone entries
   */
  OutputFile remove(Snapshot snapshot, List<EvictEntry> entries, Namespace namespace);

  /**
   * Copies the referenced tombstone file (where available) from the current snapshot to the new
   * snapshot along with appending the referenced files as an atomic commit operation.
   *
   * @param snapshot the table snapshot
   * @return an optional file path for the tombstone file
   */
  Optional<String> copyReference(Snapshot snapshot);

  /**
   * Merges the variant snapshot tombstones over base snapshot tombstones.
   * Tombstones are compared based on case-insensitive id, namespace and eviction timestamp.
   * All tombstones that exist in the variant snapshot but don't exist in the base snapshot will be added to the ones
   * from the base snapshot and will be written to a new {@link OutputFile} and the method will return its location.
   *
   * @param variant source snapshot that differs from base
   * @param base destination snapshot used as the base operator for the join operation
   * @return an optional file location for the tombstone file. If there are no tombstones associated with these
   * snapshots or the base tombstones include all the variant tombstones then the method will return the tombstone
   * output file location associated to the base summary. If not such file exists the method will return an empty
   * optional. If there are variant tombstones which are unaccounted for in base then a new file will be created with
   * the merged version the two snapshots' tombstones.
   */
  Optional<String> merge(Snapshot variant, Snapshot base);
}
