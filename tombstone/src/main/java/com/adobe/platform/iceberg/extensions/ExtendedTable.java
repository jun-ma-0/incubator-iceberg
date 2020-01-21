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

package com.adobe.platform.iceberg.extensions;

import com.adobe.platform.iceberg.extensions.tombstone.Entry;
import com.adobe.platform.iceberg.extensions.tombstone.EvictEntry;
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
   * @param entries tombstone entries that need to be added
   * @param properties bag of properties to be projected on all entries
   * @param evictionTs eviction timestamp for rows matched by the tombstone
   * @return a new {@link ExtendedAppendFiles}
   */
  ExtendedAppendFiles newAppendWithTombstonesAdd(
      Types.NestedField column, List<Entry> entries,
      Map<String, String> properties, long evictionTs);

  /**
   * Create a new {@link ExtendedAppendFiles append API} to add files and remove tombstones to this
   * table and commit as a single operation.
   *
   * @param column a column name that must be part of the schema
   * @param entries tombstone entries and associated eviction timestamp that need to be removed
   * @return a new {@link ExtendedAppendFiles}
   */
  ExtendedAppendFiles newAppendWithTombstonesRemove(Types.NestedField column, List<EvictEntry> entries);

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
  Vacuum newVacuumTombstones(
      Map.Entry<String, Types.NestedField> column, List<ExtendedEntry> entries,
      Long readSnapshotId);

  /**
   * Used for testing purposes
   *
   * @return instance of {@link ExtendedTableOperations}
   */
  ExtendedTableOperations ops();
}
