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
import com.adobe.platform.iceberg.extensions.tombstone.Namespace;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CherryPick;
import org.apache.iceberg.CherryPickFromTombstoneSnapshot;
import org.apache.iceberg.ExtendedMergeAppend;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.types.Types;

public class ExtendedBaseTable extends BaseTable implements ExtendedTable, HasTableOperations,
    Serializable {

  private TombstoneExtension tombstoneExtension;
  private ExtendedTableOperations ops;

  ExtendedBaseTable(
      ExtendedTableOperations ops, String name,
      TombstoneExtension tombstoneExtension) {
    super(ops, name);
    this.tombstoneExtension = tombstoneExtension;
    this.ops = ops;
  }

  @Override
  public ExtendedTableOperations ops() {
    return this.ops;
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
  public Vacuum newVacuumTombstones(
      Map.Entry<String, Types.NestedField> column,
      List<ExtendedEntry> entries, Long readSnapshotId) {
    return new BaseVacuum(ops, tombstoneExtension)
        .tombstones(namespaceOf(column.getValue()), column.getKey(),
            entries.stream()
                .map(e -> (EvictEntry) () -> new AbstractMap.SimpleEntry<>(e.getEntry(), e.getEvictTimestamp()))
                .collect(Collectors.toList()),
            readSnapshotId);
  }

  @Override
  public ExtendedAppendFiles newAppendWithTombstonesAdd(
      Types.NestedField column,
      List<Entry> entries,
      Map<String, String> properties,
      long evictionTs) {
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
        .appendTombstones(namespaceOf(field), entries, properties, evictionTs);
  }

  @Override
  public ExtendedAppendFiles newAppendWithTombstonesRemove(Types.NestedField column, List<EvictEntry> entries) {
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
  public List<ExtendedEntry> getSnapshotTombstones(Types.NestedField column, Snapshot snapshot) {
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
    return tombstoneExtension.get(snapshot, namespaceOf(field));
  }

  @Override
  public CherryPick cherrypick() {
    return new CherryPickFromTombstoneSnapshot(ops, tombstoneExtension);
  }

  // Externally we reference a tombstone's namespace by the field name that we apply it to, internally we use the
  // corresponding field id. Tombstones shouldn't apply in case of deleting a column and adding a new column by same
  // name (but potentially different type).
  private Namespace namespaceOf(Types.NestedField field) {
    return () -> String.valueOf(field.fieldId());
  }
}