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

import com.adobe.platform.iceberg.extensions.tombstone.ExtendedEntry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestExtendedTable extends WithSpark {

  @Test
  public void testAdditiveAppendFileWithTombstone() {
    ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
    Types.NestedField batchField = table.schema().findField("batch");
    AppendFiles first = table.newAppendWithTombstonesAdd(
        batchField,
        Lists.newArrayList(() -> "1", () -> "2", () -> "3"),
        ImmutableMap.of("purgeByMillis", "1571226183000", "reason", "test"));
    first.commit();

    AppendFiles second = table.newAppendWithTombstonesAdd(
        batchField,
        Lists.newArrayList(() -> "4", () -> "5", () -> "6"),
        ImmutableMap.of("purgeByMillis", "1571226183000", "reason", "test"));
    second.commit();

    AppendFiles third = table.newAppendWithTombstonesAdd(
        batchField,
        Lists.newArrayList(() -> "7", () -> "8", () -> "9"),
        ImmutableMap.of("purgeByMillis", "1571226183000", "reason", "test"));
    third.commit();

    List<ExtendedEntry> currentSnapshotTombstones = table.getSnapshotTombstones(
        batchField,
        table.currentSnapshot());
    Assert.assertEquals(
        "Expect all appended tombstones are available in the current snapshot",
        Lists.newArrayList("1", "2", "3", "4", "5", "6", "7", "8", "9"),
        currentSnapshotTombstones.stream().map(t -> t.getEntry().getId()).collect(Collectors.toList()));
  }

  @Test
  public void testAppendFileWithTombstoneOperations() {
    ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
    Types.NestedField batchField = table.schema().findField("batch");

    AppendFiles appendFilesAndAddTombstones = table.newAppendWithTombstonesAdd(
        batchField,
        Lists.newArrayList(() -> "1001", () -> "2002", () -> "3003"),
        ImmutableMap.of("purgeByMillis", "1571226183000", "reason", "test"));

    // Append files aPath, bPath, cPath and add tombstones `1001`, `2002` and `3003`
    appendFilesAndAddTombstones.appendFile(DataFiles.builder(SimpleRecord.spec)
        .withPath("aPath.parquet")
        .withFileSizeInBytes(12345L)
        .withRecordCount(54321L)
        .build());
    appendFilesAndAddTombstones.appendFile(DataFiles.builder(SimpleRecord.spec)
        .withPath("bPath.parquet")
        .withFileSizeInBytes(12345L)
        .withRecordCount(54321L)
        .build());
    appendFilesAndAddTombstones.appendFile(DataFiles.builder(SimpleRecord.spec)
        .withPath("cPath.parquet")
        .withFileSizeInBytes(12345L)
        .withRecordCount(54321L)
        .build());
    appendFilesAndAddTombstones.commit();

    // Append dPath and remove tombstones `1001` and `2002`
    AppendFiles appendFilesAndRemoveTombstones = table.newAppendWithTombstonesRemove(
        batchField,
        Lists.newArrayList(() -> "1001", () -> "2002"));
    appendFilesAndRemoveTombstones.appendFile(DataFiles.builder(SimpleRecord.spec)
        .withPath("dPath.parquet")
        .withFileSizeInBytes(12345L)
        .withRecordCount(54321L)
        .build());
    appendFilesAndRemoveTombstones.commit();

    // Append ePath without tombstones operations
    // It's expected that the new snapshot references the previous tombstones files
    AppendFiles appendFiles = table.newAppend();
    appendFiles.appendFile(DataFiles.builder(SimpleRecord.spec)
        .withPath("ePath.parquet")
        .withFileSizeInBytes(12345L)
        .withRecordCount(54321L)
        .build());
    appendFiles.commit();

    List<ExtendedEntry> currentSnapshotTombstones = table.getSnapshotTombstones(
        batchField,
        table.currentSnapshot());
    Assert.assertEquals("Expect only one tombstone made it to the current snapshot", 1,
        currentSnapshotTombstones.size());
    Assert.assertEquals("Expect tombstone with id `3003` the current snapshot tombstones", "3003",
        currentSnapshotTombstones.get(0).getEntry().getId());
  }

  @Test(expected = java.lang.IllegalArgumentException.class)
  public void testInvalidColumnAppendFileWithTombstone() {
    ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
    Types.NestedField noSuchColumn = table.schema().findField("noSuchColumn");

    AppendFiles first = table.newAppendWithTombstonesAdd(
        noSuchColumn,
        Lists.newArrayList(() -> "1", () -> "2", () -> "3"),
        ImmutableMap.of("purgeByMillis", "1571226183000", "reason", "test"));
    first.commit();
  }

  @Test(expected = java.lang.IllegalArgumentException.class)
  public void testInvalidColumnAppendFileWithTombstonesRemove() {
    ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
    Types.NestedField noSuchColumn = table.schema().findField("noSuchColumn");

    AppendFiles first = table.newAppendWithTombstonesRemove(
        noSuchColumn,
        Lists.newArrayList(() -> "1", () -> "2", () -> "3"));
    first.commit();
  }

  @Test(expected = java.lang.IllegalArgumentException.class)
  public void testInvalidColumnGetTombstones() {
    ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
    Types.NestedField noSuchColumn = table.schema().findField("noSuchColumn");
    table.getSnapshotTombstones(noSuchColumn, table.currentSnapshot());
  }

  @Test
  public void testAppendFileWithTombstonesAndProperties() {
    ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
    Types.NestedField batchField = table.schema().findField("batch");

    AppendFiles first = table.newAppendWithTombstonesAdd(
        batchField,
        Lists.newArrayList(() -> "1001", () -> "1002", () -> "1003"),
        ImmutableMap.of("purgeByMillis", "1571226183000", "reason", "test"));
    first.commit();

    AppendFiles second = table.newAppendWithTombstonesAdd(
        batchField,
        Lists.newArrayList(() -> "2001", () -> "2002", () -> "2003"),
        ImmutableMap.of("purgeByMillis", "1571226999000", "reason", "test"));
    second.commit();

    List<ExtendedEntry> currentSnapshotTombstones = table.getSnapshotTombstones(
        batchField,
        table.currentSnapshot());
    Assert.assertEquals("Expect that all six tombstones made it to the current snapshot", 6,
        currentSnapshotTombstones.size());

    long outsidePurge = currentSnapshotTombstones.stream()
        .filter(t -> Long.parseLong(t.getProperties().get("purgeByMillis")) < 1571226183000L)
        .filter(t -> Long.parseLong(t.getProperties().get("purgeByMillis")) > 1571226999000L)
        .count();
    Assert.assertEquals("Expect that no tombstones qualifies outside the interval", 0, outsidePurge);
  }

  @Test
  public void testAppendFileWithTombstonesAndFilterOnDifferentColumn() {
    ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
    Types.NestedField batchField = table.schema().findField("batch");

    AppendFiles appendFilesAndAddTombstones = table.newAppendWithTombstonesAdd(
        batchField,
        Lists.newArrayList(() -> "1001", () -> "2002", () -> "3003"),
        ImmutableMap.of("purgeByMillis", "1571226183000", "reason", "test"));
    appendFilesAndAddTombstones.commit();

    Types.NestedField idField = table.schema().findField("id");
    List<ExtendedEntry> currentSnapshotTombstones = table.getSnapshotTombstones(idField, table.currentSnapshot());
    Assert.assertEquals("Expect none of the tombstone apply since they reference column `batch` not `id`", 0,
        currentSnapshotTombstones.size());
  }

  @Test(expected = NoSuchTableException.class)
  public void testAppendFileWithTombstonesNoSuchTable() {
    tables.loadWithTombstoneExtension("noSuchTablePath");
  }
}
