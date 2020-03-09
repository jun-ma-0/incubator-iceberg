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
import com.adobe.platform.iceberg.extensions.tombstone.HadoopTombstoneExtension;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.exceptions.DuplicateWAPCommitException;
import org.apache.iceberg.exceptions.TombstoneThresholdViolationException;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static java.util.stream.Collectors.toList;

public class TestWapWorkflowOverTombstone extends WithSpark implements WithExecutorService {

  @Test
  public void testTombstoneWAPCommitWithValidTableProperties() {
    ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
    // Update table property
    table.updateProperties().set(HadoopTombstoneExtension.TOMBSTONE_MAX_COUNT_PROPERTY, "6").commit();
    table.refresh();

    Types.NestedField batchField = table.schema().findField("batch");

    AppendFiles first = table.newAppendWithTombstonesAdd(
        batchField,
        Lists.newArrayList(() -> "1", () -> "2", () -> "3"),
        ImmutableMap.of("purgeByMillis", "1571226183000", "reason", "test"),
        1579792561L);
    first.commit();

    AppendFiles second = table.newAppendWithTombstonesAdd(
        batchField,
        Lists.newArrayList(() -> "4", () -> "5", () -> "6"),
        ImmutableMap.of("purgeByMillis", "1571226183000", "reason", "test"),
        1579792561L);

    second.set(SnapshotSummary.STAGED_WAP_ID_PROP, "123")
        .stageOnly()
        .commit();

    // 1st staged snapshots
    List<Snapshot> snapshots = listSnapshots(table);
    Snapshot staged1Snapshot = snapshots.get(snapshots.size() - 1);

    // should work since its trying to add tombstone on top of current table
    AppendFiles third = table.newAppendWithTombstonesAdd(
        batchField,
        Lists.newArrayList(() -> "7", () -> "8", () -> "9"),
        ImmutableMap.of("purgeByMillis", "1571226183000", "reason", "test"),
        1579792561L);
    third.set(SnapshotSummary.STAGED_WAP_ID_PROP, "456")
        .commit();

    // should fail since now the limit will reach 9
    AssertHelpers.assertThrows("Exceeding tombstone limit. Max configured: 6 total entries: 9",
        TombstoneThresholdViolationException.class, "",
        () -> table.cherrypick().cherrypick(staged1Snapshot.snapshotId()).commit());
  }

  @Test
  public void testSerialCherrypickWithTombstone() {
    ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
    Types.NestedField batchField = table.schema().findField("batch");

    AppendFiles first = table.newAppendWithTombstonesAdd(
        batchField,
        Lists.newArrayList(() -> "1", () -> "2", () -> "3"),
        ImmutableMap.of("purgeByMillis", "1571226183000", "reason", "test"),
        1579792561L);
    first.commit();

    AppendFiles second = table.newAppendWithTombstonesAdd(
        batchField,
        Lists.newArrayList(() -> "4", () -> "5", () -> "6"),
        ImmutableMap.of("purgeByMillis", "1571226183000", "reason", "test"),
        1579792561L);

    second.set(SnapshotSummary.STAGED_WAP_ID_PROP, "123")
        .stageOnly()
        .commit();

    // cherrypick the 1st staged snapshots
    List<Snapshot> snapshots = listSnapshots(table);
    Snapshot staged1Snapshot = snapshots.get(snapshots.size() - 1);
    table.cherrypick().cherrypick(staged1Snapshot.snapshotId()).commit();

    List<String> tombstonesAfter = table.getSnapshotTombstones(batchField, table.currentSnapshot())
        .stream()
        .map(t -> t.getEntry().getId())
        .collect(Collectors.toList());
    Assert.assertTrue(
        "Expect all appended tombstones in first set are available in the current snapshot and no more",
        tombstonesAfter.size() == 6 && tombstonesAfter.containsAll(
            Lists.newArrayList("1", "2", "3", "4", "5", "6")));

    AppendFiles third = table.newAppendWithTombstonesAdd(
        batchField,
        Lists.newArrayList(() -> "7", () -> "8", () -> "9"),
        ImmutableMap.of("purgeByMillis", "1571226183000", "reason", "test"),
        1579792561L);
    third.set(SnapshotSummary.STAGED_WAP_ID_PROP, "456")
        .stageOnly()
        .commit();

    // cherrypick the 2nd staged snapshot
    snapshots = listSnapshots(table);
    Snapshot staged2Snapshot = snapshots.get(snapshots.size() - 1);
    table.cherrypick().cherrypick(staged2Snapshot.snapshotId()).commit();

    List<String> collect = table.getSnapshotTombstones(batchField, table.currentSnapshot())
        .stream()
        .map(t -> t.getEntry().getId())
        .collect(Collectors.toList());

    Assert.assertTrue(
        "Expect all appended tombstones in second set are available in the current snapshot and no more",
        collect.size() == 9 && collect.containsAll(
            Lists.newArrayList("1", "2", "3", "4", "5", "6", "7", "8", "9")));
  }

  private List<Snapshot> listSnapshots(ExtendedTable table) {
    table.refresh();
    return Lists.newArrayList(table.snapshots());
  }

  private Callable<String> doCherrypick(ExtendedTable table, Long snapshotId) {
    return () -> {
      table.cherrypick().cherrypick(snapshotId).commit();
      return String.valueOf(snapshotId);
    };
  }

  @Test
  public void testParallelCherrypickWithTombstone() throws InterruptedException {
    ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
    Types.NestedField field = table.schema().findField("batch");

    int stagedSnapshotCount = 100;
    LongStream stagedSnapshots  = IntStream.range(0, stagedSnapshotCount).mapToLong(i -> {
      table.newAppendWithTombstonesAdd(field,
              Lists.newArrayList(() -> String.valueOf(i)), Collections.emptyMap(), i)
              .set("wap.id", String.valueOf(i))
              .stageOnly()
              .commit();
      List<Snapshot> snapshots = listSnapshots(table);
      return snapshots.get(snapshots.size() - 1).snapshotId();
    });

    // This will generate 100 callable commit operations with all tombstones available from 0 to 100
    List<Callable<String>> commits = stagedSnapshots.mapToObj(snapshot ->
        doCherrypick(table, snapshot)).collect(toList());

    // All commits will be executed on a fixed thread pool of two threads
    ExecutorService executorService = Executors.newFixedThreadPool(2);

    try {
      executorService.invokeAll(commits, 30, TimeUnit.SECONDS);
    } finally {
      shutdownAndAwaitTermination(executorService);
    }

    int tombstonesCount = table.getSnapshotTombstones(field, table.currentSnapshot()).size();
    int snapshotCount = Iterables.size(table.snapshots());
    int publishedSnapshotCount = snapshotCount - stagedSnapshotCount;
    Assert.assertEquals("Expect published snapshot count is the same as tombstone count",
        publishedSnapshotCount, tombstonesCount);
  }

  @Test
  public void testParallelCherrypickWithIntermittentTombstoneCommits() {
    ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
    Types.NestedField batchField = table.schema().findField("batch");

    table.newAppendWithTombstonesAdd(
        batchField, Lists.newArrayList(() -> "1", () -> "2", () -> "3"), Collections.emptyMap(), 1579792561L)
        .commit();

    table.newAppendWithTombstonesAdd(
        batchField, Lists.newArrayList(() -> "4", () -> "5", () -> "6"), Collections.emptyMap(), 1579792561L)
        .set("wap.id", "456")
        .stageOnly()
        .commit();

    List<Snapshot> snapshots = listSnapshots(table);
    Snapshot staged1Snapshot = snapshots.get(snapshots.size() - 1);

    table.newAppendWithTombstonesAdd(
        batchField, Lists.newArrayList(() -> "7", () -> "8", () -> "9"), Collections.emptyMap(), 1579792561L)
        .set("wap.id", "789")
        .stageOnly()
        .commit();

    snapshots = listSnapshots(table);
    Snapshot staged2Snapshot = snapshots.get(snapshots.size() - 1);

    // Intermittent append of new tombstone
    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "99"),
        Collections.emptyMap(), 1579792561L)
        .commit();
    // Intermittent remove of same tombstones as from first staged snapshot
    table.newAppendWithTombstonesRemove(
        batchField,
        ImmutableList.of(
            () -> new AbstractMap.SimpleEntry<>(() -> "4", 1579792561L),
            () -> new AbstractMap.SimpleEntry<>(() -> "5", 1579792561L),
            () -> new AbstractMap.SimpleEntry<>(() -> "6", 1579792561L)))
        .commit();
    // cherrypick both staged snapshots to simulate parallel  cherry-picking
    table.cherrypick().cherrypick(staged1Snapshot.snapshotId()).commit();

    // Intermittent append of new tombstones
    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "98"),
        Collections.emptyMap(), 1579792561L)
        .commit();
    // Intermittent remove of same tombstones as from second staged snapshot
    table.newAppendWithTombstonesRemove(
        batchField,
        ImmutableList.of(
            () -> new AbstractMap.SimpleEntry<>(() -> "7", 1579792561L),
            () -> new AbstractMap.SimpleEntry<>(() -> "8", 1579792561L),
            () -> new AbstractMap.SimpleEntry<>(() -> "9", 1579792561L)))
        .commit();
    table.cherrypick().cherrypick(staged2Snapshot.snapshotId()).commit();

    snapshots = listSnapshots(table);
    Assert.assertEquals("Snapshot count should include both staged and published snapshots", 9,
        snapshots.size());

    table.refresh();
    List<ExtendedEntry> tombstones = table.getSnapshotTombstones(batchField, table.currentSnapshot());
    List<String> collect = tombstones.stream().map(t -> t.getEntry().getId()).collect(Collectors.toList());

    Assert.assertTrue(
        "Expect all appended tombstones in second set are available in the current snapshot and no more",
        collect.size() == 11 && collect.containsAll(
            Lists.newArrayList("1", "2", "3", "4", "5", "6", "7", "8", "9",
            "99", "98")));
  }

  @Test
  public void testCaseInsensitiveTombstoneMergeForParallelCherrypickCommits() {
    ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
    Types.NestedField batchField = table.schema().findField("batch");

    table.newAppendWithTombstonesAdd(
        batchField, Lists.newArrayList(() -> "a"), Collections.emptyMap(), 1579792561L)
        .commit();

    table.newAppendWithTombstonesAdd(
        batchField, Lists.newArrayList(() -> "test"), Collections.emptyMap(), 1579792561L)
        .set("wap.id", "100")
        .stageOnly()
        .commit();

    List<Snapshot> snapshots = listSnapshots(table);
    Snapshot staged1Snapshot = snapshots.get(snapshots.size() - 1);

    table.newAppendWithTombstonesAdd(
        batchField, Lists.newArrayList(() -> "TEST"), Collections.emptyMap(), 1579792561L)
        .set("wap.id", "101")
        .stageOnly()
        .commit();

    snapshots = listSnapshots(table);
    Snapshot staged2Snapshot = snapshots.get(snapshots.size() - 1);

    table.newAppendWithTombstonesAdd(
        batchField, Lists.newArrayList(() -> "TEst"), Collections.emptyMap(), 1579792561L)
        .set("wap.id", "102")
        .stageOnly()
        .commit();

    snapshots = listSnapshots(table);
    Snapshot staged3Snapshot = snapshots.get(snapshots.size() - 1);

    table.cherrypick().cherrypick(staged1Snapshot.snapshotId()).commit();
    table.cherrypick().cherrypick(staged2Snapshot.snapshotId()).commit();
    table.cherrypick().cherrypick(staged3Snapshot.snapshotId()).commit();

    snapshots = listSnapshots(table);
    Assert.assertEquals("Snapshot count should include both staged and published snapshots", 7, snapshots.size());

    table.refresh();
    List<ExtendedEntry> tombstones = table.getSnapshotTombstones(batchField, table.currentSnapshot());
    List<String> collect = tombstones.stream().map(t -> t.getEntry().getId()).collect(Collectors.toList());

    Assert.assertTrue(
        "Expect all appended tombstones in second set are available in the current snapshot and no more",
        collect.size() == 2 && collect.containsAll(Lists.newArrayList("a", "test")));
  }

  @Test
  public void testTombstoneWithCherryPickingWithCommitRetry() {
    TestExtendedTableOperations ops =
        new TestExtendedTableOperations(new Path(getTableLocation()), new Configuration());
    ops.failCommits(3);
    ExtendedTable table = new ExtendedBaseTable(ops, getTableLocation(),
        new HadoopTombstoneExtension(new Configuration(), ops));

    Types.NestedField batchField = table.schema().findField("batch");

    final DataFile fileA = DataFiles.builder(table.spec())
        .withPath("/path/to/data-a.parquet")
        .withFileSizeInBytes(0)
        .withRecordCount(1)
        .build();

    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "A"),
        Collections.emptyMap(), 1579792561L)
        .appendFile(fileA)
        .commit();

    table.refresh();

    // load current snapshot
    Snapshot parentSnapshot = table.currentSnapshot();
    long firstSnapshotId = parentSnapshot.snapshotId();

    final DataFile fileB = DataFiles.builder(table.spec())
        .withPath("/path/to/data-b.parquet")
        .withFileSizeInBytes(0)
        .withRecordCount(1)
        .build();

    // first WAP commit
    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "B"), Collections.emptyMap(), 1579792561L)
        .appendFile(fileB)
        .set(SnapshotSummary.STAGED_WAP_ID_PROP, "123456789")
        .stageOnly()
        .commit();

    table.refresh();

    // pick the snapshot that's staged but not committed
    Snapshot wap1Snapshot = listSnapshots(table).get(1);

    Assert.assertEquals("Should have two snapshots", 2, listSnapshots(table).size());
    Assert.assertEquals("Should have first wap id in summary", "123456789",
        wap1Snapshot.summary().get(SnapshotSummary.STAGED_WAP_ID_PROP));
    Assert.assertEquals("Current snapshot should be first commit's snapshot",
        firstSnapshotId, table.currentSnapshot().snapshotId());
    Assert.assertEquals("Parent snapshot id should be same for first WAP snapshot",
        firstSnapshotId, wap1Snapshot.parentId().longValue());
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 1,
        listSnapshots(table).stream().filter(snapshot -> !snapshot.summary().containsKey("wap.id")).count());

    parentSnapshot = table.currentSnapshot();
    // cherry-pick first snapshot
    table.cherrypick().cherrypick(wap1Snapshot.snapshotId()).commit();

    Assert.assertEquals("Should contain manifests for both files", 2,
        table.currentSnapshot().manifests().size());
    Assert.assertEquals("Should contain append from last commit", 1,
        Iterables.size(table.currentSnapshot().addedFiles()));
    Assert.assertEquals("Parent snapshot id should change to latest snapshot before commit",
        parentSnapshot.snapshotId(), table.currentSnapshot().parentId().longValue());
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 2,
        listSnapshots(table).stream().filter(snapshot -> !snapshot.summary().containsKey("wap.id")).count());

    table.refresh();

    List<ExtendedEntry> tombstones = table.getSnapshotTombstones(batchField, table.currentSnapshot());
    List<String> collect = tombstones.stream().map(t -> t.getEntry().getId()).collect(Collectors.toList());

    Assert.assertTrue(
        "Expect all appended tombstones in second set are available in the current snapshot and no more",
        collect.size() == 2 && collect.containsAll(Lists.newArrayList("A", "B")));
  }

  @Test
  public void testTombstoneWithDuplicateCherryPicking() {
    TestExtendedTableOperations ops =
        new TestExtendedTableOperations(new Path(getTableLocation()), new Configuration());
    ops.failCommits(3);
    ExtendedTable table = new ExtendedBaseTable(ops, getTableLocation(),
        new HadoopTombstoneExtension(new Configuration(), ops));

    Types.NestedField batchField = table.schema().findField("batch");

    final DataFile fileA = DataFiles.builder(table.spec())
        .withPath("/path/to/data-a.parquet")
        .withFileSizeInBytes(0)
        .withRecordCount(1)
        .build();

    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "A"),
        Collections.emptyMap(), 1579792561L)
        .appendFile(fileA)
        .commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();

    final DataFile fileB = DataFiles.builder(table.spec())
        .withPath("/path/to/data-b.parquet")
        .withFileSizeInBytes(0)
        .withRecordCount(1)
        .build();

    // first WAP commit
    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "B"),
        Collections.emptyMap(), 1579792561L)
        .appendFile(fileB)
        .set(SnapshotSummary.STAGED_WAP_ID_PROP, "123456789")
        .stageOnly()
        .commit();

    // pick the snapshot that's staged but not committed
    Snapshot wapSnapshot = listSnapshots(table).get(1);

    Assert.assertEquals("Should have both snapshots", 2, listSnapshots(table).size());
    Assert.assertEquals("Should have first wap id in summary", "123456789",
        wapSnapshot.summary().get(SnapshotSummary.STAGED_WAP_ID_PROP));
    Assert.assertEquals("Current snapshot should be first commit's snapshot",
        firstSnapshotId, table.currentSnapshot().snapshotId());
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 1,
        listSnapshots(table).stream().filter(snapshot -> !snapshot.summary().containsKey("wap.id")).count());

    // cherry-pick snapshot
    table.cherrypick().cherrypick(wapSnapshot.snapshotId()).commit();

    Assert.assertEquals("Should have three snapshots", 3, listSnapshots(table).size());
    Assert.assertEquals("Should contain manifests for both files", 2,
        table.currentSnapshot().manifests().size());
    Assert.assertEquals("Should contain append from last commit", 1,
        Iterables.size(table.currentSnapshot().addedFiles()));
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 2,
        listSnapshots(table).stream().filter(snapshot -> !snapshot.summary().containsKey("wap.id")).count());

    AssertHelpers.assertThrows("should throw exception", DuplicateWAPCommitException.class,
        String.format("Duplicate request to cherry pick wap id that was published already: %s", 12345678), () -> {
          // duplicate cherry-pick snapshot
          table.cherrypick().cherrypick(wapSnapshot.snapshotId()).commit();
        }
    );
  }

  @Test
  public void testTombstoneMergeRetainsBaseOverStaged() {
    ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
    Types.NestedField batchField = table.schema().findField("batch");

    table.newAppendWithTombstonesAdd(batchField,
        Lists.newArrayList(() -> "1", () -> "2", () -> "3"), ImmutableMap.of("purgeByMillis", "100"), 1579792561L)
        .commit();

    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "4", () -> "5", () -> "6"),
        ImmutableMap.of("purgeByMillis", "200"), 1579792561L)
        .set(SnapshotSummary.STAGED_WAP_ID_PROP, "456:100")
        .stageOnly()
        .commit();

    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "4", () -> "5", () -> "6"),
        ImmutableMap.of("purgeByMillis", "300"), 1579792561L)
        .set(SnapshotSummary.STAGED_WAP_ID_PROP, "456:200")
        .stageOnly()
        .commit();

    List<Snapshot> snapshots = listSnapshots(table);

    table.newAppendWithTombstonesAdd(batchField,
        Lists.newArrayList(() -> "4", () -> "5", () -> "6"), ImmutableMap.of("purgeByMillis", "999"), 1579792561L)
        .commit();

    Snapshot staged1Snapshot = snapshots.get(snapshots.size() - 1);
    table.cherrypick().cherrypick(staged1Snapshot.snapshotId()).commit();

    Snapshot staged2Snapshot = snapshots.get(snapshots.size() - 2);
    table.cherrypick().cherrypick(staged2Snapshot.snapshotId()).commit();

    List<ExtendedEntry> snapshotTombstones = table.getSnapshotTombstones(batchField, table.currentSnapshot());
    List<String> tombstoneIds = snapshotTombstones
        .stream()
        .map(t -> t.getEntry().getId())
        .collect(Collectors.toList());

    Assert.assertTrue(
        "Expect all appended tombstones in second set are available in the current snapshot and no more",
        tombstoneIds.size() == 6 && tombstoneIds.containsAll(
            Lists.newArrayList("1", "2", "3", "4", "5", "6")));

    Map<String, String> purgeByMillisByTombstoneId = snapshotTombstones
        .stream()
        .collect(Collectors.toMap(
            e -> e.getEntry().getId(),
            e -> e.getProperties().get("purgeByMillis")));

    Assert.assertEquals(
        "Expect that base properties retained in favour of staged properties for same tombstone id and namespace",
        purgeByMillisByTombstoneId.get("4"), "999");
    Assert.assertEquals(
        "Expect that base properties retained in favour of staged properties for same tombstone id and namespace",
        purgeByMillisByTombstoneId.get("5"), "999");
    Assert.assertEquals(
        "Expect that base properties retained in favour of staged properties for same tombstone id and namespace",
        purgeByMillisByTombstoneId.get("6"), "999");
  }
}