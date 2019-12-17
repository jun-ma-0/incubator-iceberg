package org.apache.iceberg;

import com.adobe.platform.iceberg.extensions.ExtendedTable;
import com.adobe.platform.iceberg.extensions.WithSpark;
import com.adobe.platform.iceberg.extensions.tombstone.ExtendedEntry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestWapWorkflowOverTombstone extends WithSpark {

  @Test
  public void testSerialCherrypickWithTombstone() {
    ExtendedTable table = TABLES.loadWithTombstoneExtension(getTableLocation());
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

    second.set("wap.id", "123")
        .stageOnly()
        .commit();

    // cherrypick the 1st staged snapshots
    List<Snapshot> snapshots = listSnapshots(table);
    Snapshot staged1Snapshot = snapshots.get(snapshots.size()-1);
    table.cherrypick().cherrypick(staged1Snapshot.snapshotId()).commit();

    List<ExtendedEntry> currentSnapshotTombstones = table.getSnapshotTombstones(batchField,
        table.currentSnapshot());
    Assert.assertEquals(
        "Expect all appended tombstones in first set are available in the current snapshot",
        Lists.newArrayList("1", "2", "3", "4", "5", "6"),
        currentSnapshotTombstones.stream().map(t -> t.getEntry().getId()).collect(Collectors.toList()));

    AppendFiles third = table.newAppendWithTombstonesAdd(
        batchField,
        Lists.newArrayList(() -> "7", () -> "8", () -> "9"),
        ImmutableMap.of("purgeByMillis", "1571226183000", "reason", "test"));
    third.set("wap.id", "456")
        .stageOnly()
        .commit();

    // cherrypick the 2nd staged snapshot
    snapshots = listSnapshots(table);
    Snapshot staged2Snapshot = snapshots.get(snapshots.size()-1);
    table.cherrypick().cherrypick(staged2Snapshot.snapshotId()).commit();


    currentSnapshotTombstones = table.getSnapshotTombstones(batchField,
        table.currentSnapshot());
    Assert.assertEquals(
        "Expect all appended tombstones in second set are available in the current snapshot",
        Lists.newArrayList("1", "2", "3", "4", "5", "6", "7", "8", "9"),
        currentSnapshotTombstones.stream().map(t -> t.getEntry().getId()).collect(Collectors.toList()));
  }

  List<Snapshot> listSnapshots(ExtendedTable table) {
    table.refresh();
    return Lists.newArrayList(table.snapshots());
  }
}
