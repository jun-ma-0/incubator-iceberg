package org.apache.iceberg.spark.source;

import com.adobe.platform.iceberg.extensions.ExtendedTable;
import com.adobe.platform.iceberg.extensions.ExtendedTables;
import com.adobe.platform.iceberg.extensions.SimpleRecord;
import com.adobe.platform.iceberg.extensions.WithSpark;
import com.adobe.platform.iceberg.extensions.tombstone.ExtendedEntry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.parquet.Strings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.Assert;
import org.junit.Test;

public class TestVacuumTombstones extends WithSpark {

  private static final Timestamp now = Timestamp.from(Instant.ofEpochSecond(1575381935L));

  private static final List<SimpleRecord> rows = Lists.newArrayList(
      new SimpleRecord(11, now, "A", "a"),
      new SimpleRecord(12, now, "A", "a"),
      new SimpleRecord(13, now, "A", "a"),
      new SimpleRecord(14, now, "A", "a"),
      new SimpleRecord(15, now, "A", "a"),
      new SimpleRecord(15, now, "A", "a"),
      new SimpleRecord(15, now, "A", "a"),
      new SimpleRecord(15, now, "A", "a"),
      new SimpleRecord(15, now, "A", "a"),
      new SimpleRecord(15, now, "A", "c"),
      new SimpleRecord(16, now, "B", "b"),
      new SimpleRecord(17, now, "B", "b"),
      new SimpleRecord(18, now, "B", "b"),
      new SimpleRecord(18, now, "B", "b"),
      new SimpleRecord(18, now, "B", "b"),
      new SimpleRecord(18, now, "B", "b"),
      new SimpleRecord(18, now, "B", "c"),
      new SimpleRecord(18, now, "C", "a"),
      new SimpleRecord(18, now, "C", "b"),
      new SimpleRecord(18, now, "C", "c"));


  @Override
  public void implicitTable(ExtendedTables tables, String tableLocation) {
    tables.create(SimpleRecord.SCHEMA, SimpleRecord.SPEC, tableLocation,
        ImmutableMap.of("extension.skip.inclusive.evaluation", "true")
    );
  }

  @Test
  public void testVacuumOnPartitionColumn() {
    spark.createDataFrame(rows, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .write()
        .format("iceberg.adobe")
        .option("iceberg.extension.tombstone.col", "batch")
        .option("iceberg.extension.tombstone.values", "A,B")
        .mode("append")
        .save(getTableLocation());

    ExtendedTable table = TABLES.loadWithTombstoneExtension(getTableLocation());
    long readSnapshotId = table.currentSnapshot().snapshotId();

    // Read all rows by applying tombstone filtering
    // and write data by overwriting only the files that include tombstone rows.
    spark.read()
        .format("iceberg.adobe")
        .option("iceberg.extension.tombstone.vacuum", "")
        .option("iceberg.extension.tombstone.col", "batch")
        .option("snapshot-id", readSnapshotId)
        .load(getTableLocation())
        .write()
        .mode(SaveMode.Overwrite)
        .format("iceberg.adobe")
        // This instructs the writer to use an overwrite commit of the files used by the reader
        .option("iceberg.extension.tombstone.vacuum", "")
        .option("iceberg.extension.tombstone.col", "batch")
        .option("snapshot-id", readSnapshotId)
        .save(getTableLocation());

    Dataset<Row> iceberg = spark.read().format("iceberg").load(getTableLocation());
    iceberg.show(false);
    Assert.assertEquals("Result rows should match 3", 3, iceberg.count());
  }

  @Test
  public void testVacuumOnNonPartitionColumn() {
    spark.createDataFrame(rows, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .write()
        .format("iceberg.adobe")
        .option("iceberg.extension.tombstone.col", "data")
        .option("iceberg.extension.tombstone.values", "a,b")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    ExtendedTable table = TABLES.loadWithTombstoneExtension(getTableLocation());
    long snapshotId = table.currentSnapshot().snapshotId();

    spark.read()
        .format("iceberg.adobe")
        .option("iceberg.extension.tombstone.vacuum", "")
        .option("iceberg.extension.tombstone.col", "data")
        .option("snapshot-id", snapshotId)
        .load(getTableLocation())
        .write()
        .format("iceberg.adobe")
        .mode(SaveMode.Overwrite)
        .option("iceberg.extension.tombstone.vacuum", "")
        .option("iceberg.extension.tombstone.col", "data")
        .option("snapshot-id", snapshotId)
        .save(getTableLocation());

    Dataset<Row> iceberg = spark.read().format("iceberg").load(getTableLocation());
    iceberg.show(false);
    Assert.assertEquals("Result rows should match 3", 3, iceberg.count());
  }
}
