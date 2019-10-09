package org.apache.iceberg.spark.source;

import com.adobe.platform.iceberg.extensions.ExtendedTable;
import com.adobe.platform.iceberg.extensions.ExtendedTables;
import com.adobe.platform.iceberg.extensions.SimpleRecord;
import com.adobe.platform.iceberg.extensions.WithSpark;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.hamcrest.Matcher;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;

public class TestVacuumTombstonesConcurrency extends WithSpark {

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

  private static final List<SimpleRecord> notTombstonedRows = Lists.newArrayList(
      new SimpleRecord(11, now, "X", "x"),
      new SimpleRecord(12, now, "X", "x"),
      new SimpleRecord(13, now, "X", "x"),
      new SimpleRecord(14, now, "Y", "y"),
      new SimpleRecord(15, now, "Y", "y"),
      new SimpleRecord(15, now, "Y", "y"));


  @Override
  public void implicitTable(ExtendedTables tables, String tableLocation) {
    tables.create(SimpleRecord.SCHEMA, SimpleRecord.SPEC, tableLocation,
        ImmutableMap.of("extension.skip.inclusive.evaluation", "true")
    );
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testVacuumOnNonPartitionColumnWithConcurrentAdditionOfTombstoneRows() {

    exceptionRule.expect(org.apache.spark.SparkException.class);
    exceptionRule.expectCause(Is.isA(ValidationException.class));
    exceptionRule.expectMessage("Writing job aborted.");

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

    // Reduce tombstone rows
    Dataset<Row> data = spark.read()
        .format("iceberg.adobe")
        .option("iceberg.extension.tombstone.vacuum", "")
        .option("iceberg.extension.tombstone.col", "data")
        .option("snapshot-id", snapshotId)
        .load(getTableLocation());

    // Add more data using columns marked for tombstone
    spark.createDataFrame(rows, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .write()
        .format("iceberg.adobe")
        .option("iceberg.extension.tombstone.col", "data")
        .option("iceberg.extension.tombstone.values", "a,b")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    // Write reduced tombstone rows - expect the writer to detect newly added files and abort
    data.write()
        .format("iceberg.adobe")
        .mode(SaveMode.Overwrite)
        .option("iceberg.extension.tombstone.vacuum", "")
        .option("iceberg.extension.tombstone.col", "data")
        .option("snapshot-id", snapshotId)
        .save(getTableLocation());
  }

  @Test
  public void testVacuumOnNonPartitionColumnWithConcurrentAdditionOfNoTombstoneRows() {

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

    // Reduce tombstone rows
    Dataset<Row> data = spark.read()
        .format("iceberg.adobe")
        .option("iceberg.extension.tombstone.vacuum", "")
        .option("iceberg.extension.tombstone.col", "data")
        .option("snapshot-id", snapshotId)
        .load(getTableLocation());

    // Add more data using columns NOT marked for tombstone
    spark.createDataFrame(notTombstonedRows, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .write()
        .format("iceberg.adobe")
        .option("iceberg.extension.tombstone.col", "data")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    // Add more data using columns NOT marked for tombstone
    spark.createDataFrame(notTombstonedRows, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .write()
        .format("iceberg.adobe")
        .option("iceberg.extension.tombstone.col", "data")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    // Write reduced tombstone rows - expect the writer to detect newly added files and abort
    data.write()
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
