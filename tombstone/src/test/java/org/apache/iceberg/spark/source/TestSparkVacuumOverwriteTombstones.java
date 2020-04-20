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

package org.apache.iceberg.spark.source;

import com.adobe.platform.iceberg.extensions.ExtendedTable;
import com.adobe.platform.iceberg.extensions.ExtendedTables;
import com.adobe.platform.iceberg.extensions.SimpleRecord;
import com.adobe.platform.iceberg.extensions.SparkVacuum;
import com.adobe.platform.iceberg.extensions.WithSpark;
import com.adobe.platform.iceberg.extensions.tombstone.ExtendedEntry;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import com.google.common.collect.Lists;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.expressions.Expressions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestSparkVacuumOverwriteTombstones extends WithSpark {

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
    tables.create(SimpleRecord.schema, SimpleRecord.spec, tableLocation, Collections.singletonMap(
        "write.metadata.metrics.default",
        "truncate(36)"));
  }

  @Test
  public void testVacuumOnPartitionColumn() {
    spark.createDataFrame(rows, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .write()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "batch")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST, "A,B")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_EVICT_TS, "1579792561")
        .mode("append")
        .save(getTableLocation());

    ExtendedTable extendedTable = tables.loadWithTombstoneExtension(getTableLocation());
    new SparkVacuum(spark, extendedTable, "batch")
        .load(100)
        .reduceWithAntiJoin();

    Dataset<Row> iceberg = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "batch")
        .load(getTableLocation());
    iceberg.show(false);
    Assert.assertEquals("Result rows should match 3", 3, iceberg.count());
  }

  @Test
  public void testVacuumOnNonPartitionColumn() {
    spark.createDataFrame(rows, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .write()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "data")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST, "a,b")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_EVICT_TS, "1579792561")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    ExtendedTable extendedTable = tables.loadWithTombstoneExtension(getTableLocation());
    new SparkVacuum(spark, extendedTable, "data")
        .load(100)
        .reduceWithAntiJoin();

    Dataset<Row> iceberg = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "data")
        .load(getTableLocation());
    Assert.assertEquals("Result rows should match 3", 3, iceberg.count());
  }

  @Test
  @SuppressWarnings("checkstyle:HiddenField")
  public void testVacuumOnNonPartitionColumnForMultiValueRowsInSameFile() {
    // Rows belong to the same partition _ACP_DATE=now/batch=A with data column IN [a,b,c,d]
    List<SimpleRecord> rows = Lists.newArrayList(
        new SimpleRecord(11, now, "A", "a"),
        new SimpleRecord(12, now, "A", "a"),
        new SimpleRecord(13, now, "A", "b"),
        new SimpleRecord(14, now, "A", "b"),
        new SimpleRecord(15, now, "A", "b"),
        new SimpleRecord(16, now, "A", "c"),
        new SimpleRecord(17, now, "A", "c"),
        new SimpleRecord(18, now, "A", "c"),
        new SimpleRecord(19, now, "A", "c"),
        new SimpleRecord(20, now, "A", "d"),
        new SimpleRecord(30, now, "A", "d"),
        new SimpleRecord(40, now, "A", "d"),
        new SimpleRecord(50, now, "A", "d"));

    ExtendedTable extendedTable = tables.loadWithTombstoneExtension(getTableLocation());

    // Tombstone where data IN [a,b,c]
    spark.createDataFrame(rows, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .coalesce(1) // All rows fit in only one file per partition regardless of the executors # for test spark session
        .write()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "data")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST, "a,b,c")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_EVICT_TS, "1579792561")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    extendedTable.refresh();

    new SparkVacuum(spark, extendedTable, "data")
        .load(100)
        .reduceWithAntiJoin();

    Dataset<Row> adobeIceberg = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "data")
        .load(getTableLocation());
    Assert.assertEquals("Expect 4 rows to match `data` NOT IN [a,b,c]", 4, adobeIceberg.count());

    Dataset<Row> iceberg = sparkWithTombstonesExtension.read()
        .format("iceberg")
        .load(getTableLocation());
    Assert.assertEquals("Expect 4 rows left", 4, iceberg.count());
  }

  @Test
  @SuppressWarnings("checkstyle:HiddenField")
  public void testVacuumWhenWeDeleteVacuumFileWithOnlyVacuumRows() {
    List<SimpleRecord> rows = Lists.newArrayList();
    IntStream.range(0, 1000).forEach(i -> rows.add(new SimpleRecord(i, now, "A", "a")));
    IntStream.range(0, 1000).forEach(i -> rows.add(new SimpleRecord(i, now, "A", "b")));
    IntStream.range(0, 1000).forEach(i -> rows.add(new SimpleRecord(i, now, "A", "c")));
    IntStream.range(0, 1000).forEach(i -> rows.add(new SimpleRecord(i, now, "A", "d")));
    IntStream.range(0, 1000).forEach(i -> rows.add(new SimpleRecord(i, now, "A", "e")));

    // This will write separate parquet data files for each value of `data` column
    spark.createDataFrame(rows, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .sort("data")
        .write()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "data")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST, "a,b,c")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_EVICT_TS, "1579792561")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    ExtendedTable extendedTable = tables.loadWithTombstoneExtension(getTableLocation());
    Snapshot snapshot = extendedTable.currentSnapshot();

    // Load data for vacuum
    SparkVacuum sparkVacuum = new SparkVacuum(spark, extendedTable, "data")
        .load(100);

    // We will make a new commit in Iceberg by removing the file matching `data=a`, this is expected to be vacuumed too
    Iterator<FileScanTask> iterator =
        extendedTable.newScan().filter(Expressions.equal("data", "a")).planFiles().iterator();
    DeleteFiles deleteFiles = extendedTable.newDelete();
    while (iterator.hasNext()) {
      deleteFiles.deleteFile(iterator.next().file());
    }
    deleteFiles.commit();

    // Expect vacuum to fail since it will attempt to vacuum a data file that we've already deleted
    exceptionRule.expect(org.apache.spark.SparkException.class);
    exceptionRule.expectMessage("Writing job aborted");
    exceptionRule.expectCause(Is.isA(org.apache.iceberg.exceptions.ValidationException.class));

    // Vacuum tombstone rows
    sparkVacuum.reduceWithAntiJoin();
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  @SuppressWarnings("checkstyle:HiddenField")
  public void testVacuumWhenWeDeleteVacuumFileWithNonVacuumRows() {
    List<SimpleRecord> rows = Lists.newArrayList();
    IntStream.range(0, 1000).forEach(i -> rows.add(new SimpleRecord(i, now, "A", "a")));
    IntStream.range(0, 1000).forEach(i -> rows.add(new SimpleRecord(i, now, "A", "b")));
    IntStream.range(0, 1000).forEach(i -> rows.add(new SimpleRecord(i, now, "A", "c")));
    IntStream.range(0, 1000).forEach(i -> rows.add(new SimpleRecord(i, now, "A", "d")));
    IntStream.range(0, 1000).forEach(i -> rows.add(new SimpleRecord(i, now, "A", "e")));

    spark.createDataFrame(rows, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .sort("data")
        .coalesce(1) // this will write a single parquet data file for all values of `data` column
        .write()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "data")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST, "a,b,c")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_EVICT_TS, "1579792561")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    ExtendedTable extendedTable = tables.loadWithTombstoneExtension(getTableLocation());

    // Load data for vacuum
    SparkVacuum sparkVacuum = new SparkVacuum(spark, extendedTable, "data").load(100);

    // We will make a new commit in Iceberg by removing the file matching `data=a`, this is expected to be vacuumed too
    Iterator<FileScanTask> iterator =
        extendedTable.newScan().filter(Expressions.equal("data", "a")).planFiles().iterator();
    DeleteFiles deleteFiles = extendedTable.newDelete();
    while (iterator.hasNext()) {
      deleteFiles.deleteFile(iterator.next().file());
    }
    deleteFiles.commit();

    exceptionRule.expect(org.apache.spark.SparkException.class);
    exceptionRule.expectMessage("Writing job aborted");
    exceptionRule.expectCause(Is.isA(org.apache.iceberg.exceptions.ValidationException.class));

    // Vacuum tombstone rows
    sparkVacuum.reduceWithAntiJoin();
  }

  @Test
  @SuppressWarnings("checkstyle:HiddenField")
  public void testVacuumWithDuplicateTombstones() {
    List<SimpleRecord> rows = Lists.newArrayList();
    IntStream.range(0, 1000).forEach(i -> rows.add(new SimpleRecord(i, now, "A", "a")));

    spark.createDataFrame(rows, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .write().format("iceberg.adobe").mode(SaveMode.Append)
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "data")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST, "a")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_EVICT_TS, "1579792561")
        .save(getTableLocation());
    spark.createDataFrame(rows, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .write().format("iceberg.adobe").mode(SaveMode.Append)
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "data")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST, "a")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_EVICT_TS, "2579792561")
        .save(getTableLocation());
    spark.createDataFrame(rows, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .write().format("iceberg.adobe").mode(SaveMode.Append)
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "data")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST, "a")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_EVICT_TS, "3579792561")
        .save(getTableLocation());

    // Vacuum tombstones
    ExtendedTable extendedTable = tables.loadWithTombstoneExtension(getTableLocation());
    new SparkVacuum(spark, extendedTable, "data")
        .load(100)
        .reduceWithAntiJoin();

    extendedTable.refresh();
    List<ExtendedEntry> tombstonesPostVacuum = extendedTable.getSnapshotTombstones(
        extendedTable.schema().findField("data"),
        extendedTable.currentSnapshot());
    Assert.assertTrue(tombstonesPostVacuum.isEmpty());

    Dataset<Row> adobeIceberg = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe").option(TombstoneExtension.TOMBSTONE_COLUMN, "data").load(getTableLocation());
    Assert.assertEquals("Expect 0 rows left", 0, adobeIceberg.count());

    Dataset<Row> iceberg = sparkWithTombstonesExtension.read().format("iceberg").load(getTableLocation());
    Assert.assertEquals("Expect 0 rows left", 0, iceberg.count());
  }

  @Test
  @SuppressWarnings("checkstyle:HiddenField")
  public void testVacuumLimitTombstones() {
    List<SimpleRecord> rows = Lists.newArrayList();
    IntStream.range(0, 1000).forEach(i -> rows.add(new SimpleRecord(i, now, "A", "a")));
    IntStream.range(0, 1000).forEach(i -> rows.add(new SimpleRecord(i, now, "A", "b")));
    IntStream.range(0, 1000).forEach(i -> rows.add(new SimpleRecord(i, now, "A", "c")));

    spark.createDataFrame(rows, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .write().format("iceberg.adobe").mode(SaveMode.Append)
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "data")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST, "a,b,c")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_EVICT_TS, "1579792561")
        .save(getTableLocation());

    ExtendedTable extendedTable = tables.loadWithTombstoneExtension(getTableLocation());
    new SparkVacuum(spark, extendedTable, "data")
        .load(2)  // Vacuum any 2 tombstones
        .reduceWithAntiJoin(); // Vacuum tombstone rows

    extendedTable.refresh();
    List<ExtendedEntry> postVacuumTombstones = extendedTable.getSnapshotTombstones(
        extendedTable.schema().findField("data"),
        extendedTable.currentSnapshot());
    Assert.assertEquals(1, postVacuumTombstones.size()); // Expect one tombstone still to be present

    Dataset<Row> adobeIceberg = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe").option(TombstoneExtension.TOMBSTONE_COLUMN, "data").load(getTableLocation());
    Assert.assertEquals("Expect 0 rows left", 0, adobeIceberg.count());

    Dataset<Row> iceberg = sparkWithTombstonesExtension.read().format("iceberg").load(getTableLocation());
    Assert.assertEquals("Expect 1000 rows left", 1000, iceberg.count()); // we haven't removed all ts
  }
}
