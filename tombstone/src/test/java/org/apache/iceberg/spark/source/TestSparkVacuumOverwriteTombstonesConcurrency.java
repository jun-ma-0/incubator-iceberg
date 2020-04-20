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
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import com.google.common.collect.Lists;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.Assert;
import org.junit.Test;

public class TestSparkVacuumOverwriteTombstonesConcurrency extends WithSpark {

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
    tables.create(SimpleRecord.schema, SimpleRecord.spec, tableLocation, Collections.singletonMap(
        "write.metadata.metrics.default",
        "truncate(36)"));
  }

  @Test
  public void testVacuumOnNonPartitionColumnWithConcurrentAdditionOfNoTombstoneRows() {

    spark.createDataFrame(rows, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .write()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "data")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST, "a,b,c")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_EVICT_TS, "1579792561")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    ExtendedTable extendedTable = tables.loadWithTombstoneExtension(getTableLocation());

    // Load data for vacuum
    SparkVacuum sparkVacuum = new SparkVacuum(spark, extendedTable, "data")
        .load(100);

    // Add more data using columns NOT marked for tombstone
    spark.createDataFrame(notTombstonedRows, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .write()
        .format("iceberg.adobe")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    // Vacuum tombstone rows
    sparkVacuum.reduceWithAntiJoin();

    Dataset<Row> iceberg = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe").option(TombstoneExtension.TOMBSTONE_COLUMN, "data").load(getTableLocation());
    Assert.assertEquals("Result rows should match 6 rows, only `data` IN (x,y)", 6, iceberg.count());

    List<SimpleRecord> simpleRecords = iceberg.as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals(notTombstonedRows, simpleRecords);
  }
}
