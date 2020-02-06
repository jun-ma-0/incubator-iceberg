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
import com.adobe.platform.iceberg.extensions.WithSpark;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
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
    tables.create(SimpleRecord.schema, SimpleRecord.spec, tableLocation,
        ImmutableMap.of("extension.skip.inclusive.evaluation", "true")
    );
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

    ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
    long readSnapshotId = table.currentSnapshot().snapshotId();

    // Read all rows by applying tombstone filtering
    // and write data by overwriting only the files that include tombstone rows.
    spark.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_VACUUM, "")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "batch")
        .option("snapshot-id", readSnapshotId)
        .load(getTableLocation())
        .write()
        .mode(SaveMode.Overwrite)
        .format("iceberg.adobe")
        // This instructs the writer to use an overwrite commit of the files used by the reader
        .option(TombstoneExtension.TOMBSTONE_VACUUM, "")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "batch")
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
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "data")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST, "a,b")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_EVICT_TS, "1579792561")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
    long snapshotId = table.currentSnapshot().snapshotId();

    spark.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_VACUUM, "")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "data")
        .option("snapshot-id", snapshotId)
        .load(getTableLocation())
        .write()
        .format("iceberg.adobe")
        .mode(SaveMode.Overwrite)
        .option(TombstoneExtension.TOMBSTONE_VACUUM, "")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "data")
        .option("snapshot-id", snapshotId)
        .save(getTableLocation());

    Dataset<Row> iceberg = spark.read().format("iceberg").load(getTableLocation());
    iceberg.show(false);
    Assert.assertEquals("Result rows should match 3", 3, iceberg.count());
  }
}