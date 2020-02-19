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
import com.adobe.platform.iceberg.extensions.SimpleRecord;
import com.adobe.platform.iceberg.extensions.WithSpark;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import com.google.common.collect.Lists;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.Assert;
import org.junit.Test;

public class TestExtendedReader extends WithSpark {

  @Test
  public void testReaderAppliesTombstonesFilter() {
    Timestamp now = Timestamp.from(Instant.now());

    List<SimpleRecord> appendBatchA = Lists.newArrayList(
        new SimpleRecord(1, now, "A", "a"),
        new SimpleRecord(2, now, "A", "b"),
        new SimpleRecord(3, now, "A", "c"));

    spark.createDataFrame(appendBatchA, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .write()
        .format("iceberg.adobe")
        .mode("append")
        .save(getTableLocation());

    List<SimpleRecord> appendBatchB = Lists.newArrayList(
        new SimpleRecord(4, now, "B", "d"),
        new SimpleRecord(5, now, "B", "e"),
        new SimpleRecord(6, now, "B", "f"));

    spark.createDataFrame(appendBatchB, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .write()
        .format("iceberg.adobe")
        .mode("append")
        .save(getTableLocation());

    ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
    Types.NestedField batchField = table.schema().findField("batch");

    // Tombstone batches with values `A`
    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "A"), Collections.emptyMap(), 1579792561L)
        .commit();

    List<SimpleRecord> actual = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "batch")
        .load(getTableLocation())
        .select("id", "timestamp", "batch", "data")
        .orderBy("id")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();

    Assert.assertEquals("Result rows should match only rows for `B` valued `batch` partition", appendBatchB, actual);
  }

  @Test
  public void testReaderAppliesTombstonesFilterAndAcceptsAnyValue() {
    Timestamp now = Timestamp.from(Instant.now());

    List<SimpleRecord> appendBatchA = Lists.newArrayList(
        new SimpleRecord(1, now, "A", "a"),
        new SimpleRecord(11, now, "B", "a"),
        new SimpleRecord(12, now, "C", "a"),
        new SimpleRecord(13, now, "D", "a"),
        new SimpleRecord(14, now, "E", "a"),
        new SimpleRecord(15, now, "F", "a"),
        new SimpleRecord(16, now, "G", "a"),
        new SimpleRecord(17, now, "H", "a"),
        new SimpleRecord(18, now, "I", "a"),
        new SimpleRecord(19, now, "J", "a"),
        new SimpleRecord(20, now, "K", "a"),
        new SimpleRecord(21, now, "L", "a"),
        new SimpleRecord(22, now, "M", "a"),
        new SimpleRecord(23, now, "N", "a"),
        new SimpleRecord(24, now, "O", "a"));

    spark.createDataFrame(appendBatchA, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .write()
        .format("iceberg.adobe")
        .mode("append")
        .save(getTableLocation());

    ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
    Types.NestedField batchField = table.schema().findField("batch");

    // Tombstone batches by batch field
    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "A"),
        Collections.emptyMap(), 1579792561L).commit();
    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "U"),
        Collections.emptyMap(), 1579792561L).commit();
    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "W"),
        Collections.emptyMap(), 1579792561L).commit();
    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "X"),
        Collections.emptyMap(), 1579792561L).commit();
    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "Y"),
        Collections.emptyMap(), 1579792561L).commit();
    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "Z"),
        Collections.emptyMap(), 1579792561L).commit();

    List<SimpleRecord> expectedWithoutTombstone = sparkWithTombstonesExtension.read()
        .format("iceberg")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "batch")
        .load(getTableLocation())
        .select("id", "timestamp", "batch", "data")
        .orderBy("id")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match original dataset size", 15, expectedWithoutTombstone.size());

    List<SimpleRecord> tombstoneA = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "batch")
        .load(getTableLocation())
        .select("id", "timestamp", "batch", "data")
        .orderBy("id")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match 14 (all but `batch` A)", 14, tombstoneA.size());

    // Tombstone batches by batch field
    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "B"),
        Collections.emptyMap(), 1579792561L).commit();

    List<SimpleRecord> tombstoneAB = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "batch")
        .load(getTableLocation())
        .select("id", "timestamp", "batch", "data")
        .orderBy("id")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match 13 (all but `batch` A, B)", 13, tombstoneAB.size());

    // Tombstone batches by batch field
    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "C"),
        Collections.emptyMap(), 1579792561L).commit();

    List<SimpleRecord> tombstoneABC = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "batch")
        .load(getTableLocation())
        .select("id", "timestamp", "batch", "data")
        .orderBy("id")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match 12 (all but `batch` A, B, C)", 12, tombstoneABC.size());

    // Tombstone batches by batch field
    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "D"),
        Collections.emptyMap(), 1579792561L).commit();

    List<SimpleRecord> tombstoneABCD = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "batch")
        .load(getTableLocation())
        .select("id", "timestamp", "batch", "data")
        .orderBy("id")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match 11 (all but `batch` A, B, C, D)", 11, tombstoneABCD.size());
  }

  @Test
  public void testReaderAppliesTombstonesFilterWithAggregate() {
    Timestamp now = Timestamp.from(Instant.now());

    List<SimpleRecord> dataset = Lists.newArrayList(
        new SimpleRecord(1000, now, "A", "a"),
        new SimpleRecord(1, now, "B", "a"),
        new SimpleRecord(2, now, "C", "a"),
        new SimpleRecord(5, now, "D", "a"),
        new SimpleRecord(10, now, "E", "a"),
        new SimpleRecord(10, now, "F", "a"),
        new SimpleRecord(10, now, "G", "a"),
        new SimpleRecord(10, now, "H", "a"),
        new SimpleRecord(10, now, "I", "a"),
        new SimpleRecord(10, now, "J", "a"),
        new SimpleRecord(10, now, "K", "a"),
        new SimpleRecord(10, now, "L", "a"),
        new SimpleRecord(10, now, "M", "a"),
        new SimpleRecord(10, now, "N", "a"),
        new SimpleRecord(15, now, "O", "a"));

    spark.createDataFrame(dataset, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .write()
        .format("iceberg.adobe")
        .mode("append")
        .save(getTableLocation());

    ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
    Types.NestedField batchField = table.schema().findField("batch");

    // Tombstone batches by batch field
    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "A"),
        Collections.emptyMap(), 1579792561L).commit();
    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "B"),
        Collections.emptyMap(), 1579792561L).commit();
    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "C"),
        Collections.emptyMap(), 1579792561L).commit();

    long countAfterTombstone = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "batch")
        .load(getTableLocation())
        .select("id", "timestamp", "batch", "data")
        .count();

    // TODO - Fix https://jira.corp.adobe.com/browse/PLAT-43797 and update test
    Assert.assertEquals("Rows must match 12 (count all but `batch` A, B, C)", 12, countAfterTombstone);

    Dataset<Row> verifySum = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "batch")
        .load(getTableLocation());
    long sum = verifySum.agg(functions.sum(verifySum.col("id"))).collectAsList().get(0).getLong(0);
    Assert.assertEquals("Rows sum must match 120 (sum all but `batch` A, B, C)", 120, sum);

    Dataset<Row> verifyMin = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "batch")
        .load(getTableLocation());
    long min = verifyMin.agg(functions.min(verifyMin.col("id"))).collectAsList().get(0).getInt(0);
    Assert.assertEquals("Rows min must match 5 (min but `batch` A, B, C and that's D)", 5, min);

    Dataset<Row> verifyMax = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "batch")
        .load(getTableLocation());
    long max = verifyMax.agg(functions.max(verifyMax.col("id"))).collectAsList().get(0).getInt(0);
    Assert.assertEquals("Rows max must match 15 (max but `batch` A, B, C and that's O)", 15, max);

    Dataset<Row> verifyAvg = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "batch")
        .load(getTableLocation());
    double avg = verifyAvg.agg(functions.avg(verifyAvg.col("id"))).collectAsList().get(0).getDouble(0);
    Assert.assertEquals("Rows max must match 10.0 (avg all but `batch` A, B, C)", 10.0, avg, 0.0);
  }
}
