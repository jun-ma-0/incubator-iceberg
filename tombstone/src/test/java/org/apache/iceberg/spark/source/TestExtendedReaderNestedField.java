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

import com.adobe.platform.iceberg.extensions.ExtendedTables;
import com.adobe.platform.iceberg.extensions.WithSpark;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import com.google.common.collect.Lists;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

public class TestExtendedReaderNestedField extends WithSpark {

  private static final List<StructField> SYSTEM_STRUCT = Arrays.asList(
      new StructField("acp_sourceBatchId", DataTypes.StringType, false, Metadata.empty()),
      new StructField("acp_prop_map",
          DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true,
          Metadata.empty()));

  private static final StructType SCHEMA = new StructType(
      new StructField[] {
          new StructField("_id", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("timestamp", DataTypes.TimestampType, false, Metadata.empty()),
          new StructField("batch", DataTypes.StringType, false, Metadata.empty()),
          new StructField("_acp_system_metadata", DataTypes.createStructType(SYSTEM_STRUCT), false,
              Metadata.empty())
      });

  private static final Schema ICEBERG_SCHEMA = SparkSchemaUtil.convert(SCHEMA);

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(ICEBERG_SCHEMA)
          .day("timestamp", "_ACP_DATE")
          .bucket("_id", 10)
          .identity("batch")
          .build();

  @Override
  public void implicitTable(ExtendedTables tables, String tableLocation) {
    // Override implicit Iceberg table schema test provisioning with explicit schema and spec.
    tables.create(ICEBERG_SCHEMA, SPEC, tableLocation, Collections.singletonMap(
        "write.metadata.metrics.default",
        "truncate(36)"));
    tables
        .loadWithTombstoneExtension(getTableLocation())
        .updateSchema()
        .addBloomFilter("_acp_system_metadata.acp_sourceBatchId", 0.01, 100)
        .commit();
  }

  @Test
  public void testTombstoneFilterForPartitionColumnProjection() {
    Timestamp ts = Timestamp.valueOf("2019-10-10 10:10:10.10");

    List<Row> rows = Lists.newArrayList(
        RowFactory.create(10, ts, "XYZ", RowFactory.create("D", Collections.emptyMap())),
        RowFactory.create(101, ts, "XYZ", RowFactory.create("A", Collections.emptyMap())), // tombstoned
        RowFactory.create(104, ts, "XYZ", RowFactory.create("B", Collections.emptyMap())), // tombstoned
        RowFactory.create(201, ts, "XYZ", RowFactory.create("C", Collections.emptyMap())), // tombstoned
        RowFactory.create(20, ts, "XY", RowFactory.create("D", Collections.emptyMap())),
        RowFactory.create(202, ts, "XY", RowFactory.create("A", Collections.emptyMap())), // tombstoned
        RowFactory.create(205, ts, "XY", RowFactory.create("B", Collections.emptyMap())), // tombstoned
        RowFactory.create(301, ts, "XY", RowFactory.create("C", Collections.emptyMap())), // tombstoned
        RowFactory.create(30, ts, "Y", RowFactory.create("D", Collections.emptyMap())),
        RowFactory.create(301, ts, "Y", RowFactory.create("A", Collections.emptyMap())), // tombstoned
        RowFactory.create(301, ts, "Y", RowFactory.create("B", Collections.emptyMap())), // tombstoned
        RowFactory.create(301, ts, "Y", RowFactory.create("C", Collections.emptyMap())) // tombstoned
    );

    // Tombstone all rows where `_acp_system_metadata.acp_sourceBatchId` IN (A,B or C)
    spark.createDataFrame(rows, SCHEMA)
        .select("*")
        .write()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "_acp_system_metadata.acp_sourceBatchId")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST, "A,B,C")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_EVICT_TS, "1579792561")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    Dataset<Row> load = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "_acp_system_metadata.acp_sourceBatchId")
        .option("iceberg.read.enableV1VectorizedReader", "false")
        .load(getTableLocation());

    load.createOrReplaceTempView("temp");

    // Use 'batch' partition field for SQL projection and test count
    long selectByBatchCount = load.select("batch").count();
    Assert.assertEquals(String.format(
        "Expect 3 rows with `_acp_system_metadata.acp_sourceBatchId` NOT IN (A,B or C) but got %d",
        selectByBatchCount), 3L, selectByBatchCount);

    // Use '_id' partition field for SQL projection and test aggregation functions
    Dataset<Row> selectById = load.select("_id");

    long sum = selectById.agg(functions.sum(selectById.col("_id"))).collectAsList().get(0).getLong(0);
    Assert.assertEquals("Expect sum 60 for all but tombstoned rows", 60, sum);
    long min = load.agg(functions.min(load.col("_id"))).collectAsList().get(0).getInt(0);
    Assert.assertEquals("Expect min 10 for all but tombstoned rows", 10, min);
    long max = load.agg(functions.max(load.col("_id"))).collectAsList().get(0).getInt(0);
    Assert.assertEquals("Expect max 30  for all but tombstoned rows", 30, max);
    double avg = load.agg(functions.avg(load.col("_id"))).collectAsList().get(0).getDouble(0);
    Assert.assertEquals("Expect avg 20.0 for all but tombstoned rows", 20.0, avg, 0.0);
  }

  @Test
  public void testSparkWithBloomFilter() {
    Timestamp ts = Timestamp.valueOf("2019-10-10 10:10:10.10");

    List<Row> rows = Lists.newArrayList(
        RowFactory.create(10, ts, "XYZ", RowFactory.create("D", Collections.emptyMap())),
        RowFactory.create(101, ts, "XYZ", RowFactory.create("A", Collections.emptyMap())), // tombstoned
        RowFactory.create(104, ts, "XYZ", RowFactory.create("B", Collections.emptyMap())), // tombstoned
        RowFactory.create(201, ts, "XYZ", RowFactory.create("C", Collections.emptyMap())), // tombstoned
        RowFactory.create(20, ts, "XY", RowFactory.create("D", Collections.emptyMap())),
        RowFactory.create(202, ts, "XY", RowFactory.create("A", Collections.emptyMap())), // tombstoned
        RowFactory.create(205, ts, "XY", RowFactory.create("B", Collections.emptyMap())), // tombstoned
        RowFactory.create(301, ts, "XY", RowFactory.create("C", Collections.emptyMap())), // tombstoned
        RowFactory.create(30, ts, "Y", RowFactory.create("D", Collections.emptyMap())),
        RowFactory.create(301, ts, "Y", RowFactory.create("A", Collections.emptyMap())), // tombstoned
        RowFactory.create(301, ts, "Y", RowFactory.create("B", Collections.emptyMap())), // tombstoned
        RowFactory.create(301, ts, "Y", RowFactory.create("C", Collections.emptyMap())), // tombstoned
        RowFactory.create(301, ts, "Y", RowFactory.create("Y", Collections.emptyMap()))
    );

    // Tombstone all rows where `_acp_system_metadata.acp_sourceBatchId` IN (A,B or C)
    spark.createDataFrame(rows, SCHEMA)
        .select("*")
        .write()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "_acp_system_metadata.acp_sourceBatchId")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST, "A,B,C")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_EVICT_TS, "1579792561")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    Dataset<Row> load = sparkWithTombstonesExtension.read()
        .format("iceberg")
        .option("iceberg.bloomFilter.input",
            "[{\"field\": \"_acp_system_metadata.acp_sourceBatchId\", \"type\": \"string\", \"values\": [\"Y\"]}]")
        .load(getTableLocation());

    load.createOrReplaceTempView("temp");

    load.show();

    // Use 'batch' partition field for SQL projection and test count
    long selectByBatchCount = load.select("batch").count();
    Assert.assertEquals(String.format(
        "Expect 4 rows in the file that contains `_acp_system_metadata.acp_sourceBatchId` = `Y`",
        selectByBatchCount), 4L, selectByBatchCount);

    // Use '_id' partition field for SQL projection and test aggregation functions
    Dataset<Row> selectById = load.select("_id").distinct();

    Assert.assertEquals("Expect all rows with the same _id", 1, selectById.count());
    Assert.assertEquals("Expect all rows with _id = 301", 301L, selectById.collectAsList().get(0).getList(0));
  }

//  @Test
//  public void testFilterForMap() {
//    Timestamp ts = Timestamp.valueOf("2019-10-10 10:10:10.10");
//    ImmutableMap.of("key", ImmutableIntArray.of(1));
//    RowFactory.create(10, ts, "XYZ", RowFactory.create("D", Collections.emptyMap()));
//  }
}
