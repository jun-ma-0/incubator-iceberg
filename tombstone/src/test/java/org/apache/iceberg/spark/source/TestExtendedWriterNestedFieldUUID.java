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
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

public class TestExtendedWriterNestedFieldUUID extends WithSpark {

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
          .identity("batch")
          .build();

  @Override
  public void implicitTable(ExtendedTables tables, String tableLocation) {
    // Override implicit Iceberg table schema test provisioning with explicit schema and spec.
    tables.create(ICEBERG_SCHEMA, SPEC, tableLocation);
  }

  @Test
  public void testWriterAppendFilesAndAppendTombstonesOnStructField() {
    Timestamp now = Timestamp.valueOf("2019-10-10 10:10:10.10");
    Map<Object, Object> emptyMap = Collections.emptyMap();
    List<Row> rows = Lists.newArrayList(
        RowFactory.create(101, now, "A", RowFactory.create("c40cfc85-142f-45b9-b020-13b0efc1be16", emptyMap)),
        RowFactory.create(102, now, "A", RowFactory.create("c40cfc85-142f-45b9-b020-13b0efc1be16", emptyMap)),
        RowFactory.create(103, now, "A", RowFactory.create("c40cfc85-142f-45b9-b020-13b0efc1be16", emptyMap)),
        RowFactory.create(104, now, "A", RowFactory.create("c40cfc85-142f-45b9-b020-13b0efc1be16", emptyMap)),
        RowFactory.create(105, now, "A", RowFactory.create("c40cfc85-142f-45b9-b020-13b0efc1be16", emptyMap)),
        RowFactory.create(201, now, "A", RowFactory.create("195e44d7-2583-41f9-a662-0cfe014f8bf8", emptyMap)),
        RowFactory.create(202, now, "B", RowFactory.create("3f46818b-280f-48d1-bb99-4f7598e56658", emptyMap)),
        RowFactory.create(203, now, "B", RowFactory.create("3f46818b-280f-48d1-bb99-4f7598e56658", emptyMap)),
        RowFactory.create(203, now, "B", RowFactory.create("3f46818b-280f-48d1-bb99-4f7598e56658", emptyMap)),
        RowFactory.create(203, now, "B", RowFactory.create("195e44d7-2583-41f9-a662-0cfe014f8bf8", emptyMap)),
        RowFactory.create(204, now, "C", RowFactory.create("c40cfc85-142f-45b9-b020-13b0efc1be16", emptyMap)),
        RowFactory.create(205, now, "C", RowFactory.create("3f46818b-280f-48d1-bb99-4f7598e56658", emptyMap)),
        RowFactory.create(301, now, "C", RowFactory.create("3f46818b-280f-48d1-bb99-4f7598e56658", emptyMap)));

    spark.createDataFrame(rows, SCHEMA)
        .select("*")
        .write()
        .format("iceberg.adobe")
        .mode("append")
        .save(getTableLocation());

    List<Row> thirdBatchRows = Lists.newArrayList(
        RowFactory.create(301, now, "B", RowFactory.create("3f46818b-280f-48d1-bb99-4f7598e56658", emptyMap)),
        RowFactory.create(302, now, "B", RowFactory.create("3f46818b-280f-48d1-bb99-4f7598e56658", emptyMap)));

    // Write the data and add new tombstones for X and Y
    spark.createDataFrame(thirdBatchRows, SCHEMA)
        .select("*")
        .write()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "_acp_system_metadata.acp_sourceBatchId")
        .option(
            TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST,
            "c40cfc85-142f-45b9-b020-13b0efc1be16,195e44d7-2583-41f9-a662-0cfe014f8bf8")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_EVICT_TS, "1579792561")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    Dataset<Row> load = spark.read()
        .format("iceberg.adobe")
        .option("iceberg.extension.tombstone.col", "_acp_system_metadata.acp_sourceBatchId")
        .load(getTableLocation());
    load.show(false);

    Assert.assertEquals("Result rows should only match 3f46818b-280f-48d1-bb99-4f7598e56658", 7, load.count());
  }
}
