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
import com.adobe.platform.iceberg.extensions.SparkVacuum;
import com.adobe.platform.iceberg.extensions.WithSpark;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExpressions;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
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

public class TestSparkVacuumOverwriteNestedTombstones extends WithSpark {
  private static final Map<?, ?> EMPTY_MAP = Collections.emptyMap();

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
    tables.create(ICEBERG_SCHEMA, SPEC, tableLocation, Collections.singletonMap(
        "write.metadata.metrics.default",
        "truncate(36)"));
  }

  @Test
  public void testTombstoneVacuumWithAntiJoin() {
    Timestamp timestamp = Timestamp.valueOf("2020-03-12 22:45:00.0");
    List<Row> rows = Lists.newArrayList(
        RowFactory.create(101, timestamp, "XYZ", RowFactory.create("A", Collections.emptyMap())), // tombstoned
        RowFactory.create(101, timestamp, "XYZ", RowFactory.create("B", Collections.emptyMap())), // tombstoned
        RowFactory.create(103, timestamp, "XYZ", RowFactory.create("C", Collections.emptyMap())), // tombstoned
        RowFactory.create(901, timestamp, "XYZ", RowFactory.create("D", Collections.emptyMap())),
        RowFactory.create(201, timestamp, "XY.", RowFactory.create("A", Collections.emptyMap())), // tombstoned
        RowFactory.create(202, timestamp, "XY.", RowFactory.create("B", Collections.emptyMap())), // tombstoned
        RowFactory.create(203, timestamp, "XY.", RowFactory.create("C", Collections.emptyMap())), // tombstoned
        RowFactory.create(902, timestamp, "XY.", RowFactory.create("D", Collections.emptyMap())),
        RowFactory.create(301, timestamp, "X..", RowFactory.create("A", Collections.emptyMap())),  // tombstoned
        RowFactory.create(302, timestamp, "X..", RowFactory.create("B", Collections.emptyMap())),  // tombstoned
        RowFactory.create(303, timestamp, "X..", RowFactory.create("C", Collections.emptyMap())),  // tombstoned
        RowFactory.create(903, timestamp, "X..", RowFactory.create("D", Collections.emptyMap())));

    // Write rows and tombstone any rows where `_acp_system_metadata.acp_sourceBatchId` IN (A,B,C)
    spark.createDataFrame(rows, SCHEMA)
        .select("*")
        .coalesce(1)
        .write()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "_acp_system_metadata.acp_sourceBatchId")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST, "A,B,C")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_EVICT_TS, "1579792561")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    ExtendedTable extendedTable = tables.loadWithTombstoneExtension(getTableLocation());
    List<String> tombstoneFilesBeforeVacuum = plannedFiles(
        extendedTable,
        TombstoneExpressions.matchesAny(
            "_acp_system_metadata.acp_sourceBatchId",
            Lists.newArrayList("A", "B", "C")).get());

    Dataset<Row> countBeforeTombstoneVacuum = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "_acp_system_metadata.acp_sourceBatchId")
        .option("iceberg.read.enableV1VectorizedReader", "true")
        .load(getTableLocation());
    // Assert tombstone filtering works as expected
    Assert.assertEquals(3L, countBeforeTombstoneVacuum.count());

    // Based on schema partitioning and the provided rows we're expecting 11 files to be written on disk.
    List<String> plannedFilesBeforeVacuum = plannedFiles(extendedTable, null);
    Assert.assertEquals(3L, plannedFilesBeforeVacuum.size());

    new SparkVacuum(spark, extendedTable, "_acp_system_metadata.acp_sourceBatchId")
        .load(100)
        .reduceWithAntiJoin();

    Dataset<Row> postVacuumVectorized = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "_acp_system_metadata.acp_sourceBatchId")
        .option("iceberg.read.enableV1VectorizedReader", "true")
        .load(getTableLocation());

    // Assert the available plan files after vacuum operation is reduced to only contain the non-tombstone rows, should
    // be considerable lower than before vacuum.
    extendedTable.refresh();
    List<String> plannedFilesAfterVacuum = plannedFiles(extendedTable, null);
    Assert.assertEquals(3L, plannedFilesAfterVacuum.size());

    postVacuumVectorized.show(false);
    // Assert the available number of rows after vacuum, should be the same as before vacuum.
    Assert.assertEquals(3L, postVacuumVectorized.count());
  }

  /**
   * The tests will execute the following steps:
   * > Append 3 rows as one file with sourceBatchId in range [0aae6b20... 4f0536a0] containing row with 4858fdf0...
   * > Append 3 rows as one file with sourceBatchId in range [42a3f680... 916c9290] and tombstone 4858fdf0...
   * > Starts vacuum read - both data files will match the expression used by vacuum:
   * ref(name="_acp_system_metadata.acp_sourceBatchId") == "4858fdf0-57aa-11ea-879e-23ee857a5dc6"
   * > Before writing the vacuumed rows we append 2 rows in range [373c3190... aa55d4b0] that includes 4858fdf0...
   * > Start vacuum write - the code detects the delta added file matching the expression but decides not to delete
   * that and consider it a false positive
   * > Assertions will run against for the expected number for rows before and after vacuum as well as data files
   */
  @Test
  public void testVacuumForOverlappingMetricRangesWithMixedAppendCommit() {
    Timestamp timestamp = Timestamp.valueOf("2010-03-12 22:45:00.0");
    List<Row> range0 = Lists.newArrayList(
        RowFactory.create(
            101,
            timestamp,
            "Ring0",
            RowFactory.create("0aae6b20-57ab-11ea-a88e-eba67be351d4", EMPTY_MAP)),
        RowFactory.create(
            101,
            timestamp,
            "Ring0",
            RowFactory.create("4858fdf0-57aa-11ea-879e-23ee857a5dc6", EMPTY_MAP)),
        RowFactory.create(
            103,
            timestamp,
            "Ring0",
            RowFactory.create("4f0536a0-57aa-11ea-879e-23ee857a5dc6", EMPTY_MAP)));

    List<Row> range1 = Lists.newArrayList(
        RowFactory.create(
            201,
            timestamp,
            "Ring1",
            RowFactory.create("42a3f680-57aa-11ea-a003-8b7553d103bd", EMPTY_MAP)),
        RowFactory.create(
            201,
            timestamp,
            "Ring1",
            RowFactory.create("45101480-57aa-11ea-a88e-eba67be351d4", EMPTY_MAP)),
        RowFactory.create(
            203,
            timestamp,
            "Ring1",
            RowFactory.create("916c9290-57aa-11ea-a27c-77a4cdca4585", EMPTY_MAP)));

    List<Row> range2 = Lists.newArrayList(
        RowFactory.create(
            201,
            timestamp,
            "Ring2",
            RowFactory.create("373c3190-57aa-11ea-867e-3bbd50ab7ad4", EMPTY_MAP)),
        RowFactory.create(
            203,
            timestamp,
            "Ring2",
            RowFactory.create("aa55d4b0-57aa-11ea-a003-8b7553d103bd", EMPTY_MAP)));

    spark.createDataFrame(range0, SCHEMA)
        .select("*")
        .coalesce(1)
        .write()
        .format("iceberg.adobe")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    spark.createDataFrame(range1, SCHEMA)
        .select("*")
        .coalesce(1)
        .write()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "_acp_system_metadata.acp_sourceBatchId")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST, "4858fdf0-57aa-11ea-879e-23ee857a5dc6")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_EVICT_TS, "1579792561")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    long countBeforeVacuum = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "_acp_system_metadata.acp_sourceBatchId")
        .option("iceberg.read.enableV1VectorizedReader", "true")
        .load(getTableLocation())
        .count();

    Assert.assertEquals(5L, countBeforeVacuum);

    ExtendedTable extendedTable = tables.loadWithTombstoneExtension(getTableLocation());
    List<String> planFilesBeforeVacuum = plannedFiles(extendedTable, null);

    Assert.assertEquals(2L, planFilesBeforeVacuum.size());

    // Load files to vacuum
    SparkVacuum sparkVacuum = new SparkVacuum(spark, extendedTable, "_acp_system_metadata.acp_sourceBatchId")
        .load(100);

    // Append new files
    spark.createDataFrame(range2, SCHEMA)
        .select("*")
        .coalesce(1)
        .write()
        .format("iceberg.adobe")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    // Vacuum tombstone rows
    sparkVacuum.reduceWithAntiJoin();

    Dataset<Row> afterVacuum = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "_acp_system_metadata.acp_sourceBatchId")
        .option("iceberg.read.enableV1VectorizedReader", "true")
        .load(getTableLocation());
    afterVacuum.show(false);

    // Assert the available number of rows after vacuum should account for the rows that were added ones during the
    // vacuum operation.
    Assert.assertEquals(7L, afterVacuum.count());
    extendedTable.refresh();
    List<String> planFilesAfterVacuum = plannedFiles(extendedTable, null);
    Assert.assertEquals(3L, planFilesAfterVacuum.size());

    // Assert that the two plan files are disjoint sets indicating a data rewrite
    Assert.assertTrue(Sets.intersection(Sets.newHashSet(planFilesBeforeVacuum), Sets.newHashSet(planFilesAfterVacuum))
        .isEmpty());
  }

  /**
   * This test will verify the output of the vacuum operation in the case of a false positive match on the tombstone
   * expression.
   * We expect that the false positive match file is replaced in the Iceberg table by an identical file in the same
   * partition path.
   */
  @Test
  public void testVacuumForFalsePositiveTombstoneExpressionMatch() {
    ExtendedTable extendedTable = tables.loadWithTombstoneExtension(getTableLocation());

    Timestamp christmas = Timestamp.valueOf("2019-12-25 22:45:00.0");
    List<Row> falsePositiveMatchForTombstone = Lists.newArrayList(
        RowFactory.create(101, christmas, "5e679508aa", RowFactory.create("01E327W6QPQPYED709G0EVKJ03", EMPTY_MAP)),
        RowFactory.create(101, christmas, "5e679508aa", RowFactory.create("01E327WEKGQ2AZT9P02B52V6D3", EMPTY_MAP)),
        RowFactory.create(101, christmas, "5e679508aa", RowFactory.create("01E327XJR7MW87FVKYR1QKK118", EMPTY_MAP)),
        RowFactory.create(101, christmas, "5e679508aa", RowFactory.create("01E327XP8HZA9GQTM4D73XH2BB", EMPTY_MAP)),
        RowFactory.create(101, christmas, "5e679508aa", RowFactory.create("01E327WEKGQ2AZT9P02B52V6D3", EMPTY_MAP)),
        RowFactory.create(101, christmas, "5e679508aa", RowFactory.create("01E32832PJYQ01BXP1SAA24S4K", EMPTY_MAP)),
        RowFactory.create(101, christmas, "5e679508aa", RowFactory.create("01E3283N8643T11SGNPD15F3YF", EMPTY_MAP)),
        RowFactory.create(101, christmas, "5e679508aa", RowFactory.create("01E3283VE8CCTRNFGKJ80THDWW", EMPTY_MAP)));

    spark.createDataFrame(falsePositiveMatchForTombstone, SCHEMA)
        .select("*")
        .coalesce(1)
        .write()
        .format("iceberg.adobe")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    String tombstone = "01E3280P2YFZX5M9HD1A4AQ1SJ";
    Timestamp easter2020 = Timestamp.valueOf("2020-05-21 22:45:00.0");
    List<Row> positiveMatchForTombstone = Lists.newArrayList(
        RowFactory.create(101, easter2020, "5e679506b9c", RowFactory.create("01E327WB6CAAGZV6NS5MNFBC6G", EMPTY_MAP)),
        RowFactory.create(101, easter2020, "5e679506b9c", RowFactory.create("01E327WEKGQ2AZT9P02B52V6D3", EMPTY_MAP)),
        RowFactory.create(101, easter2020, "5e679506b9c", RowFactory.create("01E327X3ZAXCGZJ7C6M0GHQC47", EMPTY_MAP)),
        RowFactory.create(101, easter2020, "5e679506b9c", RowFactory.create("01E327XBH6FQNW7QNHDJ1B2NS3", EMPTY_MAP)),
        RowFactory.create(101, easter2020, "5e679506b9c", RowFactory.create(tombstone, EMPTY_MAP)),
        RowFactory.create(101, easter2020, "5e679506b9c", RowFactory.create("01E32821E248D67ZMZX09RNMH4", EMPTY_MAP)),
        RowFactory.create(101, easter2020, "5e679506b9c", RowFactory.create("01E3283AWPMVFE0MZ8CTHNQDT8", EMPTY_MAP)),
        RowFactory.create(101, easter2020, "5e679506b9c", RowFactory.create("01E3284744MCY7D76C07J8R9HC", EMPTY_MAP)));

    spark.createDataFrame(positiveMatchForTombstone, SCHEMA)
        .select("*")
        .coalesce(1)
        .write()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "_acp_system_metadata.acp_sourceBatchId")
        .option(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST, tombstone)
        .option(TombstoneExtension.TOMBSTONE_COLUMN_EVICT_TS, "1579792561")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    Timestamp easter2019 = Timestamp.valueOf("2019-05-21 22:45:00.0");
    List<Row> allTombstoneRows = Lists.newArrayList(
        RowFactory.create(101, easter2019, "be679506b9c", RowFactory.create(tombstone, EMPTY_MAP)),
        RowFactory.create(102, easter2019, "be679506b9c", RowFactory.create(tombstone, EMPTY_MAP)),
        RowFactory.create(103, easter2019, "be679506b9c", RowFactory.create(tombstone, EMPTY_MAP)),
        RowFactory.create(104, easter2019, "be679506b9c", RowFactory.create(tombstone, EMPTY_MAP)),
        RowFactory.create(105, easter2019, "be679506b9c", RowFactory.create(tombstone, EMPTY_MAP)),
        RowFactory.create(106, easter2019, "be679506b9c", RowFactory.create(tombstone, EMPTY_MAP)),
        RowFactory.create(101, easter2019, "be679506b9c", RowFactory.create(tombstone, EMPTY_MAP)));

    spark.createDataFrame(allTombstoneRows, SCHEMA)
        .select("*")
        .coalesce(1)
        .write()
        .format("iceberg.adobe")
        .mode(SaveMode.Append)
        .save(getTableLocation());

    extendedTable.refresh();
    Assert.assertEquals(3L, plannedFiles(extendedTable, null).size());

    // Assert tombstone filtering works as expected pre vacuum
    Assert.assertEquals(15L, countAllRows(extendedTable));

    extendedTable.refresh();
    new SparkVacuum(spark, extendedTable, "_acp_system_metadata.acp_sourceBatchId")
        .load(1) // there's only one tombstone that we're vacuuming, the test should fail if we change this hypothesis
        .reduceWithAntiJoin();

    // Assert tombstone filtering works as expected post vacuum
    extendedTable.refresh();
    Assert.assertEquals(15L, countAllRows(extendedTable));

    extendedTable.refresh();
    Assert.assertEquals(2L, plannedFiles(extendedTable, null).size());
  }

  private long countAllRows(ExtendedTable extendedTable) {
    extendedTable.refresh();
    return sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "_acp_system_metadata.acp_sourceBatchId")
        .option("iceberg.read.enableV1VectorizedReader", "true")
        .load(extendedTable.location())
        .count();
  }

  private List<String> plannedFiles(ExtendedTable extendedTable, Expression expression) {
    List<String> planFiles = new ArrayList<>();
    CloseableIterable<FileScanTask> fileScanTasks = (expression == null) ?
        extendedTable.newScan().planFiles() :
        extendedTable.newScan().filter(expression).planFiles();
    for (FileScanTask ignored : fileScanTasks) {
      planFiles.add(ignored.file().path().toString());
      System.out.println("Plan file=" + ignored.file().path());
    }
    return planFiles;
  }
}
