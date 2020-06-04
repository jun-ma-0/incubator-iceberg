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

package com.adobe.platform.iceberg.extensions;

import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.actions.ActionException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.source.VacuumException;
import org.apache.parquet.Strings;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SparkVacuumTest extends WithSpark {

  private static final String TOMBSTONE_COLUMN = "_acp_system_metadata.acp_sourceBatchId";
  private static final StructType SPARK_SCHEMA = new StructType(
      new StructField[]{
          new StructField("_id", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("timestamp", DataTypes.TimestampType, false, Metadata.empty()),
          new StructField("_acp_batchId", DataTypes.StringType, false, Metadata.empty()),
          new StructField("_acp_system_metadata", DataTypes.createStructType(
              Arrays.asList(
                  new StructField("acp_sourceBatchId", DataTypes.StringType, false,
                      Metadata.empty()),
                  new StructField("acp_prop_map",
                      DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true,
                      Metadata.empty()))
          ), false, Metadata.empty())
      });
  private static final Schema ICEBERG_SCHEMA = SparkSchemaUtil.convert(SPARK_SCHEMA);
  private static final PartitionSpec ICEBERG_SPEC = PartitionSpec.builderFor(ICEBERG_SCHEMA)
      .day("timestamp", "_ACP_DATE")
      .identity("_acp_batchId")
      .build();

  private static final Map<Object, Object> EMPTY_MAP = Collections.emptyMap();

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Override
  public void implicitTable(ExtendedTables tables, String tableLocation) {
    // Override implicit Iceberg table schema test provisioning with explicit schema and spec.
    tables.create(ICEBERG_SCHEMA, ICEBERG_SPEC, tableLocation, Collections.singletonMap(
        "write.metadata.metrics.default",
        "truncate(36)"));
  }

  @Test
  public void testEmptyTableVacuumByLimit() {
    exceptionRule.expect(VacuumException.class);
    exceptionRule.expectMessage("Abort vacuum, current snapshot is null");

    new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()), TOMBSTONE_COLUMN)
        .load(100)
        .reduceWithAntiJoin();
  }

  @Test
  public void testEmptyTableVacuumByValue() {
    exceptionRule.expect(VacuumException.class);
    exceptionRule.expectMessage("Abort vacuum, current snapshot is null");

    new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()), TOMBSTONE_COLUMN)
        .load(Lists.newArrayList("?"))
        .reduceWithAntiJoin();
  }

  @Test
  public void testEmptyTableVacuumAll() {
    exceptionRule.expect(VacuumException.class);
    exceptionRule.expectMessage("Abort vacuum, current snapshot is null");

    new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()), TOMBSTONE_COLUMN)
        .load()
        .reduceWithAntiJoin();
  }

  @Test
  public void testEmptyTombstonesVacuumAll() {
    testEmptyTombstones(ActionException.class,
        "Snapshot does not contain tombstones metadata",
        () -> new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN)
            .load());
  }

  @Test
  public void testEmptyTombstonesVacuumByLimit() {
    testEmptyTombstones(VacuumException.class,
        "Abort vacuum, provided null or empty tombstones",
        () -> new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN)
            .load(1));
  }

  @Test
  public void testEmptyTombstonesVacuumByValue() {
    testEmptyTombstones(VacuumException.class,
        "Abort vacuum, provided tombstones missing from snapshot",
        () -> new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN)
            .load(Lists.newArrayList("?")));
  }

  private void testEmptyTombstones(Class<? extends Exception> expectedException,
      String errorMessage, Supplier<SparkVacuum> supplier) {
    Timestamp ts = Timestamp.valueOf("2019-10-10 10:10:10.10");
    List<Row> rows = Lists.newArrayList(
        RowFactory.create(10, ts, "A", RowFactory.create("GJO", EMPTY_MAP)),
        RowFactory.create(20, ts, "B", RowFactory.create("6JF", EMPTY_MAP)));

    append(rows, SPARK_SCHEMA);

    exceptionRule.expect(expectedException);
    exceptionRule.expectMessage(errorMessage);
    supplier.get().reduceWithAntiJoin();
  }

  @Test
  public void testAllTombstonesVacuumAll() {
    testAllTombstones(() ->
        new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN).load());
  }

  @Test
  public void testAllTombstonesVacuumByLimit() {
    testAllTombstones(() ->
        new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN).load(3));
  }

  @Test
  public void testAllTombstonesVacuumByValue() {
    testAllTombstones(() ->
        new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN)
            .load(Lists.newArrayList("GJO", "WN7", "6JF")));
  }

  private void testAllTombstones(Supplier<SparkVacuum> supplier) {
    Timestamp ts = Timestamp.valueOf("2019-10-10 10:10:10.10");
    List<Row> rows = Lists.newArrayList(
        RowFactory.create(10, ts, "A", RowFactory.create("GJO", EMPTY_MAP)),
        RowFactory.create(15, ts, "B", RowFactory.create("WN7", EMPTY_MAP)),
        RowFactory.create(21, ts, "C", RowFactory.create("6JF", EMPTY_MAP)));

    append(rows, SPARK_SCHEMA);
    appendWithTombstone(Collections.emptyList(), SPARK_SCHEMA, TOMBSTONE_COLUMN,
        Lists.newArrayList("GJO", "WN7", "6JF"));

    supplier.get().reduceWithAntiJoin();
    Assert.assertEquals(0L, rows().count());
  }

  @Test
  public void testVacuumTwiceWithTombstoneLimit() {
    testVacuumTwice("Abort vacuum, provided null or empty tombstones",
        () -> new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN)
            .load(3));
  }

  @Ignore
  public void testVacuumTwiceByValue() {
    testVacuumTwice("Abort vacuum, provided tombstones missing from snapshot",
        () -> new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN)
            .load(Lists.newArrayList("GJO", "WN7", "6JF"))
    );
  }

  @Test
  public void testVacuumTwiceWithAllAvailableTombstones() {
    testVacuumTwice("No data files detected for tombstone vacuuming",
        () -> new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN)
            .load());
  }

  public void testVacuumTwice(String errorMessage, Supplier<SparkVacuum> supplier) {
    Timestamp ts = Timestamp.valueOf("2019-10-10 10:10:10.10");
    List<Row> rows = Lists.newArrayList(
        RowFactory.create(10, ts, "A", RowFactory.create("GJO", EMPTY_MAP)),
        RowFactory.create(15, ts, "B", RowFactory.create("WN7", EMPTY_MAP)),
        RowFactory.create(21, ts, "C", RowFactory.create("6JF", EMPTY_MAP)));

    append(rows, SPARK_SCHEMA);
    appendWithTombstone(rows, SPARK_SCHEMA, TOMBSTONE_COLUMN,
        Lists.newArrayList("GJO", "WN7", "6JF"));

    supplier.get().reduceWithAntiJoin();
    exceptionRule.expect(VacuumException.class);
    exceptionRule.expectMessage(errorMessage);
    supplier.get().reduceWithAntiJoin();
  }

  @Test
  public void testRowsVacuumWithAllAvailableTombstones() {
    testRowsVacuum(
        () -> new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN).load());
  }

  @Test
  public void testRowsVacuumWithTombstoneLimit() {
    testRowsVacuum(
        () -> new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN).load(2));
  }

  @Test
  public void testRowsVacuumByValue() {
    testRowsVacuum(
        () -> new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN).load(Lists.newArrayList("GJO", "6JF")));
  }

  @Test
  public void testRowsVacuumWithTombstoneValuesForNull() {
    exceptionRule.expect(VacuumException.class);
    exceptionRule.expectMessage("Abort vacuum, provided null or empty tombstones");

    testRowsVacuum(
        () -> new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN).load(null));
  }

  @Test
  public void testRowsVacuumWithTombstoneValuesForEmpty() {
    exceptionRule.expect(VacuumException.class);
    exceptionRule.expectMessage("Abort vacuum, provided null or empty tombstones");

    testRowsVacuum(
        () -> new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN).load(Collections.emptyList()));
  }

  private void testRowsVacuum(Supplier<SparkVacuum> supplier) {
    Timestamp ts = Timestamp.valueOf("2019-10-10 10:10:10.10");
    List<Row> rows = Lists.newArrayList(
        RowFactory.create(10, ts, "A", RowFactory.create("GJO", EMPTY_MAP)),
        RowFactory.create(11, ts, "A", RowFactory.create("GJO", EMPTY_MAP)),
        RowFactory.create(21, ts, "A", RowFactory.create("6JF", EMPTY_MAP)),
        RowFactory.create(22, ts, "B", RowFactory.create("6JF", EMPTY_MAP)),
        RowFactory.create(23, ts, "B", RowFactory.create("6JF", EMPTY_MAP)));

    append(rows, SPARK_SCHEMA);

    List<Row> replay = Lists.newArrayList(
        RowFactory.create(15, ts, "A", RowFactory.create("WN7", EMPTY_MAP)),
        RowFactory.create(16, ts, "A", RowFactory.create("WN7", EMPTY_MAP)),
        RowFactory.create(30, ts, "B", RowFactory.create("2BG", EMPTY_MAP)),
        RowFactory.create(31, ts, "B", RowFactory.create("2BG", EMPTY_MAP)));

    appendWithTombstone(replay, SPARK_SCHEMA, TOMBSTONE_COLUMN, Lists.newArrayList("GJO", "6JF"));

    supplier.get().reduceWithAntiJoin();
    Assert.assertEquals(4L, rows().count());
  }

  @Test
  public void testVacuumAsDeleteFilesWithConcurrentDeleteByValue() {
    testVacuumAsDeleteFilesWithConcurrentDelete(
        () -> new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN).load(Lists.newArrayList("GJO", "6JF")));
  }

  @Test
  public void testVacuumAsDeleteFilesWithConcurrentDeleteByLimit() {
    testVacuumAsDeleteFilesWithConcurrentDelete(
        () -> new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN).load(2));
  }

  @Test
  public void testVacuumAsDeleteFilesWithConcurrentDeleteByAll() {
    testVacuumAsDeleteFilesWithConcurrentDelete(
        () -> new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN).load());
  }

  /**
   * The tests provides the condition to evaluate a spark vacuum backed by {@link
   * com.adobe.platform.iceberg.extensions.BaseVacuumDelete} It does so by assigning only rows that
   * have tombstone field values that we set tombstones for.
   *
   * @param supplier instance of {@link SparkVacuum} which may ask for a full vacuum or a bounded
   * vacuum (by values or limit, scalar)
   */
  private void testVacuumAsDeleteFilesWithConcurrentDelete(Supplier<SparkVacuum> supplier) {
    Timestamp ts = Timestamp.valueOf("2019-10-10 10:10:10.10");
    List<Row> rows = Lists.newArrayList(
        RowFactory.create(10, ts, "A", RowFactory.create("GJO", EMPTY_MAP)),
        RowFactory.create(11, ts, "A", RowFactory.create("GJO", EMPTY_MAP)),
        RowFactory.create(21, ts, "A", RowFactory.create("6JF", EMPTY_MAP)),
        RowFactory.create(22, ts, "B", RowFactory.create("6JF", EMPTY_MAP)),
        RowFactory.create(23, ts, "B", RowFactory.create("6JF", EMPTY_MAP)));

    append(rows, SPARK_SCHEMA);

    DeleteFiles deleteFiles = prepareDeleteAllFiles();
    appendWithTombstone(Collections.emptyList(), SPARK_SCHEMA, TOMBSTONE_COLUMN,
        Lists.newArrayList("GJO", "6JF"));

    // This will resolve the data files for the provided tombstones
    SparkVacuum sparkVacuum = supplier.get();
    deleteFiles.commit();

    exceptionRule.expect(org.apache.spark.SparkException.class);
    exceptionRule.expectMessage("Writing job aborted");
    exceptionRule.expectCause(Is.isA(org.apache.iceberg.exceptions.ValidationException.class));

    sparkVacuum.reduceWithAntiJoin();
  }

  @Test
  public void testVacuumAsRewriteFilesWithConcurrentDeleteByValue() {
    testVacuumAsRewriteFilesWithConcurrentDelete(
        () -> new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN).load(Lists.newArrayList("GJO", "6JF")));
  }

  @Test
  public void testVacuumAsRewriteFilesWithConcurrentDeleteByLimit() {
    testVacuumAsRewriteFilesWithConcurrentDelete(
        () -> new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN).load(2));
  }

  @Test
  public void testVacuumAsRewriteFilesWithConcurrentDeleteByAll() {
    testVacuumAsRewriteFilesWithConcurrentDelete(
        () -> new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN).load());
  }

  /**
   * The tests provides the condition to evaluate a spark vacuum backed by {@link
   * com.adobe.platform.iceberg.extensions.BaseVacuumRewrite} It does so by assigning rows with
   * tombstone field `NOOP` (not a tombstone) on same partition as rows for which the test will
   * assign tombstone for their respective tombstone field values.
   *
   * @param supplier instance of {@link SparkVacuum} which may ask for a full vacuum or a bounded
   * vacuum (by values or limit, scalar)
   */
  private void testVacuumAsRewriteFilesWithConcurrentDelete(Supplier<SparkVacuum> supplier) {
    Timestamp ts = Timestamp.valueOf("2019-10-10 10:10:10.10");
    List<Row> rows = Lists.newArrayList(
        RowFactory.create(10, ts, "A", RowFactory.create("GJO", EMPTY_MAP)),
        RowFactory.create(11, ts, "A", RowFactory.create("GJO", EMPTY_MAP)),
        RowFactory.create(21, ts, "A", RowFactory.create("NOOP", EMPTY_MAP)),
        RowFactory.create(21, ts, "A", RowFactory.create("NOOP", EMPTY_MAP)),
        RowFactory.create(22, ts, "B", RowFactory.create("6JF", EMPTY_MAP)),
        RowFactory.create(22, ts, "B", RowFactory.create("6JF", EMPTY_MAP)),
        RowFactory.create(22, ts, "B", RowFactory.create("6JF", EMPTY_MAP)),
        RowFactory.create(22, ts, "B", RowFactory.create("NOOP", EMPTY_MAP)),
        RowFactory.create(23, ts, "B", RowFactory.create("NOOP", EMPTY_MAP)));

    append(rows, SPARK_SCHEMA);

    appendWithTombstone(Collections.emptyList(), SPARK_SCHEMA, TOMBSTONE_COLUMN,
        Lists.newArrayList("GJO", "6JF"));

    DeleteFiles deleteFiles = prepareDeleteAllFiles();

    // This will resolve the data files for the provided tombstones
    SparkVacuum sparkVacuum = supplier.get();
    deleteFiles.commit();

    exceptionRule.expect(org.apache.spark.SparkException.class);
    exceptionRule.expectMessage("Writing job aborted");
    exceptionRule.expectCause(Is.isA(org.apache.iceberg.exceptions.ValidationException.class));

    // This will reduce the tombstone rows and commit the files operation to iceberg.
    sparkVacuum.reduceWithAntiJoin();
    Assert.assertEquals(1L, rows().count());
  }


  @Test
  public void testVacuumAsRewriteFilesWithConcurrentAppendByValue() {
    testVacuumAsRewriteFilesWithConcurrentAppend(
        () -> new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN).load(Lists.newArrayList("GJO", "6JF")));
  }

  @Test
  public void testVacuumAsRewriteFilesWithConcurrentAppendByLimit() {
    testVacuumAsRewriteFilesWithConcurrentAppend(
        () -> new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN).load(2));
  }

  @Test
  public void testVacuumAsRewriteFilesWithConcurrentAppendByAll() {
    testVacuumAsRewriteFilesWithConcurrentAppend(
        () -> new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
            TOMBSTONE_COLUMN).load());
  }


  /**
   * The tests provides the condition to evaluate a spark vacuum backed by {@link
   * com.adobe.platform.iceberg.extensions.BaseVacuumRewrite} It does so by assigning rows with
   * tombstone field `NOOP` (not a tombstone) on same partition as rows for which the test will
   * assign tombstone for their respective tombstone field values.
   *
   * @param supplier instance of {@link SparkVacuum} which may ask for a full vacuum or a bounded
   * vacuum (by values or limit, scalar)
   */
  private void testVacuumAsRewriteFilesWithConcurrentAppend(Supplier<SparkVacuum> supplier) {
    Timestamp ts = Timestamp.valueOf("2019-10-10 10:10:10.10");
    List<Row> rows = Lists.newArrayList(
        RowFactory.create(10, ts, "A", RowFactory.create("GJO", EMPTY_MAP)),
        RowFactory.create(11, ts, "A", RowFactory.create("GJO", EMPTY_MAP)),
        RowFactory.create(12, ts, "A", RowFactory.create("NOOP", EMPTY_MAP)),
        RowFactory.create(22, ts, "B", RowFactory.create("6JF", EMPTY_MAP)),
        RowFactory.create(23, ts, "B", RowFactory.create("6JF", EMPTY_MAP)),
        RowFactory.create(24, ts, "B", RowFactory.create("6JF", EMPTY_MAP)),
        RowFactory.create(25, ts, "B", RowFactory.create("NOOP", EMPTY_MAP)),
        RowFactory.create(32, ts, "C", RowFactory.create("6JF", EMPTY_MAP)),
        RowFactory.create(33, ts, "C", RowFactory.create("6JF", EMPTY_MAP)),
        RowFactory.create(34, ts, "C", RowFactory.create("6JF", EMPTY_MAP)),
        RowFactory.create(35, ts, "C", RowFactory.create("NOOP", EMPTY_MAP)),
        RowFactory.create(42, ts, "D", RowFactory.create("6JF", EMPTY_MAP)),
        RowFactory.create(43, ts, "D", RowFactory.create("6JF", EMPTY_MAP)),
        RowFactory.create(44, ts, "D", RowFactory.create("6JF", EMPTY_MAP)),
        RowFactory.create(45, ts, "D", RowFactory.create("NOOP", EMPTY_MAP)),
        RowFactory.create(22, ts, "F", RowFactory.create("6JF", EMPTY_MAP)),
        RowFactory.create(23, ts, "F", RowFactory.create("6JF", EMPTY_MAP)),
        RowFactory.create(24, ts, "F", RowFactory.create("6JF", EMPTY_MAP)),
        RowFactory.create(25, ts, "F", RowFactory.create("NOOP", EMPTY_MAP)));

    append(rows, SPARK_SCHEMA);

    appendWithTombstone(Collections.emptyList(), SPARK_SCHEMA, TOMBSTONE_COLUMN,
        Lists.newArrayList("GJO", "6JF"));

    // This will resolve the data files for the provided tombstones
    SparkVacuum sparkVacuum = supplier.get();

    // Tombstone `NOOP` rows
    appendWithTombstone(Collections.emptyList(), SPARK_SCHEMA, TOMBSTONE_COLUMN,
        Lists.newArrayList("NOOP"));

    // This will reduce the tombstone rows and commit the files operation to iceberg.
    sparkVacuum.reduceWithAntiJoin();
    // These are the five `NOOP` rows from the original dataset.
    Assert.assertEquals(5L, rows().count());
    // These rows should be in distinct files since they belong to distinct partition batches
    Assert.assertEquals(5L,
        Iterators.size(tables.loadWithTombstoneExtension(getTableLocation())
            .newScan().planFiles().iterator()));

    // To make sure we're not loosing tombstones that get appended while vacuum was running
    // we will run a new vacuum for only those tombstones and expect the outcome is a wiped dataset
    new SparkVacuum(spark, tables.loadWithTombstoneExtension(getTableLocation()),
        TOMBSTONE_COLUMN).load(Lists.newArrayList("NOOP"))
        .reduceWithAntiJoin();

    Assert.assertEquals(0L, rows().count());
    // There should be zero files
    Assert.assertEquals(0L,
        Iterators.size(tables.loadWithTombstoneExtension(getTableLocation())
            .newScan().planFiles().iterator()));
  }

  @Test
  public void testConcurrentVacuumOpsByRewritingSameFile() {
    Timestamp ts = Timestamp.valueOf("2019-10-10 10:10:10.10");
    List<Row> rows = Lists.newArrayList(
        // Data file for batch A
        RowFactory.create(10, ts, "A", RowFactory.create("A", EMPTY_MAP)),
        RowFactory.create(11, ts, "A", RowFactory.create("AA", EMPTY_MAP)),
        RowFactory.create(12, ts, "A", RowFactory.create("AA", EMPTY_MAP)),
        RowFactory.create(13, ts, "A", RowFactory.create("AA", EMPTY_MAP)),
        RowFactory.create(14, ts, "A", RowFactory.create("AA", EMPTY_MAP)),
        RowFactory.create(15, ts, "A", RowFactory.create("AAA", EMPTY_MAP)),
        RowFactory.create(16, ts, "A", RowFactory.create("AAAA", EMPTY_MAP)));

    appendWithTombstone(rows, SPARK_SCHEMA, TOMBSTONE_COLUMN,
        Lists.newArrayList("A", "AA", "AAA", "AAAA"));

    // Matches data file for batch A and generates a vacuum op as a rewrite since this will
    // generate a separate file with the rest of the rows, other than "A" or "AA"
    ExtendedTable extendedTable = tables.loadWithTombstoneExtension(getTableLocation());
    SparkVacuum firstVacuum = new SparkVacuum(spark, extendedTable, TOMBSTONE_COLUMN)
        .load(Lists.newArrayList("A", "AA"));

    SparkVacuum secondVacuum = new SparkVacuum(spark, extendedTable, TOMBSTONE_COLUMN)
        .load(Lists.newArrayList("AAA", "AAAA"));

    firstVacuum.reduceWithAntiJoin();

    // Expect second vacuum to fail since the previously committed vacuum has deleted the original
    // data file, same file the second vacuum will try to delete as part of a rewrite files commit
    try {
      secondVacuum.reduceWithAntiJoin();
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof ValidationException);
      Assert.assertTrue(
          e.getCause().getMessage().startsWith("Missing required files to delete: file:"));
    }

    // Assert first vacuum went through fine
    Assert.assertEquals(Lists.newArrayList("AAA", "AAAA"),
        rows().select("_acp_system_metadata.acp_sourceBatchId")
            .map((MapFunction<Row, String>) r -> r.getString(0), Encoders.STRING())
            .collectAsList());
  }

  @Test
  public void testConcurrentVacuumOpsByRewritingSameFileFalsePositiveMatch() {
    Timestamp ts = Timestamp.valueOf("2019-10-10 10:10:10.10");
    List<Row> rows = Lists.newArrayList(
        // Data file for batch A
        RowFactory.create(10, ts, "A", RowFactory.create("A", EMPTY_MAP)),
        RowFactory.create(11, ts, "A", RowFactory.create("AAA", EMPTY_MAP)),
        RowFactory.create(12, ts, "A", RowFactory.create("AAAA", EMPTY_MAP)),
        // Data file for batch B
        RowFactory.create(14, ts, "B", RowFactory.create("AA", EMPTY_MAP)));

    appendWithTombstone(rows, SPARK_SCHEMA, TOMBSTONE_COLUMN,
        Lists.newArrayList("A", "AA", "AAA", "AAAA"));

    ExtendedTable extendedTable = tables.loadWithTombstoneExtension(getTableLocation());

    // Matches data files for batch A (false positive) and B and does a rewrite
    SparkVacuum firstVacuum = new SparkVacuum(spark, extendedTable, TOMBSTONE_COLUMN)
        .load(Lists.newArrayList("AA"));

    // Matches data files for batch A and B (false positive) - commit should be a rewrite files
    SparkVacuum secondVacuum = new SparkVacuum(spark, extendedTable, TOMBSTONE_COLUMN)
        .load(Lists.newArrayList("A", "AAA"));

    firstVacuum.reduceWithAntiJoin();

    // Assert first vacuum went through fine
    Assert.assertEquals(Lists.newArrayList("A", "AAA", "AAAA"),
        rows().select("_acp_system_metadata.acp_sourceBatchId")
            .map((MapFunction<Row, String>) r -> r.getString(0), Encoders.STRING())
            .collectAsList());

    // Expect second vacuum to fail since the previously committed vacuum has deleted the original
    // data file, same file the second vacuum will try to delete as part of a rewrite files commit
    exceptionRule.expect(org.apache.spark.SparkException.class);
    exceptionRule.expectMessage("Writing job aborted");
    exceptionRule.expectCause(Is.isA(org.apache.iceberg.exceptions.ValidationException.class));

    secondVacuum.reduceWithAntiJoin();
  }

  @Test
  public void testConcurrentVacuumOpsForTombstoneWithoutMatchingRows() {
    Timestamp ts = Timestamp.valueOf("2019-10-10 10:10:10.10");
    List<Row> rows = Lists.newArrayList(
        RowFactory.create(10, ts, "A", RowFactory.create("A", EMPTY_MAP)),
        // We're missing "AA" - however we consider this to be a tombstone for a batch of 0 rows
        RowFactory.create(14, ts, "A", RowFactory.create("AAA", EMPTY_MAP)));

    appendWithTombstone(rows, SPARK_SCHEMA, TOMBSTONE_COLUMN,
        Lists.newArrayList("A", "AA", "AAA"));

    ExtendedTable extendedTable = tables.loadWithTombstoneExtension(getTableLocation());

    // Matches data files for batch A (false positive) and does a rewrite
    SparkVacuum firstVacuum = new SparkVacuum(spark, extendedTable, TOMBSTONE_COLUMN)
        .load(Lists.newArrayList("AA"));

    // Matches data file B and does a delete
    SparkVacuum secondVacuum = new SparkVacuum(spark, extendedTable, TOMBSTONE_COLUMN)
        .load(Lists.newArrayList("A", "AAA"));

    firstVacuum.reduceWithAntiJoin();
    Assert.assertEquals(2L, rows().count());

    // The test brings up an edge-case and the expectation is that the second vacuuming fails to
    // commit the data files since otherwise it would lead to rows leaking
    exceptionRule.expect(org.apache.spark.SparkException.class);
    exceptionRule.expectMessage("Writing job aborted");
    exceptionRule.expectCause(Is.isA(org.apache.iceberg.exceptions.ValidationException.class));

    secondVacuum.reduceWithAntiJoin();
  }

  private DeleteFiles prepareDeleteAllFiles() {
    DeleteFiles deleteFiles = tables.loadWithTombstoneExtension(getTableLocation()).newDelete();
    CloseableIterable<FileScanTask> fileScanTasks = tables
        .loadWithTombstoneExtension(getTableLocation()).newScan().planFiles();
    for (FileScanTask fileScanTask : fileScanTasks) {
      deleteFiles.deleteFile(fileScanTask.file().path().toString());
    }
    return deleteFiles;
  }

  private Dataset<Row> rows() {
    return spark.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_NOOP, true)
        .load(getTableLocation());
  }

  private void append(List<Row> rows, StructType schema) {
    spark.createDataFrame(rows, schema)
        .select("*")
        .coalesce(1)
        .write()
        .format("iceberg.adobe")
        .mode("append")
        .save(getTableLocation());
  }

  private void appendWithTombstone(List<Row> rows, StructType schema, String columnName,
      List<String> tombstones) {
    spark.createDataFrame(rows, schema)
        .select("*")
        .coalesce(1)
        .write()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_COLUMN, columnName)
        .option(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST, Strings.join(tombstones, ","))
        .option(TombstoneExtension.TOMBSTONE_COLUMN_EVICT_TS, "1579792561")
        .mode(SaveMode.Append)
        .save(getTableLocation());
  }
}
