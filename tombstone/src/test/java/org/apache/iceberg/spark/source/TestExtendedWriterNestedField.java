package org.apache.iceberg.spark.source;

import com.adobe.platform.iceberg.extensions.ExtendedTables;
import com.adobe.platform.iceberg.extensions.WithSpark;
import com.google.common.collect.Lists;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

public class TestExtendedWriterNestedField extends WithSpark {

  private final static List<StructField> SYSTEM_STRUCT = Arrays.asList(
      new StructField("acp_sourceBatchId", DataTypes.StringType, false, Metadata.empty()),
      new StructField("acp_prop_map", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true,
          Metadata.empty()));

  private final static StructType SCHEMA = new StructType(
      new StructField[] {
          new StructField("_id", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("timestamp", DataTypes.TimestampType, false, Metadata.empty()),
          new StructField("batch", DataTypes.StringType, false, Metadata.empty()),
          new StructField("_acp_system_metadata", DataTypes.createStructType(SYSTEM_STRUCT), false, Metadata.empty())
      });

  private final static Schema ICEBERG_SCHEMA = SparkSchemaUtil.convert(SCHEMA);

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
    Timestamp ts = Timestamp.valueOf("2019-10-10 10:10:10.10");

    List<Row> rows = Lists.newArrayList(
        RowFactory.create(101, ts, "A", RowFactory.create("X", Collections.emptyMap())),
        RowFactory.create(102, ts, "A", RowFactory.create("X", Collections.emptyMap())),
        RowFactory.create(103, ts, "A", RowFactory.create("X", Collections.emptyMap())),
        RowFactory.create(104, ts, "A", RowFactory.create("X", Collections.emptyMap())),
        RowFactory.create(105, ts, "A", RowFactory.create("X", Collections.emptyMap())),
        RowFactory.create(201, ts, "A", RowFactory.create("Y", Collections.emptyMap())),
        RowFactory.create(202, ts, "A", RowFactory.create("Y", Collections.emptyMap())),
        RowFactory.create(203, ts, "A", RowFactory.create("Y", Collections.emptyMap())),
        RowFactory.create(204, ts, "A", RowFactory.create("Y", Collections.emptyMap())),
        RowFactory.create(205, ts, "A", RowFactory.create("Y", Collections.emptyMap())));

    spark.createDataFrame(rows, SCHEMA)
        .select("*")
        .write()
        .format("iceberg.adobe")
        .mode("append")
        .save(getTableLocation());

    List<Row> thirdBatchRows = Lists.newArrayList(
        RowFactory.create(301, ts, "B", RowFactory.create("Z", Collections.emptyMap())),
        RowFactory.create(302, ts, "B", RowFactory.create("Z", Collections.emptyMap())));

    spark.createDataFrame(thirdBatchRows, SCHEMA)
        .select("*")
        .write()
        .format("iceberg.adobe")
        .option("iceberg.extension.tombstone.col", "_acp_system_metadata.acp_sourceBatchId")
        .option("iceberg.extension.tombstone.values", "X,Y")
        .mode("append")
        .save(getTableLocation());

    long count = spark.read()
        .format("iceberg.adobe")
        .option("iceberg.extension.tombstone.col", "_acp_system_metadata.acp_sourceBatchId")
        .load(getTableLocation())
        .select("_id")
        .count();

    Assert.assertEquals("Result rows should only match `_acp_system_metadata.acp_sourceBatchId` of Z", 2, count);
  }
}
