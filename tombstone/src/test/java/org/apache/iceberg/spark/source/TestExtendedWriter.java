package org.apache.iceberg.spark.source;

import com.adobe.platform.iceberg.extensions.ExtendedTable;
import com.adobe.platform.iceberg.extensions.SimpleRecord;
import com.adobe.platform.iceberg.extensions.WithSpark;
import com.google.common.collect.Lists;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Encoders;
import org.junit.Assert;
import org.junit.Test;

public class TestExtendedWriter extends WithSpark {

  @Test
  public void testWriterPropagatesTombstonesReferencesAcrossSnapshots() {
    ExtendedTable table = TABLES.loadWithTombstoneExtension(getTableLocation());
    Types.NestedField batchField = table.schema().findField("batch");

    // Tombstone batches with values `A`
    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "A"), Collections.emptyMap()).commit();

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

    List<SimpleRecord> actual = spark.read()
        .option("iceberg.extension.tombstone.col", "batch")
        .format("iceberg.adobe")
        .load(getTableLocation())
        .select("id", "timestamp", "batch", "data")
        .orderBy("id")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();

    Assert.assertEquals("Result rows should match 0 (zero)", Collections.emptyList(), actual);
  }
}
