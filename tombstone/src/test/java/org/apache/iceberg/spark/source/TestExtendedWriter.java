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
import org.apache.spark.sql.Encoders;
import org.junit.Assert;
import org.junit.Test;

public class TestExtendedWriter extends WithSpark {

  @Test
  public void testWriterPropagatesTombstonesReferencesAcrossSnapshots() {
    ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
    Types.NestedField batchField = table.schema().findField("batch");

    // Tombstone batches with values `A`
    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "A"), Collections.emptyMap(), 1579792561L)
        .commit();

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

    List<SimpleRecord> actual = sparkWithTombstonesExtension.read()
        .option(TombstoneExtension.TOMBSTONE_COLUMN, "batch")
        .format("iceberg.adobe")
        .load(getTableLocation())
        .select("id", "timestamp", "batch", "data")
        .orderBy("id")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();

    Assert.assertEquals("Result rows should match 0 (zero)", Collections.emptyList(), actual);
  }
}
