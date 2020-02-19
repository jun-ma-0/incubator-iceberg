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

import com.adobe.platform.iceberg.extensions.tombstone.TombstoneValidationException;
import com.google.common.collect.Lists;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestExtendedIcebergSource extends WithSpark implements WithExecutorService {

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testReaderVerifiesTombstoneColumnOptionIsValid() {
    exceptionRule.expect(TombstoneValidationException.class);
    exceptionRule.expectMessage("Expect tombstone column option=iceberg.extension.tombstone.col");

    sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .load(getTableLocation())
        .select("*")
        .count();
  }

  @Test
  public void testReaderTombstoneNoopOption() {
    Timestamp now = Timestamp.from(Instant.now());

    List<SimpleRecord> rows = Lists.newArrayList(
        new SimpleRecord(1, now, "A", "a"),
        new SimpleRecord(2, now, "A", "b"),
        new SimpleRecord(3, now, "A", "c"));

    sparkWithTombstonesExtension.createDataFrame(rows, SimpleRecord.class)
        .select("id", "timestamp", "batch", "data")
        .write()
        .format("iceberg.adobe")
        .mode("append")
        .save(getTableLocation());

    ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
    Types.NestedField batchField = table.schema().findField("batch");
    table.newAppendWithTombstonesAdd(batchField, Lists.newArrayList(() -> "A"), Collections.emptyMap(), 1L)
        .commit();

    long tombstonesOn = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option("iceberg.extension.tombstone.col", "batch")
        .load(getTableLocation())
        .select("*")
        .count();
    Assert.assertEquals("Expect tombstone rows are filtered out", 0, tombstonesOn);

    long tombstonesOff = sparkWithTombstonesExtension.read()
        .format("iceberg.adobe")
        .option("iceberg.extension.tombstone.noop", "true")
        .load(getTableLocation())
        .select("*")
        .count();
    Assert.assertEquals("Expect no tombstone filtering is applied when using `iceberg.extension.tombstone.noop` option",
        3, tombstonesOff);
  }
}
