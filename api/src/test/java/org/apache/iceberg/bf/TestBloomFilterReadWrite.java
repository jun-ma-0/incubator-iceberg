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

package org.apache.iceberg.bf;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.Files.localOutput;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestBloomFilterReadWrite {
  private static final Types.BloomFilterConfig bfConfig = new Types.BloomFilterConfig(true, 0.01, 10);
  private static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "f1", Types.IntegerType.get(), bfConfig),
      Types.NestedField.optional(2, "f2", Types.LongType.get(), bfConfig),
      Types.NestedField.optional(3, "f3", Types.DoubleType.get(), bfConfig),
      Types.NestedField.optional(4, "f4", Types.FloatType.get(), bfConfig),
      Types.NestedField.optional(5, "f5", Types.StringType.get(), bfConfig)
  );
  private static final Schema SCHEMA_WITH_MAP = new Schema(
      Types.NestedField.required(1, "id", Types.LongType.get()),
      Types.NestedField.optional(2, "ts", Types.TimestampType.withZone()),
      Types.NestedField.optional(3, "data", Types.StringType.get()),
      Types.NestedField.optional(4, "idMap", Types.MapType.ofOptional(5, 6, Types.StringType.get(),
          Types.ListType.ofOptional(7, Types.StructType.of(
              required(8, "id", Types.LongType.get(), new Types.BloomFilterConfig(true, 0.01, 10)),
              optional(9, "description", Types.StringType.get())))))
  );
  private static String tableLocation;

  static {
    try {
      TemporaryFolder temp = new TemporaryFolder();
      temp.create();
      File file = temp.newFolder("TestInclusiveMetricsEvaluator");
      tableLocation = file.getPath();
    } catch (IOException e) {
      throw new RuntimeIOException("Unable to create table location", e);
    }
  }

  private static String dataLocation = String.format("%s/data", tableLocation);
  private static String fileLocation = String.format("%s/file.avro", dataLocation);
  private static String bfBaseLocation = String.format("%s/metadata/bloomFilter/file.avro", tableLocation);

  private static final DataFile FILE = new TestHelpers.TestDataFile(fileLocation,
      TestHelpers.Row.of(), 10,
      // any value counts, including nulls
      ImmutableMap.of(),
      // null value counts
      ImmutableMap.of(
          1, 0L,
          2, 0L,
          3, 0L,
          4, 0L,
          5, 0L),
      // lower bounds
      ImmutableMap.of(),
      // upper bounds
      ImmutableMap.of()
  );

  private static final DataFile FILE_WITH_MAP = new TestHelpers.TestDataFile(fileLocation,
      TestHelpers.Row.of(), 10,
      // any value counts, including nulls
      ImmutableMap.of(),
      // null value counts
      ImmutableMap.of(8, 0L),
      // lower bounds
      ImmutableMap.of(),
      // upper bounds
      ImmutableMap.of()
  );

  @Test
  public void testReadWriteBloomFilter() throws IOException {
    Map<Integer, OutputFile> bloomFilterMap = ImmutableMap.of(
        1, localOutput(String.format("%s-1", bfBaseLocation)),
        2, localOutput(String.format("%s-2", bfBaseLocation)),
        3, localOutput(String.format("%s-3", bfBaseLocation)),
        4, localOutput(String.format("%s-4", bfBaseLocation)),
        5, localOutput(String.format("%s-5", bfBaseLocation))
    );
    BloomFilterWriterStore writerStore = new BloomFilterWriterStore(SCHEMA, bloomFilterMap);

    for (int i = 1; i <= 5; ++i) {
      String bfPath = String.format("%s-%d", bfBaseLocation, i);
      BloomFilterWriter writer = writerStore.getBloomFilterWriter(i);
      for (int j = 0; j < 10; ++j) {
        switch (i) {
          case 1:
            writer.write(j);
            break;
          case 2:
            writer.write(Long.valueOf(j));
            break;
          case 3:
            writer.write(Double.valueOf(j));
            break;
          case 4:
            writer.write(Float.valueOf(j));
            break;
          case 5:
            writer.write(String.valueOf(j));
            break;
        }
      }
    }

    writerStore.close();
    for (int i = 1; i <= 5; ++i) {
      String bfPath = String.format("%s-%d", bfBaseLocation, i);
      Assert.assertTrue(String.format("Bloom Filter for field %d should exist", i),
          new File(bfPath).exists());
    }

    Map<Integer, BloomFilter> bfMap = BloomFilterReader.loadBloomFilter(FILE, bfBaseLocation);
    Assert.assertTrue("Should read 5 Bloom Filters for the given data file", bfMap.size() == 5);

    for (int i = 1; i <= 5; ++i) {
      BloomFilter bf = bfMap.get(Integer.valueOf(i));
      for (int j = 0; j < 10; ++j) {
        boolean exist = false;
        switch (i) {
          case 1:
            exist = bf.findHash(bf.hash(j));
            break;
          case 2:
            exist = bf.findHash(bf.hash(Long.valueOf(j)));
            break;
          case 3:
            exist = bf.findHash(bf.hash(Double.valueOf(j)));
            break;
          case 4:
            exist = bf.findHash(bf.hash(Float.valueOf(j)));
            break;
          case 5:
            exist = bf.findHash(bf.hash(String.valueOf(j)));
            break;
        }
        Assert.assertTrue(String.format("Bloom Filter for field %d should contain %d", i, j), exist);
      }
    }
  }

  @Test
  public void testReadWriteBloomFilterOnMap() throws IOException {
    int bfFieldId = 8;
    Map<Integer, OutputFile> bloomFilterMap = ImmutableMap.of(
        bfFieldId, localOutput(String.format("%s-8", bfBaseLocation))
    );
    BloomFilterWriterStore writerStore = new BloomFilterWriterStore(SCHEMA_WITH_MAP, bloomFilterMap);

    BloomFilterWriter writer = writerStore.getBloomFilterWriter(bfFieldId);
    for (int j = 0; j < 10; ++j) {
      writer.write(Long.valueOf(j));
    }
    writerStore.close();

    String bfPath = String.format("%s-%d", bfBaseLocation, bfFieldId);
    Assert.assertTrue(String.format("Bloom Filter for field %d should exist", bfFieldId),
        new File(bfPath).exists());

    Map<Integer, BloomFilter> bfMap = BloomFilterReader.loadBloomFilter(FILE_WITH_MAP, bfBaseLocation);
    Assert.assertTrue("Should read 1 Bloom Filters for the given data file", bfMap.size() == 1);

    BloomFilter bf = bfMap.get(bfFieldId);
    for (int j = 0; j < 10; ++j) {
      boolean exist = bf.findHash(bf.hash(Long.valueOf(j)));
      Assert.assertTrue(String.format("Bloom Filter for field %d should contain %d", bfFieldId, j), exist);
    }
  }
}
