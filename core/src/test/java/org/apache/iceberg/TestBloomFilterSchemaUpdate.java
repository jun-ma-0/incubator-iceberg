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

package org.apache.iceberg;

import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestBloomFilterSchemaUpdate {
  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get()),
      optional(3, "preferences", Types.StructType.of(
          required(8, "feature1", Types.BooleanType.get()),
          optional(9, "feature2", Types.BooleanType.get())
      ), "struct of named boolean options"),
      required(4, "locations", Types.MapType.ofRequired(10, 11,
          Types.StructType.of(
              required(20, "address", Types.StringType.get()),
              required(21, "city", Types.StringType.get()),
              required(22, "state", Types.StringType.get()),
              required(23, "zip", Types.IntegerType.get())
          ),
          Types.StructType.of(
              required(12, "lat", Types.FloatType.get()),
              required(13, "long", Types.FloatType.get())
          )), "map of address to coordinate"),
      optional(5, "points", Types.ListType.ofOptional(14,
          Types.StructType.of(
              required(15, "x", Types.LongType.get()),
              required(16, "y", Types.LongType.get())
          )), "2-D cartesian points"),
      required(6, "doubles", Types.ListType.ofRequired(17,
          Types.DoubleType.get()
      )),
      optional(7, "properties", Types.MapType.ofOptional(18, 19,
          Types.StringType.get(),
          Types.StringType.get()
      ), "string map of properties")
  );

  private static final Schema SCHEMA_WITH_MAP = new Schema(
      Types.NestedField.required(1, "id", Types.LongType.get()),
      Types.NestedField.optional(2, "ts", Types.TimestampType.withZone()),
      Types.NestedField.optional(3, "data", Types.StringType.get()),
      Types.NestedField.optional(4, "idMap", Types.MapType.ofOptional(5, 6, Types.StringType.get(),
          Types.ListType.ofOptional(7, Types.StructType.of(
              required(8, "id", Types.LongType.get()),
              optional(9, "description", Types.StringType.get())))))
  );

  private static final int SCHEMA_LAST_COLUMN_ID = 23;

  @Test
  public void testAddBloomFilterForTopLevelField() {
    String fieldName = "id";
    BloomFilterSchemaUpdate bfSchema = new BloomFilterSchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
    Types.NestedField field = bfSchema.schema().findField(fieldName);
    Assert.assertTrue("Field does not have bloom filter config", field.bfConfig() == null);

    Schema updatedSchema = bfSchema.addBloomFilter(fieldName, 0.01, 10).apply();
    field = updatedSchema.findField(fieldName);
    Assert.assertTrue("Field should have bloom filter config", field.bfConfig() != null);
    Assert.assertTrue("Field should have bloom filter enabled", field.bfConfig().isEnabled());
    Assert.assertTrue("Bloom filter should have the right value for fpp", field.bfConfig().fpp() == 0.01);
    Assert.assertTrue("Bloom filter should have the right value for ndv", field.bfConfig().ndv() == 10);
  }

  @Test
  public void testAddBloomFilterForNestedField() {
    String fieldName = "locations.lat";
    SchemaUpdate bfSchema = new BloomFilterSchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
    Types.NestedField field = bfSchema.schema().findField(fieldName);
    Assert.assertTrue("Field does not have bloom filter config", field.bfConfig() == null);

    Schema updatedSchema = bfSchema.addBloomFilter(fieldName, 0.01, 10).apply();
    field = updatedSchema.findField(fieldName);
    Assert.assertTrue("Field should have bloom filter config", field.bfConfig() != null);
    Assert.assertTrue("Field should have bloom filter enabled", field.bfConfig().isEnabled());
    Assert.assertTrue("Bloom filter should have the right value for fpp", field.bfConfig().fpp() == 0.01);
    Assert.assertTrue("Bloom filter should have the right value for ndv", field.bfConfig().ndv() == 10);
  }

  @Test
  public void testAddBloomFilterForArrayNestedInMap() {
    String fieldName = "idMap.value.id";
    BloomFilterSchemaUpdate bfSchema = new BloomFilterSchemaUpdate(SCHEMA_WITH_MAP, 9);
    Types.NestedField field = bfSchema.schema().findField(fieldName);
    Assert.assertTrue("Field does not have bloom filter config", field.bfConfig() == null);

    Schema updatedSchema = bfSchema.addBloomFilter(fieldName, 0.01, 10).apply();
    field = updatedSchema.findField(fieldName);
    Assert.assertTrue("Field should have bloom filter config", field.bfConfig() != null);
    Assert.assertTrue("Field should have bloom filter enabled", field.bfConfig().isEnabled());
    Assert.assertTrue("Bloom filter should have the right value for fpp", field.bfConfig().fpp() == 0.01);
    Assert.assertTrue("Bloom filter should have the right value for ndv", field.bfConfig().ndv() == 10);
  }
}
