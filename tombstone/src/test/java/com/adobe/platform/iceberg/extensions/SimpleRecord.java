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

import java.sql.Timestamp;
import java.util.Objects;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class SimpleRecord {
  public static final transient Schema schema = new Schema(
      optional(11, "id", Types.IntegerType.get()),
      optional(22, "timestamp", Types.TimestampType.withZone()),
      optional(33, "batch", Types.StringType.get()),
      optional(44, "data", Types.StringType.get()));

  public static final transient PartitionSpec spec =
      PartitionSpec.builderFor(schema)
          .day("timestamp", "_ACP_DATE")
          .identity("batch")
          .build();

  private Integer id;
  private Timestamp timestamp;
  private String data;
  private String batch;

  public SimpleRecord() {
  }

  public SimpleRecord(Integer id, Timestamp timestamp, String batch, String data) {
    this.id = id;
    this.timestamp = timestamp;
    this.batch = batch;
    this.data = data;
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public Timestamp getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Timestamp timestamp) {
    this.timestamp = timestamp;
  }

  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
  }

  public String getBatch() {
    return batch;
  }

  public void setBatch(String batch) {
    this.batch = batch;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SimpleRecord that = (SimpleRecord) o;
    return id.equals(that.id) &&
        timestamp.equals(that.timestamp) &&
        batch.equals(that.batch);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, timestamp, batch);
  }

  @Override
  public String toString() {
    return "SimpleRecord{" +
        "id=" + id +
        ", timestamp=" + timestamp +
        ", data='" + data + '\'' +
        ", batch='" + batch + '\'' +
        '}';
  }
}
