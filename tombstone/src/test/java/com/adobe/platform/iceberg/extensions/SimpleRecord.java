package com.adobe.platform.iceberg.extensions;

import java.sql.Timestamp;
import java.util.Objects;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class SimpleRecord {
  public transient final static Schema SCHEMA = new Schema(
      optional(11, "id", Types.IntegerType.get()),
      optional(22, "timestamp", Types.TimestampType.withZone()),
      optional(33, "batch", Types.StringType.get()),
      optional(44, "data", Types.StringType.get()));

  public transient final static PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA)
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
