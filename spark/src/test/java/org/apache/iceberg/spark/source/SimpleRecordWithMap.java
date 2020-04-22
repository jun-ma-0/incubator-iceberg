package org.apache.iceberg.spark.source;

import com.google.common.base.Objects;

import java.util.Map;

public class SimpleRecordWithMap extends SimpleRecord {
  private Map<String, String> properties;

  public SimpleRecordWithMap() {}

  SimpleRecordWithMap(Integer id, String data, Map<String, String> properties) {
    super(id, data);
    this.properties = properties;
  }

  public Map<String, String> getProperties() { return properties; }

  public void setProperties(Map<String, String> properties) { this.properties = properties; }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SimpleRecordWithMap record = (SimpleRecordWithMap) o;
    return Objects.equal(getId(), record.getId()) && Objects.equal(getData(), record.getData())
        && Objects.equal(getProperties(), record.getProperties());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getId(), getData(), getProperties());
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("{\"id\"=");
    buffer.append(getId());
    buffer.append(",\"data\"=\"");
    buffer.append(getData());
    buffer.append(",\"properties\"=\"");
    buffer.append(getProperties());
    buffer.append("\"}");
    return buffer.toString();
  }

}
