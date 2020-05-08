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

import com.google.common.base.Objects;
import java.util.Map;

public class SimpleRecordWithMap extends SimpleRecord {
  private Map<String, String> properties;

  public SimpleRecordWithMap() {
  }

  SimpleRecordWithMap(Integer id, String data, Map<String, String> properties) {
    super(id, data);
    this.properties = properties;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SimpleRecordWithMap record = (SimpleRecordWithMap) o;
    return Objects.equal(getId(), record.getId()) && Objects.equal(getData(), record.getData()) &&
        Objects.equal(getProperties(), record.getProperties());
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
