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

package com.adobe.platform.iceberg.extensions.tombstone;

import java.util.Map;
import java.util.Objects;

public class Tombstone {

  private String id;
  private String namespace;
  private Long addedOn;
  private Long evictionTs;
  private Map<String, String> properties;
  private Map<String, String> internal;

  public Tombstone() {}

  public Tombstone(String id, String namespace, Long evictionTs) {
    this.id = id;
    this.namespace = namespace;
    this.evictionTs = evictionTs;
  }

  public String getIdentity() {
    return  String.format("%s.%s.%d", id.toLowerCase(), namespace.toLowerCase(), evictionTs);
  }
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public Long getAddedOn() {
    return addedOn;
  }

  public void setAddedOn(Long addedOn) {
    this.addedOn = addedOn;
  }

  public Long getEvictionTs() {
    return evictionTs;
  }

  public void setEvictionTs(Long evictionTs) {
    this.evictionTs = evictionTs;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public Map<String, String> getInternal() {
    return internal;
  }

  public void setInternal(Map<String, String> internal) {
    this.internal = internal;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Tombstone tombstone = (Tombstone) o;
    return this.getIdentity().equals(tombstone.getIdentity());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getIdentity());
  }
}
