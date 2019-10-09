package com.adobe.platform.iceberg.extensions.tombstone;

import java.util.Map;
import java.util.Objects;

public class Tombstone {
  private String id;
  private String namespace;
  private Long addedOn;

  private Map<String, String> properties;

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
    Tombstone tombstone = (Tombstone) o;
    return id.equalsIgnoreCase(tombstone.id) && namespace.equalsIgnoreCase(tombstone.namespace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, namespace);
  }
}
