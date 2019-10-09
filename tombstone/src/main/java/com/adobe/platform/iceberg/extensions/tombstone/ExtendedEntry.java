package com.adobe.platform.iceberg.extensions.tombstone;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import org.apache.spark.annotation.InterfaceStability;

/**
 * An extended interface for tombstone output data model, contains user defined properties.
 */
@InterfaceStability.Stable
public interface ExtendedEntry extends Serializable {

  Entry getEntry();

  Map<String, String> getProperties();

  Map<String, String> getInternalProperties();

  class Builder {

    public ExtendedEntry withEmptyProperties(Entry entry) {
      return new ExtendedEntry() {
        @Override
        public Entry getEntry() {
          return entry;
        }

        @Override
        public Map<String, String> getProperties() {
          return Collections.emptyMap();
        }

        @Override
        public Map<String, String> getInternalProperties() {
          return Collections.emptyMap();
        }
      };
    }
  }
}
