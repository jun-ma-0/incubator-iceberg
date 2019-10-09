package com.adobe.platform.iceberg.extensions.tombstone;

import java.util.Map;
import org.apache.spark.annotation.InterfaceStability;

/**
 * An extended interface for tombstone output data model, contains stored user defined properties.
 */
@InterfaceStability.Stable
public interface ExtendedEntry {
  Entry getEntry();

  Map<String, String> getProperties();
}
