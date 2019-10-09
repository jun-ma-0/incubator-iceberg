package com.adobe.platform.iceberg.extensions.tombstone;

import org.apache.spark.annotation.InterfaceStability;

/**
 * The base interface for tombstone entry data model.
 */
@InterfaceStability.Stable
@FunctionalInterface
public interface Entry {
  String getId();
}
