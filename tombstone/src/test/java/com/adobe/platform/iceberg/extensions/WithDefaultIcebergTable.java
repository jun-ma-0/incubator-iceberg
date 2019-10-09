package com.adobe.platform.iceberg.extensions;

public interface WithDefaultIcebergTable {

  default void implicitTable(ExtendedTables tables, String tableLocation) {
    tables.create(SimpleRecord.SCHEMA, SimpleRecord.SPEC, tableLocation);
  }
}
