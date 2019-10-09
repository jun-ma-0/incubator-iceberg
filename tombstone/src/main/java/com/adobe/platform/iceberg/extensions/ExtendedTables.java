package com.adobe.platform.iceberg.extensions;

import org.apache.iceberg.Tables;

public interface ExtendedTables extends Tables {

  ExtendedTable loadWithTombstoneExtension(String location);
}
