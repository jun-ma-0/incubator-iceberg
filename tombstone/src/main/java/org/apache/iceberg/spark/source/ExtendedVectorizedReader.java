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

import com.adobe.platform.iceberg.extensions.ExtendedTable;
import com.adobe.platform.iceberg.extensions.tombstone.ExtendedEntry;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExpressions;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

public class ExtendedVectorizedReader extends V1VectorizedReader {

  private ExtendedTable table;
  private Types.NestedField tombstoneField;
  // We preserve the dotted notation field name since org.apache.iceberg.types.Types.NestedField does not provide
  // full precedence of field using dot notation so we will fail all SQL queries
  private String tombstoneFieldName;

  public ExtendedVectorizedReader(
      ExtendedTable table,
      boolean caseSensitive,
      DataSourceOptions options,
      Configuration hadoopConf,
      int numRecordsPerBatch,
      SparkSession sparkSession,
      Types.NestedField tombstoneField,
      String tombstoneFieldName) {
    super(table, caseSensitive, options, hadoopConf, numRecordsPerBatch, sparkSession);
    this.tombstoneField = tombstoneField;
    this.table = table;
    this.tombstoneFieldName = tombstoneFieldName;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    List<ExtendedEntry> tombstones = table.getSnapshotTombstones(tombstoneField, table.currentSnapshot());
    TombstoneExpressions.notIn(tombstoneFieldName, tombstones).ifPresent(this::addFilter);
    return super.pushFilters(filters);
  }
}
