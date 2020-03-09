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

import com.adobe.platform.iceberg.extensions.ExtendedIcebergSource;
import com.adobe.platform.iceberg.extensions.ExtendedTable;
import com.adobe.platform.iceberg.extensions.tombstone.ExtendedEntry;
import com.adobe.platform.iceberg.extensions.tombstone.SupportsTombstoneFilters;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExpressions;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

public class ExtendedVectorizedReader extends V1VectorizedReader implements SupportsTombstoneFilters {

  private ExtendedTable table;
  private Types.NestedField tombstoneField;
  // We preserve the dotted notation field name since org.apache.iceberg.types.Types.NestedField does not provide
  // full precedence of field using dot notation so we will fail all SQL queries
  private String tombstoneFieldName;
  private List<String> tombstoneValues;
  private boolean shouldFilterTombstones;

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
    List<ExtendedEntry> tombstones = this.table.getSnapshotTombstones(tombstoneField, table.currentSnapshot());
    this.tombstoneValues = tombstones.stream().map(t -> t.getEntry().getId()).collect(Collectors.toList());
    this.shouldFilterTombstones = options.getBoolean(ExtendedIcebergSource.TOMBSTONE_FILTER_ENABLED, true);
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    if (shouldFilterTombstones) {
      List<ExtendedEntry> tombstones = table.getSnapshotTombstones(tombstoneField, table.currentSnapshot());
      TombstoneExpressions.notIn(tombstoneFieldName, tombstones).ifPresent(this::addFilter);
    }
    return super.pushFilters(filters);
  }


  @Override
  public String tombstoneField() {
    return tombstoneFieldName;
  }

  @Override
  public String[] tombstoneValues() {
    return (this.tombstoneValues == null) ? new String[0] : this.tombstoneValues.toArray(new String[0]);
  }

  @Override
  public boolean shouldFilterTombstones() {
    return shouldFilterTombstones;
  }
}
