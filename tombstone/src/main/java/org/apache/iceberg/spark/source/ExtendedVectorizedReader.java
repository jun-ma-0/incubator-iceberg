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
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneValidationException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtendedVectorizedReader extends V1VectorizedReader implements SupportsTombstoneFilters {
  private static final Logger LOG = LoggerFactory.getLogger(ExtendedVectorizedReader.class);

  private ExtendedTable table;
  private Types.NestedField tombstoneField;
  // We preserve the dotted notation field name since org.apache.iceberg.types.Types.NestedField does not provide
  // full precedence of field using dot notation so we will fail all SQL queries
  private String tombstoneFieldName;
  private List<String> tombstoneValues;
  private DataSourceOptions options;
  private Boolean isVacuum;
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
    this.table = table;
    this.tombstoneField = tombstoneField;
    this.tombstoneFieldName = tombstoneFieldName;
    this.options = options;
    List<ExtendedEntry> tombstones = this.table.getSnapshotTombstones(tombstoneField, table.currentSnapshot());
    this.tombstoneValues = tombstones.stream().map(t -> t.getEntry().getId()).collect(Collectors.toList());
    isVacuum = options.getBoolean(TombstoneExtension.TOMBSTONE_VACUUM, false);
    this.shouldFilterTombstones = options.getBoolean(ExtendedIcebergSource.TOMBSTONE_FILTER_ENABLED, true);
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
  public Filter[] pushFilters(Filter[] filters) {
    if (isVacuum) {
      // This will prevent the parquet reader from filtering based on residual evaluation of the same expression we've
      // used to detect tombstone files
      super.setParquetFilter(false);

      long readSnapshotId = this.options.getLong("snapshot-id", 0L);
      if (readSnapshotId <= 0L) {
        throw new RuntimeIOException("Invalid read snapshot id, expected > 0");
      }
      Optional<String> overrideTombstones = options.get(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST);
      List<ExtendedEntry> tombstones = overrideTombstones.map(s -> Arrays.stream(s.split(","))
          .map(entry -> ExtendedEntry.Builder.withEmptyProperties(() -> entry)).collect(Collectors.toList()))
          .orElseGet(() -> table.getSnapshotTombstones(tombstoneField, table.snapshot(readSnapshotId)));

      LOG.info("Vacuum read tombstones count={} with values={} on snapshotId={} and table={}", tombstoneValues.size(),
          tombstoneValues, table.currentSnapshot().snapshotId(), table.location());

      this.tombstoneValues = tombstones.stream().map(t -> t.getEntry().getId()).collect(Collectors.toList());
      // Load only files that have at least ONE tombstone row
      if (!tombstoneValues.isEmpty()) {
        TombstoneExpressions.matchesAny(tombstoneFieldName, tombstoneValues).ifPresent(this::addFilter);
      } else {
        throw new TombstoneValidationException("Vacuum expects non-empty list of tombstones");
      }
    } else if (shouldFilterTombstones) {
      List<ExtendedEntry> tombstones = table.getSnapshotTombstones(tombstoneField, table.currentSnapshot());
      this.tombstoneValues = tombstones.stream().map(t -> t.getEntry().getId()).collect(Collectors.toList());
      // Load all files BUT the ones that have all tombstone rows
      TombstoneExpressions.notIn(tombstoneFieldName, tombstones).ifPresent(this::addFilter);
    }
    return super.pushFilters(filters);
  }

  @Override
  public boolean shouldFilterTombstones() {
    return shouldFilterTombstones;
  }
}
