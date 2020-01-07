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
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

public class ExtendedReader extends Reader {

  private ExtendedTable table;
  // We preserve the dotted notation field name since org.apache.iceberg.types.Types.NestedField does not provide
  // full precedence of field using dot notation so we will fail all SQL queries
  private String tombstoneFieldName;
  private Types.NestedField tombstoneField;
  private List<ExtendedEntry> tombstones;
  private DataSourceOptions options;
  private Boolean isVacuum;

  public ExtendedReader(
      ExtendedTable table,
      boolean caseSensitive,
      DataSourceOptions options,
      Types.NestedField tombstoneField,
      String tombstoneFieldName) {
    super(table, caseSensitive, options);
    this.table = table;
    this.tombstoneField = tombstoneField;
    this.tombstoneFieldName = tombstoneFieldName;
    this.options = options;
    isVacuum = options.get(TombstoneExtension.TOMBSTONE_VACUUM).isPresent();
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    if (isVacuum) {
      long readSnapshotId = this.options.getLong("snapshot-id", 0L);
      if (readSnapshotId <= 0L) {
        throw new RuntimeIOException("Invalid read snapshot id, expected > 0");
      }
      this.tombstones = table.getSnapshotTombstones(tombstoneField, table.snapshot(readSnapshotId));
      // Load only files that have at least ONE tombstone row
      if (!tombstones.isEmpty()) {
        TombstoneExpressions.matchesAny(tombstoneFieldName,
            tombstones.stream().map(t -> t.getEntry().getId()).collect(Collectors.toList()))
            .ifPresent(this::addFilter);
      } else {
        throw new RuntimeIOException(String
            .format("Using vacuum requires list of tombstones provided via option=%s",
                TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST));
      }
    } else {
      this.tombstones = table.getSnapshotTombstones(tombstoneField, table.currentSnapshot());
      // Load all files BUT the ones that have all tombstone rows
      TombstoneExpressions.notIn(tombstoneFieldName, tombstones).ifPresent(this::addFilter);
    }
    return super.pushFilters(filters);
  }

  @Override
  public InputPartition<InternalRow> readTask(CombinedScanTask task, String tableSchemaString,
      String expectedSchemaString, FileIO fileIo, EncryptionManager encryptionManager,
      boolean caseSensitive) {
    return new ReadTask(task, tableSchemaString, expectedSchemaString, fileIo, encryptionManager,
        caseSensitive, this.tombstoneFieldName,
        this.tombstones.stream().map(t -> t.getEntry().getId()).collect(Collectors.toList()));
  }

  private static class ReadTask implements InputPartition<InternalRow>, Serializable {

    private final CombinedScanTask task;
    private final String tableSchemaString;
    private final String expectedSchemaString;
    private final FileIO fileIo;
    private final EncryptionManager encryptionManager;
    private final boolean caseSensitive;
    private String tombstoneFieldName;
    private List<String> tombstones;

    private transient Schema tableSchema = null;
    private transient Schema expectedSchema = null;

    private ReadTask(
        CombinedScanTask task, String tableSchemaString, String expectedSchemaString, FileIO fileIo,
        EncryptionManager encryptionManager, boolean caseSensitive,
        String tombstoneFieldName,
        List<String> tombstones) {
      this.task = task;
      this.tableSchemaString = tableSchemaString;
      this.expectedSchemaString = expectedSchemaString;
      this.fileIo = fileIo;
      this.encryptionManager = encryptionManager;
      this.caseSensitive = caseSensitive;
      this.tombstoneFieldName = tombstoneFieldName;
      this.tombstones = tombstones;
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
      return new ExtendedTaskDataReader(task, lazyTableSchema(), lazyExpectedSchema(), fileIo,
          encryptionManager, caseSensitive, tombstoneFieldName, tombstones);
    }

    private Schema lazyTableSchema() {
      if (tableSchema == null) {
        this.tableSchema = SchemaParser.fromJson(tableSchemaString);
      }
      return tableSchema;
    }

    private Schema lazyExpectedSchema() {
      if (expectedSchema == null) {
        this.expectedSchema = SchemaParser.fromJson(expectedSchemaString);
      }
      return expectedSchema;
    }
  }
}
