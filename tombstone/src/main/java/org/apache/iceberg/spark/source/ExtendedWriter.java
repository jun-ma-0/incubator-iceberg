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
import com.adobe.platform.iceberg.extensions.VacuumOverwrite;
import com.adobe.platform.iceberg.extensions.tombstone.Entry;
import com.adobe.platform.iceberg.extensions.tombstone.ExtendedEntry;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneValidationException;
import com.google.common.collect.ImmutableList;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtendedWriter extends Writer {
  private static final Logger LOG = LoggerFactory.getLogger(ExtendedWriter.class);

  private ExtendedTable table;
  // private List<Entry> tombstones = new ArrayList<>();
  // We preserve the dotted notation field name since org.apache.iceberg.types.Types.NestedField
  // does not provide full precedence of field name so we'd fail any SQL queries
  // private Map.Entry<String, Types.NestedField> column;
  private Boolean isVacuum;
  private DataSourceOptions options;

  public ExtendedWriter(
      ExtendedTable table,
      DataSourceOptions options,
      boolean replacePartitions,
      String applicationId, String wapId, Schema dsSchema) {
    super(table, options, replacePartitions, applicationId, wapId, dsSchema);
    this.table = table;
    this.options = options;
    this.isVacuum = options.getBoolean(TombstoneExtension.TOMBSTONE_VACUUM, false);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    if (isVacuum) {
      Map.Entry<String, Types.NestedField> column = tombstoneColumn(options);
      long readSnapshotId = options.getLong("snapshot-id", 0L);
      Optional<String> overrideTombstones = options.get(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST);
      List<ExtendedEntry> tombstones = overrideTombstones.map(s -> Arrays.stream(s.split(","))
          .map(entry -> ExtendedEntry.Builder.withEmptyProperties(() -> entry)).collect(Collectors.toList()))
          .orElseGet(() -> table.getSnapshotTombstones(column.getValue(), table.snapshot(readSnapshotId)));
      LOG.info("Vacuum commit tombstones count={} with values={} on snapshotId={} and table={}", tombstones.size(),
          tombstones.stream().map(extendedEntry -> extendedEntry.getEntry().getId()).collect(Collectors.joining(",")),
          table.currentSnapshot().snapshotId(), table.location());
      vacuum(messages, tombstones, column, readSnapshotId);
    } else {
      List<Entry> tombstones = tombstoneValues(options);
      if (tombstones.isEmpty()) {
        super.commit(messages);
      } else {
        Map.Entry<String, Types.NestedField> column = tombstoneColumn(options);
        Long tombstoneEvictTs = tombstoneEvictTs(options);
        appendWithTombstones(messages, tombstones, column.getValue(), tombstoneEvictTs);
      }
    }
  }

  @SuppressWarnings("checkstyle:HiddenField")
  private Map.Entry<String, Types.NestedField> tombstoneColumn(DataSourceOptions options) {
    Optional<String> tombstoneColumn = options.get(TombstoneExtension.TOMBSTONE_COLUMN);
    if (tombstoneColumn.isPresent()) {
      String columnName = tombstoneColumn.get();
      NestedField column = table.schema().findField(columnName);
      if (column != null) {
        return new SimpleEntry<>(columnName, column);
      }
    }
    throw new RuntimeIOException("Failed to find tombstone field by name: %s", tombstoneColumn);
  }

  @SuppressWarnings("checkstyle:HiddenField")
  private List<Entry> tombstoneValues(DataSourceOptions options) {
    Optional<String> tombstoneValues = options.get(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST);
    if (tombstoneValues.isPresent()) {
      return Stream.of(tombstoneValues.get().split(",")).map(value -> (Entry) () -> value)
          .collect(ImmutableList.toImmutableList());
    }
    return Collections.emptyList();
  }

  @SuppressWarnings("checkstyle:HiddenField")
  private Long tombstoneEvictTs(DataSourceOptions options) {
    return options.get(TombstoneExtension.TOMBSTONE_COLUMN_EVICT_TS).map(Long::valueOf)
        .orElseThrow(() -> new TombstoneValidationException(
            "Expected value of type long for option=%s",
            TombstoneExtension.TOMBSTONE_COLUMN_EVICT_TS));
  }

  private void vacuum(
      WriterCommitMessage[] messages, List<ExtendedEntry> entries,
      Map.Entry<String, Types.NestedField> column, Long readSnapshotId) {
    VacuumOverwrite vacuumOverwrite = table.newVacuumTombstones(column, entries, readSnapshotId);

    int numFiles = 0;
    for (DataFile file : files(messages)) {
      numFiles += 1;
      vacuumOverwrite.addFile(file);
    }

    LOG.info("Vacuum commit added files={} on snapshotId={} for table={}", numFiles,
        table.currentSnapshot().snapshotId(), table.location());
    commitOperation(vacuumOverwrite, numFiles, "vacuumTombstones");
  }

  private void appendWithTombstones(
      WriterCommitMessage[] messages, List<Entry> entries, Types.NestedField field, long evictionTs) {
    AppendFiles append = table.newAppendWithTombstonesAdd(field, entries, new HashMap<>(), evictionTs);

    int numFiles = 0;
    for (DataFile file : files(messages)) {
      numFiles += 1;
      append.appendFile(file);
    }

    commitOperation(append, numFiles, "appendWithTombstones");
  }
}
