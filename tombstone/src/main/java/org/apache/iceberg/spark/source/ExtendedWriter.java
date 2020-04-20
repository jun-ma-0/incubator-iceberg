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
import com.adobe.platform.iceberg.extensions.VacuumDelete;
import com.adobe.platform.iceberg.extensions.VacuumOverwrite;
import com.adobe.platform.iceberg.extensions.VacuumRewrite;
import com.adobe.platform.iceberg.extensions.tombstone.Entry;
import com.adobe.platform.iceberg.extensions.tombstone.ExtendedEntry;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneValidationException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtendedWriter extends Writer {
  private static final Logger LOG = LoggerFactory.getLogger(ExtendedWriter.class);

  private final ExtendedTable table;
  // private List<Entry> tombstones = new ArrayList<>();
  // We preserve the dotted notation field name since org.apache.iceberg.types.Types.NestedField
  // does not provide full precedence of field name so we'd fail any SQL queries
  // private Map.Entry<String, Types.NestedField> column;
  private final Boolean isVacuum;
  private final DataSourceOptions options;
  private final SparkSession sparkSession;

  public ExtendedWriter(SparkSession sparkSession,
      ExtendedTable table,
      DataSourceOptions options,
      boolean replacePartitions,
      String applicationId, String wapId, Schema dsSchema) {
    super(table, options, replacePartitions, applicationId, wapId, dsSchema);
    this.sparkSession = sparkSession;
    this.table = table;
    this.options = options;
    this.isVacuum = options.getBoolean(TombstoneExtension.TOMBSTONE_VACUUM, false);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    if (isVacuum) {
      // Resolve column name and the tombstone field based on provided options
      Map.Entry<String, Types.NestedField> column = tombstoneColumn(options);

      // We need to make sure we run the data file resolution of tombstone rows on the same snapshot id as the reader
      long snapshotId = options.getLong("snapshot-id", table.currentSnapshot().snapshotId());

      // There are multiple options for passing in the tombstones you want to vacuum:
      // As a list of exact tombstones values you intend to vacuum via TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST
      // As a scalar for how many tombstones you intend to vacuum via TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIMIT
      // To signal a full vacuum set a negative value for TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIMIT
      List<String> tombstones = options.get(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST)
          .map(s -> Arrays.stream(s.split(",")).collect(Collectors.toList()))
          .orElseGet(() -> {
            int limit = options.get(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIMIT)
                .map(Integer::parseInt).orElse(0);
            List<ExtendedEntry> snapshotTombstones = (limit > 0) ? table
                .getSnapshotTombstones(column.getValue(), table.snapshot(snapshotId), limit)
                : table.getSnapshotTombstones(column.getValue(), table.snapshot(snapshotId));
            return snapshotTombstones.stream().map(e -> e.getEntry().getId())
                .collect(Collectors.toList());
          });

      // Resolve data files that are carrying tombstones
      Set<DataFile> vacuumFiles = Actions.forTable(sparkSession, table)
          .filterDatafileWithTombstones()
          .withSnapshot(snapshotId)
          .withTombstoneColumn(column.getKey())
          .withTombstones(tombstones)
          .execute();

      SnapshotUpdate<?> snapshotUpdate = vacuum(messages, vacuumFiles, tombstones, column, snapshotId);
      commitOperation(snapshotUpdate, vacuumFiles.size(), "vacuumTombstones");
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
      NestedField field = table.schema().findField(columnName);
      if (field != null) {
        return new SimpleEntry<>(columnName, field);
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

  /**
   * @deprecated replaced by {@link #vacuum(WriterCommitMessage[], Set, List, Map.Entry, Long)
   * vacuum}
   */
  @Deprecated
  private void vacuum(
      WriterCommitMessage[] messages, List<String> tombstones,
      Map.Entry<String, Types.NestedField> column, Long readSnapshotId) {
    VacuumOverwrite vacuumOverwrite = table.newVacuumOverwrite(column, tombstones, readSnapshotId);

    int numFiles = 0;
    for (DataFile file : files(messages)) {
      numFiles += 1;
      vacuumOverwrite.addFile(file);
    }

    LOG.info("Vacuum commit added files={} on snapshotId={} for table={}", numFiles,
        table.currentSnapshot().snapshotId(), table.location());
    commitOperation(vacuumOverwrite, numFiles, "vacuumTombstones");
  }

  private SnapshotUpdate<?> vacuum(WriterCommitMessage[] messages, Set<DataFile> vacuumFiles,
      List<String> tombstones, Map.Entry<String, Types.NestedField> column, Long readSnapshotId) {
    Iterable<DataFile> appendFiles = files(messages);
    int numAddedFiles = Iterables.size(appendFiles);

    if (vacuumFiles.isEmpty()) {
      throw new VacuumException("Vacuum resulted in non-zero files to add and 0 files to delete");
    } else if (Iterables.size(appendFiles) == 0) {
      if (vacuumFiles.isEmpty()) {
        throw new VacuumException("Vacuum resulted in 0 files to add and 0 files to delete");
      } else {
        VacuumDelete vacuumDelete = table.newVacuumDelete(column, tombstones, readSnapshotId);
        vacuumFiles.forEach(vacuumDelete::deleteFile);
        LOG.info(
            "Vacuum type=delete added-files={} deleted-files={} on snapshotId={} for table={} for tombstones={}",
            numAddedFiles, vacuumFiles.size(), table.currentSnapshot().snapshotId(),
            table.location(), tombstones.size());
        return vacuumDelete;
      }
    } else {
      VacuumRewrite vacuumRewrite = table.newVacuumRewrite(column, tombstones, readSnapshotId);
      vacuumRewrite.rewriteFiles(vacuumFiles, Sets.newHashSet(appendFiles));
      LOG.info(
          "Vacuum type=rewrite added-files={} deleted-files={} on snapshotId={} for table={} for tombstones={}",
          numAddedFiles, vacuumFiles.size(), table.currentSnapshot().snapshotId(), table.location(),
          tombstones.size());
      return vacuumRewrite;
    }
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
