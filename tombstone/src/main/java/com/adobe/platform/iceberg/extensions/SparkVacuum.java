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

package com.adobe.platform.iceberg.extensions;

import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.actions.FilterDatafilesWithTombstoneAction;
import org.apache.iceberg.spark.source.VacuumException;
import org.apache.parquet.Strings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkVacuum {

  private static final Logger LOG = LoggerFactory.getLogger(SparkVacuum.class);

  private final SparkSession spark;
  private final ExtendedTable table;
  private final String column;
  private final Long snapshotId;


  private Set<DataFile> files;
  private Dataset<Row> tombstones;

  // Since there are multiple reader implementations for resolving tombstones (by limit, by values
  // or by all available) we need a solution to signal to the writer which one of these options was
  // used so it can apply same logic to resolve the same tombstones that the reader selected.
  private Supplier<Map<String, String>> writerOptions = HashMap::new;

  public SparkVacuum(SparkSession spark, ExtendedTable table, String column) {
    this.spark = spark;
    this.table = table;
    this.column = column;

    if (table.currentSnapshot() == null) {
      throw new VacuumException("Abort vacuum, current snapshot is null");
    }

    this.snapshotId = table.currentSnapshot().snapshotId();
  }

  public SparkVacuum load() {
    FilterDatafilesWithTombstoneAction filterAction = Actions.forTable(spark, table)
        .filterDatafileWithTombstones()
        .withSnapshot(snapshotId)
        .withTombstoneColumn(column);

    resolveDatasets(filterAction);
    return this;
  }

  @SuppressWarnings("checkstyle:HiddenField")
  public SparkVacuum load(List<String> tombstones) {
    this.writerOptions = () -> Collections
        .singletonMap(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST,
            Strings.join(tombstones, ","));

    loadByValue(tombstones);
    return this;
  }

  @SuppressWarnings("checkstyle:HiddenField")
  public SparkVacuum load(int limit) {
    List<String> snapshotTombstones =
        table.getSnapshotTombstones(table.schema().findField(column),
            table.snapshot(snapshotId), limit).stream()
            .map(extendedEntry -> extendedEntry.getEntry().getId()).collect(Collectors.toList());

    this.writerOptions = () -> Collections
        .singletonMap(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIMIT, String.valueOf(limit));

    loadByValue(snapshotTombstones);
    return this;
  }

  public void reduceWithAntiJoin() {
    LOG.info("Vacuum files={} for tombstones={} column={} table={}", files.size(), column,
        table.location(), tombstones.count());

    Dataset<Row> parquet = spark.read()
        .parquet(files.stream().map(f -> f.path().toString()).toArray(String[]::new));

    parquet.join(tombstones, org.apache.spark.sql.functions.column(column)
            .equalTo(tombstones.col("id")), "left_anti")
        .write()
        .format("iceberg.adobe")
        .mode(SaveMode.Overwrite)
        .option("snapshot-id", snapshotId)
        .option(TombstoneExtension.TOMBSTONE_VACUUM, true)
        .option(TombstoneExtension.TOMBSTONE_COLUMN, column)
        .options(this.writerOptions.get())
        .save(table.location());
  }

  @SuppressWarnings("checkstyle:HiddenField")
  private void loadByValue(List<String> tombstones) {
    if (tombstones == null || tombstones.isEmpty()) {
      throw new VacuumException("Abort vacuum, provided null or empty tombstones");
    }

    List<String> snapshotTombstones = table
        .getSnapshotTombstones(table.schema().findField(column), table.snapshot(snapshotId))
        .stream().map(e -> e.getEntry().getId()).collect(Collectors.toList());

    if (!snapshotTombstones.containsAll(tombstones)) {
      throw new VacuumException("Abort vacuum, provided tombstones missing from snapshot");
    }

    FilterDatafilesWithTombstoneAction filterAction = Actions.forTable(spark, table)
        .filterDatafileWithTombstones()
        .withSnapshot(snapshotId)
        .withTombstoneColumn(column)
        .withTombstones(tombstones);

    resolveDatasets(filterAction);
  }

  private void resolveDatasets(FilterDatafilesWithTombstoneAction filterAction) {
    tombstones = filterAction.getTombstonesDataset();
    files = filterAction.execute();
    if (files.isEmpty()) {
      throw new VacuumException("No data files detected for tombstone vacuuming");
    }
  }
}
