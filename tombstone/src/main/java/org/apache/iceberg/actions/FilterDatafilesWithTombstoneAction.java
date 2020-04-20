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

package org.apache.iceberg.actions;

import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An action that resolves data files that may contain tombstones - it does so provided a mandatory
 * column name that the tombstones were set on, an optional snapshot id for consistent resolution
 * (defaults to table current snapshots) and a set of tombstone values (defaults to all available
 * tombstones for that same snapshot).
 *
 * It will join the data files table for the indicated snapshot id by matching any of the provided
 * tombstones fall in the range provided by the upper and lower limit of the indicated column data
 * file metric.
 *
 * By default, this action requires only a column name and it will remove all tombstones from
 * current snapshot.
 */
public class FilterDatafilesWithTombstoneAction implements Action<Set<DataFile>> {

  private static final Logger LOG = LoggerFactory
      .getLogger(FilterDatafilesWithTombstoneAction.class);

  private final SparkSession spark;
  private final Table table;

  private Dataset<Row> tombstones;
  private Long snapshotId;
  private NestedField tombstoneField;

  FilterDatafilesWithTombstoneAction(SparkSession spark, Table table) {
    this.spark = spark;
    this.table = table;
    Preconditions.checkArgument(table.currentSnapshot() != null,
        "Action not allowed to run for null snapshot id");
    this.snapshotId = table.currentSnapshot().snapshotId();
  }

  @SuppressWarnings("checkstyle:HiddenField")
  public FilterDatafilesWithTombstoneAction withSnapshot(Long snapshotId) {
    Preconditions.checkArgument(snapshotId != null,
        "Action not allowed to run for null snapshot id");
    this.snapshotId = snapshotId;
    return this;
  }

  public FilterDatafilesWithTombstoneAction withTombstoneColumn(String columnName) {
    Preconditions.checkArgument(columnName != null && !columnName.isEmpty(),
        "Action not allowed to run on null column name");
    this.tombstoneField = table.schema().findField(columnName);
    Preconditions.checkArgument(this.tombstoneField != null,
        "Action not allowed to run on missing field: %s", columnName);
    return this;
  }

  @SuppressWarnings("checkstyle:HiddenField")
  public FilterDatafilesWithTombstoneAction withTombstones(List<String> tombstones) {
    Preconditions.checkArgument(tombstones != null && !tombstones.isEmpty(),
        "Action not allowed to run for zero tombstones");
    this.tombstones = spark.createDataset(tombstones, Encoders.STRING())
        .withColumn("id", org.apache.spark.sql.functions.expr("CAST(value AS STRING)"))
        .drop("value");
    return this;
  }

  /**
   * Provides a {@link Dataset} for the filtered files to leave it up to invoker to resolve RDD lazy
   * or eagerly.
   *
   * @return instance of {@link Dataset}
   */
  public Dataset<Row> buildDatasetForTombstoneFiles() {
    // We call this method here to make sure that we are consistent w/ the tombstones dataset
    // retrieved via the getter method - this accounts for a fallback implementation detail
    Dataset<Row> tombstonesDataset = getTombstonesDataset();

    Dataset<Row> files = spark.read()
        .option("snapshot-id", snapshotId)
        .format("iceberg")
        .load(String.format("%s#files", table.location()));

    Dataset<Row> projection = files.select(
        files.col("file_path"),
        files.col("partition"),
        files.col("record_count"),
        files.col("file_size_in_bytes"),
        files.col("upper_bounds")
            .getItem(tombstoneField.fieldId()).cast("string").alias("upper_bounds_ts_field"),
        files.col("lower_bounds")
            .getItem(tombstoneField.fieldId()).cast("string").alias("lower_bounds_ts_field"));

    // Finds all data files that have a tombstone match the range between the upper and the lower
    // limits provided by the Iceberg data files metrics for the tombstone's respective field
    Dataset<Row> join = projection.join(tombstonesDataset,
        projection.col("upper_bounds_ts_field").geq(tombstonesDataset.col("id"))
            .and(projection.col("lower_bounds_ts_field").leq(tombstonesDataset.col("id"))));

    return join.select(
        join.col("file_path").as("filePath"),
        join.col("record_count").as("recordCount"),
        join.col("file_size_in_bytes").as("fileSizeInBytes"))
        .distinct();
  }

  @Override
  public Set<DataFile> execute() {
    Dataset<Row> tombstoneFilesDataset = buildDatasetForTombstoneFiles();

    Set<DataFile> tombstoneFiles = tombstoneFilesDataset
        .as(Encoders.bean(FilterDataFileProjection.class))
        .collectAsList()
        .stream()
        .map(this::map)
        .collect(Collectors.toSet());

    LOG.info("Tombstone files:{} column:{} table:{}", tombstoneFiles.size(), tombstoneField.name(),
        table.location());
    return tombstoneFiles;
  }

  public Dataset<Row> getTombstonesDataset() {
    if (this.tombstones == null) {
      String tombstoneFile = table.snapshot(snapshotId).summary()
          .get(TombstoneExtension.SNAPSHOT_TOMBSTONE_FILE_PROPERTY);

      ActionExceptions
          .check(tombstoneFile != null, "Snapshot does not contain tombstones metadata");

      Dataset<Row> allTombstones = spark.read().format("avro").load(tombstoneFile);
      this.tombstones = allTombstones
          .filter(allTombstones.col(
              "namespace").equalTo(String.valueOf(tombstoneField.fieldId())));
    }
    return this.tombstones;
  }

  private DataFile map(FilterDataFileProjection file) {
    return new DataFiles.Builder(table.spec())
        .withPath(file.getFilePath())
        // Iceberg will infer the file's partitioning schema from partition path segments only
        .withPartitionPath(Arrays.stream(file.getFilePath().split("/"))
            .filter(s -> s.split("=").length == 2)
            .collect(Collectors.joining("/")))
        .withRecordCount(file.getRecordCount())
        .withFileSizeInBytes(file.getFileSizeInBytes())
        .build();
  }

  public static class FilterDataFileProjection {

    private String filePath;
    private Long recordCount;
    private Long fileSizeInBytes;

    public String getFilePath() {
      return filePath;
    }

    public void setFilePath(String filePath) {
      this.filePath = filePath;
    }

    public Long getRecordCount() {
      return recordCount;
    }

    public void setRecordCount(Long recordCount) {
      this.recordCount = recordCount;
    }

    public Long getFileSizeInBytes() {
      return fileSizeInBytes;
    }

    public void setFileSizeInBytes(Long fileSizeInBytes) {
      this.fileSizeInBytes = fileSizeInBytes;
    }
  }

}
