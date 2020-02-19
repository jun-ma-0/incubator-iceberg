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
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkVacuum {

  private SparkSession spark;
  private ExtendedTable extendedTable;
  private String column;
  private Long snapshotId;

  private Dataset<Row> rows;
  private Dataset<Row> tombstones;
  private String tombstonesCommaSeparatedList;

  public SparkVacuum(SparkSession spark, ExtendedTable extendedTable, String column) {
    this.spark = spark;
    this.extendedTable = extendedTable;
    this.column = column;
    this.snapshotId = extendedTable.currentSnapshot().snapshotId();
  }

  public SparkVacuum load(int limit) {
    this.tombstones =
        extendedTable.getSnapshotTombstonesDataset(column, snapshotId, spark, limit)
            .orElseThrow(() -> new RuntimeIOException("Abort vacuum since there are no available tombstones"));

    this.tombstonesCommaSeparatedList =
        String.join(",", tombstones.select("id").as(Encoders.STRING()).collectAsList());

    this.rows = spark.read()
        .format("iceberg.adobe")
        .option(TombstoneExtension.TOMBSTONE_VACUUM, true)
        .option(TombstoneExtension.TOMBSTONE_COLUMN, column)
        .option(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST, tombstonesCommaSeparatedList)
        .option("iceberg.read.enableV1VectorizedReader", "true")
        .option("snapshot-id", snapshotId)
        .load(extendedTable.location());
    return this;
  }

  public void reduceWithAntiJoin() {
    rows.join(
        tombstones,
        rows.col(column).equalTo(tombstones.col("id")),
        "left_anti")
        .write()
        .format("iceberg.adobe")
        .mode(SaveMode.Overwrite)
        .option(TombstoneExtension.TOMBSTONE_VACUUM, true)
        .option(TombstoneExtension.TOMBSTONE_COLUMN, column)
        .option(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST, tombstonesCommaSeparatedList)
        .option("snapshot-id", snapshotId)
        .save(extendedTable.location());
  }
}
