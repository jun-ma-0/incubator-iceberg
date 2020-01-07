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
import com.google.common.base.Preconditions;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.source.ExtendedReader;
import org.apache.iceberg.spark.source.ExtendedVectorizedReader;
import org.apache.iceberg.spark.source.ExtendedWriter;
import org.apache.iceberg.spark.source.IcebergSource;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtendedIcebergSource extends IcebergSource
    implements DataSourceV2, ReadSupport, WriteSupport, DataSourceRegister {

  private static final Logger LOG = LoggerFactory.getLogger(ExtendedIcebergSource.class);

  @Override
  public String shortName() {
    return "iceberg.adobe";
  }

  @Override
  protected DataSourceReader reader(Table table, boolean caseSensitive, DataSourceOptions options) {
    String tombstoneFieldName = options.get(TombstoneExtension.TOMBSTONE_COLUMN).orElse("");
    if (!tombstoneFieldName.isEmpty()) {
      Types.NestedField tombstoneField = table.schema().caseInsensitiveFindField(tombstoneFieldName);
      Preconditions.checkArgument(tombstoneField != null, "Invalid tombstone column: %s", tombstoneFieldName);
      LOG.debug("Use extended reader based on matching tombstone field={} for table={}", tombstoneField.name(),
          table.location());
      ExtendedTable extendedTable = getTableAndResolveHadoopConfiguration(options, new Configuration(lazyBaseConf()));
      return new ExtendedReader(extendedTable, caseSensitive, options, tombstoneField, tombstoneFieldName);
    }
    LOG.debug("Fallback to default Iceberg reader, missing or empty tombstone option={} for table={}",
        TombstoneExtension.TOMBSTONE_COLUMN, table.location());
    return super.reader(table, caseSensitive, options);
  }

  @Override
  protected DataSourceReader vectorizedReader(
      Table table, boolean caseSensitive, DataSourceOptions options,
      Configuration hadoopConf, int numRecordsPerBatch, SparkSession sparkSession) {
    Configuration configuration = new Configuration(lazyBaseConf());

    String tombstoneColumnName = options.get(TombstoneExtension.TOMBSTONE_COLUMN).orElse("");
    if (!tombstoneColumnName.isEmpty()) {
      Types.NestedField tombstoneField = table.schema().caseInsensitiveFindField(tombstoneColumnName);
      Preconditions.checkArgument(tombstoneField != null, "Invalid tombstone column: %s", tombstoneColumnName);
      LOG.debug(
          "Use extended vectorized reader based on matching tombstone field={} for table={}",
          tombstoneField.name(),
          table.location());
      return new ExtendedVectorizedReader(getTableAndResolveHadoopConfiguration(options, configuration),
          caseSensitive, options, configuration, numRecordsPerBatch, sparkSession, tombstoneField, tombstoneColumnName);
    }
    LOG.debug("Fallback to vectorized Iceberg reader, missing or empty tombstone option={} for table={}",
        TombstoneExtension.TOMBSTONE_COLUMN, table.location());
    return super.vectorizedReader(table, caseSensitive, options, hadoopConf, numRecordsPerBatch, sparkSession);
  }

  @Override
  public Optional<DataSourceWriter> createWriter(
      String jobId, StructType dsStruct, SaveMode mode,
      DataSourceOptions options) {
    Preconditions.checkArgument(mode == SaveMode.Append || mode == SaveMode.Overwrite,
        "Save mode %s is not supported", mode);

    Configuration conf = new Configuration(lazyBaseConf());
    ExtendedTable table = getTableAndResolveHadoopConfiguration(options, conf);
    Schema dsSchema = SparkSchemaUtil.convert(table.schema(), dsStruct);
    validateWriteSchema(table.schema(), dsSchema, checkNullability(options));
    validatePartitionTransforms(table.spec());
    String appId = lazySparkSession().sparkContext().applicationId();
    String wapId = options.get("wap-id").orElse(lazySparkSession().conf().get("spark.wap.id", null));
    return Optional.of(new ExtendedWriter(table, options, mode == SaveMode.Overwrite, appId, wapId, dsSchema));
  }

  private ExtendedTable getTableAndResolveHadoopConfiguration(
      DataSourceOptions options, Configuration conf) {
    // Overwrite configurations from the Spark Context with configurations from the options.
    mergeIcebergHadoopConfs(conf, options.asMap());
    Optional<String> location = options.get("path");
    Preconditions.checkArgument(
        location.isPresent(), "Cannot open table without a location: path option is not set");
    return new HadoopExtendedTables(conf).loadWithTombstoneExtension(location.get());
  }
}
