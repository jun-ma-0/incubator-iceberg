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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveCatalogs;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.UnknownTransform;
import org.apache.iceberg.types.CheckCompatibility;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.streaming.StreamExecution;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.StreamWriteSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSource implements DataSourceV2, ReadSupport, WriteSupport, DataSourceRegister, StreamWriteSupport {

  public static final String ICEBERG_READ_ENABLE_V1_VECTORIZATION_CONF = "iceberg.read.enableV1VectorizedReader";
  public static final String ICEBERG_READ_NUM_RECORDS_BATCH_CONF = "iceberg.read.numrecordsperbatch";

  private SparkSession lazySpark = null;
  private Configuration lazyConf = null;

  private static final Logger LOG = LoggerFactory.getLogger(IcebergSource.class);

  @Override
  public String shortName() {
    return "iceberg";
  }

  @Override
  public DataSourceReader createReader(DataSourceOptions options) {
    return createReader(null, options);
  }

  @Override
  public DataSourceReader createReader(StructType readSchema, DataSourceOptions options) {
    Configuration conf = new Configuration(lazyBaseConf());
    Table table = getTableAndResolveHadoopConfiguration(options, conf);
    String caseSensitive = lazySparkSession().conf().get("spark.sql.caseSensitive", "true");

    // look for split behavior overrides in options
    Optional<String> enableV1VectorizedReadOpt = options.get(ICEBERG_READ_ENABLE_V1_VECTORIZATION_CONF);
    Optional<String> numRecordsPerBatchOpt = options.get(ICEBERG_READ_NUM_RECORDS_BATCH_CONF);

    boolean enableV1VectorizedRead =
        enableV1VectorizedReadOpt.isPresent() && Boolean.parseBoolean(enableV1VectorizedReadOpt.get());

    int numRecordsPerBatch = numRecordsPerBatchOpt.isPresent() ?
        Integer.parseInt(numRecordsPerBatchOpt.get()) : V1VectorizedReader.DEFAULT_NUM_ROWS_IN_BATCH;
    if (enableV1VectorizedRead) {
      LOG.debug("V1VectorizedReader engaged.");
      V1VectorizedReader reader = (V1VectorizedReader) vectorizedReader(table, Boolean.valueOf(caseSensitive), options,
          conf, numRecordsPerBatch, lazySparkSession());
      if (readSchema != null) {
        // convert() will fail if readSchema contains fields not in table.schema()
        SparkSchemaUtil.convert(table.schema(), readSchema);
        reader.pruneColumns(readSchema);
      }
      return reader;
    } else {
      LOG.debug("Non-VectorizedReader engaged.");
      Reader reader = (Reader) reader(table, Boolean.valueOf(caseSensitive), options);
      if (readSchema != null) {
        // convert() will fail if readSchema contains fields not in table.schema()
        SparkSchemaUtil.convert(table.schema(), readSchema);
        reader.pruneColumns(readSchema);
      }
      return reader;
    }
  }

  // PLAT-41559 - override from {@link com.adobe.platform.iceberg.extensions.ExtendedIcebergSource}
  protected DataSourceReader reader(Table table, boolean caseSensitive, DataSourceOptions options) {
    return new Reader(table, Boolean.valueOf(caseSensitive), options);
  }

  // PLAT-41559 - override from {@link com.adobe.platform.iceberg.extensions.ExtendedIcebergSource}
  protected DataSourceReader vectorizedReader(
      Table table, boolean caseSensitive, DataSourceOptions options,
      Configuration hadoopConf, int numRecordsPerBatch, SparkSession sparkSession) {
    LOG.debug("V1VectorizedReader engaged.");
    return new V1VectorizedReader(table, caseSensitive, options, hadoopConf, numRecordsPerBatch, sparkSession);
  }

  @Override
  public Optional<DataSourceWriter> createWriter(String jobId, StructType dsStruct, SaveMode mode,
                                                 DataSourceOptions options) {
    Preconditions.checkArgument(mode == SaveMode.Append || mode == SaveMode.Overwrite,
        "Save mode %s is not supported", mode);
    Configuration conf = new Configuration(lazyBaseConf());
    Table table = getTableAndResolveHadoopConfiguration(options, conf);
    Schema dsSchema = SparkSchemaUtil.convert(table.schema(), dsStruct);
    validateWriteSchema(table.schema(), dsSchema, checkNullability(options));
    validatePartitionTransforms(table.spec());
    String appId = lazySparkSession().sparkContext().applicationId();
    String wapId = options.get("wap-id").orElse(lazySparkSession().conf().get("spark.wap.id", null));
    return Optional.of(new Writer(table, options, mode == SaveMode.Overwrite, appId, wapId, dsSchema));
  }

  @Override
  public StreamWriter createStreamWriter(String runId, StructType dsStruct,
                                         OutputMode mode, DataSourceOptions options) {
    Preconditions.checkArgument(
        mode == OutputMode.Append() || mode == OutputMode.Complete(),
        "Output mode %s is not supported", mode);
    Configuration conf = new Configuration(lazyBaseConf());
    Table table = getTableAndResolveHadoopConfiguration(options, conf);
    Schema dsSchema = SparkSchemaUtil.convert(table.schema(), dsStruct);
    validateWriteSchema(table.schema(), dsSchema, checkNullability(options));
    validatePartitionTransforms(table.spec());
    // Spark 2.4.x passes runId to createStreamWriter instead of real queryId,
    // so we fetch it directly from sparkContext to make writes idempotent
    String queryId = lazySparkSession().sparkContext().getLocalProperty(StreamExecution.QUERY_ID_KEY());
    String appId = lazySparkSession().sparkContext().applicationId();
    return new StreamingWriter(table, options, queryId, mode, appId, dsSchema);
  }

  protected Table findTable(DataSourceOptions options, Configuration conf) {
    Optional<String> path = options.get("path");
    Preconditions.checkArgument(path.isPresent(), "Cannot open table: path is not set");

    if (path.get().contains("/")) {
      HadoopTables tables = new HadoopTables(conf);
      return tables.load(path.get());
    } else {
      HiveCatalog hiveCatalog = HiveCatalogs.loadCatalog(conf);
      TableIdentifier tableIdentifier = TableIdentifier.parse(path.get());
      return hiveCatalog.loadTable(tableIdentifier);
    }
  }

  protected SparkSession lazySparkSession() {
    if (lazySpark == null) {
      this.lazySpark = SparkSession.builder().getOrCreate();
    }
    return lazySpark;
  }

  protected Configuration lazyBaseConf() {
    if (lazyConf == null) {
      this.lazyConf = lazySparkSession().sessionState().newHadoopConf();
    }
    return lazyConf;
  }

  private Table getTableAndResolveHadoopConfiguration(
      DataSourceOptions options, Configuration conf) {
    // Overwrite configurations from the Spark Context with configurations from the options.
    mergeIcebergHadoopConfs(conf, options.asMap());
    Table table = findTable(options, conf);
    // Set confs from table properties
    mergeIcebergHadoopConfs(conf, table.properties());
    // Re-overwrite values set in options and table properties but were not in the environment.
    mergeIcebergHadoopConfs(conf, options.asMap());
    return table;
  }

  // PLAT-41559 - available for accessing from {@link com.adobe.platform.iceberg.extensions.ExtendedIcebergSource}
  protected static void mergeIcebergHadoopConfs(
      Configuration baseConf, Map<String, String> options) {
    options.keySet().stream()
        .filter(key -> key.startsWith("hadoop."))
        .forEach(key -> baseConf.set(key.replaceFirst("hadoop.", ""), options.get(key)));
  }

  protected void validateWriteSchema(Schema tableSchema, Schema dsSchema, Boolean checkNullability) {
    List<String> errors;
    if (checkNullability) {
      errors = CheckCompatibility.writeCompatibilityErrors(tableSchema, dsSchema);
    } else {
      errors = CheckCompatibility.typeCompatibilityErrors(tableSchema, dsSchema);
    }
    if (!errors.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("Cannot write incompatible dataset to table with schema:\n")
          .append(tableSchema)
          .append("\nProblems:");
      for (String error : errors) {
        sb.append("\n* ").append(error);
      }
      throw new IllegalArgumentException(sb.toString());
    }
  }

  protected void validatePartitionTransforms(PartitionSpec spec) {
    if (spec.fields().stream().anyMatch(field -> field.transform() instanceof UnknownTransform)) {
      String unsupported = spec.fields().stream()
          .map(PartitionField::transform)
          .filter(transform -> transform instanceof UnknownTransform)
          .map(Transform::toString)
          .collect(Collectors.joining(", "));

      throw new UnsupportedOperationException(
          String.format("Cannot write using unsupported transforms: %s", unsupported));
    }
  }

  protected boolean checkNullability(DataSourceOptions options) {
    boolean sparkCheckNullability = Boolean.parseBoolean(lazySpark.conf()
        .get("spark.sql.iceberg.check-nullability", "true"));
    boolean dataFrameCheckNullability = options.getBoolean("check-nullability", true);
    return sparkCheckNullability && dataFrameCheckNullability;
  }
}
