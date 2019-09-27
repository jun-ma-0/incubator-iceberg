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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SystemProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.Statistics;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.v2.reader.SupportsReportStatistics;
import org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;

class V1VectorizedReader implements SupportsScanColumnarBatch,
    DataSourceReader,
    SupportsPushDownFilters,
    SupportsPushDownRequiredColumns,
    SupportsReportStatistics {
  private static final Logger LOG = LoggerFactory.getLogger(Reader.class);

  private static final Filter[] NO_FILTERS = new Filter[0];

  private final Table table;
  private final Long snapshotId;
  private final Long asOfTimestamp;
  private final Long splitSize;
  private final Integer splitLookback;
  private final Long splitOpenFileCost;
  private final FileIO fileIo;
  private final EncryptionManager encryptionManager;
  private final boolean caseSensitive;
  private final int numRecordsPerBatch;
  private final Configuration hadoopConf;
  private final SparkSession sparkSession;
  // default as per SQLConf.PARQUET_VECTORIZED_READER_BATCH_SIZE default
  public static final int DEFAULT_NUM_ROWS_IN_BATCH = 4096;
  private DataSourceOptions options;

  private StructType requestedSchema = null;
  private List<Expression> filterExpressions = null;
  private Filter[] pushedFilters = NO_FILTERS;

  // lazy variables
  private Schema schema = null;
  private StructType type = null; // cached because Spark accesses it multiple times
  private List<CombinedScanTask> tasks = null; // lazy cache of tasks

  V1VectorizedReader(Table table, boolean caseSensitive, DataSourceOptions options,
      Configuration hadoopConf, int numRecordsPerBatch, SparkSession sparkSession) {

    this.table = table;
    this.snapshotId = options.get("snapshot-id").map(Long::parseLong).orElse(null);
    this.asOfTimestamp = options.get("as-of-timestamp").map(Long::parseLong).orElse(null);
    if (snapshotId != null && asOfTimestamp != null) {
      throw new IllegalArgumentException(
          "Cannot scan using both snapshot-id and as-of-timestamp to select the table snapshot");
    }

    this.numRecordsPerBatch = numRecordsPerBatch;


    this.splitSize = options.get("split-size").map(Long::parseLong).orElse(null);
    this.splitLookback = options.get("lookback").map(Integer::parseInt).orElse(null);
    this.splitOpenFileCost = options.get("file-open-cost").map(Long::parseLong).orElse(null);

    this.schema = table.schema();
    this.fileIo = table.io();
    this.encryptionManager = table.encryption();
    this.caseSensitive = caseSensitive;
    this.hadoopConf = hadoopConf;
    this.sparkSession = sparkSession;
    this.options = options;

    LOG.debug("=> Set Config numRecordsPerBatch: {}, " +
            "Split size: {}, " +
            "Planning Thread count: {}, " +
            "Max Num Vectorized Fields: {}",
        numRecordsPerBatch,
        splitSize,
        System.getProperty(SystemProperties.WORKER_THREAD_POOL_SIZE_PROP),
        Integer.valueOf(sparkSession.conf().get("spark.sql.codegen.maxFields", "100")));
  }

  private Schema lazySchema() {
    if (schema == null) {
      if (requestedSchema != null) {
        this.schema = SparkSchemaUtil.prune(table.schema(), requestedSchema);
      } else {
        this.schema = table.schema();
      }
    }
    return schema;
  }

  private StructType lazyType() {
    if (type == null) {
      this.type = SparkSchemaUtil.convert(lazySchema());
    }
    return type;
  }

  @Override
  public StructType readSchema() {
    return lazyType();
  }

  @Override
  public List<InputPartition<ColumnarBatch>> planBatchInputPartitions() {

    long start = System.currentTimeMillis();

    String tableSchemaString = SchemaParser.toJson(table.schema());
    String expectedSchemaString = SchemaParser.toJson(lazySchema());

    //
    // pre-process static parameters here, once
    //

    // prepare filters
    Filter[] processedFilters = pushFilters(pushedFilters);
    // prepare filter seq
    scala.collection.mutable.ArrayBuffer<Filter> filtersAsArrayBuf =  new ArrayBuffer(processedFilters.length);
    for (Filter f : processedFilters) {
      filtersAsArrayBuf.$plus$eq(f);
    }
    Seq<Filter> filterAsSeq = filtersAsArrayBuf.toSeq();
    // prepare hadoopconf
    hadoopConf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED().key(), "true");
    hadoopConf.set(SQLConf.PARQUET_VECTORIZED_READER_BATCH_SIZE().key(),
        Integer.toString(this.numRecordsPerBatch));
    hadoopConf.set(SQLConf.WHOLESTAGE_MAX_NUM_FIELDS().key(),
        sparkSession.conf().get("spark.sql.codegen.maxFields", "100"));
    sparkSession.sessionState().conf().setConfString(SQLConf.PARQUET_VECTORIZED_READER_ENABLED().key(), "true");
    sparkSession.sessionState().conf().setConfString(SQLConf.PARQUET_VECTORIZED_READER_BATCH_SIZE().key(),
        Integer.toString(this.numRecordsPerBatch));
    sparkSession.sessionState().conf().setConfString(SQLConf.WHOLESTAGE_MAX_NUM_FIELDS().key(),
        sparkSession.conf().get("spark.sql.codegen.maxFields", "100"));

    // prepare sparkReadSchema
    StructType sparkReadSchema = SparkSchemaUtil.convert(lazySchema());
    // Build function for V1 Partition Reader which is passed over from Driver to Executors
    ParquetFileFormat fileFormatInstance = new ParquetFileFormat();
    scala.Function1<PartitionedFile, scala.collection.Iterator<InternalRow>> partitionFunction =
        fileFormatInstance.buildReaderWithPartitionValues(sparkSession,
          sparkReadSchema,
          new StructType(),
          sparkReadSchema,
          filterAsSeq, // List$.MODULE$.empty(),
          null, hadoopConf);

    List<InputPartition<ColumnarBatch>> readTasks = Lists.newArrayList();
    for (CombinedScanTask task : tasks()) {
      long readTaskStart = System.currentTimeMillis();
      readTasks.add(
          new ReadTask(task, tableSchemaString, expectedSchemaString, fileIo, encryptionManager, caseSensitive,
              partitionFunction));
      LOG.debug("=> ReadTask creating time took {} ms.", System.currentTimeMillis() - readTaskStart);
    }
    LOG.debug("=> Input Task planning took {} seconds.", (System.currentTimeMillis() - start) / 1000.0f);
    LOG.debug("=> Spark Schema: {}", sparkReadSchema);

    return readTasks;
  }

  @Override
  public List<InputPartition<InternalRow>> planInputPartitions() {
    // if we are here, it means we cannot do vectorized reads
    // so just use the regular Reader.
    Reader reader = new Reader(table, caseSensitive, options);
    reader.pushFilters(pushedFilters);
    reader.pruneColumns(requestedSchema);

    return reader.planInputPartitions();
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    this.tasks = null; // invalidate cached tasks, if present

    List<Expression> expressions = Lists.newArrayListWithExpectedSize(filters.length);
    List<Filter> pushed = Lists.newArrayListWithExpectedSize(filters.length);

    for (Filter filter : filters) {
      Expression expr = SparkFilters.convert(filter);
      if (expr != null) {
        expressions.add(expr);
        pushed.add(filter);
      }
    }

    this.filterExpressions = expressions;
    this.pushedFilters = pushed.toArray(new Filter[0]);

    // invalidate the schema that will be projected
    this.schema = null;
    this.type = null;

    // Spark doesn't support residuals per task, so return all filters
    // to get Spark to handle record-level filtering
    return filters;
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public void pruneColumns(StructType newRequestedSchema) {
    this.requestedSchema = newRequestedSchema;

    LOG.debug("=> Prune columns : {}", newRequestedSchema.prettyJson());
    // invalidate the schema that will be projected
    this.schema = null;
    this.type = null;
  }

  @Override
  public boolean enableBatchRead() {
    boolean isRunningOnDatabricks =
        sparkSession.conf().get("spark.databricks.clusterUsageTags.clusterId", null) != null;
    int maxFields = Integer.valueOf(sparkSession.conf().get("spark.sql.codegen.maxFields", "100"));

    if (isRunningOnDatabricks) {
      // databricks runtime can do vectorized reads on complex types
      return numOfNestedFields(lazySchema().asStruct()) <= maxFields;
    } else {
      // any other runtime, including vanilla Spark, can only do vectorized reads on primitive types
      boolean areAllColumnsPrimitive = lazySchema().columns().stream().allMatch(c -> c.type().isPrimitiveType());
      return areAllColumnsPrimitive && numOfNestedFields(lazySchema().asStruct()) <= maxFields;
    }
  }

  // based out of org.apache.spark.sql.execution.WholeStageCodegenExec#numOfNestedFields
  private int numOfNestedFields(Type dataType) {
    if (dataType instanceof Types.StructType) {
      Types.StructType st = (Types.StructType) dataType;
      return st.fields().stream().map(f -> numOfNestedFields(f.type())).reduce(0, Integer::sum);
    } else if (dataType instanceof Types.MapType) {
      Types.MapType mt = (Types.MapType) dataType;
      return numOfNestedFields(mt.keyType()) + numOfNestedFields(mt.valueType());
    } else if (dataType instanceof Types.ListType) {
      Types.ListType lt = (Types.ListType) dataType;
      return numOfNestedFields(lt.elementType());
    } else {
      return 1;
    }
  }

  @Override
  public Statistics estimateStatistics() {
    long sizeInBytes = 0L;
    long numRows = 0L;

    for (CombinedScanTask task : tasks()) {
      for (FileScanTask file : task.files()) {
        sizeInBytes += file.length();
        numRows += file.file().recordCount();
      }
    }

    return new Stats(sizeInBytes, numRows);
  }

  private List<CombinedScanTask> tasks() {
    if (tasks == null) {
      TableScan scan = table
          .newScan()
          .caseSensitive(caseSensitive)
          .project(lazySchema());

      if (snapshotId != null) {
        scan = scan.useSnapshot(snapshotId);
      }

      if (asOfTimestamp != null) {
        scan = scan.asOfTime(asOfTimestamp);
      }

      if (splitSize != null) {
        scan = scan.option(TableProperties.SPLIT_SIZE, splitSize.toString());
      }

      if (splitLookback != null) {
        scan = scan.option(TableProperties.SPLIT_LOOKBACK, splitLookback.toString());
      }

      if (splitOpenFileCost != null) {
        scan = scan.option(TableProperties.SPLIT_OPEN_FILE_COST, splitOpenFileCost.toString());
      }

      if (filterExpressions != null) {
        for (Expression filter : filterExpressions) {
          scan = scan.filter(filter);
        }
      }

      try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
        this.tasks = Lists.newArrayList(tasksIterable);
      }  catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to close table scan: %s", scan);
      }
    }

    return tasks;
  }

  @Override
  public String toString() {
    return String.format(
        "V1VectorizedIcebergScan(numPerBatch=%s, table=%s, type=%s, filters=%s, caseSensitive=%s)",
        numRecordsPerBatch, table, lazySchema().asStruct(), filterExpressions, caseSensitive);
  }

  private static class ReadTask implements InputPartition<ColumnarBatch>, Serializable {
    private final CombinedScanTask task;
    private final String tableSchemaString;
    private final String expectedSchemaString;
    private final FileIO fileIo;
    private final EncryptionManager encryptionManager;
    private final boolean caseSensitive;
    private final scala.Function1<PartitionedFile, scala.collection.Iterator<InternalRow>> buildReaderFunc;

    private transient Schema tableSchema = null;
    private transient Schema expectedSchema = null;

    private ReadTask(
        CombinedScanTask task, String tableSchemaString, String expectedSchemaString, FileIO fileIo,
        EncryptionManager encryptionManager, boolean caseSensitive,
        scala.Function1<PartitionedFile, scala.collection.Iterator<InternalRow>> partitionFunction) {
      this.task = task;
      this.tableSchemaString = tableSchemaString;
      this.expectedSchemaString = expectedSchemaString;
      this.fileIo = fileIo;
      this.encryptionManager = encryptionManager;
      this.caseSensitive = caseSensitive;

      LOG.debug("=> Build partition reader function ");
      this.buildReaderFunc = partitionFunction;
    }

    @Override
    public InputPartitionReader<ColumnarBatch> createPartitionReader() {

      LOG.debug("=> Create Partition Reader");
      return new V1VectorizedTaskDataReader(task, lazyTableSchema(), lazyExpectedSchema(), fileIo,
            encryptionManager, caseSensitive, buildReaderFunc);
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
