package org.apache.iceberg.spark.source;

import com.adobe.platform.iceberg.extensions.ExtendedTable;
import com.adobe.platform.iceberg.extensions.tombstone.ExtendedEntry;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExpressions;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

public class ExtendedVectorizedReader extends V1VectorizedReader {

  private ExtendedTable table;
  private Types.NestedField tombstoneField;
  // We preserve the dotted notation field name since org.apache.iceberg.types.Types.NestedField does not provide
  // full precedence of field using dot notation so we will fail all SQL queries
  private String tombstoneFieldName;

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
    this.tombstoneField = tombstoneField;
    this.table = table;
    this.tombstoneFieldName = tombstoneFieldName;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    List<ExtendedEntry> tombstones = table.getSnapshotTombstones(tombstoneField, table.currentSnapshot());
    TombstoneExpressions.notIn(tombstoneFieldName, tombstones).ifPresent(this::addFilter);
    return super.pushFilters(filters);
  }
}
