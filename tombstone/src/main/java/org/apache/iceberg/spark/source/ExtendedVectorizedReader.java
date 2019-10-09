package org.apache.iceberg.spark.source;

import com.adobe.platform.iceberg.extensions.ExtendedTable;
import com.adobe.platform.iceberg.extensions.tombstone.ExtendedEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

public class ExtendedVectorizedReader extends V1VectorizedReader {
  private ExtendedTable table;
  private Types.NestedField tombstoneField;

  public ExtendedVectorizedReader(
      ExtendedTable table,
      boolean caseSensitive,
      DataSourceOptions options,
      Configuration hadoopConf,
      int numRecordsPerBatch,
      SparkSession sparkSession,
      Types.NestedField tombstoneField) {
    super(table, caseSensitive, options, hadoopConf, numRecordsPerBatch, sparkSession);
    this.tombstoneField = tombstoneField;
    this.table = table;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    List<ExtendedEntry> tombstones = this.table.getCurrentSnapshotTombstones(tombstoneField);
    List<Expression> filterExpressions = new ArrayList<>();

    // For any variable values of preset `tombstones` we can write an equivalent expression to support the missing
    // support for NOT_IN filtering, i.e. given tombstone values [`1`, `2`, `3`, ...] we must build a logically
    // equivalent expression and(not("col","1"), not("col","2"), not("col","3"), ...)
    // TODO - revisit current implementation after merge of https://github.com/apache/incubator-iceberg/pull/357/files
    if (!tombstones.isEmpty()) {
      List<UnboundPredicate<String>> registry =
          tombstones.stream()
              .map(t -> Expressions.notEqual(tombstoneField.name(), t.getEntry().getId()))
              .collect(Collectors.toList());
      if (tombstones.size() > 2) {
        filterExpressions.add(Expressions.and(
            Expressions.notNull(tombstoneField.name()),
            registry.subList(0, 1).get(0),
            registry.subList(1, registry.size()).toArray(new Expression[registry.size() - 2])));
      } else if (tombstones.size() == 1) {
        filterExpressions.add(Expressions.and(Expressions.notNull(tombstoneField.name()), registry.get(0)));
      }
      if (!filterExpressions.isEmpty()) {
        addFilters(filterExpressions);
      }
    }

    return super.pushFilters(filters);
  }
}
