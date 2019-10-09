package org.apache.iceberg.spark.source;

import com.adobe.platform.iceberg.extensions.ExtendedTable;
import com.adobe.platform.iceberg.extensions.tombstone.ExtendedEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

public class ExtendedReader extends Reader {
  private ExtendedTable table;
  private Types.NestedField tombstoneField;
  // We preserve the dotted notation field name since org.apache.iceberg.types.Types.NestedField does not provide
  // full precedence of field using dot notation so we will fail all SQL queries
  private String tombstoneFieldName;

  public ExtendedReader(
      ExtendedTable table,
      boolean caseSensitive,
      DataSourceOptions options,
      Types.NestedField tombstoneField,
      String tombstoneFieldName) {
    super(table, caseSensitive, options);
    this.tombstoneField = tombstoneField;
    this.table = table;
    this.tombstoneFieldName = tombstoneFieldName;
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
              .map(t -> Expressions.notEqual(tombstoneFieldName, t.getEntry().getId()))
              .collect(Collectors.toList());
      if (tombstones.size() >= 2) {
        filterExpressions.add(Expressions.and(
            Expressions.notNull(tombstoneFieldName),
            registry.subList(0, 1).get(0),
            registry.subList(1, registry.size()).toArray(new Expression[registry.size() - 1])));
      } else if (tombstones.size() == 1) {
        filterExpressions.add(Expressions.and(Expressions.notNull(tombstoneFieldName), registry.get(0)));
      }
      if (!filterExpressions.isEmpty()) {
        addFilters(filterExpressions);
      }
    }
    return super.pushFilters(filters);
  }
}
