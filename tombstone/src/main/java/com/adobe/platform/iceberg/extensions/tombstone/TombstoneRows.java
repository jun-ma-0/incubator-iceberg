package com.adobe.platform.iceberg.extensions.tombstone;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TombstoneRows {

  private String tombstoneFieldName;
  private List<String> tombstones;

  public TombstoneRows(String tombstoneFieldName, List<String> tombstones) {
    this.tombstoneFieldName = tombstoneFieldName;
    this.tombstones = tombstones;
  }

  public Iterator<InternalRow> filter(Schema schema, Iterator<InternalRow> iter) {
    String[] split = tombstoneFieldName.split("\\.");

    List<Integer> fieldNameIndices = deepFieldIndexLookup(SparkSchemaUtil.convert(schema),
        Lists.newArrayList(split), Lists.newArrayList());

    Predicate<InternalRow> anyTombstoneMatch = internalRow -> {
      String s = deepFindRow(internalRow, fieldNameIndices);
      return tombstones.stream().anyMatch(e -> s != null && s.equalsIgnoreCase(e));
    };

    return Iterators.filter(iter, Predicates.not(anyTombstoneMatch));
  }

  /**
   * Walks the internal row recursively to return a match (if exists) for the given list of indices
   *
   * @param internalRow the internal row to look up the index
   * @param indices list of indices used for look up of nested fields inside the nested internal
   * row
   */
  private String deepFindRow(InternalRow internalRow, List<Integer> indices) {
    if (indices == null || indices.isEmpty()) {
      return null;
    }
    if (indices.size() == 1) {
      return internalRow.getUTF8String(indices.get(0)).toString();
    }
    InternalRow struct = internalRow.getStruct(indices.get(0), indices.get(1));
    return deepFindRow(struct, indices.subList(1, indices.size()));
  }

  /**
   * Walks the schema recursively to return the ordered sequence of field indices that are matching
   * the provided field name tokens. Each time the method finds a matching field name part (token)
   * in the provided structType it will recursively continue to look for the next field name in the
   * matching field's data type.
   *
   * @param schema schema to lookup the corresponding matching token
   * @param tokens list of nested field parts (split by dot character)
   * @param collect the collected (returned) ordered sequence of matching field indices
   */
  private List<Integer> deepFieldIndexLookup(StructType schema, List<String> tokens,
      List<Integer> collect) {
    if (tokens == null || tokens.isEmpty()) {
      return collect;
    }
    if (tokens.size() == 1) {
      try {
        int i = schema.fieldIndex(tokens.get(0));
        collect.add(i);
        return collect;
      } catch (IllegalArgumentException e) {
        return Collections.emptyList();
      }
    } else {
      try {
        int i = schema.fieldIndex(tokens.get(0));
        StructField nestedField = schema.fields()[i];
        if (nestedField.dataType() instanceof StructType) {
          collect.add(i);
          return deepFieldIndexLookup((StructType) nestedField.dataType(),
              tokens.subList(1, tokens.size()), collect);
        }
      } catch (IllegalArgumentException e) {
        return Collections.emptyList();
      }
    }
    return collect;
  }
}
