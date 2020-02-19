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

package com.adobe.platform.iceberg.extensions.tombstone;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;

public class TombstoneExpressions implements Serializable {

  /** Utility classes should not have a public or default constructor. [HideUtilityClassConstructor] **/
  private TombstoneExpressions() {}

  /**
   * Returns the expression used to filter out rows having tombstoned value for the provided field.
   * For any variable values of preset `tombstones` we can write an equivalent expression to support
   * the missing support for NOT_IN filtering, i.e. given tombstone values [`1`, `2`, `3`, ...] we
   * must build a logically equivalent expression and(not("col","1"), not("col","2"),
   * not("col","3"), ...)
   *
   * @param fieldName tombstone column - we preserve the dotted notation field name since
   * org.apache.iceberg.types.Types.NestedField does not provide full precedence of field using dot
   * notation so we will fail all SQL queries
   * @param tombstones tombstone values
   * @return an {@link Optional < Expression >} an optional expression should tombstone filter by
   * applied
   */
  public static <T extends ExtendedEntry> Optional<Expression> notIn(
      String fieldName,
      List<T> tombstones) {
    // TODO - revisit after merge of https://github.com/apache/incubator-iceberg/pull/357/files
    if (!tombstones.isEmpty()) {
      List<UnboundPredicate<String>> registry =
          tombstones.stream()
              .map(t -> Expressions.notEqual(fieldName, t.getEntry().getId()))
              .collect(Collectors.toList());
      if (tombstones.size() == 1) {
        return Optional.of(Expressions.and(Expressions.notNull(fieldName), registry.get(0)));
      } else if (tombstones.size() > 1) {
        Expression innerExpression = registry.get(0);
        for (UnboundPredicate<String> stringUnboundPredicate : registry.subList(1, registry.size())) {
          innerExpression = Expressions.and(innerExpression, stringUnboundPredicate);
        }
        return Optional.of(innerExpression);
      }
    }
    return Optional.empty();
  }

  public static <T> Optional<Expression> matchesAny(String field, List<T> entries) {
    // TODO - revisit after merge of https://github.com/apache/incubator-iceberg/pull/357/files
    if (!entries.isEmpty()) {
      List<UnboundPredicate<T>> registry =
          entries.stream()
              .map(t -> Expressions.equal(field, t))
              .collect(Collectors.toList());
      if (entries.size() > 0) {
        return Optional.ofNullable(or(registry));
      }
    }
    return Optional.empty();
  }

  private static <T> Expression or(List<UnboundPredicate<T>> registry) {
    if (registry.size() == 1) {
      return registry.get(0);
    } else if (registry.size() > 1) {
      Expression innerExpression = registry.get(0);
      for (UnboundPredicate<T> stringUnboundPredicate : registry.subList(1, registry.size())) {
        innerExpression = Expressions.or(innerExpression, stringUnboundPredicate);
      }
      return innerExpression;
    }
    return null;
  }
}
