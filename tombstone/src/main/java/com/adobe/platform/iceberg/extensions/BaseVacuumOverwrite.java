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

import com.adobe.platform.iceberg.extensions.tombstone.Namespace;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExpressions;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.BaseOverwriteFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated in favour of com.adobe.platform.iceberg.extensions.BaseVacuumRewrite
 */
@Deprecated
class BaseVacuumOverwrite extends BaseOverwriteFiles implements VacuumOverwrite, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(BaseVacuumOverwrite.class);
  private final ExtendedTableOperations ops;
  private final TombstoneExtension tombstoneExtension;
  private Namespace namespace;
  private List<String> toBeRemoved = null;

  BaseVacuumOverwrite(ExtendedTableOperations ops, TombstoneExtension tombstoneExtension) {
    super(ops);
    this.ops = ops;
    this.tombstoneExtension = tombstoneExtension;
    // Requires the file overwrite API to resolve files strictly on the provided expression
    this.setStrictExpressionEvaluation();
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base) {
    applyTombstoneChanges(toBeRemoved, tombstoneExtension, ops, namespace)
        .map(f -> this.set(TombstoneExtension.SNAPSHOT_TOMBSTONE_FILE_PROPERTY, f));
    return super.apply(base);
  }

  /**
   * @param namespace full column name used tombstone
   * @param columnName full column name used tombstone
   * @param entries tombstone values
   * @param readSnapshotId the snapshot id that was used to read the data
   */
  @SuppressWarnings("checkstyle:HiddenField")
  public VacuumOverwrite tombstones(
      Namespace namespace,
      String columnName,
      List<String> entries,
      Long readSnapshotId) {

    Preconditions.checkArgument(readSnapshotId > 0L,
        "Invalid read snapshot id, expected greater than 0 (zero)");

    this.namespace = namespace;
    this.toBeRemoved = entries;

    Optional<Expression> expression = TombstoneExpressions.matchesAny(columnName, entries);

    if (expression.isPresent()) {
      this.overwriteByRowFilter(expression.get());
      this.validateNoConflictingAppends(readSnapshotId, expression.get());
      LOG.info("Vacuum tombstones={} on snapshotId={} for namespace={} and columnName={}",
          toBeRemoved.size(),
          readSnapshotId, namespace, columnName);
      return this;
    }

    throw new RuntimeIOException("Expected non-empty result for tombstone match any expression");
  }
}
