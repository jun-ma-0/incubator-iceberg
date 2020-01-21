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

import com.adobe.platform.iceberg.extensions.tombstone.EvictEntry;
import com.adobe.platform.iceberg.extensions.tombstone.Namespace;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExpressions;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseOverwriteFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.OutputFile;

class BaseVacuum extends BaseOverwriteFiles implements Vacuum, Serializable {

  private Namespace namespace;
  private List<EvictEntry> toBeRemoved = null;
  private ExtendedTableOperations ops;
  private TombstoneExtension tombstoneExtension;

  BaseVacuum(ExtendedTableOperations ops, TombstoneExtension tombstoneExtension) {
    super(ops);
    this.ops = ops;
    this.tombstoneExtension = tombstoneExtension;
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base) {
    if (toBeRemoved != null && !toBeRemoved.isEmpty()) {
      OutputFile outputFile = tombstoneExtension
          .remove(ops.current().currentSnapshot(), toBeRemoved, namespace);
      // Atomic guarantee - bind the tombstone avro output file location to the new snapshot summary property
      // Iceberg will do an atomic commit of the snapshot w/ both the data files and the tombstone file or neither
      this.set(TombstoneExtension.SNAPSHOT_TOMBSTONE_FILE_PROPERTY, outputFile.location());
    } else {
      // Assuming there are no tombstones to remove we fallback to referencing the tombstone file for new snapshot
      tombstoneExtension.copyReference(ops.current().currentSnapshot())
          // Atomic guarantee - bind the tombstone avro output file location to the new snapshot summary property
          // Iceberg will do an atomic commit of the snapshot w/ both the data files and the tombstone file or neither
          .map(f -> this.set(TombstoneExtension.SNAPSHOT_TOMBSTONE_FILE_PROPERTY, f));
    }

    return super.apply(base);
  }

  /**
   * @param namespace full column name used tombstone
   * @param columnName full column name used tombstone
   * @param entries tombstone values
   * @param readSnapshotId the snapshot id that was used to read the data
   */
  @SuppressWarnings("checkstyle:HiddenField")
  public Vacuum tombstones(Namespace namespace, String columnName, List<EvictEntry> entries, Long readSnapshotId) {
    Preconditions.checkArgument(readSnapshotId > 0L, "Invalid read snapshot id, expected greater than 0 (zero)");

    this.namespace = namespace;
    this.toBeRemoved = entries;

    Optional<Expression> expression = TombstoneExpressions.matchesAny(
        columnName,
        entries.stream().map(e -> e.get().getKey().getId()).collect(Collectors.toList()));

    if (expression.isPresent()) {
      this.overwriteByRowFilter(expression.get());
      this.validateNoConflictingAppends(readSnapshotId, expression.get());
      return this;
    }

    throw new RuntimeIOException("Expected non-empty expression resulted from tombstone pruning");
  }
}
