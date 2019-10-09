package com.adobe.platform.iceberg.extensions;

import com.adobe.platform.iceberg.extensions.tombstone.Entry;
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
  private List<Entry> toBeRemoved = null;
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
  public Vacuum tombstones(Namespace namespace, String columnName, List<Entry> entries,
      Long readSnapshotId) {
    Preconditions.checkArgument(readSnapshotId > 0L, "Invalid read snapshot id, expected greater than 0 (zero)");

    this.namespace = namespace;
    this.toBeRemoved = entries;

    Optional<Expression> expression = TombstoneExpressions.matchesAny(columnName,
        entries.stream().map(Entry::getId).collect(Collectors.toList()));

    if (expression.isPresent()) {
      this.overwriteByRowFilter(expression.get());
      this.validateNoConflictingAppends(readSnapshotId, expression.get());
      return this;
    }

    throw new RuntimeIOException("Expected non-empty expression resulted from tombstone pruning");
  }
}
