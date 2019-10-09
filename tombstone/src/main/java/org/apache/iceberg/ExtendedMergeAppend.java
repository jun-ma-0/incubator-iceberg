package org.apache.iceberg;

import com.adobe.platform.iceberg.extensions.ExtendedAppendFiles;
import com.adobe.platform.iceberg.extensions.tombstone.Entry;
import com.adobe.platform.iceberg.extensions.tombstone.Namespace;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.OutputFile;

public class ExtendedMergeAppend extends MergeAppend implements ExtendedAppendFiles {
  private TableOperations ops;
  private TombstoneExtension tombstoneExtension;

  private Namespace namespace;
  private List<Entry> toBeAdded = null;
  private List<Entry> toBeRemoved = null;
  private Map<String, String> toBeAddedProperties = new HashMap<>();

  public ExtendedMergeAppend(TableOperations ops, TombstoneExtension tombstoneExtension) {
    super(ops);
    this.ops = ops;
    this.tombstoneExtension = tombstoneExtension;
  }

  @Override
  public ExtendedAppendFiles appendTombstones(
      Namespace namespace, List<Entry> entries, Map<String, String> properties) {
    this.namespace = namespace;
    this.toBeAdded = entries;
    this.toBeAddedProperties = properties;
    return this;
  }

  @Override
  public ExtendedAppendFiles removeTombstones(Namespace namespace, List<Entry> entries) {
    this.namespace = namespace;
    this.toBeRemoved = entries;
    return this;
  }

  @Override
  protected ExtendedAppendFiles self() {
    return this;
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base) {
    // This method is being evaluated on each Iceberg retry operation so any current snapshot relative operations
    // should be pushed down into this method so that upon commit exceptions which trigger internal retries we can run
    // the code against the refreshed current snapshot.
    if (this.toBeAdded != null) {
      OutputFile outputFile = tombstoneExtension.append(ops.current().currentSnapshot(),
          this.toBeAdded, this.namespace, this.toBeAddedProperties);
      // Atomic guarantee - bind the tombstone avro output file location to the new snapshot summary property
      // Iceberg will do an atomic commit of the snapshot w/ both the data files and the tombstone file or neither
      this.set(TombstoneExtension.SNAPSHOT_TOMBSTONE_FILE_PROPERTY, outputFile.location());
    } else if (this.toBeRemoved != null) {
      OutputFile outputFile = tombstoneExtension.remove(ops.current().currentSnapshot(), this.toBeRemoved, namespace);
      // Atomic guarantee - bind the tombstone avro output file location to the new snapshot summary property
      // Iceberg will do an atomic commit of the snapshot w/ both the data files and the tombstone file or neither
      this.set(TombstoneExtension.SNAPSHOT_TOMBSTONE_FILE_PROPERTY, outputFile.location());
    } else {
      // Assuming this is just a file append operation
      tombstoneExtension.copyReference(ops.current().currentSnapshot())
          // Atomic guarantee - bind the tombstone avro output file location to the new snapshot summary property
          // Iceberg will do an atomic commit of the snapshot w/ both the data files and the tombstone file or neither
          .map(f -> this.set(TombstoneExtension.SNAPSHOT_TOMBSTONE_FILE_PROPERTY, f));
    }
    return super.apply(base);
  }
}
