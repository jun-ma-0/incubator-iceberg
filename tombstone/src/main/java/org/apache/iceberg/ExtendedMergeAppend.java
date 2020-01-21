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

package org.apache.iceberg;

import com.adobe.platform.iceberg.extensions.ExtendedAppendFiles;
import com.adobe.platform.iceberg.extensions.tombstone.Entry;
import com.adobe.platform.iceberg.extensions.tombstone.EvictEntry;
import com.adobe.platform.iceberg.extensions.tombstone.Namespace;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.io.OutputFile;

public class ExtendedMergeAppend extends MergeAppend implements ExtendedAppendFiles {
  private TableOperations ops;
  private TombstoneExtension tombstoneExtension;

  private Namespace namespace;
  private List<EvictEntry> toBeAdded = null;
  private List<EvictEntry> toBeRemoved = null;
  private Map<String, String> toBeAddedProperties = new HashMap<>();

  public ExtendedMergeAppend(TableOperations ops, TombstoneExtension tombstoneExtension) {
    super(ops);
    this.ops = ops;
    this.tombstoneExtension = tombstoneExtension;
  }

  @Override
  @SuppressWarnings("checkstyle:HiddenField")
  public ExtendedAppendFiles appendTombstones(
      Namespace namespace, List<Entry> entries, Map<String, String> properties, long evictionTs) {
    this.namespace = namespace;
    this.toBeAdded =
        entries.stream()
            .map(e -> (EvictEntry) () -> new AbstractMap.SimpleEntry<>(e, evictionTs))
            .collect(Collectors.toList());
    this.toBeAddedProperties = properties;
    return this;
  }

  @Override
  @SuppressWarnings("checkstyle:HiddenField")
  public ExtendedAppendFiles removeTombstones(Namespace namespace, List<EvictEntry> entries) {
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
          this.toBeAdded, this.namespace, this.toBeAddedProperties, snapshotId());
      // Atomic guarantee - bind the tombstone avro output file location to the new snapshot summary property
      // Iceberg will do an atomic commit of the snapshot w/ both the data files and the tombstone file or neither
      this.set(TombstoneExtension.SNAPSHOT_TOMBSTONE_FILE_PROPERTY, outputFile.location());
    } else if (this.toBeRemoved != null) {
      OutputFile outputFile = tombstoneExtension.remove(ops.current().currentSnapshot(),
          this.toBeRemoved,
          namespace);
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
