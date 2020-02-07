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

import com.adobe.platform.iceberg.extensions.ExtendedTableOperations;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import java.util.List;
import java.util.Optional;

public class CherryPickFromTombstoneSnapshot extends CherryPickFromSnapshot {

  private TombstoneExtension tombstoneExtension;

  public CherryPickFromTombstoneSnapshot(ExtendedTableOperations ops, TombstoneExtension tombstoneExtension) {
    super(ops);
    this.tombstoneExtension = tombstoneExtension;
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base) {
    List<ManifestFile> apply = super.apply(base);

    // Handle tombstones merge
    // Note: We should use `base` instead of `getBase()` to get current snapshot. Because there is a refresh happening
    //      in SnapshotProducer.apply(), which may cause getBase() referring to a staled table. `base` is the latest
    //      version that is used to generate new snapshot.
    Optional<String> outputFilePath = tombstoneExtension.merge(
        base.snapshot(getCherryPickSnapshotId()),
        base.currentSnapshot());

    // Atomic guarantee - bind the tombstone avro output file location to the new snapshot summary property
    // Iceberg will do an atomic commit of the snapshot w/ both the data files and the tombstone file or neither
    outputFilePath.map(path -> set(TombstoneExtension.SNAPSHOT_TOMBSTONE_FILE_PROPERTY, path));

    return apply;
  }
}
