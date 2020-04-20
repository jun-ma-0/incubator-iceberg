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
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.SnapshotUpdate;

/**
 * This holds the default logic for how we're supposed to preserve tombstone changes across new
 * snapshots to be applied for all supported vacuum operations regardless of their implementation
 * details wrt to files.
 */
public interface VacuumSnapshotUpdate<T> extends SnapshotUpdate<T> {

  default Optional<String> applyTombstoneChanges(List<String> toBeRemoved,
      TombstoneExtension tombstoneExtension,
      ExtendedTableOperations ops,
      Namespace namespace) {

    if (toBeRemoved != null && !toBeRemoved.isEmpty()) {
      return Optional.of(tombstoneExtension
          .removeById(ops.current().currentSnapshot(), toBeRemoved, namespace).location());
    } else {
      // If there are no tombstones to be removed then reference tombstone file of current snapshot
      return tombstoneExtension.copyReference(ops.current().currentSnapshot());
    }
  }
}
