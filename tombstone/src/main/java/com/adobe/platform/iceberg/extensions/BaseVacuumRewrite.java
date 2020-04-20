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
import java.io.Serializable;
import java.util.List;
import org.apache.iceberg.BaseRewriteFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.TableMetadata;

class BaseVacuumRewrite extends BaseRewriteFiles implements VacuumRewrite, Serializable {

  private final ExtendedTableOperations ops;
  private final TombstoneExtension tombstoneExtension;
  private final List<String> toBeRemoved;
  private final Namespace namespace;

  BaseVacuumRewrite(ExtendedTableOperations ops,
      TombstoneExtension tombstoneExtension,
      List<String> toBeRemoved,
      Namespace namespace) {
    super(ops);
    this.ops = ops;
    this.tombstoneExtension = tombstoneExtension;
    this.toBeRemoved = toBeRemoved;
    this.namespace = namespace;
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base) {
    applyTombstoneChanges(toBeRemoved, tombstoneExtension, ops, namespace)
        .map(f -> this.set(TombstoneExtension.SNAPSHOT_TOMBSTONE_FILE_PROPERTY, f));
    return super.apply(base);
  }
}
