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

import org.apache.iceberg.ManifestEntry.Status;
import org.junit.Assert;
import org.junit.Test;

public class TestDeleteFiles extends TableTestBase {
  @Test
  public void testMultipleDeletes() {
    table.newAppend()
        .appendFile(fileA)
        .appendFile(fileB)
        .appendFile(fileC)
        .commit();

    Assert.assertEquals("Metadata should be at version 1", 1L, (long) version());
    Snapshot append = readMetadata().currentSnapshot();
    validateSnapshot(null, append, fileA, fileB, fileC);

    table.newDelete()
        .deleteFile(fileA)
        .commit();

    Assert.assertEquals("Metadata should be at version 2", 2L, (long) version());
    Snapshot delete = readMetadata().currentSnapshot();
    Assert.assertEquals("Should have 1 manifest", 1, delete.manifests().size());
    validateManifestEntries(delete.manifests().get(0),
        ids(delete.snapshotId(), append.snapshotId(), append.snapshotId()),
        files(fileA, fileB, fileC),
        statuses(Status.DELETED, Status.EXISTING, Status.EXISTING),
        table.location());

    table.newDelete()
        .deleteFile(fileB)
        .commit();

    Assert.assertEquals("Metadata should be at version 3", 3L, (long) version());
    Snapshot delete2 = readMetadata().currentSnapshot();
    Assert.assertEquals("Should have 1 manifest", 1, delete2.manifests().size());
    validateManifestEntries(delete2.manifests().get(0),
        ids(delete2.snapshotId(), append.snapshotId()),
        files(fileB, fileC),
        statuses(Status.DELETED, Status.EXISTING),
        table.location());
  }
}
