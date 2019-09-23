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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.util.PathUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestFastAppend extends TableTestBase {

  @Test
  public void testEmptyTableAppend() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    Snapshot pending = table.newFastAppend()
        .appendFile(fileA)
        .appendFile(fileB)
        .apply();

    validateSnapshot(base.currentSnapshot(), pending, fileA, fileB);
  }

  @Test
  public void testEmptyTableAppendManifest() throws IOException {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    ManifestFile manifest = writeManifest(fileA, fileB);
    Snapshot pending = table.newFastAppend()
        .appendManifest(manifest)
        .apply();

    validateSnapshot(base.currentSnapshot(), pending, fileA, fileB);
  }

  @Test
  public void testEmptyTableAppendFilesAndManifest() throws IOException {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    ManifestFile manifest = writeManifest(fileA, fileB);
    Snapshot pending = table.newFastAppend()
        .appendFile(fileC)
        .appendFile(fileD)
        .appendManifest(manifest)
        .apply();

    long pendingId = pending.snapshotId();

    validateManifest(pending.manifests().get(0),
        ids(pendingId, pendingId),
        files(fileC, fileD),
        table.location());
    validateManifest(pending.manifests().get(1),
        ids(pendingId, pendingId),
        files(fileA, fileB),
        table.location());
  }

  @Test
  public void testNonEmptyTableAppend() {
    table.newAppend()
        .appendFile(fileA)
        .appendFile(fileB)
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertNotNull("Should have a current snapshot", base.currentSnapshot());
    List<ManifestFile> v2manifests = base.currentSnapshot().manifests();
    Assert.assertEquals("Should have one existing manifest", 1, v2manifests.size());

    // prepare a new append
    Snapshot pending = table.newFastAppend()
        .appendFile(fileC)
        .appendFile(fileD)
        .apply();

    Assert.assertNotEquals("Snapshots should have unique IDs",
        base.currentSnapshot().snapshotId(), pending.snapshotId());
    validateSnapshot(base.currentSnapshot(), pending, fileC, fileD);
  }

  @Test
  public void testNoMerge() {
    table.newAppend()
        .appendFile(fileA)
        .commit();

    table.newFastAppend()
        .appendFile(fileB)
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertNotNull("Should have a current snapshot", base.currentSnapshot());
    List<ManifestFile> v3manifests = base.currentSnapshot().manifests();
    Assert.assertEquals("Should have 2 existing manifests", 2, v3manifests.size());

    // prepare a new append
    Snapshot pending = table.newFastAppend()
        .appendFile(fileC)
        .appendFile(fileD)
        .apply();

    Set<Long> ids = Sets.newHashSet();
    for (Snapshot snapshot : base.snapshots()) {
      ids.add(snapshot.snapshotId());
    }
    ids.add(pending.snapshotId());
    Assert.assertEquals("Snapshots should have 3 unique IDs", 3, ids.size());

    validateSnapshot(base.currentSnapshot(), pending, fileC, fileD);
  }

  @Test
  public void testRefreshBeforeApply() {
    // load a new copy of the table that will not be refreshed by the commit
    Table stale = load();

    table.newAppend()
        .appendFile(fileA)
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertNotNull("Should have a current snapshot", base.currentSnapshot());
    List<ManifestFile> v2manifests = base.currentSnapshot().manifests();
    Assert.assertEquals("Should have 1 existing manifest", 1, v2manifests.size());

    // commit from the stale table
    AppendFiles append = stale.newFastAppend()
        .appendFile(fileD);
    Snapshot pending = append.apply();

    // table should have been refreshed before applying the changes
    validateSnapshot(base.currentSnapshot(), pending, fileD);
  }

  @Test
  public void testRefreshBeforeCommit() {
    // commit from the stale table
    AppendFiles append = table.newFastAppend()
        .appendFile(fileD);
    Snapshot pending = append.apply();

    validateSnapshot(null, pending, fileD);

    table.newAppend()
        .appendFile(fileA)
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertNotNull("Should have a current snapshot", base.currentSnapshot());
    List<ManifestFile> v2manifests = base.currentSnapshot().manifests();
    Assert.assertEquals("Should have 1 existing manifest", 1, v2manifests.size());

    append.commit();

    TableMetadata committed = readMetadata();

    // apply was called before the conflicting commit, but the commit was still consistent
    validateSnapshot(base.currentSnapshot(), committed.currentSnapshot(), fileD);

    List<ManifestFile> committedManifests = Lists.newArrayList(committed.currentSnapshot().manifests());
    committedManifests.removeAll(base.currentSnapshot().manifests());
    Assert.assertEquals("Should reused manifest created by apply",
        pending.manifests().get(0), committedManifests.get(0));
  }

  @Test
  public void testFailure() {
    // inject 5 failures
    TestTables.TestTableOperations ops = table.ops();
    ops.failCommits(5);

    AppendFiles append = table.newFastAppend().appendFile(fileB);
    Snapshot pending = append.apply();
    ManifestFile newManifest = pending.manifests().get(0);
    Assert.assertTrue("Should create new manifest",
            new File(PathUtil.getAbsolutePath(table.location(), newManifest.path())).exists());

    AssertHelpers.assertThrows("Should retry 4 times and throw last failure",
        CommitFailedException.class, "Injected failure", append::commit);

    Assert.assertFalse("Should clean up new manifest",
            new File(PathUtil.getAbsolutePath(table.location(), newManifest.path())).exists());
  }

  @Test
  public void testAppendManifestCleanup() throws IOException {
    // inject 5 failures
    TestTables.TestTableOperations ops = table.ops();
    ops.failCommits(5);

    ManifestFile manifest = writeManifest(fileA, fileB);
    AppendFiles append = table.newFastAppend().appendManifest(manifest);
    Snapshot pending = append.apply();
    ManifestFile newManifest = pending.manifests().get(0);
    Assert.assertTrue("Should create new manifest",
        new File(PathUtil.getAbsolutePath(table.location(), newManifest.path())).exists());

    AssertHelpers.assertThrows("Should retry 4 times and throw last failure",
        CommitFailedException.class, "Injected failure", append::commit);

    Assert.assertFalse("Should clean up new manifest",
        new File(PathUtil.getAbsolutePath(table.location(), newManifest.path())).exists());
  }

  @Test
  public void testRecoveryWithManifestList() {
    table.updateProperties().set(TableProperties.MANIFEST_LISTS_ENABLED, "true").commit();

    // inject 3 failures, the last try will succeed
    TestTables.TestTableOperations ops = table.ops();
    ops.failCommits(3);

    AppendFiles append = table.newFastAppend().appendFile(fileB);
    Snapshot pending = append.apply();
    ManifestFile newManifest = pending.manifests().get(0);
    Assert.assertTrue("Should create new manifest",
        new File(PathUtil.getAbsolutePath(table.location(), newManifest.path())).exists());

    append.commit();

    TableMetadata metadata = readMetadata();

    validateSnapshot(null, metadata.currentSnapshot(), fileB);
    Assert.assertTrue("Should commit same new manifest",
        new File(PathUtil.getAbsolutePath(table.location(), newManifest.path())).exists());
    Assert.assertTrue("Should commit the same new manifest",
        metadata.currentSnapshot().manifests().contains(newManifest));
  }

  @Test
  public void testRecoveryWithoutManifestList() {
    table.updateProperties().set(TableProperties.MANIFEST_LISTS_ENABLED, "false").commit();

    // inject 3 failures, the last try will succeed
    TestTables.TestTableOperations ops = table.ops();
    ops.failCommits(3);

    AppendFiles append = table.newFastAppend().appendFile(fileB);
    Snapshot pending = append.apply();
    ManifestFile newManifest = pending.manifests().get(0);
    Assert.assertTrue("Should create new manifest",
        new File(PathUtil.getAbsolutePath(table.location(), newManifest.path())).exists());

    append.commit();

    TableMetadata metadata = readMetadata();

    validateSnapshot(null, metadata.currentSnapshot(), fileB);
    Assert.assertTrue("Should commit same new manifest",
        new File(PathUtil.getAbsolutePath(table.location(), newManifest.path())).exists());
    Assert.assertTrue("Should commit the same new manifest",
        metadata.currentSnapshot().manifests().contains(newManifest));
  }
}
