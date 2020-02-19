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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.CharSequenceWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides implementation for resolving the deleted files of manifest matching strictly the provided expression
 * This implementation is currently used in {@link MergingSnapshotProducer} as alternative to default implementation
 * that resolves deleted files based on expression conditioned by the requirement that all rows of the matching files
 * match the provided expression and also that if the original expression matches a row, then the projected
 * expression will match that row's partition.
 */
public class StrictExpressionEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(StrictExpressionEvaluator.class);

  /** Utility classes should not have a public or default constructor. [HideUtilityClassConstructor] **/
  private StrictExpressionEvaluator() {}

  public static StrictExpressionEvaluator.Builder builder() {
    return new StrictExpressionEvaluator.Builder();
  }

  public static class Builder {
    private ManifestFile manifest;
    private ManifestWriter writer;
    private SnapshotSummary.Builder summaryBuilder;
    private Set<CharSequenceWrapper> deletedPaths;
    private List<DataFile> deletedFiles;

    @SuppressWarnings("checkstyle:HiddenField")
    public Builder withManifest(ManifestFile manifest) {
      this.manifest = manifest;
      return this;
    }

    @SuppressWarnings("checkstyle:HiddenField")
    public Builder withWriter(ManifestWriter writer) {
      this.writer = writer;
      return this;
    }

    @SuppressWarnings("checkstyle:HiddenField")
    public Builder withSummaryBuilder(SnapshotSummary.Builder summaryBuilder) {
      this.summaryBuilder = summaryBuilder;
      return this;
    }

    @SuppressWarnings("checkstyle:HiddenField")
    public Builder withDeletedPaths(Set<CharSequenceWrapper> deletedPaths) {
      this.deletedPaths = deletedPaths;
      return this;
    }

    @SuppressWarnings("checkstyle:HiddenField")
    public Builder withDeletedFiles(List<DataFile> deletedFiles) {
      this.deletedFiles = deletedFiles;
      return this;
    }

    public Context context(
        Boolean isStrictExpressionEvaluation,
        ManifestReader reader,
        Expression deleteExpression,
        Set<CharSequenceWrapper> deltaAddedFiles)
        throws IOException {
      if (isStrictExpressionEvaluation) {
        List<CharSequence> matchedByExpression = new ArrayList<>();
        FilteredManifest dataFiles = reader.filterRows(deleteExpression);
        try (CloseableIterable<ManifestEntry> manifestEntries = dataFiles.allEntries()) {
          for (ManifestEntry manifestEntry : manifestEntries) {
            matchedByExpression.add(manifestEntry.file().path());
          }
        }
        LOG.info("Vacuum will overwrite files={} matched by expression={}", matchedByExpression.size(),
            deleteExpression.toString());
        return new Context(
            manifest,
            writer,
            summaryBuilder,
            deletedPaths,
            deletedFiles,
            matchedByExpression,
            deltaAddedFiles);
      }
      return new Context();
    }
  }

  // This class holds all the invariants used to evaluate manifest entries
  public static class Context {
    private ManifestFile manifest;
    private ManifestWriter writer;
    private SnapshotSummary.Builder summaryBuilder;
    private Set<CharSequenceWrapper> deletedPaths;
    private List<DataFile> deletedFiles;
    // This is provided by the Builder based on evaluation
    private List<CharSequence> matchedByExpression;
    private Set<CharSequenceWrapper> deltaAddedFiles;

    private Context() {}

    private Context(
        ManifestFile manifest, ManifestWriter writer, SnapshotSummary.Builder summaryBuilder,
        Set<CharSequenceWrapper> deletedPaths, List<DataFile> deletedFiles, List<CharSequence> matchedByExpression,
        Set<CharSequenceWrapper> deltaAddedFiles) {
      this.manifest = manifest;
      this.writer = writer;
      this.summaryBuilder = summaryBuilder;
      this.deletedPaths = deletedPaths;
      this.deletedFiles = deletedFiles;
      this.matchedByExpression = matchedByExpression;
      this.deltaAddedFiles = deltaAddedFiles;
    }

    // This method holds the variants of evaluating each manifest entry
    public void evaluate(Boolean fileDelete, DataFile file, ManifestEntry entry) {
      // Delete only files that match the delete expression
      if (fileDelete || (matchedByExpression.contains(file.path())) &&
          // But don't delete those which were added and match the expression in between the read and commit snapshots
          // Strict expression evaluation may return false positives for matching data files since the scan relies on
          // file metrics collect as upper/ lower limits which provide just an available range that the value MAY
          // match but not necessarily showing up in any of the data file rows.
          !deltaAddedFiles.contains(CharSequenceWrapper.wrap(file.path()))) {
        writer.delete(entry);

        CharSequenceWrapper wrapper = CharSequenceWrapper.wrap(entry.file().path());

        if (deletedPaths.contains(wrapper)) {
          LOG.warn("Deleting a duplicate path from manifest {}: {}", manifest.path(), wrapper.get());
          summaryBuilder.incrementDuplicateDeletes();
        } else {
          // only add the file to deletes if it is a new delete
          // this keeps the snapshot summary accurate for non-duplicate data
          deletedFiles.add(entry.file().copyWithoutStats());
        }
        deletedPaths.add(wrapper);
      } else {
        writer.existing(entry);
      }
    }
  }
}
