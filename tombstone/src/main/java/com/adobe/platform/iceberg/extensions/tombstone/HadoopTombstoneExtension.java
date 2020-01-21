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

package com.adobe.platform.iceberg.extensions.tombstone;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopTombstoneExtension implements TombstoneExtension {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopTombstoneExtension.class);

  // Figure out the directory where we're going to write/read tombstone data from
  private static final String METADATA_TOMBSTONE_DIR_PROPERTY = "adobe.tombstone.dir";
  private static final String TOMBSTONE_DIR_DEFAULT = "adobe/tombstone";

  private Configuration conf;
  private TableOperations ops;

  public HadoopTombstoneExtension(Configuration conf, TableOperations ops) {
    this.conf = conf;
    this.ops = ops;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Optional<String> copyReference(Snapshot snapshot) {
    if (snapshot == null || snapshot.summary() == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(snapshot.summary().get(SNAPSHOT_TOMBSTONE_FILE_PROPERTY));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OutputFile remove(Snapshot snapshot, List<EvictEntry> entries, Namespace namespace) {
    List<Tombstone> current = load(snapshot);
    OutputFile outputFile = newTombstonesFile(ops.current());
    try {
      current.removeAll(fromExternal(entries, namespace));
      new Tombstones(this.conf).write(current, outputFile.location());
      return outputFile;
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write tombstones to file: %s", outputFile.location());
    } catch (URISyntaxException e) {
      throw new RuntimeException(String.format("Failed to write tombstones to file: %s", outputFile.location()), e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OutputFile append(
      Snapshot snapshot, List<EvictEntry> entries, Namespace namespace,
      Map<String, String> props, long newSnapshotId) {
    OutputFile outputFile = newTombstonesFile(ops.current());
    List<Tombstone> current = load(snapshot);
    try {
      // All entries share the same addedOn property as the number of millis since the epoch of 1970-01-01T00:00:00Z
      current
          .addAll(fromExternal(entries, Instant.now().toEpochMilli(), props, namespace,
              Collections.singletonMap("snapshot", String.valueOf(newSnapshotId))));
      new Tombstones(this.conf).write(current, outputFile.location());
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write tombstones to file: %s", outputFile.location());
    } catch (URISyntaxException e) {
      throw new RuntimeException(String.format("Failed to write tombstones to file: %s", outputFile.location()), e);
    }
    return outputFile;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<ExtendedEntry> get(Snapshot snapshot, Namespace namespace) {
    return load(snapshot).stream()
        .filter(tombstone -> tombstone.getNamespace().equalsIgnoreCase(namespace.getId()))
        .map(tombstone -> new ExtendedEntry() {
          @Override
          public Entry getEntry() {
            return tombstone::getId;
          }

          @Override
          public Long getEvictTimestamp() {
            return tombstone.getEvictionTs();
          }

          @Override
          public Map<String, String> getProperties() {
            return tombstone.getProperties();
          }

          @Override
          public Map<String, String> getInternalProperties() {
            return tombstone.getInternal();
          }
        }).collect(Collectors.toList());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Optional<String> merge(Snapshot variant, Snapshot base) {
    List<Tombstone> variantTombstones = load(variant);
    List<Tombstone> baseTombstones = load(base);
    if (baseTombstones.containsAll(variantTombstones)) {
      // noop, just make sure we pick up the current snapshot tombstone file, if available.
      return Optional.ofNullable(base.summary().get(TombstoneExtension.SNAPSHOT_TOMBSTONE_FILE_PROPERTY));
    } else {
      OutputFile outputFile = newTombstonesFile(ops.current());
      try {
        new Tombstones(this.conf).write(
            Lists.newArrayList(Stream.of(variantTombstones, baseTombstones)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet())), // Equality is based on tombstone id and namespace only.
            outputFile.location());
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to write tombstones union to file: %s", outputFile.location());
      } catch (URISyntaxException e) {
        throw new RuntimeException(String.format("Failed to write tombstones to file: %s", outputFile.location()), e);
      }
      return Optional.of(outputFile.location());
    }
  }

  private List<Tombstone> load(Snapshot snapshot) {
    if (snapshot == null || snapshot.summary() == null) {
      LOG.debug("No available tombstones, expected non-null snapshot value");
      return Lists.newArrayList();
    }
    String file = snapshot.summary().getOrDefault(SNAPSHOT_TOMBSTONE_FILE_PROPERTY, "");
    if (file.isEmpty()) {
      LOG.debug(
          "No available tombstones, invalid snapshot summary property: {}",
          SNAPSHOT_TOMBSTONE_FILE_PROPERTY);
      return Lists.newArrayList();
    }
    try {
      return new Tombstones(conf).load(file);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to read tombstones from file: %s", file);
    }
  }

  // TODO - move this to external/internal domain logic class - look into schema validation/ evolution
  private List<Tombstone> fromExternal(
      List<EvictEntry> entries, long addedOn, Map<String, String> properties,
      Namespace namespace, Map<String, String> internal) {
    return entries.stream().map(t -> {
      Tombstone tombstone = new Tombstone(t.get().getKey().getId(),
          namespace.getId(), t.get().getValue());
      tombstone.setAddedOn(addedOn);
      tombstone.setProperties(properties);
      tombstone.setInternal(internal);
      return tombstone;
    }).collect(Collectors.toList());
  }

  // TODO - move this to external/internal domain logic class - look into schema validation/ evolution
  private List<Tombstone> fromExternal(List<EvictEntry> entries, Namespace namespace) {
    return entries.stream()
        .map(t -> new Tombstone(t.get().getKey().getId(), namespace.getId(), t.get().getValue()))
        .collect(Collectors.toList());
  }

  private OutputFile newTombstonesFile(TableMetadata tableMetadata) {
    // We keep tombstone Avro files in a separate directory then Iceberg `metadata` so we avoid have operation conflicts
    String extensionDirectory = tableMetadata.properties().getOrDefault(
        METADATA_TOMBSTONE_DIR_PROPERTY,
        TOMBSTONE_DIR_DEFAULT);
    return ops.io().newOutputFile(new Path(String.format("%s/%s/%s.avro",
        tableMetadata.location(), extensionDirectory, UUID.randomUUID().toString())).toString());
  }
}
