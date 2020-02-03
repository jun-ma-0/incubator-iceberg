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

package com.adobe.platform.iceberg.extensions.adls;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an implementation of TableOperations that is aware of ADLS filesystem's limitations. ADLS currently
 * doesn't allow us to overwrite a version_hint atomically. This resulted in the need for
 * https://jira.corp.adobe.com/browse/PLAT-41915. It is functionally similar to HadoopTableOperations but makes the
 * version_hint reading and increment extendable. This allows sub-classes to implement their own version
 * reading/update mechanism which is expected to be atomic.
 */
public abstract class BaseAdlsTableOperations implements TableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(BaseAdlsTableOperations.class);

  // copied from HadoopTableOps
  private final Configuration conf;
  private final Path location;
  private TableMetadata currentMetadata = null;
  private Integer version = null;
  private boolean shouldRefresh = true;
  private HadoopFileIO defaultFileIo = null;

  public BaseAdlsTableOperations(Path location, Configuration conf) {
    this.conf = conf;
    this.location = location;
  }

  // Copied as is from HadoopTableOps
  @Override
  public TableMetadata current() {
    if (shouldRefresh) {
      return refresh();
    }
    return currentMetadata;
  }

  @Override
  public TableMetadata refresh() {
    // should be able to read latest version and refresh current table state
    int ver = version != null ? version : readVersionHint();

    // do refresh using current version
    try {
      Path metadataFile = getMetadataFile(ver);
      if (version == null && metadataFile == null && ver == 0) {
        // no v0 metadata means the table doesn't exist yet
        return null;
      } else if (metadataFile == null) {
        throw new ValidationException("Metadata file for version %d is missing", ver);
      }

      Path nextMetadataFile = getMetadataFile(ver + 1);
      while (nextMetadataFile != null) {
        ver += 1;
        metadataFile = nextMetadataFile;
        nextMetadataFile = getMetadataFile(ver + 1);
      }

      this.version = ver;

      TableMetadata newMetadata = TableMetadataParser.read(this, io().newInputFile(metadataFile.toString()));
      String newUUID = newMetadata.uuid();
      if (currentMetadata != null) {
        Preconditions.checkState(newUUID == null || newUUID.equals(currentMetadata.uuid()),
            "Table UUID does not match: current=%s != refreshed=%s", currentMetadata.uuid(), newUUID);
      }

      this.currentMetadata = newMetadata;
      this.shouldRefresh = false;
      return currentMetadata;
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to refresh the table");
    }
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    if (base != current()) {
      throw new CommitFailedException("Cannot commit changes based on stale table metadata");
    }

    if (base == metadata) {
      LOG.info("Nothing to commit.");
      return;
    }

    Preconditions.checkArgument(base == null || base.location().equals(metadata.location()),
        "Hadoop path-based tables cannot be relocated");
    Preconditions.checkArgument(
        !metadata.properties().containsKey(TableProperties.WRITE_METADATA_LOCATION),
        "Hadoop path-based tables cannot relocate metadata");

    String codecName = metadata.property(
        TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT);
    TableMetadataParser.Codec codec = TableMetadataParser.Codec.fromName(codecName);
    String fileExtension = TableMetadataParser.getFileExtension(codec);
    Path tempMetadataFile = metadataPath(UUID.randomUUID().toString() + fileExtension);
    TableMetadataParser.write(metadata, io().newOutputFile(tempMetadataFile.toString()));

    int nextVersion = (version != null ? version : 0) + 1;
    Path finalMetadataFile = metadataFilePath(nextVersion, codec);
    FileSystem fs = getFileSystem(tempMetadataFile, conf);

    try {
      if (fs.exists(finalMetadataFile)) {
        throw new CommitFailedException(
            "Version %d already exists: %s", nextVersion, finalMetadataFile);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e,
          "Failed to check if next version exists: " + finalMetadataFile);
    }

    // this rename operation is the atomic commit operation. ADLS provides atomic rename.
    renameToFinal(fs, tempMetadataFile, finalMetadataFile);

    // set the next version
    setAtomicVersionHint(nextVersion);

    this.shouldRefresh = true;
  }

  @Override
  public FileIO io() {
    if (defaultFileIo == null) {
      defaultFileIo = new HadoopFileIO(conf);
    }
    return defaultFileIo;
  }

  @Override
  public LocationProvider locationProvider() {
    return LocationProviders.locationsFor(current().location(), current().properties());
  }

  @Override
  public String metadataFileLocation(String fileName) {
    return metadataPath(fileName).toString();
  }


  private Path getMetadataFile(int metadataVersion) throws IOException {
    for (TableMetadataParser.Codec codec : TableMetadataParser.Codec.values()) {
      Path metadataFile = metadataFilePath(metadataVersion, codec);
      FileSystem fs = getFileSystem(metadataFile, conf);
      if (fs.exists(metadataFile)) {
        return metadataFile;
      }

      if (codec.equals(TableMetadataParser.Codec.GZIP)) {
        // we have to be backward-compatible with .metadata.json.gz files
        metadataFile = oldMetadataFilePath(metadataVersion, codec);
        fs = getFileSystem(metadataFile, conf);
        if (fs.exists(metadataFile)) {
          return metadataFile;
        }
      }
    }

    return null;
  }

  protected Path metadataFilePath(int metadataVersion, TableMetadataParser.Codec codec) {
    return metadataPath("v" + metadataVersion + TableMetadataParser.getFileExtension(codec));
  }

  protected Path oldMetadataFilePath(int metadataVersion, TableMetadataParser.Codec codec) {
    return metadataPath("v" + metadataVersion + TableMetadataParser.getOldFileExtension(codec));
  }

  protected Path metadataPath(String filename) {
    return new Path(new Path(location, "metadata"), filename);
  }

  /**
   * Renames the source file to destination, using the provided file system. If the rename failed,
   * an attempt will be made to delete the source file.
   *
   * @param fs the filesystem used for the rename
   * @param src the source file
   * @param dst the destination file
   */
  private void renameToFinal(FileSystem fs, Path src, Path dst) {
    try {
      if (!fs.rename(src, dst)) {
        CommitFailedException cfe = new CommitFailedException(
            "Failed to commit changes using rename: %s", dst);
        RuntimeException re = tryDelete(src);
        if (re != null) {
          cfe.addSuppressed(re);
        }
        throw cfe;
      }
    } catch (IOException e) {
      CommitFailedException cfe = new CommitFailedException(e,
          "Failed to commit changes using rename: %s", dst);
      RuntimeException re = tryDelete(src);
      if (re != null) {
        cfe.addSuppressed(re);
      }
      throw cfe;
    }
  }

  /**
   * Deletes the file from the file system. Any RuntimeException will be caught and returned.
   *
   * @param path the file to be deleted.
   * @return RuntimeException caught, if any. null otherwise.
   */
  private RuntimeException tryDelete(Path path) {
    try {
      io().deleteFile(path.toString());
      return null;
    } catch (RuntimeException re) {
      return re;
    }
  }

  private FileSystem getFileSystem(Path path, Configuration hadoopConf) {
    try {
      return path.getFileSystem(hadoopConf);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to get file system for path: %s", path);
    }
  }

  /**
   * A specialized implementation should provide a way to read the global version counter.
   * @return the current version. If there is no counter yet for this table then return 0.
   */
  protected abstract int readVersionHint();

  /**
   * A specialized implementation should be setting a global version counter. This implementation is
   * expected to be global and atomic.
   * @param nextVersion current version to be set on global counter
   */
  protected abstract void setAtomicVersionHint(int nextVersion);
}
