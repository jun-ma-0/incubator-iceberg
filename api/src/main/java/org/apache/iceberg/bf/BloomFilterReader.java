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

package org.apache.iceberg.bf;

import com.google.common.io.ByteStreams;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

public class BloomFilterReader {
  private BloomFilterReader() {
  }

  public static BloomFilter loadBloomFilter(String bloomFilterBaseLocation, FileIO io, int fieldId) {
    String bloomFilterPath = String.format("%s-%d", bloomFilterBaseLocation, fieldId);
    InputFile inputFile = io.newInputFile(bloomFilterPath);

    if (!inputFile.exists()) {
      return null;
    }

    byte[] bfArray = null;
    try (SeekableInputStream is = inputFile.newStream()) {
      bfArray = ByteStreams.toByteArray(is);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }

    return new BlockSplitBloomFilter(bfArray);
  }

  /**
   * For test
   */
  public static Map<Integer, BloomFilter> loadBloomFilter(DataFile file, String bloomFilterBaseLocation) {
    Map<Integer, BloomFilter> bloomFilterMap = new HashMap<>();
    if (file.nullValueCounts() == null) {
      return bloomFilterMap;
    }

    Set<Integer> columns = file.nullValueCounts().keySet();
    for (int columnId : columns) {
      String bloomFilterPath = String.format("%s-%d", bloomFilterBaseLocation, columnId);
      File bfFile = new File(bloomFilterPath);
      if (bfFile.exists()) {
        byte[] bfArray = null;
        try {
          bfArray = Files.readAllBytes(Paths.get(bloomFilterPath));
        } catch (IOException e) {
          throw new RuntimeIOException(e);
        }
        BloomFilter bf = new BlockSplitBloomFilter(bfArray);
        bloomFilterMap.put(columnId, bf);
      }
    }
    return bloomFilterMap;
  }
}
