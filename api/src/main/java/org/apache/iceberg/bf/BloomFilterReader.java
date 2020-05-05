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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;

public class BloomFilterReader {
  private BloomFilterReader() {
  }

  public static boolean bloomFilterExists(String path) {
    File file = new File(path);
    if (!file.exists()) {
      return false;
    }
    return true;
  }

  public static Map<Integer, BloomFilter> loadBloomFiltersForFile(DataFile file, String bloomFilterBaseLocation)
      throws IOException {
    Map<Integer, BloomFilter> bloomFilterMap = new HashMap<>();
    if (file.nullValueCounts() == null) {
      return bloomFilterMap;
    }

    Set<Integer> columns = file.nullValueCounts().keySet();
    for (int columnId : columns) {
      String bloomFilterPath = String.format("%s-%d", bloomFilterBaseLocation, columnId);
      if (bloomFilterExists(bloomFilterPath)) {
        BloomFilter bf = BlockSplitBloomFilter.read(bloomFilterPath);
        bloomFilterMap.put(columnId, bf);
      }
    }
    return bloomFilterMap;
  }
}
