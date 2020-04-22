package org.apache.iceberg.bf;

import org.apache.iceberg.DataFile;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BloomFilterReader {
  private static boolean bloomFilterExists(String path) {
    File file = new File(path);
    if (!file.exists()) return false;
    return true;
  }

  public static Map<Integer, BloomFilter> loadBloomFiltersForFile(DataFile file) throws IOException {
    Map<Integer, BloomFilter> bloomFilterMap = new HashMap<>();
    if(file.nullValueCounts() == null) return bloomFilterMap;

    Set<Integer> columns = file.nullValueCounts().keySet();
    for(int columnId: columns) {
      String bloomFilterPath = String.format("%s-%d",file.path(), columnId);
      if(bloomFilterExists(bloomFilterPath)) {
        BloomFilter bf = BlockSplitBloomFilter.read(bloomFilterPath);
        bloomFilterMap.put(columnId, bf);
      }
    }
    return bloomFilterMap;
  }
}
