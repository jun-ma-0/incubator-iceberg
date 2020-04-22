package org.apache.iceberg.bf;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BloomFilterWriterStore {
  private Map<Integer, BloomFilterWriter> bloomFilterWriters;

  public BloomFilterWriterStore(Schema schema, String outputBasePath) {
    List<Types.NestedField> fields = schema.columns();
    bloomFilterWriters = new HashMap<>();
    // todo - heihei - handling for map/list!!!
    for(Types.NestedField field : fields) {
      if (field.isBloomFilterEnabled()) {
        String path = String.format("%s-%s", outputBasePath, field.fieldId());
        bloomFilterWriters.put(field.fieldId(), new BloomFilterWriter(field.fpp(), field.ndv(), path));
      }
    }
  }

  public BloomFilterWriter getBloomFilterWriter(int fieldId) {
    return bloomFilterWriters.get(fieldId);
  }


  // Call close to write all Bloom Filters into disk
  public void close() throws IOException {
    for(BloomFilterWriter writer : bloomFilterWriters.values()) {
      writer.close();
    }
  }

  public static String getBloomFilterBasePathFromFilePath(String filePath) {
    // todo - heihei
    return filePath;
  }
}
