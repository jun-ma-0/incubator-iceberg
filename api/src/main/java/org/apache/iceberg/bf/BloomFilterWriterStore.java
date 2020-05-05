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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

public class BloomFilterWriterStore {
  private Map<Integer, BloomFilterWriter> bloomFilterWriters;

  public BloomFilterWriterStore(Schema schema, String outputBasePath) {
    List<Types.NestedField> fields = schema.fields();
    bloomFilterWriters = new HashMap<>();
    for (Types.NestedField field : fields) {
      Types.BloomFilterConfig bfConfig = field.bfConfig();
      if (bfConfig != null && bfConfig.isEnabled()) {
        String path = String.format("%s-%s", outputBasePath, field.fieldId());
        bloomFilterWriters.put(field.fieldId(), new BloomFilterWriter(bfConfig.fpp(), bfConfig.ndv(), path));
      }
    }
  }

  public BloomFilterWriter getBloomFilterWriter(int fieldId) {
    return bloomFilterWriters.get(fieldId);
  }


  // Call close to write all Bloom Filters into disk
  public void close() throws IOException {
    for (BloomFilterWriter writer : bloomFilterWriters.values()) {
      writer.close();
    }
  }
}
