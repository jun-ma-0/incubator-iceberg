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

import com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class BloomFilterSchemaUpdate extends SchemaUpdate {
  public BloomFilterSchemaUpdate(TableOperations ops) {
    super(ops);
  }

  /**
   * For testing only.
   */
  BloomFilterSchemaUpdate(Schema schema, int lastColumnId) {
    super(schema, lastColumnId);
  }

  public SchemaUpdate addBloomFilter(String name, double fpp, long ndv) {
    Types.NestedField field = schema().findField(name);
    Preconditions.checkArgument(field != null, "Cannot rename missing column: %s", name);
    Preconditions.checkArgument(fpp > 0 && fpp < 1.0, "FPP must between (0, 1)");
    Preconditions.checkArgument(ndv > 0, "NDV must > 0");

    int fieldId = field.fieldId();
    Types.NestedField update = updates().get(fieldId);
    Types.BloomFilterConfig bfConfig = new Types.BloomFilterConfig(true, fpp, ndv);
    if (update != null) {
      updates().put(fieldId,
          Types.NestedField.of(fieldId, update.isOptional(), update.name(), update.type(), update.doc(), bfConfig));
    } else {
      updates().put(fieldId,
          Types.NestedField.of(fieldId, field.isOptional(), field.name(), field.type(), field.doc(), bfConfig));
    }
    return this;
  }

  public BloomFilterSchemaUpdate addColumn(String name, Type type, String doc) {
    super.addColumn(name, type, doc);
    return this;
  }
}
