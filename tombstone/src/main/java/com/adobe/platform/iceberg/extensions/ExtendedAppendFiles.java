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

package com.adobe.platform.iceberg.extensions;

import com.adobe.platform.iceberg.extensions.tombstone.Entry;
import com.adobe.platform.iceberg.extensions.tombstone.EvictEntry;
import com.adobe.platform.iceberg.extensions.tombstone.Namespace;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.AppendFiles;

public interface ExtendedAppendFiles extends AppendFiles {

  /**
   * Append tombstone {@link Entry} to the snapshot.
   *
   * @param namespace a namespace
   * @param entries tombstone values
   * @param properties tombstone properties
   * @param evictionTs a timestamp that indicates the expected maximum retention of data marked by this tombstone
   * @return this for method chaining
   */
  ExtendedAppendFiles appendTombstones(
      Namespace namespace,
      List<Entry> entries,
      Map<String, String> properties,
      long evictionTs);

  /**
   * Removes tombstone {@link Entry} to the snapshot.
   *
   * @param namespace a namespace
   * @param entries tombstone with associated eviction timestamps
   * @return this for method chaining
   */
  ExtendedAppendFiles removeTombstones(Namespace namespace, List<EvictEntry> entries);
}
