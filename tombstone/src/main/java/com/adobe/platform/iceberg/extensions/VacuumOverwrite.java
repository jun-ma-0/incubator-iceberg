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

import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.Snapshot;

/**
 * API for overwriting files in a table for tombstone rows vacuuming
 * <p>
 * This API accumulates file additions and produces a new {@link Snapshot} of the table by replacing
 * all the deleted files with the set of additions. This operation is used to implement idempotent
 * writes that always replace a section of a table with new data or update/delete operations that
 * eagerly overwrite files.
 * <p>
 * Overwrites can be validated. The default validation mode is idempotent, meaning the overwrite is
 * correct and should be committed out regardless of other concurrent changes to the table.
 * This API can be configured for overwriting certain files by providing a filter expression
 * while assuming that new data that would need to be filtered has been added would only qualify as
 * false positive metrics matches.
 * <p>
 * When committing, these changes will be applied to the latest table snapshot. Commit conflicts
 * will be resolved by applying the changes to the new latest snapshot and reattempting the commit.
 */
public interface VacuumOverwrite extends OverwriteFiles {

}
