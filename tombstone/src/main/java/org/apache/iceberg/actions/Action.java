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

package org.apache.iceberg.actions;

// TODO: THIS CODE IS DUPLICATE spark/src/main/java/org/apache/iceberg/actions/Action.java
// THIS SHOULD BE REMOVED ON NEXT REBASE W/ UPSTREAM SO WE CAN DITCH DUPLICATE CODE
/**
 * An action performed on a table.
 *
 * @param <R> the Java type of the result produced by this action
 */
public interface Action<R> {
  /**
   * Executes this action.
   *
   * @return the result of this action
   */
  R execute();
}