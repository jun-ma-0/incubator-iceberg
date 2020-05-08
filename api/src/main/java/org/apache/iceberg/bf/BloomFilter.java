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

import java.io.OutputStream;

/**
 * Defines the Bloom Filter data structure. It usually contains a bit array and one or more hash function(s).
 */
public interface BloomFilter<T> {

  enum HashStrategy {
    XXH64;

    @Override
    public String toString() {
      return "xxhash";
    }
  }

  enum Algorithm {
    BLOCK;

    @Override
    public String toString() {
      return "block";
    }
  }

  void writeTo(OutputStream out);

  long hash(T value);

  long hash(int value);

  long hash(long value);

  long hash(double value);

  long hash(float value);

  long hash(String value);

  void insertHash(long hash);

  boolean findHash(long hash);

  Algorithm getAlgorithm();

  HashStrategy getHashStrategy();

  int getBitsetSize();
}
