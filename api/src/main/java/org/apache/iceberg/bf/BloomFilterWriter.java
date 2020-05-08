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

import org.apache.iceberg.io.OutputFile;

/**
 * Defines the interface to write values into a Bloom Filter
 */
public class BloomFilterWriter {
  // Insert value into Bloom Filter
  private final BloomFilter bloomFilter;
  private final OutputFile outputFile;

  public BloomFilterWriter(double fpp, long ndv, OutputFile outputFile) {
    int bits = BlockSplitBloomFilter.optimalNumOfBits(ndv, fpp);
    this.bloomFilter = new BlockSplitBloomFilter(bits / 8);
    this.outputFile = outputFile;
  }

  public void write(int value) {
    long hash = bloomFilter.hash(value);
    bloomFilter.insertHash(hash);
  }

  public void write(long value) {
    long hash = bloomFilter.hash(value);
    bloomFilter.insertHash(hash);
  }

  public void write(double value) {
    long hash = bloomFilter.hash(value);
    bloomFilter.insertHash(hash);
  }

  public void write(float value) {
    long hash = bloomFilter.hash(value);
    bloomFilter.insertHash(hash);
  }

  public void write(String value) {
    long hash = bloomFilter.hash(value);
    bloomFilter.insertHash(hash);
  }

  // Call close to write Bloom Filter into disk. It should be called when all values in a column are inserted.
  void close() {
    bloomFilter.writeTo(outputFile.create());
  }
}
