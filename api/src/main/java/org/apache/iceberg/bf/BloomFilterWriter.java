package org.apache.iceberg.bf;

import java.io.IOException;

/**
 * Defines the interface to write values into a Bloom Filter
 */
public class BloomFilterWriter {
  // Insert value into Bloom Filter
  private BloomFilter bloomFilter;

  public BloomFilterWriter(double fpp, long ndv, String path) {
    int bits = BlockSplitBloomFilter.optimalNumOfBits(ndv, fpp);
    bloomFilter = new BlockSplitBloomFilter(bits, path);
  }

  //  public void write(boolean value) {} // Is this needed?

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
  void close() throws IOException {
    bloomFilter.write();
  }
}
