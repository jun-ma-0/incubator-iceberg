package org.apache.iceberg.bf;

import java.io.IOException;

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

  // Bloom filter algorithm.
  enum Algorithm {
    BLOCK;

    @Override
    public String toString() {
      return "block";
    }
  }

  // Write to disk
  void write() throws IOException;

  long hash(T value);

  // Get hash for a value
//  long hash(boolean value); // Is this needed?
  long hash(int value);
  long hash(long value);
  long hash(double value);
  long hash(float value);
  long hash(String value);
//  long hash(Object value);


  // Insert hash into bit array
  void insertHash(long hash);


  // Check if a hash is added into the Bloom Filter. No false negative. May have false positive.
  boolean findHash(long hash);

  Algorithm getAlgorithm();

  HashStrategy getHashStrategy();

  int getBitsetSize();

}