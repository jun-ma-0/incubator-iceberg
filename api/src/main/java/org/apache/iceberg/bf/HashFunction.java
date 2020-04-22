package org.apache.iceberg.bf;

import java.nio.ByteBuffer;

/**
 * A interface contains a set of hash functions used by Bloom filter.
 */
public interface HashFunction {

  /**
   * compute the hash value for a byte array.
   * @param input the input byte array
   * @return a result of long value.
   */
  long hashBytes(byte[] input);

  /**
   * compute the hash value for a ByteBuffer.
   * @param input the input ByteBuffer
   * @return a result of long value.
   */
  long hashByteBuffer(ByteBuffer input);
}