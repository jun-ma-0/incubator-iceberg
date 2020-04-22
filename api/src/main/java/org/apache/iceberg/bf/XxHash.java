package org.apache.iceberg.bf;

import net.openhft.hashing.LongHashFunction;

import java.nio.ByteBuffer;

/**
 * The implementation of HashFunction interface. The XxHash uses XXH64 version xxHash
 * with a seed of 0.
 */
public class XxHash implements HashFunction {
  @Override
  public long hashBytes(byte[] input) {
    return LongHashFunction.xx(0).hashBytes(input);
  }

  @Override
  public long hashByteBuffer(ByteBuffer input) {
    return LongHashFunction.xx(0).hashBytes(input);
  }
}
