/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.conf.Configured;

import java.nio.ByteBuffer;

/**
 * A common class of basic facilities to be shared by encoder and decoder
 *
 * It implements the {@link RawErasureCoder} interface.
 */
public abstract class AbstractRawErasureCoder
    extends Configured implements RawErasureCoder {

  // Hope to reset coding buffers a little faster using it
  private byte[] zeroChunkBytes;

  private int numDataUnits;
  private int numParityUnits;
  private int chunkSize;

  @Override
  public void initialize(int numDataUnits, int numParityUnits,
                         int chunkSize) {
    this.numDataUnits = numDataUnits;
    this.numParityUnits = numParityUnits;
    this.chunkSize = chunkSize;

    zeroChunkBytes = new byte[chunkSize]; // With ZERO by default
  }

  @Override
  public int getNumDataUnits() {
    return numDataUnits;
  }

  @Override
  public int getNumParityUnits() {
    return numParityUnits;
  }

  @Override
  public int getChunkSize() {
    return chunkSize;
  }

  @Override
  public boolean preferDirectBuffer() {
    return false;
  }

  @Override
  public void release() {
    // Nothing to do by default
  }

  /**
   * Ensure output buffer filled with ZERO bytes fully in chunkSize.
   * @param buffer a buffer ready to write chunk size bytes
   * @return the buffer itself, with ZERO bytes written, remaining the original
   * position
   */
  protected ByteBuffer resetOutputBuffer(ByteBuffer buffer) {
    int pos = buffer.position();
    buffer.put(zeroChunkBytes, 0, buffer.remaining());
    buffer.position(pos);

    return buffer;
  }

  /**
   * Ensure the buffer (either input or output) ready to read or write with ZERO
   * bytes fully in chunkSize.
   * @param buffer bytes array buffer
   * @return the buffer itself
   */
  protected byte[] resetBuffer(byte[] buffer, int offset, int len) {
    System.arraycopy(zeroChunkBytes, 0, buffer, offset, len);

    return buffer;
  }

  /**
   * Check and ensure the buffers are of the length specified by dataLen.
   * @param buffers
   * @param dataLen
   */
  protected void ensureLength(ByteBuffer[] buffers, int dataLen) {
    for (int i = 0; i < buffers.length; ++i) {
      if (buffers[i].remaining() != dataLen) {
        throw new IllegalArgumentException("Invalid buffer not of length "
            + dataLen);
      }
    }
  }

  /**
   * Check and ensure the buffers are of the length specified by dataLen.
   * @param buffers
   * @param dataLen
   */
  protected void ensureLength(byte[][] buffers, int dataLen) {
    for (int i = 0; i < buffers.length; ++i) {
      if (buffers[i].length != dataLen) {
        throw new IllegalArgumentException("Invalid buffer not of length "
            + dataLen);
      }
    }
  }
}
