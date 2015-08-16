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

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configured;

import java.nio.ByteBuffer;

/**
 * A common class of basic facilities to be shared by encoder and decoder
 *
 * It implements the {@link RawErasureCoder} interface.
 */
public abstract class AbstractRawErasureCoder
    extends Configured implements RawErasureCoder {

  private static byte[] emptyChunk = new byte[4096];

  protected final int numDataUnits;
  protected final int numParityUnits;
  protected final int numAllUnits;

  protected boolean allowInputDataDirty = true;

  public AbstractRawErasureCoder(int numDataUnits, int numParityUnits) {
    this.numDataUnits = numDataUnits;
    this.numParityUnits = numParityUnits;
    this.numAllUnits = numDataUnits + numParityUnits;
  }

  /**
   * Make sure to return an empty chunk buffer for the desired length.
   * @param desiredLength
   * @return empty chunk of zero bytes
   */
  protected static byte[] getEmptyChunk(int desiredLength) {
    if (emptyChunk.length >= desiredLength) {
      return emptyChunk; // In most time
    }

    synchronized (AbstractRawErasureCoder.class) {
      emptyChunk = new byte[desiredLength];
    }

    return emptyChunk;
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
  public boolean preferDirectBuffer() {
    return false;
  }

  @Override
  public void release() {
    // Nothing to do by default
  }

  /**
   * Convert an input bytes array to direct ByteBuffer.
   * @param input
   * @return direct ByteBuffer
   */
  protected ByteBuffer convertInputBuffer(byte[] input, int offset, int len) {
    if (input == null) { // an input can be null, if erased or not to read
      return null;
    }

    ByteBuffer directBuffer = ByteBuffer.allocateDirect(len);
    directBuffer.put(input, offset, len);
    directBuffer.flip();
    return directBuffer;
  }

  /**
   * Convert an output bytes array buffer to direct ByteBuffer.
   * @param output
   * @return direct ByteBuffer
   */
  protected ByteBuffer convertOutputBuffer(byte[] output, int len) {
    ByteBuffer directBuffer = ByteBuffer.allocateDirect(len);
    return directBuffer;
  }

  /**
   * Ensure a buffer filled with ZERO bytes from current readable/writable
   * position.
   * @param buffer a buffer ready to read / write certain size bytes
   * @return the buffer itself, with ZERO bytes written, the position and limit
   *         are not changed after the call
   */
  protected ByteBuffer resetBuffer(ByteBuffer buffer) {
    int pos = buffer.position();
    for (int i = pos; i < buffer.limit(); ++i) {
      buffer.put((byte) 0);
    }
    buffer.position(pos);

    return buffer;
  }

  /**
   * Ensure the buffer (either input or output) ready to read or write with ZERO
   * bytes fully in specified length of len.
   * @param buffer bytes array buffer
   * @return the buffer itself
   */
  protected byte[] resetBuffer(byte[] buffer, int offset, int len) {
    for (int i = offset; i < len; ++i) {
      buffer[i] = (byte) 0;
    }

    return buffer;
  }

  /**
   * Check and ensure the buffers are of the length specified by dataLen, also
   * ensure the buffers are direct buffers or not according to isDirectBuffer.
   * @param buffers the buffers to check
   * @param allowNull whether to allow any element to be null or not
   * @param dataLen the length of data available in the buffer to ensure with
   * @param isDirectBuffer is direct buffer or not to ensure with
   * @param isOutputs is output buffer or not
   */
  protected void checkParameterBuffers(ByteBuffer[] buffers, boolean
      allowNull, int dataLen, boolean isDirectBuffer, boolean isOutputs) {
    ByteBuffer empty = ByteBuffer.wrap(getEmptyChunk(dataLen), 0, dataLen);
    for (ByteBuffer buffer : buffers) {
      if (buffer == null && !allowNull) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer found, not allowing null");
      } else if (buffer != null) {
        if (buffer.remaining() != dataLen) {
          throw new HadoopIllegalArgumentException(
              "Invalid buffer, not of length " + dataLen);
        }
        if (buffer.isDirect() != isDirectBuffer) {
          throw new HadoopIllegalArgumentException(
              "Invalid buffer, isDirect should be " + isDirectBuffer);
        }
        if (isOutputs) {
          empty.position(0);
          buffer.mark();
          buffer.put(empty);
          buffer.reset();
        }
      }
    }
  }

  /**
   * Check and ensure the buffers are of the length specified by dataLen. If is
   * output buffers, ensure they will be ZEROed.
   * @param buffers the buffers to check
   * @param allowNull whether to allow any element to be null or not
   * @param dataLen the length of data available in the buffer to ensure with
   * @param isOutputs is output buffer or not
   */
  protected void checkParameterBuffers(byte[][] buffers, boolean allowNull,
                                       int dataLen, boolean isOutputs) {
    byte[] empty = getEmptyChunk(dataLen);
    for (byte[] buffer : buffers) {
      if (buffer == null && !allowNull) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer found, not allowing null");
      } else if (buffer != null && buffer.length != dataLen) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer not of length " + dataLen);
      } else if (isOutputs) {
        System.arraycopy(empty, 0, buffer, 0, dataLen);
      }
    }
  }
}
