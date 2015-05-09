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

import org.apache.hadoop.io.erasurecode.ECChunk;

import java.nio.ByteBuffer;

/**
 * An abstract raw erasure decoder that's to be inherited by new decoders.
 *
 * It implements the {@link RawErasureDecoder} interface.
 */
public abstract class AbstractRawErasureDecoder extends AbstractRawErasureCoder
    implements RawErasureDecoder {

  @Override
  public void decode(ByteBuffer[] inputs, int[] erasedIndexes,
                     ByteBuffer[] outputs) {
    checkParameters(inputs, erasedIndexes, outputs);

    boolean usingDirectBuffer = ! inputs[0].hasArray();
    if (usingDirectBuffer) {
      doDecode(inputs, erasedIndexes, outputs);
      return;
    }

    int[] inputOffsets = new int[inputs.length];
    int inputLen = inputs[0].remaining();
    int[] outputOffsets = new int[outputs.length];
    byte[][] newInputs = new byte[inputs.length][];
    byte[][] newOutputs = new byte[outputs.length][];

    ByteBuffer buffer;
    for (int i = 0; i < inputs.length; ++i) {
      buffer = inputs[i];
      inputOffsets[i] = buffer.position();
      newInputs[i] = buffer.array();
    }

    for (int i = 0; i < outputs.length; ++i) {
      buffer = outputs[i];
      outputOffsets[i] = buffer.position();
      newOutputs[i] = buffer.array();
    }

    doDecode(newInputs, inputOffsets, inputLen,
        erasedIndexes, newOutputs, outputOffsets);
  }

  /**
   * Perform the real decoding using Direct ByteBuffer.
   * @param inputs Direct ByteBuffers expected
   * @param erasedIndexes
   * @param outputs Direct ByteBuffers expected
   */
  protected abstract void doDecode(ByteBuffer[] inputs, int[] erasedIndexes,
                                   ByteBuffer[] outputs);

  @Override
  public void decode(byte[][] inputs, int[] erasedIndexes, byte[][] outputs) {
    checkParameters(inputs, erasedIndexes, outputs);

    int[] inputOffsets = new int[inputs.length]; // ALL ZERO
    int inputLen = inputs[0].length;
    int[] outputOffsets = new int[outputs.length]; // ALL ZERO

    doDecode(inputs, inputOffsets, inputLen, erasedIndexes, outputs,
        outputOffsets);
  }

  /**
   * Perform the real decoding using bytes array, supporting offsets and lengths.
   * @param inputs
   * @param inputOffsets
   * @param inputLen
   * @param erasedIndexes
   * @param outputs
   * @param outputOffsets
   */
  protected abstract void doDecode(byte[][] inputs, int[] inputOffsets,
                                   int inputLen, int[] erasedIndexes,
                                   byte[][] outputs, int[] outputOffsets);

  @Override
  public void decode(ECChunk[] inputs, int[] erasedIndexes,
                     ECChunk[] outputs) {
    ByteBuffer[] newInputs = ECChunk.toBuffers(inputs);
    ByteBuffer[] newOutputs = ECChunk.toBuffers(outputs);
    decode(newInputs, erasedIndexes, newOutputs);
  }

  /**
   * Check and validate decoding parameters, throw exception accordingly. The
   * checking assumes it's a MDS code. Other code  can override this.
   * @param inputs
   * @param erasedIndexes
   * @param outputs
   */
  protected void checkParameters(Object[] inputs, int[] erasedIndexes,
                                 Object[] outputs) {
    if (inputs.length != getNumParityUnits() + getNumDataUnits()) {
      throw new IllegalArgumentException("Invalid inputs length");
    }

    if (erasedIndexes.length != outputs.length) {
      throw new IllegalArgumentException(
          "erasedIndexes and outputs mismatch in length");
    }

    if (erasedIndexes.length > getNumParityUnits()) {
      throw new IllegalArgumentException(
          "Too many erased, not recoverable");
    }
  }
}
