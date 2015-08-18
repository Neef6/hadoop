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
import org.apache.hadoop.io.erasurecode.rawcoder.util.DumpUtil;
import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil2;
import org.apache.hadoop.io.erasurecode.rawcoder.util.GF256;
import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A raw erasure decoder in RS code scheme in pure Java in case native one
 * isn't available in some environment. Please always use native implementations
 * when possible. This new Java coder is about 5X faster than the one originated
 * from HDFS-RAID, and also compatible with the native/ISA-L coder.
 */
public class RSRawDecoder2 extends AbstractRawErasureDecoder {
  private byte[] encodeMatrix;

  private byte[] decodeMatrix;
  private byte[] invertMatrix;
  private byte[] tmpMatrix;
  private byte[] gftbls;
  private int[] erasedIndexes;
  private int[] validIndexes;
  private int numErasedDataUnits;
  private boolean[] erasureFlags;

  public RSRawDecoder2(int numDataUnits, int numParityUnits) {
    super(numDataUnits, numParityUnits);
    if (numDataUnits + numParityUnits >= RSUtil.GF.getFieldSize()) {
      throw new HadoopIllegalArgumentException(
              "Invalid numDataUnits and numParityUnits");
    }

    GF256.init();

    int numAllUnits = numDataUnits + numParityUnits;
    encodeMatrix = new byte[numAllUnits * numDataUnits];
    RSUtil2.genCauchyMatrix(encodeMatrix, numAllUnits, numDataUnits);
    DumpUtil.dumpMatrix(encodeMatrix, numDataUnits, numAllUnits);
  }

  @Override
  protected void doDecode(ByteBuffer[] inputs, int[] erasedIndexes,
                          ByteBuffer[] outputs) {
    prepareDecoding(inputs, erasedIndexes);

    ByteBuffer[] realInputs = new ByteBuffer[numDataUnits];
    for (int i = 0; i < numDataUnits; i++) {
      realInputs[i] = inputs[validIndexes[i]];
    }
    RSUtil2.encodeData(gftbls, realInputs, outputs);
  }

  @Override
  protected void doDecode(byte[][] inputs, int[] inputOffsets,
                          int dataLen, int[] erasedIndexes,
                          byte[][] outputs, int[] outputOffsets) {
    prepareDecoding(inputs, erasedIndexes);

    byte[][] realInputs = new byte[numDataUnits][];
    int[] realInputOffsets = new int[numDataUnits];
    for (int i = 0; i < numDataUnits; i++) {
      realInputs[i] = inputs[validIndexes[i]];
      realInputOffsets[i] = inputOffsets[validIndexes[i]];
    }
    RSUtil2.encodeData(gftbls, dataLen, realInputs, realInputOffsets,
        outputs, outputOffsets);
  }

  private <T> void prepareDecoding(T[] inputs, int[] erasedIndexes) {
    int[] tmpValidIndexes = new int[numDataUnits];
    makeValidIndexes(inputs, tmpValidIndexes);
    if (Arrays.equals(this.erasedIndexes, erasedIndexes) &&
        Arrays.equals(this.validIndexes, tmpValidIndexes)) {
      return; // Optimization. Nothing to do
    }
    this.erasedIndexes = Arrays.copyOf(erasedIndexes, erasedIndexes.length);
    this.validIndexes = Arrays.copyOf(tmpValidIndexes, tmpValidIndexes.length);

    processErasures(erasedIndexes);
  }

  private void processErasures(int[] erasedIndexes) {
    this.decodeMatrix = new byte[numAllUnits * numDataUnits];
    this.tmpMatrix = new byte[numAllUnits * numDataUnits];
    this.invertMatrix = new byte[numAllUnits * numDataUnits];
    this.gftbls = new byte[numAllUnits * getNumDataUnits() * 32];

    this.erasureFlags = new boolean[numAllUnits];
    this.numErasedDataUnits = 0;

    for (int i = 0; i < erasedIndexes.length; i++) {
      int index = erasedIndexes[i];
      erasureFlags[index] = true;
      if (index < numDataUnits) {
        numErasedDataUnits++;
      }
    }

    generateDecodeMatrix(erasedIndexes);

    RSUtil2.initTables(numDataUnits, erasedIndexes.length, decodeMatrix, 0,
        gftbls);
    //System.out.println(DumpUtil.bytesToHex(gftbls, 9999999));
  }

  // Generate decode matrix from encode matrix
  private void generateDecodeMatrix(int[] erasedIndexes) {
    int i, j, r, p;
    byte s;

    // Construct matrix tmpMatrix by removing error rows
    for (i = 0; i < numDataUnits; i++) {
      r = validIndexes[i];
      for (j = 0; j < numDataUnits; j++) {
        tmpMatrix[numDataUnits * i + j] = encodeMatrix[numDataUnits * r + j];
      }
    }

    GF256.gfInvertMatrix(tmpMatrix, invertMatrix, numDataUnits);

    for (i = 0; i < numErasedDataUnits; i++) {
      for (j = 0; j < numDataUnits; j++) {
          decodeMatrix[numDataUnits * i + j] =
              invertMatrix[numDataUnits * erasedIndexes[i] + j];
      }
    }

    for (p = numErasedDataUnits; p < erasedIndexes.length; p++) {
      for (i = 0; i < numDataUnits; i++) {
        s = 0;
        for (j = 0; j < numDataUnits; j++) {
          s ^= GF256.gfMul(invertMatrix[j * numDataUnits + i],
              encodeMatrix[numDataUnits * erasedIndexes[p] + j]);
        }
        decodeMatrix[numDataUnits * p + i] = s;
      }
    }
  }

}
