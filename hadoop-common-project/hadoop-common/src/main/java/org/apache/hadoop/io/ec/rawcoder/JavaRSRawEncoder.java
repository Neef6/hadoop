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
package org.apache.hadoop.io.ec.rawcoder;


import org.apache.hadoop.io.ec.rawcoder.util.GaloisField;
import org.apache.hadoop.io.ec.rawcoder.util.RSUtil;

import java.nio.ByteBuffer;

/**
 * A raw erasure encoder in pure Java for test usage and also in case native one isn't available in some environment.
 */
public class JavaRSRawEncoder extends AbstractRawErasureEncoder {
  private GaloisField GF = GaloisField.getInstance();
  private int[] generatingPolynomial;

  public JavaRSRawEncoder(int dataSize, int paritySize, int chunkSize) {
    super(dataSize, paritySize, chunkSize);
    init();
  }

  private void init() {
    assert (dataSize() + paritySize() < GF.getFieldSize());
    int[] primitivePower = RSUtil.getPrimitivePower(dataSize(), paritySize());
    // compute generating polynomial
    int[] gen = {1};
    int[] poly = new int[2];
    for (int i = 0; i < paritySize(); i++) {
      poly[0] = primitivePower[i];
      poly[1] = 1;
      gen = GF.multiply(gen, poly);
    }
    // generating polynomial has all generating roots
    generatingPolynomial = gen;
  }

  @Override
  protected void doEncode(ByteBuffer[] inputs, ByteBuffer[] outputs) {
    ByteBuffer[] data = new ByteBuffer[dataSize() + paritySize()];
    for (int i = 0; i < paritySize(); i++) {
      data[i] = outputs[i];
    }
    for (int i = 0; i < dataSize(); i++) {
      data[i + paritySize()] = inputs[i];
    }

    // Compute the remainder
    GF.remainder(data, generatingPolynomial);
  }

  @Override
  protected void doEncode(byte[][] inputs, byte[][] outputs) {
    byte[][] data = new byte[dataSize() + paritySize()][];
    for (int i = 0; i < paritySize(); i++) {
      data[i] = outputs[i];
    }
    for (int i = 0; i < dataSize(); i++) {
      data[i + paritySize()] = inputs[i];
    }

    // Compute the remainder
    GF.remainder(data, generatingPolynomial);
  }
}
