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

package org.apache.hadoop.hdfs.ec.coder.old.impl;

import org.apache.hadoop.hdfs.ec.coder.old.ErasureCode;
import org.apache.hadoop.hdfs.ec.coder.old.impl.help.GaloisField;

public class ReedSolomonCode implements ErasureCode {

  private final int stripeSize;
  private final int paritySize;
  private final int[] generatingPolynomial;
  private final int PRIMITIVE_ROOT = 2;
  private final int[] primitivePower;
  private final GaloisField GF = GaloisField.getInstance();
  private final int[] paritySymbolLocations;
  private final int[] dataBuff;
  private int[] errSignature;

  public ReedSolomonCode(int stripeSize, int paritySize) {
    assert (stripeSize + paritySize < GF.getFieldSize());
    this.stripeSize = stripeSize;
    this.paritySize = paritySize;
    this.errSignature = new int[paritySize];
    this.paritySymbolLocations = new int[paritySize];
    this.dataBuff = new int[paritySize + stripeSize];
    for (int i = 0; i < paritySize; i++) {
      paritySymbolLocations[i] = i;
    }

    this.primitivePower = new int[stripeSize + paritySize];
    // compute powers of the primitive root
    for (int i = 0; i < stripeSize + paritySize; i++) {
      primitivePower[i] = GF.power(PRIMITIVE_ROOT, i);
    }
    // compute generating polynomial
    int[] gen = {1};
    int[] poly = new int[2];
    for (int i = 0; i < paritySize; i++) {
      System.out.println("--------------------------" + i);
      poly[0] = primitivePower[i];
      poly[1] = 1;
      System.out.println("ploy[0]=" + poly[0]);
      gen = GF.multiply(gen, poly);

    }
    // generating polynomial has all generating roots
    generatingPolynomial = gen;

  }

  public static void main(String[] args) {
    ReedSolomonCode rs = new ReedSolomonCode(3, 3);
    int[] message = {3, 13, 9};
    int[] parity = {1, 1, 1};
    rs.encode(message, parity);
    for (int i = 0; i < parity.length; i++) {
      System.out.println("parity[" + i + "]==" + parity[i]);
    }
  }

  @Override
  public void encode(int[] message, int[] parity) {
    assert (message.length == stripeSize && parity.length == paritySize);
    for (int i = 0; i < paritySize; i++) {
      dataBuff[i] = 0;//initialize the parity to 0
    }
    for (int i = 0; i < stripeSize; i++) {
      dataBuff[i + paritySize] = message[i];
    }
    GF.remainder(dataBuff, generatingPolynomial);
    for (int i = 0; i < paritySize; i++) {
      parity[i] = dataBuff[i];
    }
  }

  @Override
  public void decode(int[] data, int[] erasedLocation, int[] erasedValue) {
    if (erasedLocation.length == 0) {
      return;
    }
    assert (erasedLocation.length == erasedValue.length);
    for (int i = 0; i < erasedLocation.length; i++) {
      data[erasedLocation[i]] = 0;
    }
    for (int i = 0; i < erasedLocation.length; i++) {
      errSignature[i] = primitivePower[erasedLocation[i]];
      erasedValue[i] = GF.substitute(data, primitivePower[i]);
    }
    GF.solveVandermondeSystem(errSignature, erasedValue, erasedLocation.length);
  }

  @Override
  public int stripeSize() {
    return this.stripeSize;
  }

  @Override
  public int paritySize() {
    return this.paritySize;
  }

  @Override
  public int symbolSize() {
    return (int) Math.round(Math.log(GF.getFieldSize()) / Math.log(2));//default = 8
  }
}
