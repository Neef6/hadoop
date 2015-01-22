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

import org.apache.hadoop.io.ec.ECChunk;

import java.nio.ByteBuffer;

/**
 * Raw Erasure Encoder that corresponds to an erasure code algorithm
 */
public interface RawErasureEncoder {

  /**
   * Encode with inputs and generates outputs
   * @param inputs
   * @param outputs
   */
  public void encode(ByteBuffer[] inputs, ByteBuffer[] outputs);

  /**
   * Encode with inputs and generates outputs
   * @param inputs
   * @param outputs
   */
  public void encode(byte[][] inputs, byte[][] outputs);

  /**
   * Encode with inputs and generates outputs
   * @param inputs
   * @param outputs
   */
  public void encode(ECChunk[] inputs, ECChunk[] outputs);

  /**
   * The number of data elements in the code.
   */
  public int dataSize();

  /**
   * The number of parity elements in the code.
   */
  public int paritySize();

  /**
   * Should be called when release this coder
   */
  public void clean();
}
