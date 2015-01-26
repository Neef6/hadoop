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

/**
 * RawErasureCoder performs encoding/decoding given chunks of input data and generates chunks of outputs that
 * corresponds to an erasure code scheme, like XOR and Reed-Solomon.
 *
 * RawErasureCoder is part of ErasureCodec framework, where ErasureCoder is used to encode/decode
 * a group of blocks (BlockGroup) according to the codec specific BlockGroup layout and logic.
 *
 * An ErasureCoder extracts chunks of data from the blocks and can employ various low level
 * RawErasureCoders to perform encoding/decoding against the chunks.
 *
 * To distinguish from ErasureCoder, here RawErasureCoder is used to mean the low level constructs,
 * since it only takes care of the math calculation with a group of byte buffers.
 */
public interface RawErasureCoder {

  /**
   * Initialize with the important parameters for the code.
   * @param dataSize, how many data inputs for the coding
   * @param paritySize, how many parity outputs the coding generates
   * @param chunkSize, the size of the input/output buffer
   */
  public void initialize(int dataSize, int paritySize, int chunkSize);

  /**
   * The number of data inputs for the coding.
   */
  public int dataSize();

  /**
   * The number of parity outputs for the coding.
   */
  public int paritySize();

  /**
   * Should be called when release this coder
   */
  public void release();
}
