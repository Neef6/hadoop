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
package org.apache.hadoop.io.erasurecode.blockcoder;

import org.apache.hadoop.io.erasurecode.rawcoder.TestRawCoderBase;
import org.apache.hadoop.io.erasurecode.rawcoder.XorRawDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.XorRawEncoder;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

/**
 * Test XOR encoding and decoding.
 */
public class TestXorErasureCoder extends TestErasureCoderBase {

  @Before
  public void setup() {
    this.encoderClass = XorErasureEncoder.class;
    this.decoderClass = XorErasureDecoder.class;

    this.numDataUnits = 10;
    this.numParityUnits = 1;
    this.erasedIndexes = new int[] {0};

    this.numChunksInBlock = 10;
  }

  @Test
  public void testCodingNoDirectBuffer() {
    testCoding(false);
  }

  //@Test
  public void testCodingDirectBuffer() {
    testCoding(true);
  }

}
