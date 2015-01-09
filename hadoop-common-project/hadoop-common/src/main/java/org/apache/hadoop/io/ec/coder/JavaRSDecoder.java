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
package org.apache.hadoop.io.ec.coder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.ec.BlockGroup;
import org.apache.hadoop.io.ec.ECBlock;
import org.apache.hadoop.io.ec.ECChunk;
import org.apache.hadoop.io.ec.SubBlockGroup;
import org.apache.hadoop.io.ec.rawcoder.JavaRSRawDecoder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class JavaRSDecoder extends RSDecoder {

  public JavaRSDecoder(int dataSize, int paritySize, int chunkSize) {
    super(new JavaRSRawDecoder(dataSize, paritySize, chunkSize));
  }

  @Override
  protected void end() {
    //do-nothing
  }
}
