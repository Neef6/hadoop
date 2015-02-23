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
package org.apache.hadoop.io.ec.codec;


import org.apache.hadoop.io.ec.coder.ErasureDecoder;
import org.apache.hadoop.io.ec.coder.ErasureEncoder;
import org.apache.hadoop.io.ec.coder.IsaLRCDecoder;
import org.apache.hadoop.io.ec.coder.IsaLRCEncoder;

/**
 * LRC codec implemented using ISA library
 */
public class IsaLRCErasureCodec extends LRCErasureCodec {


  @Override
  public ErasureEncoder createEncoder() {
    return new IsaLRCEncoder(getSchema().getDataBlocks(), getSchema().getParityBlocks(), getSchema().getChunkSize());
  }

  @Override
  public ErasureDecoder createDecoder() {
    return new IsaLRCDecoder(getSchema().getDataBlocks(), getSchema().getParityBlocks(), getSchema().getChunkSize());
  }
}
