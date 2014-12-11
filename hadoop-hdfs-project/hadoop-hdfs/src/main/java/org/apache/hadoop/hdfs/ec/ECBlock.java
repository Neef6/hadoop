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
package org.apache.hadoop.hdfs.ec;

import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.ec.coder.ErasureCoderCallback;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

public class ECBlock {

  private ExtendedBlockId blockId;
  private boolean isParity;
  private boolean isMissing;

  public ECBlock(ExtendedBlockId blockId, boolean isParity) {
    this.blockId = blockId;
    this.isParity = isParity;
  }
  
  public long getBlockId() {
	  return blockId.getBlockId();
  }

  public String getBlockPoolId() {
	  return blockId.getBlockPoolId();
  }

  public ExtendedBlockId getExtendedBlockId() {
    return blockId;
  }

  public void setMissing(boolean isMissing) {
    this.isMissing = isMissing;
  }

  public boolean isParity() {
    return isParity;
  }

  public boolean isMissing() {
    return isMissing;
  }

  /**
   * Below info are to be provided
   */
  public StorageType getStorageType() {
    //TODO
    return null;
  }

  public DatanodeInfo getDatanodeInfo() {
    //TODO
    return null;
  }

  public ExtendedBlock getExtendedBlock() {
    //TODO
    return null;
  }
}
