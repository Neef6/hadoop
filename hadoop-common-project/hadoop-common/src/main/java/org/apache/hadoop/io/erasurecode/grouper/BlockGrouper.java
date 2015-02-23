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
package org.apache.hadoop.io.ec.grouper;


import org.apache.hadoop.io.ec.*;

import java.util.List;

/**
 * As part of a codec, to handle how to form a block group for encoding
 * and how to recover a missing block from a block group
 */
public abstract class BlockGrouper {

  private ECSchema schema;

  public void initWith(ECSchema schema) {
    this.schema = schema;
  }

  /**
   * Get required data blocks count in a BlockGroup,
   * to be called by ECManager when calculating BlockGroup.
   * @return
   */
  public int getDataBlocks() {
    return schema.getDataBlocks();
  }

  /**
   * Get required parity blocks count in a BlockGroup,
   * to be called by ECManager when calculating BlockGroup.
   * @return
   */
  public int getParityBlocks() {
    return schema.getParityBlocks();
  }

  /**
   * Calculating and organizing BlockGroup, to be called by ECManager
   * @param dataBlocks Data blocks to compute parity blocks against
   * @param parityBlocks To be computed parity blocks
   * @return
   */
  public abstract BlockGroup makeBlockGroup(List<? extends ECBlockId> dataBlocks,
                                            List<? extends ECBlockId> parityBlocks);

  /**
   * Given a BlockGroup, tell if any of the missing blocks can be recovered,
   * to be called by ECManager
   * @param blockGroup a blockGroup that may contain missing blocks but not sure
   *                   recoverable or not
   * @return
   */
  public boolean anyRecoverable(BlockGroup blockGroup) {
    int missingCount = 0;
    for (SubBlockGroup subBlockGroup : blockGroup.getSubGroups()) {
      for (ECBlock dataECBlock : subBlockGroup.getDataBlocks()) {
        if (dataECBlock.isMissing()) missingCount++;
      }

      for (ECBlock parityECBlock : subBlockGroup.getParityBlocks()) {
        if (parityECBlock.isMissing()) missingCount++;
      }
    }

    return missingCount <= getParityBlocks();
  }

  /**
   * Given a BlockGroup with missing block(s), construct a recoverable group.
   * The result group may be the same with the original one, but may be not and
   * rearranged according to the codec; it can be a subset of the original one;
   * and to make the decoding work convenient, BlockGroup can be extended
   * to contain extra annotations or hints for the coder to recognize the context.
   *
   * @param blockGroup original blockGroup that contains missing blocks
   * @return a recoverable blockGroup just for recovery usage
   */
  public abstract BlockGroup makeRecoverableGroup(BlockGroup blockGroup);

  protected ECSchema getSchema() {
    return schema;
  }

  protected ECBlock[] convert(List<? extends ECBlockId> blockIds, boolean isParity) {
    ECBlock[] blocks = new ECBlock[blockIds.size()];
    for (int i = 0; i < blockIds.size(); i++) {
      blocks[i] = new ECBlock(blockIds.get(i), isParity);
    }
    return blocks;
  }
}
