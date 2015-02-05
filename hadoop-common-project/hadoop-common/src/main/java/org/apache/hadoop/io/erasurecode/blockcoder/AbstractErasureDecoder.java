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

import org.apache.hadoop.io.erasurecode.ECBlock;
import org.apache.hadoop.io.erasurecode.ECGroup;

/**
 * An abstract erasure decoder that's to be inherited by new decoders.
 *
 * It implements the {@link ErasureDecoder} interface.
 */
public abstract class AbstractErasureDecoder extends AbstractErasureCoder
    implements ErasureDecoder {

  @Override
  public CodingStep decode(ECGroup group) {
    return performDecoding(group);
  }

  protected abstract CodingStep performDecoding(ECGroup group);

  /**
   * We have all the data blocks and parity blocks as input blocks for
   * recovering by default.
   * @param blockGroup
   * @return
   */
  protected ECBlock[] getInputBlocks(ECGroup blockGroup) {
    ECBlock[] inputBlocks = new ECBlock[getNumParityUnits()
        + getNumDataUnits()];

    int idx = 0;
    for (int i = 0; i < getNumParityUnits(); i++) {
      inputBlocks[idx ++] = blockGroup.getParityBlocks()[i];
    }
    for (int i = 0; i < getNumDataUnits(); i++) {
      inputBlocks[idx ++] = blockGroup.getDataBlocks()[i];
    }

    return inputBlocks;
  }

  /**
   * Which blocks were erased ? Then have those blocks as output block because
   * we attempt to recover them in most often cases by default.
   * @param blockGroup
   * @return
   */
  protected ECBlock[] getOutputBlocks(ECGroup blockGroup) {
    ECBlock[] outputBlocks = new ECBlock[getNumErasedBlocks(blockGroup)];

    int idx = 0;
    for (int i = 0; i < getNumParityUnits(); i++) {
      if (blockGroup.getParityBlocks()[i].isErased()) {
        outputBlocks[idx ++] = blockGroup.getParityBlocks()[i];
      }
    }

    for (int i = 0; i < getNumDataUnits(); i++) {
      if (blockGroup.getDataBlocks()[i].isErased()) {
        outputBlocks[idx ++] = blockGroup.getDataBlocks()[i];
      }
    }

    return outputBlocks;
  }

  protected int getNumErasedBlocks(ECGroup blockGroup) {
    int num = 0;
    for (int i = 0; i < getNumParityUnits(); i++) {
      if (blockGroup.getParityBlocks()[i].isErased()) {
        num ++;
      }
    }
    for (int i = 0; i < getNumDataUnits(); i++) {
      if (blockGroup.getDataBlocks()[i].isErased()) {
        num ++;
      }
    }

    return num;
  }

  /**
   * Find out how many blocks are erased.
   * @param inputBlocks all the input blocks
   * @return number of erased blocks
   */
  protected static int getNumErasedBlocks(ECBlock[] inputBlocks) {
    int numErased = 0;
    for (int i = 0; i < inputBlocks.length; i++) {
      if (inputBlocks[i].isErased()) {
        numErased ++;
      }
    }

    return numErased;
  }

  /**
   * Get indexes of erased blocks from inputBlocks
   * @param inputBlocks
   * @return indexes of erased blocks from inputBlocks
   */
  protected int[] getErasedIndexes(ECBlock[] inputBlocks) {
    int numErased = getNumErasedBlocks(inputBlocks);
    if (numErased == 0) {
      return new int[0];
    }

    int[] erasedIndexes = new int[numErased];
    int j = 0;
    for (int i = 0; i < inputBlocks.length; i++) {
      if (inputBlocks[i].isErased()) {
        erasedIndexes[j ++] = i;
      }
    }

    return erasedIndexes;
  }

  /**
   * Get erased input blocks from inputBlocks
   * @param inputBlocks
   * @return an array of erased blocks from inputBlocks
   */
  protected ECBlock[] getErasedBlocks(ECBlock[] inputBlocks) {
    int numErased = getNumErasedBlocks(inputBlocks);
    if (numErased == 0) {
      return new ECBlock[0];
    }

    ECBlock[] erasedBlocks = new ECBlock[numErased];
    for (int i = 0; i < inputBlocks.length; i++) {
      if (inputBlocks[i].isErased()) {
        erasedBlocks[i] = inputBlocks[i];
      }
    }

    return erasedBlocks;
  }

}
