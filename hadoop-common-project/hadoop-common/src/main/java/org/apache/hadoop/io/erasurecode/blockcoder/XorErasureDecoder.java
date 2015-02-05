package org.apache.hadoop.io.erasurecode.blockcoder;

import org.apache.hadoop.io.erasurecode.ECBlock;
import org.apache.hadoop.io.erasurecode.ECBlockGroup;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.XorRawDecoder;

/**
 * Xor erasure decoder that decodes a block group.
 *
 * It implements {@link ErasureDecoder}.
 */
public class XorErasureDecoder extends AbstractErasureDecoder {

  @Override
  protected ErasureCodingStep performDecoding(final ECBlockGroup blockGroup) {
    // May be configured
    RawErasureDecoder rawDecoder = new XorRawDecoder();
    rawDecoder.initialize(getNumDataUnits(),
        getNumParityUnits(), getChunkSize());

    ECBlock[] inputBlocks = getInputBlocks(blockGroup);

    return new ErasureDecodingStep(inputBlocks,
        getErasedIndexes(inputBlocks),
        getOutputBlocks(blockGroup), rawDecoder);
  }

}
