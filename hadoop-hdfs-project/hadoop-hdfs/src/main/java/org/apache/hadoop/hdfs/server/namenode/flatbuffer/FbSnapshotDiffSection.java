// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.namenode.flatbuffer;
import java.nio.*;
import java.lang.*;

import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class FbSnapshotDiffSection extends Table {
  public static FbSnapshotDiffSection getRootAsFbSnapshotDiffSection(ByteBuffer _bb) { return getRootAsFbSnapshotDiffSection(_bb, new FbSnapshotDiffSection()); }
  public static FbSnapshotDiffSection getRootAsFbSnapshotDiffSection(ByteBuffer _bb, FbSnapshotDiffSection obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public FbSnapshotDiffSection __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }


  public static void startFbSnapshotDiffSection(FlatBufferBuilder builder) { builder.startObject(0); }
  public static int endFbSnapshotDiffSection(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishFbSnapshotDiffSectionBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }

};

