// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.namenode.flatbuffer;

import java.nio.*;
import java.lang.*;

import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class FbFileSummary extends Table {
  public static FbFileSummary getRootAsFbFileSummary(ByteBuffer _bb) { return getRootAsFbFileSummary(_bb, new FbFileSummary()); }
  public static FbFileSummary getRootAsFbFileSummary(ByteBuffer _bb, FbFileSummary obj) {

      _bb.order(ByteOrder.LITTLE_ENDIAN);
      return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb));

  }


  public FbFileSummary __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public long ondiskVersion() { int o = __offset(4); return o != 0 ? (long)bb.getInt(o + bb_pos) & 0xFFFFFFFFL : 0; }
  public long layoutVersion() { int o = __offset(6); return o != 0 ? (long)bb.getInt(o + bb_pos) & 0xFFFFFFFFL : 0; }
  public String codec() { int o = __offset(8); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer codecAsByteBuffer() { return __vector_as_bytebuffer(8, 1); }
  public FbSection sections(int j) { return sections(new FbSection(), j); }
  public FbSection sections(FbSection obj, int j) { int o = __offset(10); return o != 0 ? obj.__init(__indirect(__vector(o) + j * 4), bb) : null; }
  public int sectionsLength() { int o = __offset(10); return o != 0 ? __vector_len(o) : 0; }

  public static int createFbFileSummary(FlatBufferBuilder builder,
      long ondiskVersion,
      long layoutVersion,
      int codec,
      int sections) {
    builder.startObject(4);
    FbFileSummary.addSections(builder, sections);
    FbFileSummary.addCodec(builder, codec);
    FbFileSummary.addLayoutVersion(builder, layoutVersion);
    FbFileSummary.addOndiskVersion(builder, ondiskVersion);
    return FbFileSummary.endFbFileSummary(builder);
  }

  public static void startFbFileSummary(FlatBufferBuilder builder) { builder.startObject(4); }
  public static void addOndiskVersion(FlatBufferBuilder builder, long ondiskVersion) { builder.addInt(0, (int)(ondiskVersion & 0xFFFFFFFFL), 0); }
  public static void addLayoutVersion(FlatBufferBuilder builder, long layoutVersion) { builder.addInt(1, (int)(layoutVersion & 0xFFFFFFFFL), 0); }
  public static void addCodec(FlatBufferBuilder builder, int codecOffset) { builder.addOffset(2, codecOffset, 0); }
  public static void addSections(FlatBufferBuilder builder, int sectionsOffset) { builder.addOffset(3, sectionsOffset, 0); }
  public static int createSectionsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startSectionsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endFbFileSummary(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishFbFileSummaryBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }

};

