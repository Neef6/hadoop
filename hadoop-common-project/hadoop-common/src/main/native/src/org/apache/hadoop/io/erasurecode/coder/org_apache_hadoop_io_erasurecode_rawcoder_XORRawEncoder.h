/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class org_apache_hadoop_io_erasurecode_rawcoder_NativeXORRawEncoder */

#ifndef _Included_org_apache_hadoop_io_erasurecode_rawcoder_NativeXORRawEncoder
#define _Included_org_apache_hadoop_io_erasurecode_rawcoder_NativeXORRawEncoder
#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_apache_hadoop_io_erasurecode_rawcoder_NativeXORRawEncoder
 * Method:    init
 * Signature: (II[I)I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_erasurecode_rawcoder_NativeXORRawEncoder_init
  (JNIEnv *, jclass, jint, jint);

/*
 * Class:     org_apache_hadoop_io_erasurecode_rawcoder_NativeXORRawEncoder
 * Method:    encode
 * Signature: ([Ljava/nio/ByteBuffer;[Ljava/nio/ByteBuffer;I)I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_erasurecode_rawcoder_NativeXORRawEncoder_encode
  (JNIEnv *, jclass, jobjectArray, jobjectArray, jint);

/*
 * Class:     org_apache_hadoop_io_erasurecode_rawcoder_NativeXORRawEncoder
 * Method:    destroy
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_erasurecode_rawcoder_NativeXORRawEncoder_destroy
  (JNIEnv *, jclass);

#ifdef __cplusplus
}
#endif
#endif
