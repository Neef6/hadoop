/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class org_apache_hadoop_io_erasurecode_rawcoder_ISARSRawDecoder */

#ifndef _Included_org_apache_hadoop_io_erasurecode_rawcoder_ISARSRawDecoder
#define _Included_org_apache_hadoop_io_erasurecode_rawcoder_ISARSRawDecoder
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     org_apache_hadoop_io_erasurecode_rawcoder_ISARSRawDecoder
 * Method:    loadLib
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_erasurecode_rawcoder_ISARSRawDecoder_loadLib
  (JNIEnv *, jclass);

/*
 * Class:     org_apache_hadoop_io_erasurecode_rawcoder_ISARSRawDecoder
 * Method:    init
 * Signature: (II[I)I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_erasurecode_rawcoder_ISARSRawDecoder_init
  (JNIEnv *, jclass, jint, jint, jintArray);

/*
 * Class:     org_apache_hadoop_io_erasurecode_rawcoder_ISARSRawDecoder
 * Method:    decode
 * Signature: ([Ljava/nio/ByteBuffer;[II)I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_erasurecode_rawcoder_ISARSRawDecoder_decode
  (JNIEnv *, jclass, jobjectArray, jintArray, jint);

/*
 * Class:     org_apache_hadoop_io_erasurecode_rawcoder_ISARSRawDecoder
 * Method:    destroy
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_erasurecode_rawcoder_ISARSRawDecoder_destroy
  (JNIEnv *, jclass);

#ifdef __cplusplus
}
#endif
#endif
