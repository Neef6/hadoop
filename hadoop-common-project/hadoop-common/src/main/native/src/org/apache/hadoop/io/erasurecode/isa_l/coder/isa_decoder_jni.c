/**********************************************************************
INTEL CONFIDENTIAL
Copyright 2012 Intel Corporation All Rights Reserved.

The source code contained or described herein and all documents
related to the source code ("Material") are owned by Intel Corporation
or its suppliers or licensors. Title to the Material remains with
Intel Corporation or its suppliers and licensors. The Material may
contain trade secrets and proprietary and confidential information of
Intel Corporation and its suppliers and licensors, and is protected by
worldwide copyright and trade secret laws and treaty provisions. No
part of the Material may be used, copied, reproduced, modified,
published, uploaded, posted, transmitted, distributed, or disclosed in
any way without Intel's prior express written permission.

No license under any patent, copyright, trade secret or other
intellectual property right is granted to or conferred upon you by
disclosure or delivery of the Materials, either expressly, by
implication, inducement, estoppel or otherwise. Any license under such
intellectual property rights must be express and approved by Intel in
writing.

Unless otherwise agreed by Intel in writing, you may not remove or
alter this notice or any other notice embedded in Materials by Intel
or Intel's suppliers or licensors in any way.
**********************************************************************/

#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>  // for memset, memcmp

#include "../include/erasure_code.h"
#include "../include/types.h"
#include "../include/gf_vect_mul.h"
#include "org_apache_hadoop_io_erasurecode_rawcoder_ISARSRawDecoder.h"
#include <jni.h>
#include <pthread.h>
#include <signal.h>
#include <dlfcn.h>
#include "config.h"

#define MMAX 30
#define KMAX 20

typedef unsigned char u8;

pthread_key_t de_key;
//extern pthread_key_t en_key;
static pthread_once_t key_once = PTHREAD_ONCE_INIT;

typedef struct _codec_parameter{
    int paritySize;
    int stripeSize;
    u8 a[MMAX*KMAX];
    u8 b[MMAX*KMAX];
    u8 c[MMAX*KMAX];
    u8 d[MMAX*KMAX];
    u8 e[MMAX*KMAX];
    u8 g_tbls[MMAX*KMAX*32];
    u8 * recov[MMAX];
    u8 *temp_buffs[MMAX];
    u8 ** data;
    u8 ** code;
    jobject * datajbuf;
    int * erasured;
}Codec_Parameter;

static void (*dlsym_ec_init_tables)(int, int, unsigned char*, unsigned char*);
static void (*dlsym_ec_encode_data)(int, int, int, unsigned char*, unsigned char*, unsigned char*);

static void make_key(){
    (void) pthread_key_create(&de_key, NULL);
//    (void) pthread_key_create(&en_key, NULL);
}



JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_ec_rawcoder_IsaRSRawDecoder_loadLib
  (JNIEnv *env, jclass myclass) {
    // Load liberasure_code.so
  void *libec = dlopen(HADOOP_ERASURECODE_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
  if (!libec) {
    char msg[1000];
    snprintf(msg, 1000, "%s (%s)!", "Cannot load " HADOOP_ERASURECODE_LIBRARY, dlerror());
    THROW(env, "java/lang/UnsatisfiedLinkError", msg);
    return 0;
  }

  dlerror();                                 // Clear any existing error
  LOAD_DYNAMIC_SYMBOL(dlsym_ec_init_tables, env, libec, "ec_init_tables");
  LOAD_DYNAMIC_SYMBOL(dlsym_ec_encode_data, env, libec, "ec_encode_data");
  return 0;
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_ec_rawcoder_IsaRSRawDecoder_init
(JNIEnv *env, jclass myclass, jint stripeSize, jint paritySize, jintArray matrix) {
        fprintf(stdout, "[Decoder init]before init.\n");
        Codec_Parameter * pCodecParameter = NULL;
        jint * jmatrix = NULL;

        pthread_once(&key_once, make_key);

        if(NULL == (pCodecParameter = (Codec_Parameter *)pthread_getspecific(de_key))){
            pCodecParameter = (Codec_Parameter *)malloc(sizeof(Codec_Parameter));
            if(!pCodecParameter){
                fprintf(stderr, "[Decoder init]Out of memory in ISA decoder init\n");
                return -1;
            }

            if (stripeSize > KMAX || paritySize > (MMAX - KMAX)){
                fprintf(stderr, "[Decoder init]max stripe size is %d and max parity size is %d\n", KMAX, MMAX - KMAX);
                return -2;
            }

            int totalSize = paritySize + stripeSize;
            pCodecParameter->paritySize = paritySize;
            pCodecParameter->stripeSize = stripeSize;
            pCodecParameter->data = (u8 **)malloc(sizeof(u8 *) * (stripeSize + paritySize));
            pCodecParameter->code = (u8 **)malloc(sizeof(u8 *) * (paritySize));
            pCodecParameter->datajbuf = (jobject *)malloc(sizeof(jobject) * (stripeSize + paritySize));
            pCodecParameter->erasured = (int *)malloc(sizeof(int) * (stripeSize + paritySize));


            //gf_mk_field();
            //gf_gen_rs_matrix(pCodecParameter->a, totalSize, stripeSize);
            int i, j;
            jmatrix = (*env)->GetIntArrayElements(env, matrix, JNI_FALSE);
            memset(pCodecParameter->a, 0, stripeSize*totalSize);
            for(i=0; i<stripeSize; i++){
                 pCodecParameter->a[stripeSize*i + i] = 1;
            }
            for(i=stripeSize; i<totalSize; i++){
                for(j=0; j<stripeSize; j++){
                   pCodecParameter->a[stripeSize*i+j] = jmatrix[stripeSize*(i-stripeSize)+j];
                   //printf(".....=%d\n",pCodecParameter->a[stripeSize*i+j]);
                }
            }
               
            dlsym_ec_init_tables(stripeSize, paritySize, &(pCodecParameter->a)[stripeSize * stripeSize], pCodecParameter->g_tbls);
            (void) pthread_setspecific(de_key, pCodecParameter);
        }
        fprintf(stdout, "[Decoder init]init success.\n");
        return 0;
 }

JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_ec_rawcoder_IsaRSRawDecoder_decode
  (JNIEnv *env, jclass myclass, jobjectArray alldata, jintArray erasures, jint chunkSize) {
      fprintf(stderr, "[Decoding]before decode.\n");
      Codec_Parameter * pCodecParameter = NULL;
      pthread_once(&key_once, make_key);
      jboolean isCopy;

      int * erasured = NULL;
      int i, j, p, r, k, errorlocation;
      int alldataLen = (*env)->GetArrayLength(env, alldata);
      int erasureLen = (*env)->GetArrayLength(env, erasures);
      int src_error_list[erasureLen];
      u8 s;

      // Check all the parameters.

      if(NULL == (pCodecParameter = (Codec_Parameter *)pthread_getspecific(de_key))){
          fprintf(stderr, "[Decoding]ReedSolomonDecoder DE not initilized!\n");
          return -3;
      }

      if(erasureLen > pCodecParameter->paritySize){
          fprintf(stderr, "[Decoding]Too many erasured data!\n");
          return -4;
      }

      if(alldataLen != pCodecParameter->stripeSize + pCodecParameter->paritySize){
          fprintf(stderr, "[Decoding]Wrong data and parity data size.\n");
          return -5;
      }

      for(j = 0; j < pCodecParameter->stripeSize + pCodecParameter->paritySize; j++){
          pCodecParameter->erasured[j] = -1;
      }

      int * tmp = (int *)(*env)->GetIntArrayElements(env, erasures, &isCopy);
      int parityErrors = 0;

      for(j = 0; j < erasureLen; j++){
          if (tmp[j] >= pCodecParameter->paritySize) {  // errors in parity will not be fixed
              errorlocation = tmp[j] - pCodecParameter->paritySize;
              pCodecParameter->erasured[errorlocation] = 1;
          }
          else if (tmp[j] >= 0){ // put error parity postion
              pCodecParameter->erasured[tmp[j] + pCodecParameter->stripeSize] = 1;
              parityErrors++;
          }
      }
      
      // make the src_error_list in the right order
      for(j = 0, r = 0; j < pCodecParameter->paritySize + pCodecParameter->stripeSize; j++ ) {
          if(pCodecParameter->erasured[j] == 1)    src_error_list[r++] = j ;
      }

      for(j = pCodecParameter->paritySize, r = 0, i = 0; j < pCodecParameter->paritySize + pCodecParameter->stripeSize; j++){
          pCodecParameter->datajbuf[j] = (*env)->GetObjectArrayElement(env, alldata, j);
          pCodecParameter->data[j] = (u8 *)(*env)->GetDirectBufferAddress(env, pCodecParameter->datajbuf[j]);

          if(pCodecParameter->erasured[j - pCodecParameter->paritySize] == -1){
               pCodecParameter->recov[r++] = pCodecParameter->data[j];
          }
          else{
               pCodecParameter->code[i++] = pCodecParameter->data[j];
          }
      }

        //first parity length elements in alldata are saving parity data
      for (j = 0; j < pCodecParameter->paritySize ; j++){
          pCodecParameter->datajbuf[j] = (*env)->GetObjectArrayElement(env, alldata, j);
          pCodecParameter->data[j] = (u8 *)(*env)->GetDirectBufferAddress(env, pCodecParameter->datajbuf[j]);

          if(pCodecParameter->erasured[j + pCodecParameter->stripeSize] == -1) {
              pCodecParameter->recov[r++] = pCodecParameter->data[j];
          } else {
              pCodecParameter->code[i++] = pCodecParameter->data[j];
          }
      }
     
      for(i = 0, r = 0; i < pCodecParameter->stripeSize; i++, r++){
          while(pCodecParameter->erasured[r] == 1) r++;
            for(j = 0; j < pCodecParameter->stripeSize; j++){
                 pCodecParameter->b[pCodecParameter->stripeSize * i + j] = 
                            pCodecParameter->a[pCodecParameter->stripeSize * r + j];
            }
      }

      //Construct d, the inverted matrix.

      if(gf_invert_matrix(pCodecParameter->b, pCodecParameter->d, pCodecParameter->stripeSize) < 0){
          fprintf(stderr, "[Decoding]BAD MATRIX!\n");
          return -6;
      }
      int srcErrors = erasureLen - parityErrors;

      for(i = 0; i < srcErrors; i++){
          for(j = 0; j < pCodecParameter->stripeSize; j++){

              //store all the erasured line numbers's to the c. 
              pCodecParameter->c[pCodecParameter->stripeSize * i + j] = 
                    pCodecParameter->d[pCodecParameter->stripeSize * src_error_list[i] + j];
          }
      }

      // recover data
      for(i = srcErrors, p = 0; i < erasureLen; i++, p++) {
         for(j = 0; j < pCodecParameter->stripeSize; j++)  {
            pCodecParameter->e[pCodecParameter->stripeSize * p + j] = pCodecParameter->a[pCodecParameter->stripeSize * src_error_list[i] + j];
         }
      }
      
      // e * invert - b
      for(p = 0; p < parityErrors; p++) {
         for(i = 0; i < pCodecParameter->stripeSize; i++) {
            s = 0; 
            for(j = 0; j < pCodecParameter->stripeSize; j++)
              s ^= gf_mul(pCodecParameter->d[j * pCodecParameter->stripeSize + i], pCodecParameter->e[pCodecParameter->stripeSize * p + j]);
            pCodecParameter->c[pCodecParameter->stripeSize * (srcErrors + p) + i] = s;
         }
      }
      ec_init_tables(pCodecParameter->stripeSize, erasureLen, pCodecParameter->c, pCodecParameter->g_tbls);

    // Get all the repaired data into pCodecParameter->data, in the first erasuredLen rows.
      dlsym_ec_encode_data(chunkSize, pCodecParameter->stripeSize, erasureLen, pCodecParameter->g_tbls,
                        pCodecParameter->recov, pCodecParameter->code);

    // Set the repaired data to alldata. 
      for(j = 0; j < pCodecParameter->stripeSize + pCodecParameter->paritySize ; j++){
          if(pCodecParameter->erasured[j - pCodecParameter->paritySize] != -1){
              (*env)->SetObjectArrayElement(env, alldata, j, pCodecParameter->datajbuf[j]);
          }
      }

      if(isCopy){
          (*env)->ReleaseIntArrayElements(env, erasures, (jint *)tmp, JNI_ABORT);
      }
      fprintf(stderr, "[Decoding]decode success.\n");
      return 0;
 }

JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_ec_rawcoder_IsaRSRawDecoder_destroy
  (JNIEnv *env, jclass myclass){
      fprintf(stdout, "[Decoder destory]before destory\n");
      Codec_Parameter * pCodecParameter = NULL;
      if(NULL == (pCodecParameter = (Codec_Parameter *)pthread_getspecific(de_key))){
          fprintf(stderr, "[Decoder destory]ISA decoder not initilized!\n");
          return 0;
      }

      free(pCodecParameter->data);
      free(pCodecParameter->datajbuf);
      free(pCodecParameter->code);
      free(pCodecParameter->erasured);
      free(pCodecParameter);
      (void)pthread_setspecific(de_key, NULL);
      fprintf(stdout, "[Decoder destory]destory success.\n");
      return 0;

  }
