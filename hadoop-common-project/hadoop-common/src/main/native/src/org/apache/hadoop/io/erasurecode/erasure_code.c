/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#ifdef UNIX
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dlfcn.h>

#include "config.h"
#endif

#ifdef WINDOWS
#include <Windows.h>
#endif

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "org_apache_hadoop.h"
#include "../include/gf_util.h"
#include "../include/erasure_code.h"

/**
 *  erasure_code.c
 *  Implementation erasure code utilities based on lib of erasure_code.so.
 *  Building of this codes won't rely on any ISA-L source codes, but running
 *  into this will rely on successfully loading of the dynamic library.
 *
 */

/**
 * A helper function to dlsym a 'symbol' from a given library-handle.
 */

#ifdef UNIX

static __attribute__ ((unused))
void *my_dlsym(void *handle, const char *symbol) {
  void *func_ptr = dlsym(handle, symbol);
  return func_ptr;
}

/* A helper macro to dlsym the requisite dynamic symbol in NON-JNI env. */
#define LOAD_DYNAMIC_SYMBOL(func_ptr, handle, symbol) \
  if ((func_ptr = my_dlsym(handle, symbol)) == NULL) { \
    return "Failed to load symbol" symbol; \
  }

#endif

#ifdef WINDOWS

#define snprintf _snprintf

static FARPROC WINAPI my_dlsym(HMODULE handle, LPCSTR symbol) {
  FARPROC func_ptr = GetProcAddress(handle, symbol);
  return func_ptr;
}

/* A helper macro to dlsym the requisite dynamic symbol in NON-JNI env. */
#define LOAD_DYNAMIC_SYMBOL(func_type, func_ptr, handle, symbol) \
  if ((func_ptr = (func_type)my_dlsym(handle, symbol)) == NULL) { \
    return "Failed to load symbol" symbol; \
  }

#endif


#ifdef UNIX
// For gf_util.h
static unsigned char (*d_gf_mul)(unsigned char, unsigned char);
static unsigned char (*d_gf_inv)(unsigned char);
static void (*d_gf_gen_rs_matrix)(unsigned char *, int, int);
static void (*d_gf_gen_cauchy_matrix)(unsigned char *, int, int);
static int (*d_gf_invert_matrix)(unsigned char *, unsigned char *, const int);
static int (*d_gf_vect_mul)(int, unsigned char *, void *, void *);

// For erasure_code.h
static void (*d_ec_init_tables)(int, int, unsigned char*, unsigned char*);
static void (*d_ec_encode_data)(int, int, int, unsigned char*,
                                          unsigned char**, unsigned char**);
static void (*d_ec_encode_data_update)(int, int, int, int, unsigned char*,
                                             unsigned char*, unsigned char**);
#endif

#ifdef WINDOWS
// For erasure_code.h
typedef unsigned char (__cdecl *__d_gf_mul)(unsigned char, unsigned char);
static __d_gf_mul d_gf_mul;
typedef unsigned char (__cdecl *__d_gf_inv)(unsigned char);
static __d_gf_inv d_gf_inv;
typedef void (__cdecl *__d_gf_gen_rs_matrix)(unsigned char *, int, int);
static __d_gf_gen_rs_matrix d_gf_gen_rs_matrix;
typedef void (__cdecl *__d_gf_gen_cauchy_matrix)(unsigned char *, int, int);
static __d_gf_gen_cauchy_matrix d_gf_gen_cauchy_matrix;
typedef int (__cdecl *__d_gf_invert_matrix)(unsigned char *,
                                                   unsigned char *, const int);
static __d_gf_invert_matrix d_gf_invert_matrix;
typedef int (__cdecl *__d_gf_vect_mul)(int, unsigned char *, void *, void *);
static __d_gf_vect_mul d_gf_vect_mul;

// For erasure_code.h
typedef void (__cdecl *__d_ec_init_tables)(int, int,
                                                unsigned char*, unsigned char*);
static __d_ec_init_tables d_ec_init_tables;
typedef void (__cdecl *__d_ec_encode_data)(int, int, int, unsigned char*,
                                             unsigned char**, unsigned char**);
static __d_ec_encode_data d_ec_encode_data;
typedef void (__cdecl *__d_ec_encode_data_update)(int, int, int, int, unsigned char*,
                                             unsigned char*, unsigned char**);
static __d_ec_encode_data_update d_ec_encode_data_update;
#endif

static const char* load_functions(void* libec) {
#ifdef UNIX
  LOAD_DYNAMIC_SYMBOL(d_gf_mul, libec, "gf_mul");
  LOAD_DYNAMIC_SYMBOL(d_gf_inv, libec, "gf_inv");
  LOAD_DYNAMIC_SYMBOL(d_gf_gen_rs_matrix, libec, "gf_gen_rs_matrix");
  LOAD_DYNAMIC_SYMBOL(d_gf_gen_cauchy_matrix, libec, "gf_gen_cauchy1_matrix");
  LOAD_DYNAMIC_SYMBOL(d_gf_invert_matrix, libec, "gf_invert_matrix");
  LOAD_DYNAMIC_SYMBOL(d_gf_vect_mul, libec, "gf_vect_mul");

  LOAD_DYNAMIC_SYMBOL(d_ec_init_tables, libec, "ec_init_tables");
  LOAD_DYNAMIC_SYMBOL(d_ec_encode_data, libec, "ec_encode_data");
  LOAD_DYNAMIC_SYMBOL(d_ec_encode_data_update, libec, "ec_encode_data_update");
#endif

#ifdef WINDOWS
  LOAD_DYNAMIC_SYMBOL(__d_gf_mul, d_gf_mul, libec, "gf_mul");
  LOAD_DYNAMIC_SYMBOL(__d_gf_inv, d_gf_inv, libec, "gf_inv");
  LOAD_DYNAMIC_SYMBOL(__d_gf_gen_rs_matrix, d_gf_gen_rs_matrix, libec, "gf_gen_rs_matrix");
  LOAD_DYNAMIC_SYMBOL(__d_gf_gen_cauchy_matrix, d_gf_gen_cauchy_matrix, libec, "gf_gen_cauchy1_matrix");
  LOAD_DYNAMIC_SYMBOL(__d_gf_invert_matrix, d_gf_invert_matrix, libec, "gf_invert_matrix");
  LOAD_DYNAMIC_SYMBOL(__d_gf_vect_mul, d_gf_vect_mul, libec, "gf_vect_mul");

  LOAD_DYNAMIC_SYMBOL(__d_ec_init_tables, d_ec_init_tables, libec, "ec_init_tables");
  LOAD_DYNAMIC_SYMBOL(__d_ec_encode_data, d_ec_encode_data, libec, "ec_encode_data");
  LOAD_DYNAMIC_SYMBOL(__d_ec_encode_data_update, d_ec_encode_data_update, libec, "ec_encode_data_update");
#endif

  return NULL;
}

void load_erasurecode_lib(char* err, size_t err_len) {
  static void* libec = NULL;
  const char* libecName;
  const char* errMsg;

  err[0] = '\0';

  if (libec != NULL) {
    return;
  }

  libecName = HADOOP_ERASURECODE_LIBRARY;

  // Load Intel ISA-L
  #ifdef UNIX
  libec = dlopen(libecName, RTLD_LAZY | RTLD_GLOBAL);
  if (libec == NULL) {
    snprintf(err, err_len, "Failed to load %s! (%s)", libecName, dlerror());
    return;
  }
  // Clear any existing error
  dlerror();
  #endif

  #ifdef WINDOWS
  libec = LoadLibrary(libecName);
  if (libec == NULL) {
    snprintf(err, err_len, "Failed to load %s", libecName);
    return;
  }
  #endif

  errMsg = load_functions(libec);
  if (errMsg != NULL) {
    snprintf(err, err_len, "Loading functions from %s failed: %s",
                                                          libecName, errMsg);
  }
}

unsigned char h_gf_mul(unsigned char a, unsigned char b) {
  return d_gf_mul(a, b);
}

unsigned char h_gf_inv(unsigned char a) {
  return d_gf_inv(a);
}

void h_gf_gen_rs_matrix(unsigned char *a, int m, int k) {
  d_gf_gen_rs_matrix(a, m, k);
}

void h_gf_gen_cauchy_matrix(unsigned char *a, int m, int k) {
  d_gf_gen_cauchy_matrix(a, m, k);
}

int h_gf_invert_matrix(unsigned char *in, unsigned char *out, const int n) {
  return d_gf_invert_matrix(in, out, n);
}

int h_gf_vect_mul(int len, unsigned char *gftbl, void *src, void *dest) {
  return d_gf_vect_mul(len, gftbl, src, dest);
}

void h_ec_init_tables(int k, int rows, unsigned char* a, unsigned char* gftbls) {
  d_ec_init_tables(k, rows, a, gftbls);
}

void h_ec_encode_data(int len, int k, int rows, unsigned char *gftbls,
    unsigned char **data, unsigned char **coding) {
  d_ec_encode_data(len, k, rows, gftbls, data, coding);
}

void h_ec_encode_data_update(int len, int k, int rows, int vec_i,
         unsigned char *gftbls, unsigned char *data, unsigned char **coding) {
  d_ec_encode_data_update(len, k, rows, vec_i, gftbls, data, coding);
}