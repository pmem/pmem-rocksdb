//  Copyright (c) 2019, Intel Corporation. All rights reserved.
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef KVS_ON_DCPMM

#include "dcpmm/kvs_dcpmm.h"

//#ifndef __STDC_DCPMM_MACROS
//#define __STDC_DCPMM_MACROS
//#endif

#include <iostream>
#include <string>
#include <functional>
#include <atomic>
#include <libpmem.h>
#include <libpmemobj.h>
#include <snappy.h>
#include "util/compression.h"

namespace rocksdb {

static PMEMobjpool* pool_;
static uint64_t pool_uuid_lo_;
static size_t pool_base_addr_;
static size_t kvs_value_thres_;
static bool compress_value_;
static size_t dcpmm_avail_size_min_;

static std::atomic<size_t> dcpmm_avail_size(0);
static std::atomic<bool> dcpmm_is_avail(true);

struct kvs_root {
  size_t size;
};

PMEMobjpool* KVSOpen(const char* path, size_t size)
{
  PMEMoid root;
  struct kvs_root *rootp;
  pool_ = pmemobj_create(path, "store_rocksdb_value", size, 0666);
  if (pool_ == NULL) {
    pool_ = pmemobj_open(path, "store_rocksdb_value");
    if (pool_ == NULL) {
      fprintf(stderr,"Cannot init persistent memory poolset file\n");
      return nullptr;
    }
    root = pmemobj_root(pool_, sizeof(struct kvs_root));
    rootp = (struct kvs_root *)pmemobj_direct(root);
  } else {
    root = pmemobj_root(pool_, sizeof(struct kvs_root));
    rootp = (struct kvs_root *)pmemobj_direct(root);
    rootp->size = size;
    pmemobj_persist(pool_, &rootp->size, sizeof(rootp->size));
  }

  pool_uuid_lo_ = root.pool_uuid_lo;
  pool_base_addr_ = (size_t)pool_;

  // hard code it as 1/10 of total dcpmm size.
  dcpmm_avail_size_min_= size/10;

  return pool_;
}

void KVSClose(PMEMobjpool* pool)
{
  (void)(pool);
  // TODO(Peifeng)
}

enum ValueEncoding KVSGetEncoding(const void *ptr)
{
  struct KVSHdrBase* p_hdr_base = (struct KVSHdrBase*)ptr;
  return (enum ValueEncoding)(p_hdr_base->encoding);
}

uint64_t KVSGetUUID()
{
  assert(pool_);
  return pool_uuid_lo_;
}

bool KVSEnabled()
{
  return pool_ ? true : false;
}

bool KVSEncodeValue(const Slice& value, bool compress, struct KVSHdr *hdr)
{
  assert(pool_);

  PMEMoid oid;
  // If dcpmm has not enough space, the caller need to fallback to non-kvs.
  if (!dcpmm_is_avail) {
    return false;
  }

  if (compress) {
    // So far, just support to use snappy for value compression.
#ifdef SNAPPY
    char *compressed = new char[snappy::MaxCompressedLength(value.size())];
    size_t outsize;
    snappy::RawCompress(value.data(), value.size(), compressed, &outsize);
#else
    fprintf(stderr, "Doesn't support snappy.\n");
    assert(0);
#endif

    // Reserve space from libpmemobj and publish it after writing WAL.
    struct pobj_action *pact;
    pact =(struct pobj_action *) malloc(sizeof(struct pobj_action));
    oid = pmemobj_reserve(pool_, pact, sizeof(struct KVSHdrBase) + outsize, 0);
    if (OID_IS_NULL(oid)) {
      dcpmm_is_avail = false;
      delete[] compressed;
      return false;
    }

    void *tmp = pmemobj_direct(oid);

    // Fill a header structure, and the caller will insert it instead of the
    // original value.
    hdr->base.encoding = kEncodingPtrCompressed;
    hdr->off = (size_t)tmp - pool_base_addr_;
    hdr->size = outsize;
    hdr->pact = pact;

    // Prefix the encoding type of value content.
    pmem_memcpy_nodrain(tmp, &(hdr->base), sizeof(hdr->base));
    pmem_memcpy_persist((void*)((size_t)tmp + sizeof(struct KVSHdrBase)),
                        compressed, outsize);
    delete[] compressed;
  } else {
    struct pobj_action *pact;
    pact = (struct pobj_action *) malloc(sizeof(struct pobj_action));
    oid = pmemobj_reserve(pool_, pact, sizeof(struct KVSHdrBase) + value.size(),
                          0);
    if (OID_IS_NULL(oid)) {
      dcpmm_is_avail = false;
      return false;
    }

    void *tmp = pmemobj_direct(oid);
    hdr->base.encoding = kEncodingPtrUncompressed;
    hdr->pact = pact;
    hdr->off = (size_t)tmp - pool_base_addr_;
    hdr->size = value.size();

    // Prefix the encoding type of the value content.
    pmem_memcpy_nodrain(tmp, &(hdr->base), sizeof(hdr->base));
    pmem_memcpy_persist((void*)((size_t)tmp + sizeof(struct KVSHdrBase)),
                        value.data(), value.size());
  }
  return true;
}

void KVSDumpFromValueRef(const char* input,
                           std::function<void(const Slice& value)> add) {
  assert(pool_);
  struct KVSHdr* p_hdr = (struct KVSHdr*)input;
  if (p_hdr->base.encoding == kEncodingPtrCompressed ||
      p_hdr->base.encoding == kEncodingPtrUncompressed) {
    struct KVSHdrBase* p_hdr_base =
      (struct KVSHdrBase*)(pool_base_addr_ + p_hdr->off);

    if (p_hdr->base.encoding == kEncodingPtrCompressed)
      p_hdr_base->encoding = kEncodingRawCompressed;
    else
      p_hdr_base->encoding = kEncodingRawUncompressed;

    // Prefix encoding type of the value content.
    add(Slice((char*)(pool_base_addr_ +
                        p_hdr->off),
                        p_hdr->size + sizeof(struct KVSHdrBase)));
    PMEMoid oid;
    oid.off = p_hdr->off;
    oid.pool_uuid_lo = pool_uuid_lo_;
    pmemobj_free(&oid);
    if (!dcpmm_is_avail) {
      dcpmm_avail_size.fetch_add(p_hdr->size, std::memory_order_relaxed);
      if (dcpmm_avail_size > dcpmm_avail_size_min_) {
        dcpmm_avail_size = 0;
        dcpmm_is_avail = true;
      }
    }
  }
}

void KVSDecodeValueRef(const char* input, std::string* value)
{
  assert(input!=nullptr);
  struct KVSHdr* p_hdr = (struct KVSHdr*)input;
  if (p_hdr->base.encoding == kEncodingPtrCompressed) {
    void* data = (void*)((size_t)p_hdr->off +
                               pool_base_addr_ + sizeof(struct KVSHdr));
    size_t ulength = 0;
    if (Snappy_GetUncompressedLength((char*)data, (size_t)p_hdr->size,
                                     &ulength)) {
      char* buf = new char[ulength];
      Snappy_Uncompress((char*)input, p_hdr->size, buf);
      value->assign(buf, ulength);
      delete[] buf;
    } else {
      abort();
    }
  } else if (p_hdr->base.encoding == kEncodingPtrUncompressed) {
    (void)(input);
    // TODO(Peifeng)
    abort();
  }
}

size_t KVSGetExtraValueSize(const Slice& value)
{
  struct KVSHdr* p_hdr = (struct KVSHdr*)value.data();
  if (p_hdr->base.encoding == kEncodingRawCompressed ||
      p_hdr->base.encoding == kEncodingRawUncompressed)
    return 0;
  else
    return (size_t)p_hdr->size;
}

void KVSFreeValue(const Slice& value)
{
  struct KVSHdr* p_hdr = (struct KVSHdr*)value.data();
  if (p_hdr->base.encoding != kEncodingRawCompressed &&
      p_hdr->base.encoding != kEncodingRawUncompressed) {
    PMEMoid oid;
    oid.off = p_hdr->off;
    oid.pool_uuid_lo = pool_uuid_lo_;
    pmemobj_free(&oid);
    if (!dcpmm_is_avail) {
      dcpmm_avail_size.fetch_add(p_hdr->size, std::memory_order_relaxed);
      if (dcpmm_avail_size > dcpmm_avail_size_min_) {
        dcpmm_avail_size = 0;
        dcpmm_is_avail = true;
      }
    }
  }
  return;
}

void KVSSetKVSValueThres(size_t thres)
{
  kvs_value_thres_ = thres;
}

size_t KVSGetKVSValueThres()
{
  return kvs_value_thres_;
}

void KVSSetCompressKnob(bool compress)
{
  compress_value_ = compress;
}

bool KVSGetCompressKnob()
{
  return compress_value_;
}

int KVSPublish(struct pobj_action* act, size_t actvcnt)
{
  assert(pool_);
  return pmemobj_publish(pool_, act, actvcnt);
}
}  // namespace rocksdb
#endif
