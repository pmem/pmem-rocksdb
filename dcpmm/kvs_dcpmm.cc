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

#include <atomic>
#include <string>
#include <vector>
#include <snappy.h>
#include <libpmem.h>
#include <libpmemobj.h>

#include "util/compression.h"

namespace rocksdb {

struct Pool {
  PMEMobjpool* pool;
  uint64_t uuid_lo;
  size_t base_addr;

  Pool() : pool(nullptr) {
  }

  ~Pool() {
    if (pool) {
      pmemobj_close(pool);
    }
  }
};

struct PobjAction : pobj_action {
  unsigned int pool_index;
};

static struct Pool* pools_ = nullptr;
static size_t pool_count_;
static std::atomic<size_t> next_pool_index_(0);

static size_t kvs_value_thres_ = 0;
static bool compress_value_ = false;
static size_t dcpmm_avail_size_min_ = 0;

static std::atomic<size_t> dcpmm_avail_size_(0);
static std::atomic<bool> dcpmm_is_avail_(true);

int KVSOpen(const char* path, size_t size, size_t pool_count) {
  assert(!pools_);
  pools_ = new struct Pool[pool_count];
  pool_count_ = pool_count;

  size_t pool_size = size / pool_count;
  for (size_t i = 0; i < pool_count; i++) {
    std::string pool_path(path);
    pool_path.append(".").append(std::to_string(i));

    struct KVSRoot {
      size_t size;
    };
    PMEMoid root;
    KVSRoot *rootp;
    auto* pool = pmemobj_create(pool_path.data(), "store_rocksdb_value",
                                        pool_size, 0666);
    if (pool) {
      root = pmemobj_root(pool, sizeof(struct KVSRoot));
      rootp = (struct KVSRoot*)pmemobj_direct(root);
      rootp->size = pool_size;
      pmemobj_persist(pool, &(rootp->size), sizeof(rootp->size));
    }
    else {
      pool = pmemobj_open(pool_path.data(), "store_rocksdb_value");
      if (pool == nullptr) {
        delete[] pools_;
        pools_ = nullptr;
        return -EIO;
      }
      root = pmemobj_root(pool, sizeof(struct KVSRoot));
      rootp = (struct KVSRoot*)pmemobj_direct(root);
    }

    pools_[i].pool = pool;
    pools_[i].uuid_lo = root.pool_uuid_lo;
    pools_[i].base_addr = (size_t)pool;
  }
  // hard code it as 1/10 of total dcpmm size.
  dcpmm_avail_size_min_ = size / 10;
  return 0;
}

void KVSClose() {
  delete[] pools_;
  pools_ = nullptr;
}

enum ValueEncoding KVSGetEncoding(const void *ptr) {
  auto* hdr = (KVSHdr*)ptr;
  return (enum ValueEncoding)(hdr->encoding);
}

bool KVSEnabled() {
  return pools_ != nullptr;
}

static bool ReservePmem(size_t size, unsigned int* p_pool_index, PMEMoid* p_oid,
                        struct pobj_action** p_pact) {
  size_t pool_index = (next_pool_index_++) % pool_count_;
  size_t retry_loop = pool_count_;

  PMEMoid oid;
  auto* pact = new PobjAction;

  for (size_t i = 0; i < retry_loop; i++) {
    auto* pool = pools_[pool_index].pool;
    oid = pmemobj_reserve(pool, pact, size, 0);
    if (!OID_IS_NULL(oid)) {
      *p_pool_index = pool_index;
      *p_oid = oid;
      pact->pool_index = pool_index;
      *p_pact = pact;
      return true;
    }
    pool_index++;
    if (pool_index >= pool_count_) {
      pool_index = 0;
    }
  }

  dcpmm_is_avail_ = false;
  delete pact;
  return false;
}

bool KVSEncodeValue(const Slice& value, bool compress,
                    struct KVSRef* ref, struct pobj_action** p_pact) {
  assert(pools_);

  // If dcpmm has not enough space, the caller need to fallback to non-kvs.
  if (!dcpmm_is_avail_) {
    return false;
  }

  if (!compress) {
    PMEMoid oid;
    if (!ReservePmem(sizeof(struct KVSHdr) + value.size(), &(ref->pool_index),
                      &oid, p_pact)) {
      return false;
    }
    void *buf = pmemobj_direct(oid);
    ref->hdr.encoding = kEncodingPtrUncompressed;
    ref->size = value.size();
    assert((size_t)buf >= pools_[ref->pool_index].base_addr);
    ref->off_in_pool = (size_t)buf - pools_[ref->pool_index].base_addr;

    // Prefix the encoding type of the value content.
    pmem_memcpy_nodrain(buf, &(ref->hdr), sizeof(ref->hdr));
    pmem_memcpy_persist((char*)buf + sizeof(ref->hdr),
                        value.data(), value.size());
  }

  else {
    // So far, just support to use snappy for value compression.
#ifdef SNAPPY
    char *compressed = new char[snappy::MaxCompressedLength(value.size())];
    size_t outsize;
    snappy::RawCompress(value.data(), value.size(), compressed, &outsize);
#else
    fprintf(stderr, "Doesn't support snappy.\n");
    assert(0);
#endif

    PMEMoid oid;
    if (!ReservePmem(sizeof(struct KVSHdr) + outsize, &(ref->pool_index),
                      &oid, p_pact)) {
      delete[] compressed;
      return false;
    }
    void *buf = pmemobj_direct(oid);
    // Fill a header structure, and the caller will insert it instead of the
    // original value.
    ref->hdr.encoding = kEncodingPtrCompressed;
    ref->size = outsize;
    assert((size_t)buf >= pools_[ref->pool_index].base_addr);
    ref->off_in_pool = (size_t)buf - pools_[ref->pool_index].base_addr;

    // Prefix the encoding type of value content.
    pmem_memcpy_nodrain(buf, &(ref->hdr), sizeof(ref->hdr));
    pmem_memcpy_persist((char*)buf + sizeof(ref->hdr),
                        compressed, outsize);
    delete[] compressed;
  }

  return true;
}

static void FreePmem(struct KVSRef* ref) {
  PMEMoid oid;
  oid.pool_uuid_lo = pools_[ref->pool_index].uuid_lo;
  oid.off = ref->off_in_pool;
  pmemobj_free(&oid);
  if (!dcpmm_is_avail_) {
    if ((dcpmm_avail_size_ += ref->size) > dcpmm_avail_size_min_) {
      dcpmm_avail_size_ = 0;
      dcpmm_is_avail_ = true;
    }
  }
}

void KVSDumpFromValueRef(const char* input,
                           std::function<void(const Slice& value)> add) {
  assert(pools_);
  auto* ref = (struct KVSRef*)input;
  if (ref->hdr.encoding == kEncodingPtrCompressed ||
      ref->hdr.encoding == kEncodingPtrUncompressed) {
    auto* hdr = (struct KVSHdr*)(pools_[ref->pool_index].base_addr
                                      + ref->off_in_pool);
    if (ref->hdr.encoding == kEncodingPtrCompressed) {
      hdr->encoding = kEncodingRawCompressed;
    }
    else {
      hdr->encoding = kEncodingRawUncompressed;
    }

    // Prefix encoding type of the value content.
    add(Slice((char*)hdr, ref->size + sizeof(struct KVSHdr)));

    FreePmem(ref);
  }
}

void KVSDecodeValueRef(const char* input, std::string* value) {
  assert(input);
  auto* ref = (struct KVSRef*)input;
  const auto* data = (char*)pools_[ref->pool_index].base_addr + ref->off_in_pool
                + sizeof(struct KVSHdr);

  if (ref->hdr.encoding == kEncodingPtrUncompressed) {
    value->assign(data, ref->size);
  }
  else if (ref->hdr.encoding == kEncodingPtrCompressed) {
    size_t ulength = 0;
    if (Snappy_GetUncompressedLength(data, ref->size, &ulength)) {
      char* buf = new char[ulength];
      Snappy_Uncompress(data, ref->size, buf);
      value->assign(buf, ulength);
      delete buf;
    } else {
      abort();
    }
  }
}

size_t KVSGetExtraValueSize(const Slice& value) {
  auto* ref = (struct KVSRef*)value.data();
  if (ref->hdr.encoding == kEncodingRawCompressed ||
      ref->hdr.encoding == kEncodingRawUncompressed) {
    return 0;
  }
  else {
    return (size_t)ref->size;
  }
}

void KVSFreeValue(const Slice& value) {
  auto* ref = (struct KVSRef*)value.data();
  if (ref->hdr.encoding != kEncodingRawCompressed &&
      ref->hdr.encoding != kEncodingRawUncompressed) {
    FreePmem(ref);
  }
}

void KVSSetKVSValueThres(size_t thres) {
  kvs_value_thres_ = thres;
}

size_t KVSGetKVSValueThres() {
  return kvs_value_thres_;
}

void KVSSetCompressKnob(bool compress) {
  compress_value_ = compress;
}

bool KVSGetCompressKnob() {
  return compress_value_;
}

int KVSPublish(struct pobj_action** pact_array, size_t actvcnt) {
  assert(pools_);
  auto* pacts_of_pools = new std::vector<struct pobj_action>[pool_count_];
  for (size_t i = 0; i < actvcnt; i++) {
    auto* pact = (struct PobjAction*)pact_array[i];
    assert(pact->pool_index < pool_count_);
    pacts_of_pools[pact->pool_index].push_back(*((struct pobj_action*)pact));
    delete pact;
  }
  for (size_t i = 0; i < pool_count_; i++) {
    auto* pool = pools_[i].pool;
    auto& pacts = pacts_of_pools[i];
    auto* data = pacts.data();
    size_t count = pacts.size();
    // FIXME: pmemobj_publish() crashs if count >= 40, so we split it.
    size_t index = 0;
    while(index < count) {
      auto n = std::min(count - index, 32UL);
      pmemobj_publish(pool, data + index, n);
      index += n;
    }
    assert(index == count);
  }
  delete[] pacts_of_pools;

  return 0;
}

}  // namespace rocksdb
#endif
