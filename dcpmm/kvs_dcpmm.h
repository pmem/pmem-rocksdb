// Copyright (c) 2019, Intel Corporation. All rights reserved.
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#ifdef ON_DCPMM
#include <functional>
#include <libpmemobj.h>

#include "rocksdb/slice.h"

namespace rocksdb {

struct KVSHdr {
  unsigned char encoding;
};

struct KVSRef {
  // Specify the encoding type of value content.
  struct KVSHdr hdr;
  // The size of value after encoding.
  size_t size;
  size_t pool_index;
  // The offset of value on DCPMM in the pool.
  size_t off_in_pool;
};

enum ValueEncoding {
  kEncodingRawCompressed = 0x0,
  kEncodingRawUncompressed = 0x1,
  kEncodingPtrCompressed = 0x2,
  kEncodingPtrUncompressed = 0x3,
  kEncodingUnknown
};

// Create or open the space on DCPMM for storing value.
extern int KVSOpen(const char* path, size_t size, size_t pool_count = 16);

// Close it.
void KVSClose();

// Return true if KVS is enabled.
extern bool KVSEnabled();

// Encode value to value reference.
extern bool KVSEncodeValue(const Slice& value, bool compress,
                           struct KVSRef* ref);

// Get the value content from value reference and call the add function
// to insert it into SST files.
extern Slice KVSDumpFromValueRef(const Slice& input,
                                std::function<void(const Slice& value)> add);

// Get the value content for value reference, decompress it if needed.
extern void KVSDecodeValueRef(const char* input, size_t size, std::string* dst);

// For value reference, return the value content size.
extern size_t KVSGetExtraValueSize(const Slice& value);

// For value reference, free the space used by value content.
extern void KVSFreeValue(const Slice& value);

// Do KVS only for value size >= thres.
extern void KVSSetKVSValueThres(size_t thres);

// Return current value size thres for KVS.
extern size_t KVSGetKVSValueThres();

// Enable or disable compress value for KVS.
extern void KVSSetCompressKnob(bool compress);

// Return if need to compress the value for KVS.
extern bool KVSGetCompressKnob();

// To make the objects recoverable after restarting.
extern int KVSPublish(struct pobj_action** pact_array, size_t actvcnt);

// Return the value encoding type.
enum ValueEncoding KVSGetEncoding(const void *ptr);
}  // namespace rocksdb
#endif
