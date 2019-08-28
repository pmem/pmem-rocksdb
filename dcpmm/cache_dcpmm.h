//  Copyright (c) 2019, Intel Corporation. All rights reserved.
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#ifdef BC_ON_DCPMM
#include <string>
#include <vector>
#include <atomic>
#include <memkind.h>
#include "rocksdb/memory_allocator.h"

namespace rocksdb {

class DCPMMMemoryAllocator : public MemoryAllocator {
 public:
  DCPMMMemoryAllocator(const std::string &path) {
    int err = memkind_create_pmem(path.c_str(), 0, &kind_);
    if(err) {
      char error_message[MEMKIND_ERROR_MESSAGE_SIZE];
      memkind_error_message(err, error_message, MEMKIND_ERROR_MESSAGE_SIZE);
      fprintf(stderr, "%s\n", error_message);
      abort();
    }
  }

  const char* Name() const override { return "DCPMMMemoryAllocator"; }

  void* Allocate(size_t size) override {
    return memkind_malloc(kind_, size);
  }

  void Deallocate(void* p) override {
    memkind_free(kind_, p);
  }

 private:
  memkind_t kind_;
};
}  // namespace rocksdb
#endif
