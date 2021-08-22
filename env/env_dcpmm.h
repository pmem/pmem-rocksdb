//  Copyright (c) 2019, Intel Corporation. All rights reserved.
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include <mutex>
#include <string>
#include <vector>
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/file_system.h"

#define UNUSED(x)         ((void)(x))

#ifdef ON_DCPMM
#include "libpmem.h"

namespace rocksdb {

class DCPMMMmapFile : public FSWritableFile {
 private:
  std::string filename_;
  int fd_;
  size_t page_size_;
  size_t map_size_;
  char* base_;
  char* limit_;
  char* dst_;
  uint64_t file_offset_;
#ifdef ROCKSDB_FALLOCATE_PRESENT
  bool allow_fallocate_;  // If false, fallocate calls are bypassed
  bool fallocate_with_keep_size_;
#endif

  // Roundup x to a multiple of y
  static size_t Roundup(size_t x, size_t y) { return ((x + y - 1) / y) * y; }

  size_t TruncateToPageBoundary(size_t s) {
    s -= (s & (page_size_ - 1));
    assert((s % page_size_) == 0);
    return s;
  }

  IOStatus MapNewRegion();
  IOStatus UnmapCurrentRegion();
  IOStatus Msync() {
    pmem_drain();
    return IOStatus::OK();
  };

 public:
  DCPMMMmapFile(const std::string& fname, int fd, size_t page_size,
                const EnvOptions& options);
  ~DCPMMMmapFile();

  // Means Close() will properly take care of truncate
  // and it does not need any additional information
  virtual IOStatus Truncate(uint64_t /*size*/, const IOOptions& /*opts*/,
                            IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }
  virtual IOStatus Close(const IOOptions& opts, IODebugContext* dbg) override;
  virtual IOStatus Append(const Slice& data, const IOOptions& opts,
                          IODebugContext* dbg) override;
  virtual IOStatus Flush(const IOOptions& opts, IODebugContext* dbg) override;
  virtual IOStatus Sync(const IOOptions& opts, IODebugContext* dbg) override;
  virtual IOStatus Fsync(const IOOptions& opts, IODebugContext* dbg) override;
  virtual uint64_t GetFileSize(const IOOptions& opts,
                               IODebugContext* dbg) override;
  virtual IOStatus InvalidateCache(size_t offset, size_t length) override;
#ifdef ROCKSDB_FALLOCATE_PRESENT
  virtual IOStatus Allocate(uint64_t offset, uint64_t len,
                            const IOOptions& opts,
                            IODebugContext* dbg) override;
#endif
};

class DCPMMEnv : public EnvWrapper {
public:
  explicit DCPMMEnv(const DCPMMEnvOptions& _options, Env* base_env) :
    EnvWrapper(base_env), options(_options), recycle_logs_inited(false) {
}

  // we should use DCPMM-awared filesystem for WAL file
  Status NewWritableFile(const std::string& fname,
                                 std::unique_ptr<WritableFile>* result,
                                 const EnvOptions& env_options) override;

  // if to delete WAL file, we should recycle it to avoid zero-out again
  Status DeleteFile(const std::string& fname) override;

  // The WAL file may have extra format, so should be handled before reading
  Status NewSequentialFile(const std::string& fname,
                                   std::unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options) override;

private:
  DCPMMEnvOptions options;
  bool recycle_logs_inited;               // whether we have list the files
                                          // in wal_dir to init recycle_logs
  std::vector<std::string> recycle_logs;  // file names of recycled log files
  std::mutex lock_recycle_logs;           // lock protecting recycle_logs
};

}  // namespace rocksdb

#else

namespace rocksdb {

class DCPMMEnv : public EnvWrapper {
public:
  explicit DCPMMEnv(const DCPMMEnvOptions& options, Env* base_env) :
    EnvWrapper(base_env) {
    UNUSED(options);
    fprintf(stderr, "You have not build rocksdb with DCPMM support\n");
    fprintf(stderr, "Please see dcpmm/README for details\n");
    abort();
  }
};

}  // namespace rocksdb

#endif