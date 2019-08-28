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

#define UNUSED(x)         ((void)(x))

#ifdef WAL_ON_DCPMM

namespace rocksdb {

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
