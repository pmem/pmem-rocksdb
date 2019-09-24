// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace rocksdb;

std::string kDBPath = "/tmp/rocksdb_kvs_example";
std::string walPath = "/mnt/pmem0/rocksdb";
std::string kvsPath = "/mnt/pmem0/rocksdb_value/rocksdb.value";
constexpr int key_count = 1000;

int main() {
  DB* db;
  Options options;

  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // Create the DB if it's not already present
  options.create_if_missing = true;
  options.wal_dir = walPath;
  options.dcpmm_kvs_mmapped_file_fullpath = kvsPath;

  // Open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());
  uint64_t k = 0;

  WriteOptions wo;
  wo.sync = true;
  wo.disableWAL = false;

  // Fill keys
  WriteBatch batch;
  while (k < key_count) {
    std::string key = "key";
    key.append(std::to_string(k));
    batch.Clear();
    batch.Put(Slice(key), "0123456789-0123456789-0123456789-0123456789-0123456789-0123456789-0123456789");
    db->Write(wo, &batch);
    assert(s.ok());
    k++;
  }

  // Iterate all keys
  std::cout << "---Iter all keys---" << std::endl;
  ReadOptions ropts;
  ropts.verify_checksums = true;
  ropts.total_order_seek = true;
  Iterator* iter = db->NewIterator(ropts);
  iter->SeekToFirst();
  for (; iter->Valid(); iter->Next()) {
    std::cout << iter->key().ToString() << " " << iter->value().ToString() << std::endl;
  }

  // Read all keys
  std::cout << "---Read all keys---" << std::endl;
  std::string value;
  k = 0;
  while (k < key_count) {
    std::string key = "key";
    key.append(std::to_string(k));
    s = db->Get(ReadOptions(), Slice(key), &value);
    std::cout<< key << " " << value << " " << value.size() << std::endl;
    assert(s.ok());
    k++;
  }

  delete db;

  return 0;
}
