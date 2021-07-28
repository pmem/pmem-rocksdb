//
// Created by root on 12/23/20.
//
#include "dcpmm_arena.h"

#include <zconf.h>

#include <iostream>

#include "libpmem.h"

namespace ROCKSDB_NAMESPACE {
const size_t DcpmmArena::kMapSize = 512 << 20;
std::atomic<uint64_t> DcpmmArena::arena_num{0};

DcpmmArena::DcpmmArena(std::string pmem_file_path, size_t map_size)
    : filename_(pmem_file_path + std::to_string(arena_num.fetch_add(
                                     1, std::memory_order_relaxed)%4)),
      map_size_(map_size) {
  static bool arena = false;
  if(!arena){
    arena = true;
    std::cerr<<"dcpmm arena!"<<std::endl;
  }
}

DcpmmArena::~DcpmmArena() {
//  unlink(filename_.c_str());
    pmem_unmap(base_, capacity_);
  // nothing todo for now
}

Status DcpmmArena::MapNewRegion() {
  size_t mmap_len;
  int is_pmem;
  // TODO record block unit size on file head
  // TODO reuse existing files
  base_ = (char*)pmem_map_file(filename_.c_str(), offset_ + map_size_,
                               PMEM_FILE_EXCL, 0666, &mmap_len, &is_pmem);
  if (base_ == nullptr) {
    base_ = (char*)pmem_map_file(filename_.c_str(), offset_ + map_size_,
                                 PMEM_FILE_CREATE, 0666, &mmap_len, &is_pmem);
  }
  if (base_ == nullptr || !is_pmem || mmap_len != offset_ + map_size_) {
    return Status::IOError("dcpmm arena: map new region error");
  }
  capacity_ = mmap_len;
//  std::cerr<<"map new region done\n";
  return Status::OK();
}

char* DcpmmArena::Allocate(size_t bytes) {
  bytes = bytes + (64 - (bytes&63)); //align
  if (base_ == nullptr || offset_ + bytes >= capacity_) {
    auto s = MapNewRegion();
    if (!s.ok()) {
      std::cerr<<s.ToString()<<std::endl;
      fprintf(stderr, "%s\n", pmem_errormsg());
      return nullptr;
    }
  }
  // TODO record size
  char* ret = base_ + offset_;
  offset_ += bytes;
  return ret;
}

char* DcpmmArena::AllocateAligned(size_t bytes, size_t, Logger*) {
  // TODO: aligned allocate
  return Allocate(bytes);
}

}  // namespace ROCKSDB_NAMESPACE