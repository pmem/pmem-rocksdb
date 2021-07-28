//
// Created by root on 12/23/20.
//

#ifndef ROCKSDB_DCPMM_ARENA_H
#define ROCKSDB_DCPMM_ARENA_H

#endif  // ROCKSDB_DCPMM_ARENA_H

#include "memory/allocator.h"

namespace ROCKSDB_NAMESPACE {
class DcpmmArena : Allocator {
 public:
  DcpmmArena(const DcpmmArena&) = delete;
  void operator=(const DcpmmArena&) = delete;
  ~DcpmmArena();

  static const size_t kMapSize;
  static std::atomic<uint64_t > arena_num;

  explicit DcpmmArena(std::string pmem_file_path = "/mnt/pmem0/pmem_arena/",
                      size_t map_size = kMapSize);

  char* Allocate(size_t bytes) override;

  char* AllocateAligned(size_t bytes, size_t huge_page_size = 0,
                        Logger* logger = nullptr) override;

  size_t BlockSize() const override { return map_size_; }

  size_t AllocatedAndUnused() const { return capacity_ - offset_; }

  size_t ApproximateMemoryUsage() const {
    return offset_;
  }

  size_t MemoryAllocatedBytes() const { return offset_; }

  size_t IrregularBlockNum() const { return 0; }

  bool IsInInlineBlock() const {
    // TODO figure out
    return true;
  }

 private:
  //TODO restore
  Status MapNewRegion();

  std::string filename_;
  char* base_;
  size_t offset_{0};  // offset of block unit
  size_t map_size_;
  size_t capacity_{0};
};
}  // namespace ROCKSDB_NAMESPACE