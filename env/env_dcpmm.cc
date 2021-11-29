//  Copyright (c) 2019, Intel Corporation. All rights reserved.
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "env_dcpmm.h"

#include "io_posix.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"

#ifdef ON_DCPMM
#include <errno.h>
#include <fcntl.h>
#include <libpmem.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>

#include "env_dcpmm.h"
#include "libpmem.h"

namespace rocksdb {

#ifndef MAP_SYNC
#define MAP_SYNC MAP_SHARED
#endif

#ifndef MAP_SHARED_VALIDATE
#define MAP_SHARED_VALIDATE MAP_SHARED
#endif

DCPMMMmapFile::DCPMMMmapFile(const std::string& fname, int fd, size_t page_size,
                             const EnvOptions& options)
    : filename_(fname),
      fd_(fd),
      page_size_(page_size),
      map_size_(Roundup(1 << 25 /*32MB*/, page_size)),
      base_(nullptr),
      limit_(nullptr),
      dst_(nullptr),
      file_offset_(0) {
#ifdef ROCKSDB_FALLOCATE_PRESENT
  allow_fallocate_ = options.allow_fallocate;
  fallocate_with_keep_size_ = options.fallocate_with_keep_size;
#else
  (void)options;
#endif
  assert((page_size & (page_size - 1)) == 0);
  assert(options.use_mmap_writes);
  assert(!options.use_direct_writes);
}

IOStatus DCPMMMmapFile::UnmapCurrentRegion() {
  pmem_unmap(base_, limit_ - base_);
  return IOStatus::OK();
}

#ifdef ROCKSDB_FALLOCATE_PRESENT
IOStatus DCPMMMmapFile::Allocate(uint64_t, uint64_t, const IOOptions&,
                                 IODebugContext*) {
  return IOStatus::NotSupported();
}
#endif

DCPMMMmapFile::~DCPMMMmapFile() {
  if (fd_ >= 0) {
    close(fd_);
  }
}

IOStatus DCPMMMmapFile::MapNewRegion() {
  size_t mmap_len;
  int is_pmem;
  UnmapCurrentRegion();
  base_ = (char*)pmem_map_file(filename_.c_str(), file_offset_ + map_size_,
                               PMEM_FILE_EXCL, 0666, &mmap_len, &is_pmem);
  if (base_ == nullptr) {
    base_ = (char*)pmem_map_file(filename_.c_str(), file_offset_ + map_size_,
                                 PMEM_FILE_CREATE, 0666, &mmap_len, &is_pmem);
    if (base_ == nullptr) {
      return IOStatus::IOError("dcpmm map file error");
    }
  }
  limit_ = base_ + mmap_len;
  dst_ = base_ + file_offset_;
  return IOStatus::OK();
}

IOStatus DCPMMMmapFile::Append(const Slice& data, const IOOptions&,
                               IODebugContext*) {
  const char* src = data.data();
  size_t left = data.size();
  while (left > 0) {
    assert(base_ <= dst_);
    assert(dst_ <= limit_);
    size_t avail = limit_ - dst_;
    if (avail == 0) {
      IOStatus s = MapNewRegion();
      if (!s.ok()) {
        return s;
      }
    }

    size_t n = (left <= avail) ? left : avail;
    assert(dst_);
    pmem_memcpy_nodrain(dst_, src, n);
    file_offset_ += n;
    dst_ += n;
    src += n;
    left -= n;
  }
  return IOStatus::OK();
}

IOStatus DCPMMMmapFile::Close(const IOOptions&, IODebugContext*) {
  IOStatus s;
  size_t unused = limit_ - dst_;
  UnmapCurrentRegion();

  if (unused > 0) {
    if (ftruncate(fd_, file_offset_) < 0) {
      s = IOError("While ftruncating dcpmm mmaped file", filename_, errno);
    }
  }

  if (close(fd_) < 0) {
    if (s.ok()) {
      s = IOError("While closing mmapped file", filename_, errno);
    }
  }

  fd_ = -1;
  base_ = nullptr;
  limit_ = nullptr;
  return s;
}

IOStatus DCPMMMmapFile::Flush(const IOOptions&, IODebugContext*) {
  return IOStatus::OK();
}

IOStatus DCPMMMmapFile::Sync(const IOOptions&, IODebugContext*) {
  pmem_drain();
  return IOStatus::OK();
}

IOStatus DCPMMMmapFile::Fsync(const IOOptions&, IODebugContext*) {
  pmem_drain();
  return IOStatus::OK();
}

uint64_t DCPMMMmapFile::GetFileSize(const IOOptions&, IODebugContext*) {
  return file_offset_;
}

IOStatus DCPMMMmapFile::InvalidateCache(size_t, size_t) {
  return IOStatus::OK();
}

// namespace {

class DCPMMWritableFile : public WritableFile {
 public:
  // this flag strictly requires the file system to be on DCPMM
  static const int MMAP_FLAGS = MAP_SHARED_VALIDATE | MAP_SYNC;

  // create a new WAL file on DCPMM
  static Status Create(const std::string& fname, WritableFile** p_file,
                       size_t init_size, size_t size_addition) {
    auto s = CheckArguments(init_size, size_addition);
    if (!s.ok()) {
      return s;
    }
    // explicit create new file
    int fd = open(fname.c_str(), O_CREAT | O_EXCL | O_RDWR, 0777);
    if (fd < 0) {
      return Status::IOError(std::string("create '")
                                 .append(fname)
                                 .append("' failed: ")
                                 .append(strerror(errno)));
    }
    // pre-allocate space
    if (fallocate(fd, 0, 0, init_size) != 0) {
      close(fd);
      return Status::IOError(std::string("fallocate '")
                                 .append(fname)
                                 .append("' failed: ")
                                 .append(strerror(errno)));
    }
    return MapFile(fd, init_size, size_addition, p_file);
  }

  // reuse a WAL file on DCPMM
  static Status Reuse(const std::string& old_fname,
                      const std::string& new_fname, WritableFile** p_file,
                      size_t init_size, size_t size_addition) {
    auto s = CheckArguments(init_size, size_addition);
    if (!s.ok()) {
      return s;
    }
    // O_RDWR, no create file
    int fd = open(old_fname.c_str(), O_RDWR);
    if (fd < 0) {
      return Status::IOError(std::string("open '")
                                 .append(old_fname)
                                 .append("' failed: ")
                                 .append(strerror(errno)));
    }
    // get the file size
    struct stat stat;
    if (fstat(fd, &stat) < 0) {
      close(fd);
      return Status::IOError(std::string("fstat '")
                                 .append(old_fname)
                                 .append("' failed: ")
                                 .append(strerror(errno)));
    }
    if ((size_t)stat.st_size < init_size) {
      close(fd);
      return Status::InvalidArgument(
          std::string("the file '")
              .append(old_fname)
              .append("' to reuse is less than init_size"));
    }
    // rename it
    if (rename(old_fname.c_str(), new_fname.c_str()) < 0) {
      close(fd);
      return Status::IOError(std::string("rename '")
                                 .append(old_fname)
                                 .append("' to '")
                                 .append(new_fname)
                                 .append("' failed: ")
                                 .append(strerror(errno)));
    }
    return MapFile(fd, init_size, size_addition, p_file);
  }

  Status Append(const Slice& slice) override {
    size_t len = slice.size();
    // the least file size to write the new slice
    size_t least_file_size = sizeof(size_t) + data_length + len;
    // left space is not enough, need expand
    if (least_file_size > file_size) {
      // the multiple of size_addition to add
      size_t count = (least_file_size - file_size - 1) / size_addition + 1;
      size_t add_size = count * size_addition;
      // allocate new space
      if (fallocate(fd, 0, file_size, add_size) != 0) {
        return Status::IOError(
            std::string("expand file failed: ").append(strerror(errno)));
      }
      size_t new_file_size = file_size + add_size;
      assert(new_file_size >= least_file_size);
      // the offset in file to mmap(), align to 4K
      size_t offset = sizeof(size_t) + data_length;
      size_t offset_aligned = offset & ~(4096 - 1);
      assert(offset_aligned <= offset);
      // mmaped() the new area, but because aligned,
      // usually overlip with current mmap()-ed range
      size_t new_map_length = new_file_size - offset_aligned;
      void* new_map_base =
          mmap(0, new_map_length, PROT_WRITE, MMAP_FLAGS, fd, offset_aligned);
      if (new_map_base == MAP_FAILED) {
        return Status::IOError(
            std::string("mmap new range failed: ").append(strerror(errno)));
      }
      // unmap the previous range
      munmap(map_base, map_length);
      file_size = new_file_size;
      map_base = new_map_base;
      map_length = new_map_length;
      buffer = (uint8_t*)new_map_base + offset - offset_aligned - data_length;
    }
    MemcpyNT(buffer + data_length, slice.data(), len);
    // need sfence, make sure all data is persisted to DCPMM
    __builtin_ia32_sfence();
    data_length += len;
    SetSizeNT(p_length, data_length);
    // need sfence, make sure all data is persisted to DCPMM
    __builtin_ia32_sfence();
    return Status::OK();
  }

  Status Truncate(uint64_t size) override {
    data_length = size;
    SetSizeNT(p_length, data_length);
    __builtin_ia32_sfence();
    return Status::OK();
  }

  Status Close() override {
    close(fd);
    munmap(p_length, sizeof(size_t));
    munmap(map_base, map_length);
    return Status::OK();
  }

  Status Flush() override {
    // DCPMM need no flush
    return Status::OK();
  }

  Status Sync() override {
    // NT write need on sync
    return Status::OK();
  }

  uint64_t GetFileSize() override {
    // it is the data length, not the physical space size
    return data_length;
  }

 private:
  static Status CheckArguments(size_t init_size, size_t size_addition) {
    if (init_size < sizeof(size_t)) {
      return Status::InvalidArgument("too small init_size");
    }
    if (size_addition == 0) {
      return Status::InvalidArgument("size_addition is zero");
    }
    return Status::OK();
  }

  static Status MapFile(int fd, size_t init_size, size_t size_addition,
                        WritableFile** p_file) {
    // first size_t is to record length of data
    void* p_length = mmap(0, sizeof(size_t), PROT_WRITE, MMAP_FLAGS, fd, 0);
    if (p_length == MAP_FAILED) {
      close(fd);
      return Status::IOError(
          std::string("mmap first size_t failed: ").append(strerror(errno)));
    }
    // whole file
    void* base = mmap(0, init_size, PROT_WRITE, MMAP_FLAGS, fd, 0);
    if (base == MAP_FAILED) {
      close(fd);
      munmap(p_length, sizeof(size_t));
      return Status::IOError(
          std::string("mmap whole file failed: ").append(strerror(errno)));
    }
    *p_file = new DCPMMWritableFile(fd, (size_t*)p_length, base, init_size,
                                    size_addition);
    return Status::OK();
  }

  // memcpy non-temporally
  static void MemcpyNT(void* dst, const void* src, size_t len) {
    pmem_memcpy(dst, src, len, PMEM_F_MEM_NONTEMPORAL);
  }

  // set a 64-bit non-temporally
  static void SetSizeNT(size_t* dst, size_t value) {
    assert(sizeof(size_t) == 8);
    assert((size_t)dst % sizeof(size_t) == 0);
    __builtin_ia32_movnti64((long long*)dst, value);
  }

  DCPMMWritableFile(int _fd, size_t* _p_length, void* base, size_t init_size,
                    size_t _size_addition)
      : data_length(0),
        file_size(init_size),
        size_addition(_size_addition),
        fd(_fd),
        p_length(_p_length),
        map_base(base),
        map_length(init_size),
        buffer((uint8_t*)base + sizeof(size_t)) {
    // reset the data length
    SetSizeNT(p_length, 0);
    __builtin_ia32_sfence();
  }

 private:
  size_t data_length;    // the writen data length
  size_t file_size;      // the length of the whole file
  size_t size_addition;  // expand size_addition bytes
                         // every time left space is not enough
  int fd;                // the file descriptor
  size_t* p_length;      // the first size_t to record data length
  void* map_base;        // mmap()-ed area
  size_t map_length;     // mmap()-ed length
  uint8_t* buffer;       // the base address of buffer to write
};

class DCPMMSequentialFile : public SequentialFile {
 public:
  static Status Open(const std::string& fname, SequentialFile** p_file) {
    int fd = open(fname.c_str(), O_RDONLY);
    if (fd < 0) {
      return Status::IOError(std::string("open '")
                                 .append(fname)
                                 .append("' failed: ")
                                 .append(strerror(errno)));
    }
    // get the file size
    struct stat stat;
    if (fstat(fd, &stat) < 0) {
      close(fd);
      return Status::IOError(std::string("fstat '")
                                 .append(fname)
                                 .append("' failed: ")
                                 .append(strerror(errno)));
    }
    // whole file
    void* base = mmap(0, (size_t)stat.st_size, PROT_READ, MAP_SHARED, fd, 0);
    // no matter map ok or fail, we can close now
    close(fd);
    if (base == MAP_FAILED) {
      return Status::IOError(
          std::string("mmap file failed: ").append(strerror(errno)));
    }
    *p_file = new DCPMMSequentialFile(base, (size_t)stat.st_size);
    return Status::OK();
  }

  Status Read(size_t n, Slice* result, char* scratch) override {
    UNUSED(scratch);
    assert(seek <= data_length);
    auto len = std::min(data_length - seek, n);
    *result = Slice((char*)data + seek, len);
    seek += len;
    return Status::OK();
  }

  Status Skip(uint64_t n) override {
    assert(seek <= data_length);
    auto len = std::min(data_length - seek, n);
    seek += len;
    return Status::OK();
  }

  ~DCPMMSequentialFile() {
    void* base = (void*)(data - sizeof(size_t));
    munmap(base, file_size);
  }

 private:
  DCPMMSequentialFile(void* base, size_t _file_size) : file_size(_file_size) {
    data_length = *((size_t*)base);
    data = (uint8_t*)base + sizeof(size_t);
    seek = 0;
  }

  size_t file_size;    // the length of the whole file
  uint8_t* data;       // the base address of data
  size_t data_length;  // the length of data
  size_t seek;         // next offset to read
};

//}  // Anonymous namespace

static bool EndsWith(const std::string& str, const std::string& suffix) {
  auto str_len = str.length();
  auto suffix_len = suffix.length();
  if (str_len < suffix_len) {
    return false;
  }
  return memcmp(str.c_str() + str_len - suffix_len, suffix.c_str(),
                suffix_len) == 0;
}

static bool IsWALFile(const std::string& fname) {
  return EndsWith(fname, ".log");
}

#define RECYCLE_SUFFIX ".recycle"

Status DCPMMEnv::NewWritableFile(const std::string& fname,
                                 std::unique_ptr<WritableFile>* result,
                                 const EnvOptions& env_options) {
  (void)env_options;
  // if it is not a log file, then fall back to original logic
  if (!IsWALFile(fname)) {
    return EnvWrapper::NewWritableFile(fname, result, env_options);
  }
  // else it is a log file
  // get a recycled log
  std::string recycle_fname;
  bool found = false;
  lock_recycle_logs.lock();
  // if we have not find the *.recycle file in wal_dir
  if (!recycle_logs_inited) {
    auto wal_dir = fname.substr(0, fname.rfind("/"));
    std::vector<std::string> files;
    auto status = GetChildren(wal_dir, &files);
    if (!status.ok()) {
      return status;
    }
    // find all files whose name end with .recycle
    for (auto& file : files) {
      if (EndsWith(file, RECYCLE_SUFFIX)) {
        recycle_logs.push_back(wal_dir + "/" + file);
      }
    }
    recycle_logs_inited = true;
  }
  if (!recycle_logs.empty()) {
    recycle_fname = recycle_logs.back();
    recycle_logs.pop_back();
    found = true;
  }
  lock_recycle_logs.unlock();
  WritableFile* file = nullptr;

  Status status;
  if (found) {
    // reuse the file
    status = DCPMMWritableFile::Reuse(recycle_fname, fname, &file,
                                      options.wal_init_size,
                                      options.wal_size_addition);
    if (!status.ok()) {
      EnvWrapper::DeleteFile(recycle_fname);
    }
  }

  if (!found || !status.ok()) {
    // create it
    status = DCPMMWritableFile::Create(fname, &file, options.wal_init_size,
                                       options.wal_size_addition);
  }

  if (status.ok()) {
    result->reset(file);
  }
  return status;
}

Status DCPMMEnv::DeleteFile(const std::string& fname) {
  // if it is not a log file, then fall back to original logic
  if (!IsWALFile(fname)) {
    return EnvWrapper::DeleteFile(fname);
  }
  // the recycled name
  std::string tmp_fname(fname);
  tmp_fname.append(RECYCLE_SUFFIX);
  if (rename(fname.c_str(), tmp_fname.c_str()) < 0) {
    return Status::IOError(std::string("rename '")
                               .append(fname)
                               .append("' to '")
                               .append(tmp_fname)
                               .append("' failed: ")
                               .append(strerror(errno)));
  }
  lock_recycle_logs.lock();
  recycle_logs.push_back(tmp_fname);
  lock_recycle_logs.unlock();
  return Status::OK();
}

Status DCPMMEnv::NewSequentialFile(const std::string& fname,
                                   std::unique_ptr<SequentialFile>* result,
                                   const EnvOptions& env_options) {
  UNUSED(env_options);
  // if it is not a log file, then fall back to original logic
  if (!IsWALFile(fname)) {
    return EnvWrapper::NewSequentialFile(fname, result, env_options);
  }
  SequentialFile* file;
  auto status = DCPMMSequentialFile::Open(fname, &file);
  if (status.ok()) {
    result->reset(file);
  }
  return status;
}

Env* NewDCPMMEnv(const DCPMMEnvOptions& options, Env* base_env) {
  std::cerr << "new dcpmm env!" << std::endl;
  return new DCPMMEnv(options, base_env ? base_env : rocksdb::Env::Default());
}

}  // namespace rocksdb

#else

namespace rocksdb {

Env* NewDCPMMEnv(const DCPMMEnvOptions& options, Env* base_env) {
  UNUSED(options);
  UNUSED(base_env);
  fprintf(stderr, "You have not build rocksdb with DCPMM support\n");
  fprintf(stderr, "Please see dcpmm/README for details\n");
  abort();
  return nullptr;
}

}  // namespace rocksdb

#endif
