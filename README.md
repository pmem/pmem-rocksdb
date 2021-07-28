## RocksDB: A Persistent Key-Value Store for Flash and RAM Storage

### Intel optimized features for pmem

These are experimental features, don't use them in production environment.

#### Build

make ROCKSDB_ON_DCPMM=1 install-static -j

#### Reuse obsoleted sst files

To avoid page-fault and page-zeroing overhead on pmem

usage:

options.recycle_dcpmm_sst = true;

#### Write wal with nt-store

usage:

options.env = rocksdb::NewDCPMMEnv(rocksdb::DCPMMEnvOptions());

#### Key-value separation

allocate values with libpmemobj

usage:

options.env = rocksdb::NewDCPMMEnv(rocksdb::DCPMMEnvOptions());

options.dcpmm_kvs_enable = true;

options.dcpmm_kvs_mmapped_file_fullpath = {path to libpmemobj file};

options.dcpmm_kvs_mmapped_file_size = {libpmemobj file size};

options.dcpmm_kvs_value_thres = 64;  // minimal size to do kv sep

options.dcpmm_compress_value = false;

#### Optimized mmap read for pmem

usage:

options.use_mmap_read = true;

options.cache_index_and_filter_blocks_for_mmap_read = true;

rocksdb::BlockBasedTableOptions bbto;

bbto.block_size = 256 (512,1024, ... etc);

options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));

[![Linux/Mac Build Status](https://travis-ci.org/facebook/rocksdb.svg?branch=master)](https://travis-ci.org/facebook/rocksdb)
[![Windows Build status](https://ci.appveyor.com/api/projects/status/fbgfu0so3afcno78/branch/master?svg=true)](https://ci.appveyor.com/project/Facebook/rocksdb/branch/master)
[![PPC64le Build Status](http://140.211.168.68:8080/buildStatus/icon?job=Rocksdb)](http://140.211.168.68:8080/job/Rocksdb)

RocksDB is developed and maintained by Facebook Database Engineering Team.
It is built on earlier work on [LevelDB](https://github.com/google/leveldb) by Sanjay Ghemawat (sanjay@google.com)
and Jeff Dean (jeff@google.com)

This code is a library that forms the core building block for a fast
key-value server, especially suited for storing data on flash drives.
It has a Log-Structured-Merge-Database (LSM) design with flexible tradeoffs
between Write-Amplification-Factor (WAF), Read-Amplification-Factor (RAF)
and Space-Amplification-Factor (SAF). It has multi-threaded compactions,
making it especially suitable for storing multiple terabytes of data in a
single database.

Start with example usage here: https://github.com/facebook/rocksdb/tree/master/examples

See the [github wiki](https://github.com/facebook/rocksdb/wiki) for more explanation.

The public interface is in `include/`.  Callers should not include or
rely on the details of any other header files in this package.  Those
internal APIs may be changed without warning.

Design discussions are conducted in https://www.facebook.com/groups/rocksdb.dev/ and https://rocksdb.slack.com/

## License

RocksDB is dual-licensed under both the GPLv2 (found in the COPYING file in the root directory) and Apache 2.0 License (found in the LICENSE.Apache file in the root directory).  You may select, at your option, one of the above-listed licenses.
