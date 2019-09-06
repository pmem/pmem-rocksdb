#!/usr/bin/bash
#kvs write.sh
ulimit -n 1048576

db=/mnt/pmem8/rocksdb_sst
wal=/mnt/pmem8/rocksdb_wal

rm -f /mnt/pmem8/rocksdb_sst/* /mnt/pmem8/rocksdb_wal/*

export LD_LIBRARY_PATH=/usr/local/lib64/:/usr/local/lib
numactl --cpunodebind=1 --membind=1 \
./db_bench \
    --benchmarks="fillrandom,stats,levelstats" \
    --enable_write_thread_adaptive_yield=false \
    --disable_auto_compactions=false \
    --max_background_compactions=32 \
    --max_background_flushes=4 \
    --value_size=1024 \
    --key_size=128 \
    --db=${db} \
    --wal_dir=${wal} \
    --enable_pipelined_write=true \
    --allow_concurrent_memtable_write=true \
    --batch_size=1 \
    --histogram=true \
    --use_direct_io_for_flush_and_compaction=true \
    --target_file_size_base=67108864 \
    --disable_wal=false \
    --sync=true \
    \
    --writes=1000000 \
    --num=100000000 \
    --threads=100 \
    \
    --report_interval_seconds=10 \
    --report_file=report.csv
