//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef GFLAGS
#ifdef NUMA
#include <numa.h>
#include <numaif.h>
#endif
#ifndef OS_WIN
#include <unistd.h>
#endif
#include <fcntl.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <atomic>
#include <cinttypes>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "db/db_impl/db_impl.h"
#include "db/malloc_stats.h"
#include "db/version_set.h"
#include "hdfs/env_hdfs.h"
#include "monitoring/histogram.h"
#include "monitoring/statistics.h"
#include "options/cf_options.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/stats_history.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/sim_cache.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/write_batch.h"
#include "test_util/testutil.h"
#include "test_util/transaction_test_util.h"
#include "util/cast_util.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/gflags_compat.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"
#include "util/xxhash.h"
#include "utilities/blob_db/blob_db.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/bytesxor.h"
#include "utilities/merge_operators/sortlist.h"
#include "utilities/persistent_cache/block_cache_tier.h"
#include "rocksdb/ycsb_tool.h"
#include "util/generator.h"
#include "util/workload.h"

#ifdef OS_WIN
#include <io.h>  // open/close
#endif

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

DEFINE_uint64(insert_start, 0, "insert start");

DEFINE_uint64(lower_bound, 0, "lower bound");

DEFINE_uint64(min_scan_len, 0, "minimum scan length");

DEFINE_uint64(max_scan_len, 0, "maximum scan length");

DEFINE_double(read_proportion, 0.0, "read proportion");

DEFINE_double(insert_proportion, 0.0, "insert proportion");

DEFINE_double(scan_proportion, 0.0, "scan proportion");

DEFINE_double(update_proportion, 0.0, "update proportion");

DEFINE_double(readmodifywrite_proportion, 0.0, "readmodifywrite proportion");

DEFINE_int32(request_distribution, 0, "request distribution");

DEFINE_int32(scan_length_distribution, 1, "scan length distribution");

DEFINE_int32(thread_per_subtable, 1, "thread per subtable");

DEFINE_int64(num, 1000000, "Number of key/values to place in database");

DEFINE_int32(num_column_families, 1, "Number of Column Families to use.");

DEFINE_int32(
    num_hot_column_families, 0,
    "Number of Hot Column Families. If more than 0, only write to this "
    "number of column families. After finishing all the writes to them, "
    "create new set of column families and insert to them. Only used "
    "when num_column_families > 1.");

DEFINE_string(column_family_distribution, "",
              "Comma-separated list of percentages, where the ith element "
              "indicates the probability of an op using the ith column family. "
              "The number of elements must be `num_hot_column_families` if "
              "specified; otherwise, it must be `num_column_families`. The "
              "sum of elements must be 100. E.g., if `num_column_families=4`, "
              "and `num_hot_column_families=0`, a valid list could be "
              "\"10,20,30,40\".");

DEFINE_int32(bloom_locality, 0, "Control bloom filter probes locality");

DEFINE_int64(seed, 0,
             "Seed base for random number generators. "
             "When 0 it is deterministic.");

DEFINE_int32(threads, 1, "Number of concurrent threads to run.");

DEFINE_int32(duration, 0,
             "Time in seconds for the random-ops tests to run."
             " When 0 then num & reads determine the test duration");

DEFINE_int32(value_size, 100, "Size of each value");

DEFINE_bool(use_uint64_comparator, false, "use Uint64 user comparator");

static bool ValidateKeySize(const char* /*flagname*/, int32_t /*value*/) {
  return true;
}

static bool ValidateUint32Range(const char* flagname, uint64_t value) {
  if (value > std::numeric_limits<uint32_t>::max()) {
    fprintf(stderr, "Invalid value for --%s: %lu, overflow\n", flagname,
            (unsigned long)value);
    return false;
  }
  return true;
}

DEFINE_int32(key_size, 16, "size of each key");

DEFINE_int32(num_multi_db, 0,
             "Number of DBs used in the benchmark. 0 means single DB.");

DEFINE_double(compression_ratio, 0.5,
              "Arrange to generate values that shrink"
              " to this fraction of their original size after compression");

DEFINE_double(read_random_exp_range, 0.0,
              "Read random's key will be generated using distribution of "
              "num * exp(-r) where r is uniform number from 0 to this value. "
              "The larger the number is, the more skewed the reads are. "
              "Only used in readrandom and multireadrandom benchmarks.");

DEFINE_bool(histogram, false, "Print histogram of operation timings");

DEFINE_bool(enable_numa, false,
            "Make operations aware of NUMA architecture and bind memory "
            "and cpus corresponding to nodes together. In NUMA, memory "
            "in same node as CPUs are closer when compared to memory in "
            "other nodes. Reads can be faster when the process is bound to "
            "CPU and memory of same node. Use \"$numactl --hardware\" command "
            "to see NUMA memory architecture.");

DEFINE_int64(db_write_buffer_size, rocksdb::Options().db_write_buffer_size,
             "Number of bytes to buffer in all memtables before compacting");

DEFINE_bool(cost_write_buffer_to_cache, false,
            "The usage of memtable is costed to the block cache");

DEFINE_int64(write_buffer_size, rocksdb::Options().write_buffer_size,
             "Number of bytes to buffer in memtable before compacting");

DEFINE_int32(max_write_buffer_number,
             rocksdb::Options().max_write_buffer_number,
             "The number of in-memory memtables. Each memtable is of size"
             " write_buffer_size bytes.");

DEFINE_int32(min_write_buffer_number_to_merge,
             rocksdb::Options().min_write_buffer_number_to_merge,
             "The minimum number of write buffers that will be merged together"
             "before writing to storage. This is cheap because it is an"
             "in-memory merge. If this feature is not enabled, then all these"
             "write buffers are flushed to L0 as separate files and this "
             "increases read amplification because a get request has to check"
             " in all of these files. Also, an in-memory merge may result in"
             " writing less data to storage if there are duplicate records "
             " in each of these individual write buffers.");

DEFINE_int32(max_write_buffer_number_to_maintain,
             rocksdb::Options().max_write_buffer_number_to_maintain,
             "The total maximum number of write buffers to maintain in memory "
             "including copies of buffers that have already been flushed. "
             "Unlike max_write_buffer_number, this parameter does not affect "
             "flushing. This controls the minimum amount of write history "
             "that will be available in memory for conflict checking when "
             "Transactions are used. If this value is too low, some "
             "transactions may fail at commit time due to not being able to "
             "determine whether there were any write conflicts. Setting this "
             "value to 0 will cause write buffers to be freed immediately "
             "after they are flushed.  If this value is set to -1, "
             "'max_write_buffer_number' will be used.");

DEFINE_int64(max_write_buffer_size_to_maintain,
             rocksdb::Options().max_write_buffer_size_to_maintain,
             "The total maximum size of write buffers to maintain in memory "
             "including copies of buffers that have already been flushed. "
             "Unlike max_write_buffer_number, this parameter does not affect "
             "flushing. This controls the minimum amount of write history "
             "that will be available in memory for conflict checking when "
             "Transactions are used. If this value is too low, some "
             "transactions may fail at commit time due to not being able to "
             "determine whether there were any write conflicts. Setting this "
             "value to 0 will cause write buffers to be freed immediately "
             "after they are flushed.  If this value is set to -1, "
             "'max_write_buffer_number' will be used.");

DEFINE_int32(max_background_jobs, rocksdb::Options().max_background_jobs,
             "The maximum number of concurrent background jobs that can occur "
             "in parallel.");

DEFINE_int32(num_bottom_pri_threads, 0,
             "The number of threads in the bottom-priority thread pool (used "
             "by universal compaction only).");

DEFINE_int32(num_high_pri_threads, 0,
             "The maximum number of concurrent background compactions"
             " that can occur in parallel.");

DEFINE_int32(num_low_pri_threads, 0,
             "The maximum number of concurrent background compactions"
             " that can occur in parallel.");

DEFINE_int32(max_background_compactions,
             rocksdb::Options().max_background_compactions,
             "The maximum number of concurrent background compactions"
             " that can occur in parallel.");

DEFINE_int32(base_background_compactions, -1, "DEPRECATED");

DEFINE_uint64(subcompactions, 1,
              "Maximum number of subcompactions to divide L0-L1 compactions "
              "into.");
static const bool FLAGS_subcompactions_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_subcompactions, &ValidateUint32Range);

DEFINE_int32(max_background_flushes, rocksdb::Options().max_background_flushes,
             "The maximum number of concurrent background flushes"
             " that can occur in parallel.");

static rocksdb::CompactionStyle FLAGS_compaction_style_e;
DEFINE_int32(compaction_style, (int32_t)rocksdb::Options().compaction_style,
             "style of compaction: level-based, universal and fifo");

static rocksdb::CompactionPri FLAGS_compaction_pri_e;
DEFINE_int32(compaction_pri, (int32_t)rocksdb::Options().compaction_pri,
             "priority of files to compaction: by size or by data age");

DEFINE_int32(universal_size_ratio, 0,
             "Percentage flexibility while comparing file size"
             " (for universal compaction only).");

DEFINE_int32(universal_min_merge_width, 0,
             "The minimum number of files in a"
             " single compaction run (for universal compaction only).");

DEFINE_int32(universal_max_merge_width, 0,
             "The max number of files to compact"
             " in universal style compaction");

DEFINE_int32(universal_max_size_amplification_percent, 0,
             "The max size amplification for universal style compaction");

DEFINE_int32(universal_compression_size_percent, -1,
             "The percentage of the database to compress for universal "
             "compaction. -1 means compress everything.");

DEFINE_bool(universal_allow_trivial_move, false,
            "Allow trivial move in universal compaction.");

DEFINE_int64(cache_size, 8 << 20,  // 8MB
             "Number of bytes to use as a cache of uncompressed data");

DEFINE_int32(cache_numshardbits, 6,
             "Number of shards for the block cache"
             " is 2 ** cache_numshardbits. Negative means use default settings."
             " This is applied only if FLAGS_cache_size is non-negative.");

DEFINE_double(cache_high_pri_pool_ratio, 0.0,
              "Ratio of block cache reserve for high pri blocks. "
              "If > 0.0, we also enable "
              "cache_index_and_filter_blocks_with_high_priority.");

DEFINE_bool(use_clock_cache, false,
            "Replace default LRU block cache with clock cache.");

DEFINE_int64(simcache_size, -1,
             "Number of bytes to use as a simcache of "
             "uncompressed data. Nagative value disables simcache.");

DEFINE_bool(cache_index_and_filter_blocks, false,
            "Cache index/filter blocks in block cache.");

DEFINE_bool(partition_index_and_filters, false,
            "Partition index and filter blocks.");

DEFINE_bool(partition_index, false, "Partition index blocks");

DEFINE_int64(metadata_block_size,
             rocksdb::BlockBasedTableOptions().metadata_block_size,
             "Max partition size when partitioning index/filters");

// The default reduces the overhead of reading time with flash. With HDD, which
// offers much less throughput, however, this number better to be set to 1.
DEFINE_int32(ops_between_duration_checks, 1000,
             "Check duration limit every x ops");

DEFINE_bool(pin_l0_filter_and_index_blocks_in_cache, false,
            "Pin index/filter blocks of L0 files in block cache.");

DEFINE_bool(
    pin_top_level_index_and_filter, false,
    "Pin top-level index of partitioned index/filter blocks in block cache.");

DEFINE_int32(block_size,
             static_cast<int32_t>(rocksdb::BlockBasedTableOptions().block_size),
             "Number of bytes in a block.");

DEFINE_int32(
    format_version,
    static_cast<int32_t>(rocksdb::BlockBasedTableOptions().format_version),
    "Format version of SST files.");

DEFINE_int32(block_restart_interval,
             rocksdb::BlockBasedTableOptions().block_restart_interval,
             "Number of keys between restart points "
             "for delta encoding of keys in data block.");

DEFINE_int32(index_block_restart_interval,
             rocksdb::BlockBasedTableOptions().index_block_restart_interval,
             "Number of keys between restart points "
             "for delta encoding of keys in index block.");

DEFINE_int32(read_amp_bytes_per_bit,
             rocksdb::BlockBasedTableOptions().read_amp_bytes_per_bit,
             "Number of bytes per bit to be used in block read-amp bitmap");

DEFINE_bool(enable_index_compression,
            rocksdb::BlockBasedTableOptions().enable_index_compression,
            "Compress the index block");

DEFINE_bool(block_align, rocksdb::BlockBasedTableOptions().block_align,
            "Align data blocks on page size");

DEFINE_bool(use_data_block_hash_index, false,
            "if use kDataBlockBinaryAndHash "
            "instead of kDataBlockBinarySearch. "
            "This is valid if only we use BlockTable");

DEFINE_double(data_block_hash_table_util_ratio, 0.75,
              "util ratio for data block hash index table. "
              "This is only valid if use_data_block_hash_index is "
              "set to true");

DEFINE_int64(compressed_cache_size, -1,
             "Number of bytes to use as a cache of compressed data.");

DEFINE_int64(row_cache_size, 0,
             "Number of bytes to use as a cache of individual rows"
             " (0 = disabled).");

DEFINE_int32(open_files, rocksdb::Options().max_open_files,
             "Maximum number of files to keep open at the same time"
             " (use default if == 0)");

DEFINE_int32(file_opening_threads, rocksdb::Options().max_file_opening_threads,
             "If open_files is set to -1, this option set the number of "
             "threads that will be used to open files during DB::Open()");

DEFINE_bool(new_table_reader_for_compaction_inputs, true,
            "If true, uses a separate file handle for compaction inputs");

DEFINE_int32(compaction_readahead_size, 0, "Compaction readahead size");

DEFINE_int32(random_access_max_buffer_size, 1024 * 1024,
             "Maximum windows randomaccess buffer size");

DEFINE_int32(writable_file_max_buffer_size, 1024 * 1024,
             "Maximum write buffer for Writable File");

DEFINE_int32(bloom_bits, -1,
             "Bloom filter bits per key. Negative means"
             " use default settings.");
DEFINE_double(memtable_bloom_size_ratio, 0,
              "Ratio of memtable size used for bloom filter. 0 means no bloom "
              "filter.");
DEFINE_bool(memtable_whole_key_filtering, false,
            "Try to use whole key bloom filter in memtables.");
DEFINE_bool(memtable_use_huge_page, false,
            "Try to use huge page in memtables.");

DEFINE_bool(use_existing_db, false,
            "If true, do not destroy the existing"
            " database.  If you set this flag and also specify a benchmark that"
            " wants a fresh database, that benchmark will fail.");

DEFINE_bool(show_table_properties, false,
            "If true, then per-level table"
            " properties will be printed on every stats-interval when"
            " stats_interval is set and stats_per_interval is on.");

DEFINE_string(db, "", "Use the db with the following name.");

// Read cache flags

DEFINE_string(read_cache_path, "",
              "If not empty string, a read cache will be used in this path");

DEFINE_int64(read_cache_size, 4LL * 1024 * 1024 * 1024,
             "Maximum size of the read cache");

DEFINE_bool(read_cache_direct_write, true,
            "Whether to use Direct IO for writing to the read cache");

DEFINE_bool(read_cache_direct_read, true,
            "Whether to use Direct IO for reading from read cache");

DEFINE_bool(use_keep_filter, false, "Whether to use a noop compaction filter");

static bool ValidateCacheNumshardbits(const char* flagname, int32_t value) {
  if (value >= 20) {
    fprintf(stderr, "Invalid value for --%s: %d, must be < 20\n", flagname,
            value);
    return false;
  }
  return true;
}

DEFINE_bool(verify_checksum, true,
            "Verify checksum for every block read"
            " from storage");

DEFINE_bool(statistics, false, "Database statistics");
DEFINE_int32(stats_level, rocksdb::StatsLevel::kExceptDetailedTimers,
             "stats level for statistics");
DEFINE_string(statistics_string, "", "Serialized statistics string");
static class std::shared_ptr<rocksdb::Statistics> dbstats;

DEFINE_bool(sync, false, "Sync all writes to disk");

DEFINE_bool(use_fsync, false, "If true, issue fsync instead of fdatasync");

DEFINE_bool(disable_wal, false, "If true, do not write WAL for write.");

DEFINE_string(wal_dir, "", "If not empty, use the given dir for WAL");

DEFINE_string(truth_db, "/dev/shm/truth_db/dbbench",
              "Truth key/values used when using verify");

DEFINE_int32(num_levels, 7, "The total number of levels");

DEFINE_int64(target_file_size_base, rocksdb::Options().target_file_size_base,
             "Target file size at level-1");

DEFINE_int32(target_file_size_multiplier,
             rocksdb::Options().target_file_size_multiplier,
             "A multiplier to compute target level-N file size (N >= 2)");

DEFINE_uint64(max_bytes_for_level_base,
              rocksdb::Options().max_bytes_for_level_base,
              "Max bytes for level-1");

DEFINE_bool(level_compaction_dynamic_level_bytes, false,
            "Whether level size base is dynamic");

DEFINE_double(max_bytes_for_level_multiplier, 10,
              "A multiplier to compute max bytes for level-N (N >= 2)");

static std::vector<int> FLAGS_max_bytes_for_level_multiplier_additional_v;
DEFINE_string(max_bytes_for_level_multiplier_additional, "",
              "A vector that specifies additional fanout per level");

DEFINE_int32(level0_stop_writes_trigger,
             rocksdb::Options().level0_stop_writes_trigger,
             "Number of files in level-0"
             " that will trigger put stop.");

DEFINE_int32(level0_slowdown_writes_trigger,
             rocksdb::Options().level0_slowdown_writes_trigger,
             "Number of files in level-0"
             " that will slow down writes.");

DEFINE_int32(level0_file_num_compaction_trigger,
             rocksdb::Options().level0_file_num_compaction_trigger,
             "Number of files in level-0"
             " when compactions start");

DEFINE_bool(optimize_filters_for_hits, false,
            "Optimizes bloom filters for workloads for most lookups return "
            "a value. For now this doesn't create bloom filters for the max "
            "level of the LSM to reduce metadata that should fit in RAM. ");

#ifndef ROCKSDB_LITE
// Transactions Options
DEFINE_bool(optimistic_transaction_db, false,
            "Open a OptimisticTransactionDB instance. "
            "Required for randomtransaction benchmark.");

DEFINE_bool(transaction_db, false,
            "Open a TransactionDB instance. "
            "Required for randomtransaction benchmark.");

DEFINE_string(
    options_file, "",
    "The path to a RocksDB options file.  If specified, then db_bench will "
    "run with the RocksDB options in the default column family of the "
    "specified options file. "
    "Note that with this setting, db_bench will ONLY accept the following "
    "RocksDB options related command-line arguments, all other arguments "
    "that are related to RocksDB options will be ignored:\n"
    "\t--use_existing_db\n"
    "\t--statistics\n"
    "\t--row_cache_size\n"
    "\t--row_cache_numshardbits\n"
    "\t--enable_io_prio\n"
    "\t--dump_malloc_stats\n"
    "\t--num_multi_db\n");

// FIFO Compaction Options
DEFINE_uint64(fifo_compaction_max_table_files_size_mb, 0,
              "The limit of total table file sizes to trigger FIFO compaction");

DEFINE_bool(fifo_compaction_allow_compaction, true,
            "Allow compaction in FIFO compaction.");

DEFINE_uint64(fifo_compaction_ttl, 0, "TTL for the SST Files in seconds.");

// Blob DB Options
DEFINE_bool(use_blob_db, false,
            "Open a BlobDB instance. "
            "Required for large value benchmark.");

DEFINE_bool(blob_db_enable_gc,
            rocksdb::blob_db::BlobDBOptions().enable_garbage_collection,
            "Enable BlobDB garbage collection.");

DEFINE_double(blob_db_gc_cutoff,
              rocksdb::blob_db::BlobDBOptions().garbage_collection_cutoff,
              "Cutoff ratio for BlobDB garbage collection.");

DEFINE_bool(blob_db_is_fifo, rocksdb::blob_db::BlobDBOptions().is_fifo,
            "Enable FIFO eviction strategy in BlobDB.");

DEFINE_uint64(blob_db_max_db_size,
              rocksdb::blob_db::BlobDBOptions().max_db_size,
              "Max size limit of the directory where blob files are stored.");

DEFINE_uint64(blob_db_ttl_range_secs,
              rocksdb::blob_db::BlobDBOptions().ttl_range_secs,
              "TTL bucket size to use when creating blob files.");

DEFINE_uint64(blob_db_min_blob_size,
              rocksdb::blob_db::BlobDBOptions().min_blob_size,
              "Smallest blob to store in a file. Blobs smaller than this "
              "will be inlined with the key in the LSM tree.");

DEFINE_uint64(blob_db_bytes_per_sync,
              rocksdb::blob_db::BlobDBOptions().bytes_per_sync,
              "Bytes to sync blob file at.");

DEFINE_uint64(blob_db_file_size,
              rocksdb::blob_db::BlobDBOptions().blob_file_size,
              "Target size of each blob file.");

// Secondary DB instance Options
DEFINE_bool(use_secondary_db, false,
            "Open a RocksDB secondary instance. A primary instance can be "
            "running in another db_bench process.");

DEFINE_string(secondary_path, "",
              "Path to a directory used by the secondary instance to store "
              "private files, e.g. info log.");

DEFINE_int32(secondary_update_interval, 5,
             "Secondary instance attempts to catch up with the primary every "
             "secondary_update_interval seconds.");

#endif  // ROCKSDB_LITE

DEFINE_bool(report_bg_io_stats, false,
            "Measure times spents on I/Os while in compactions. ");

DEFINE_bool(use_stderr_info_logger, false,
            "Write info logs to stderr instead of to LOG file. ");

DEFINE_string(trace_file, "", "Trace workload to a file. ");

DEFINE_int32(block_cache_trace_sampling_frequency, 1,
             "Block cache trace sampling frequency, termed s. It uses spatial "
             "downsampling and samples accesses to one out of s blocks.");
DEFINE_int64(
    block_cache_trace_max_trace_file_size_in_bytes,
    uint64_t{64} * 1024 * 1024 * 1024,
    "The maximum block cache trace file size in bytes. Block cache accesses "
    "will not be logged if the trace file size exceeds this threshold. Default "
    "is 64 GB.");
DEFINE_string(block_cache_trace_file, "", "Block cache trace file path.");

static enum rocksdb::CompressionType StringToCompressionType(
    const char* ctype) {
  assert(ctype);

  if (!strcasecmp(ctype, "none"))
    return rocksdb::kNoCompression;
  else if (!strcasecmp(ctype, "snappy"))
    return rocksdb::kSnappyCompression;
  else if (!strcasecmp(ctype, "zlib"))
    return rocksdb::kZlibCompression;
  else if (!strcasecmp(ctype, "bzip2"))
    return rocksdb::kBZip2Compression;
  else if (!strcasecmp(ctype, "lz4"))
    return rocksdb::kLZ4Compression;
  else if (!strcasecmp(ctype, "lz4hc"))
    return rocksdb::kLZ4HCCompression;
  else if (!strcasecmp(ctype, "xpress"))
    return rocksdb::kXpressCompression;
  else if (!strcasecmp(ctype, "zstd"))
    return rocksdb::kZSTD;

  fprintf(stdout, "Cannot parse compression type '%s'\n", ctype);
  return rocksdb::kSnappyCompression;  // default value
}

static std::string ColumnFamilyName(size_t i) {
  if (i == 0) {
    return rocksdb::kDefaultColumnFamilyName;
  } else {
    char name[100];
    snprintf(name, sizeof(name), "column_family_name_%06zu", i);
    return std::string(name);
  }
}

DEFINE_string(compression_type, "snappy",
              "Algorithm to use to compress the database");
static enum rocksdb::CompressionType FLAGS_compression_type_e =
    rocksdb::kSnappyCompression;

DEFINE_int64(sample_for_compression, 0, "Sample every N block for compression");

DEFINE_int32(compression_level, rocksdb::CompressionOptions().level,
             "Compression level. The meaning of this value is library-"
             "dependent. If unset, we try to use the default for the library "
             "specified in `--compression_type`");

DEFINE_int32(compression_max_dict_bytes,
             rocksdb::CompressionOptions().max_dict_bytes,
             "Maximum size of dictionary used to prime the compression "
             "library.");

DEFINE_int32(compression_zstd_max_train_bytes,
             rocksdb::CompressionOptions().zstd_max_train_bytes,
             "Maximum size of training data passed to zstd's dictionary "
             "trainer.");

DEFINE_int32(min_level_to_compress, -1,
             "If non-negative, compression starts"
             " from this level. Levels with number < min_level_to_compress are"
             " not compressed. Otherwise, apply compression_type to "
             "all levels.");

static bool ValidateTableCacheNumshardbits(const char* flagname,
                                           int32_t value) {
  if (0 >= value || value > 20) {
    fprintf(stderr, "Invalid value for --%s: %d, must be  0 < val <= 20\n",
            flagname, value);
    return false;
  }
  return true;
}
DEFINE_int32(table_cache_numshardbits, 4, "");

#ifndef ROCKSDB_LITE
DEFINE_string(env_uri, "",
              "URI for registry Env lookup. Mutually exclusive"
              " with --hdfs.");
#endif  // ROCKSDB_LITE
DEFINE_string(hdfs, "",
              "Name of hdfs environment. Mutually exclusive with"
              " --env_uri.");

static std::shared_ptr<rocksdb::Env> env_guard;

static rocksdb::Env* FLAGS_env = rocksdb::Env::Default();

DEFINE_int64(stats_interval, 0,
             "Stats are reported every N operations when "
             "this is greater than zero. When 0 the interval grows over time.");

DEFINE_int64(stats_interval_seconds, 0,
             "Report stats every N seconds. This "
             "overrides stats_interval when both are > 0.");

DEFINE_int32(stats_per_interval, 0,
             "Reports additional stats per interval when"
             " this is greater than 0.");

DEFINE_int64(report_interval_seconds, 0,
             "If greater than zero, it will write simple stats in CVS format "
             "to --report_file every N seconds");

DEFINE_string(report_file, "report.csv",
              "Filename where some simple stats are reported to (if "
              "--report_interval_seconds is bigger than 0)");

DEFINE_int32(thread_status_per_interval, 0,
             "Takes and report a snapshot of the current status of each thread"
             " when this is greater than 0.");

DEFINE_int32(perf_level, rocksdb::PerfLevel::kDisable,
             "Level of perf collection");

static bool ValidateRateLimit(const char* flagname, double value) {
  const double EPSILON = 1e-10;
  if (value < -EPSILON) {
    fprintf(stderr, "Invalid value for --%s: %12.6f, must be >= 0.0\n",
            flagname, value);
    return false;
  }
  return true;
}
DEFINE_double(soft_rate_limit, 0.0, "DEPRECATED");

DEFINE_double(hard_rate_limit, 0.0, "DEPRECATED");

DEFINE_uint64(soft_pending_compaction_bytes_limit, 64ull * 1024 * 1024 * 1024,
              "Slowdown writes if pending compaction bytes exceed this number");

DEFINE_uint64(hard_pending_compaction_bytes_limit, 128ull * 1024 * 1024 * 1024,
              "Stop writes if pending compaction bytes exceed this number");

DEFINE_uint64(delayed_write_rate, 8388608u,
              "Limited bytes allowed to DB when soft_rate_limit or "
              "level0_slowdown_writes_trigger triggers");

DEFINE_bool(enable_pipelined_write, true,
            "Allow WAL and memtable writes to be pipelined");

DEFINE_bool(unordered_write, false,
            "Allow WAL and memtable writes to be pipelined");

DEFINE_bool(allow_concurrent_memtable_write, true,
            "Allow multi-writers to update mem tables in parallel.");

DEFINE_bool(inplace_update_support, rocksdb::Options().inplace_update_support,
            "Support in-place memtable update for smaller or same-size values");

DEFINE_uint64(inplace_update_num_locks,
              rocksdb::Options().inplace_update_num_locks,
              "Number of RW locks to protect in-place memtable updates");

DEFINE_bool(enable_write_thread_adaptive_yield, true,
            "Use a yielding spin loop for brief writer thread waits.");

DEFINE_uint64(
    write_thread_max_yield_usec, 100,
    "Maximum microseconds for enable_write_thread_adaptive_yield operation.");

DEFINE_uint64(write_thread_slow_yield_usec, 3,
              "The threshold at which a slow yield is considered a signal that "
              "other processes or threads want the core.");

DEFINE_int32(rate_limit_delay_max_milliseconds, 1000,
             "When hard_rate_limit is set then this is the max time a put will"
             " be stalled.");

DEFINE_uint64(rate_limiter_bytes_per_sec, 0, "Set options.rate_limiter value.");

DEFINE_bool(rate_limiter_auto_tuned, false,
            "Enable dynamic adjustment of rate limit according to demand for "
            "background I/O");

DEFINE_bool(rate_limit_bg_reads, false,
            "Use options.rate_limiter on compaction reads");

DEFINE_uint64(max_compaction_bytes, rocksdb::Options().max_compaction_bytes,
              "Max bytes allowed in one compaction");

#ifndef ROCKSDB_LITE
DEFINE_bool(readonly, false, "Run read only benchmarks.");

DEFINE_bool(print_malloc_stats, false,
            "Print malloc stats to stdout after benchmarks finish.");
#endif  // ROCKSDB_LITE

DEFINE_bool(disable_auto_compactions, false, "Do not auto trigger compactions");

DEFINE_uint64(wal_ttl_seconds, 0, "Set the TTL for the WAL Files in seconds.");
DEFINE_uint64(wal_size_limit_MB, 0,
              "Set the size limit for the WAL Files"
              " in MB.");
DEFINE_uint64(max_total_wal_size, 0, "Set total max WAL size");

DEFINE_bool(mmap_read, rocksdb::Options().allow_mmap_reads,
            "Allow reads to occur via mmap-ing files");

DEFINE_bool(mmap_write, rocksdb::Options().allow_mmap_writes,
            "Allow writes to occur via mmap-ing files");

DEFINE_bool(use_direct_reads, rocksdb::Options().use_direct_reads,
            "Use O_DIRECT for reading data");

DEFINE_bool(use_direct_io_for_flush_and_compaction,
            rocksdb::Options().use_direct_io_for_flush_and_compaction,
            "Use O_DIRECT for background flush and compaction writes");

DEFINE_bool(advise_random_on_open, rocksdb::Options().advise_random_on_open,
            "Advise random access on table file open");

DEFINE_string(compaction_fadvice, "NORMAL",
              "Access pattern advice when a file is compacted");
static auto FLAGS_compaction_fadvice_e =
    rocksdb::Options().access_hint_on_compaction_start;

DEFINE_bool(use_adaptive_mutex, rocksdb::Options().use_adaptive_mutex,
            "Use adaptive mutex");

DEFINE_uint64(bytes_per_sync, rocksdb::Options().bytes_per_sync,
              "Allows OS to incrementally sync SST files to disk while they are"
              " being written, in the background. Issue one request for every"
              " bytes_per_sync written. 0 turns it off.");

DEFINE_uint64(wal_bytes_per_sync, rocksdb::Options().wal_bytes_per_sync,
              "Allows OS to incrementally sync WAL files to disk while they are"
              " being written, in the background. Issue one request for every"
              " wal_bytes_per_sync written. 0 turns it off.");

DEFINE_uint64(
    time_range, 100000,
    "Range of timestamp that store in the database (used in TimeSeries"
    " only).");

DEFINE_int32(max_successive_merges, 0,
             "Maximum number of successive merge"
             " operations on a key in the memtable");

DEFINE_bool(enable_io_prio, false,
            "Lower the background flush/compaction "
            "threads' IO priority");
DEFINE_bool(enable_cpu_prio, false,
            "Lower the background flush/compaction "
            "threads' CPU priority");
DEFINE_bool(identity_as_first_hash, false,
            "the first hash function of cuckoo "
            "table becomes an identity function. This is only valid when key "
            "is 8 bytes");
DEFINE_bool(dump_malloc_stats, true, "Dump malloc stats in LOG ");
DEFINE_uint64(stats_dump_period_sec, rocksdb::Options().stats_dump_period_sec,
              "Gap between printing stats to log in seconds");
DEFINE_uint64(stats_persist_period_sec,
              rocksdb::Options().stats_persist_period_sec,
              "Gap between persisting stats in seconds");
DEFINE_bool(persist_stats_to_disk, rocksdb::Options().persist_stats_to_disk,
            "whether to persist stats to disk");
DEFINE_uint64(stats_history_buffer_size,
              rocksdb::Options().stats_history_buffer_size,
              "Max number of stats snapshots to keep in memory");

enum RepFactory {
  kSkipList,
  kPrefixHash,
  kVectorRep,
  kHashLinkedList,
};

static enum RepFactory StringToRepFactory(const char* ctype) {
  assert(ctype);

  if (!strcasecmp(ctype, "skip_list"))
    return kSkipList;
  else if (!strcasecmp(ctype, "prefix_hash"))
    return kPrefixHash;
  else if (!strcasecmp(ctype, "vector"))
    return kVectorRep;
  else if (!strcasecmp(ctype, "hash_linkedlist"))
    return kHashLinkedList;

  fprintf(stdout, "Cannot parse memreptable %s\n", ctype);
  return kSkipList;
}

static enum RepFactory FLAGS_rep_factory;
DEFINE_string(memtablerep, "skip_list", "");
DEFINE_int64(hash_bucket_count, 1024 * 1024, "hash bucket count");
DEFINE_bool(use_plain_table, false,
            "if use plain table "
            "instead of block-based table format");
DEFINE_bool(use_cuckoo_table, false, "if use cuckoo table format");
DEFINE_double(cuckoo_hash_ratio, 0.9, "Hash ratio for Cuckoo SST table.");
DEFINE_bool(use_hash_search, false,
            "if use kHashSearch "
            "instead of kBinarySearch. "
            "This is valid if only we use BlockTable");
DEFINE_bool(use_block_based_filter, false,
            "if use kBlockBasedFilter "
            "instead of kFullFilter for filter block. "
            "This is valid if only we use BlockTable");
DEFINE_string(merge_operator, "",
              "The merge operator to use with the database."
              "If a new merge operator is specified, be sure to use fresh"
              " database The possible merge operators are defined in"
              " utilities/merge_operators.h");
DEFINE_int32(skip_list_lookahead, 0,
             "Used with skip_list memtablerep; try "
             "linear search first for this many steps from the previous "
             "position");
DEFINE_bool(report_file_operations, false,
            "if report number of file "
            "operations");
DEFINE_int32(readahead_size, 0, "Iterator readahead size");

static const bool FLAGS_soft_rate_limit_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_soft_rate_limit, &ValidateRateLimit);

static const bool FLAGS_hard_rate_limit_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_hard_rate_limit, &ValidateRateLimit);

static const bool FLAGS_key_size_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_key_size, &ValidateKeySize);

static const bool FLAGS_cache_numshardbits_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_cache_numshardbits,
                          &ValidateCacheNumshardbits);

DEFINE_int32(disable_seek_compaction, false,
             "Not used, left here for backwards compatibility");

static const bool FLAGS_table_cache_numshardbits_dummy
    __attribute__((__unused__)) = RegisterFlagValidator(
        &FLAGS_table_cache_numshardbits, &ValidateTableCacheNumshardbits);

namespace rocksdb {
namespace ycsb {

using namespace util;

namespace {
struct ReportFileOpCounters {
  std::atomic<int> open_counter_;
  std::atomic<int> read_counter_;
  std::atomic<int> append_counter_;
  std::atomic<uint64_t> bytes_read_;
  std::atomic<uint64_t> bytes_written_;
};

// A special Env to records and report file operations in db_bench
class ReportFileOpEnv : public EnvWrapper {
 public:
  explicit ReportFileOpEnv(Env* base) : EnvWrapper(base) { reset(); }

  void reset() {
    counters_.open_counter_ = 0;
    counters_.read_counter_ = 0;
    counters_.append_counter_ = 0;
    counters_.bytes_read_ = 0;
    counters_.bytes_written_ = 0;
  }

  Status NewSequentialFile(const std::string& f,
                           std::unique_ptr<SequentialFile>* r,
                           const EnvOptions& soptions) override {
    class CountingFile : public SequentialFile {
     private:
      std::unique_ptr<SequentialFile> target_;
      ReportFileOpCounters* counters_;

     public:
      CountingFile(std::unique_ptr<SequentialFile>&& target,
                   ReportFileOpCounters* counters)
          : target_(std::move(target)), counters_(counters) {}

      Status Read(size_t n, Slice* result, char* scratch) override {
        counters_->read_counter_.fetch_add(1, std::memory_order_relaxed);
        Status rv = target_->Read(n, result, scratch);
        counters_->bytes_read_.fetch_add(result->size(),
                                         std::memory_order_relaxed);
        return rv;
      }

      Status Skip(uint64_t n) override { return target_->Skip(n); }
    };

    Status s = target()->NewSequentialFile(f, r, soptions);
    if (s.ok()) {
      counters()->open_counter_.fetch_add(1, std::memory_order_relaxed);
      r->reset(new CountingFile(std::move(*r), counters()));
    }
    return s;
  }

  Status NewRandomAccessFile(const std::string& f,
                             std::unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& soptions) override {
    class CountingFile : public RandomAccessFile {
     private:
      std::unique_ptr<RandomAccessFile> target_;
      ReportFileOpCounters* counters_;

     public:
      CountingFile(std::unique_ptr<RandomAccessFile>&& target,
                   ReportFileOpCounters* counters)
          : target_(std::move(target)), counters_(counters) {}
      Status Read(uint64_t offset, size_t n, Slice* result,
                  char* scratch) const override {
        counters_->read_counter_.fetch_add(1, std::memory_order_relaxed);
        Status rv = target_->Read(offset, n, result, scratch);
        counters_->bytes_read_.fetch_add(result->size(),
                                         std::memory_order_relaxed);
        return rv;
      }
    };

    Status s = target()->NewRandomAccessFile(f, r, soptions);
    if (s.ok()) {
      counters()->open_counter_.fetch_add(1, std::memory_order_relaxed);
      r->reset(new CountingFile(std::move(*r), counters()));
    }
    return s;
  }

  Status NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,
                         const EnvOptions& soptions) override {
    class CountingFile : public WritableFile {
     private:
      std::unique_ptr<WritableFile> target_;
      ReportFileOpCounters* counters_;

     public:
      CountingFile(std::unique_ptr<WritableFile>&& target,
                   ReportFileOpCounters* counters)
          : target_(std::move(target)), counters_(counters) {}

      Status Append(const Slice& data) override {
        counters_->append_counter_.fetch_add(1, std::memory_order_relaxed);
        Status rv = target_->Append(data);
        counters_->bytes_written_.fetch_add(data.size(),
                                            std::memory_order_relaxed);
        return rv;
      }

      Status Truncate(uint64_t size) override {
        return target_->Truncate(size);
      }
      Status Close() override { return target_->Close(); }
      Status Flush() override { return target_->Flush(); }
      Status Sync() override { return target_->Sync(); }
    };

    Status s = target()->NewWritableFile(f, r, soptions);
    if (s.ok()) {
      counters()->open_counter_.fetch_add(1, std::memory_order_relaxed);
      r->reset(new CountingFile(std::move(*r), counters()));
    }
    return s;
  }

  // getter
  ReportFileOpCounters* counters() { return &counters_; }

 private:
  ReportFileOpCounters counters_;
};

}  // namespace

static void AppendWithSpace(std::string* str, Slice msg) {
  if (msg.empty()) return;
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data(), msg.size());
}

struct DBWithColumnFamilies {
  std::vector<ColumnFamilyHandle*> cfh;
  DB* db;
#ifndef ROCKSDB_LITE
  OptimisticTransactionDB* opt_txn_db;
#endif                              // ROCKSDB_LITE
  std::atomic<size_t> num_created;  // Need to be updated after all the
                                    // new entries in cfh are set.
  size_t num_hot;  // Number of column families to be queried at each moment.
                   // After each CreateNewCf(), another num_hot number of new
                   // Column families will be created and used to be queried.
  port::Mutex create_cf_mutex;  // Only one thread can execute CreateNewCf()
  std::vector<int> cfh_idx_to_prob;  // ith index holds probability of operating
                                     // on cfh[i].

  DBWithColumnFamilies()
      : db(nullptr)
#ifndef ROCKSDB_LITE
        ,
        opt_txn_db(nullptr)
#endif  // ROCKSDB_LITE
  {
    cfh.clear();
    num_created = 0;
    num_hot = 0;
  }

  DBWithColumnFamilies(const DBWithColumnFamilies& other)
      : cfh(other.cfh),
        db(other.db),
#ifndef ROCKSDB_LITE
        opt_txn_db(other.opt_txn_db),
#endif  // ROCKSDB_LITE
        num_created(other.num_created.load()),
        num_hot(other.num_hot),
        cfh_idx_to_prob(other.cfh_idx_to_prob) {
  }

  void DeleteDBs() {
    std::for_each(cfh.begin(), cfh.end(),
                  [](ColumnFamilyHandle* cfhi) { delete cfhi; });
    cfh.clear();
#ifndef ROCKSDB_LITE
    if (opt_txn_db) {
      delete opt_txn_db;
      opt_txn_db = nullptr;
    } else {
      delete db;
      db = nullptr;
    }
#else
    delete db;
    db = nullptr;
#endif  // ROCKSDB_LITE
  }

  ColumnFamilyHandle* GetCfhByID(int64_t num) {
    return cfh[num];
  }

  ColumnFamilyHandle* GetCfh(int64_t rand_num) {
    assert(num_hot > 0);
    size_t rand_offset = 0;
    if (!cfh_idx_to_prob.empty()) {
      assert(cfh_idx_to_prob.size() == num_hot);
      int sum = 0;
      while (sum + cfh_idx_to_prob[rand_offset] < rand_num % 100) {
        sum += cfh_idx_to_prob[rand_offset];
        ++rand_offset;
      }
      assert(rand_offset < cfh_idx_to_prob.size());
    } else {
      rand_offset = rand_num % num_hot;
    }
    return cfh[num_created.load(std::memory_order_acquire) - num_hot +
               rand_offset];
  }

  // stage: assume CF from 0 to stage * num_hot has be created. Need to create
  //        stage * num_hot + 1 to stage * (num_hot + 1).
  void CreateNewCf(ColumnFamilyOptions options, int64_t stage) {
    MutexLock l(&create_cf_mutex);
    if ((stage + 1) * num_hot <= num_created) {
      // Already created.
      return;
    }
    auto new_num_created = num_created + num_hot;
    assert(new_num_created <= cfh.size());
    for (size_t i = num_created; i < new_num_created; i++) {
      Status s =
          db->CreateColumnFamily(options, ColumnFamilyName(i), &(cfh[i]));
      if (!s.ok()) {
        fprintf(stderr, "create column family error: %s\n",
                s.ToString().c_str());
        abort();
      }
    }
    num_created.store(new_num_created, std::memory_order_release);
  }
};

// a class that reports stats to CSV file
class ReporterAgent {
 public:
  ReporterAgent(Env* env, const std::string& fname,
                uint64_t report_interval_secs)
      : env_(env),
        total_ops_done_(0),
        last_report_(0),
        report_interval_secs_(report_interval_secs),
        stop_(false) {
    auto s = env_->NewWritableFile(fname, &report_file_, EnvOptions());
    if (s.ok()) {
      s = report_file_->Append(Header() + "\n");
    }
    if (s.ok()) {
      s = report_file_->Flush();
    }
    if (!s.ok()) {
      fprintf(stderr, "Can't open %s: %s\n", fname.c_str(),
              s.ToString().c_str());
      abort();
    }

    reporting_thread_ = port::Thread([&]() { SleepAndReport(); });
  }

  ~ReporterAgent() {
    {
      std::unique_lock<std::mutex> lk(mutex_);
      stop_ = true;
      stop_cv_.notify_all();
    }
    reporting_thread_.join();
  }

  // thread safe
  void ReportFinishedOps(int64_t num_ops) {
    total_ops_done_.fetch_add(num_ops);
  }

 private:
  std::string Header() const { return "secs_elapsed,interval_qps"; }
  void SleepAndReport() {
    auto time_started = env_->NowMicros();
    while (true) {
      {
        std::unique_lock<std::mutex> lk(mutex_);
        if (stop_ ||
            stop_cv_.wait_for(lk, std::chrono::seconds(report_interval_secs_),
                              [&]() { return stop_; })) {
          // stopping
          break;
        }
        // else -> timeout, which means time for a report!
      }
      auto total_ops_done_snapshot = total_ops_done_.load();
      // round the seconds elapsed
      auto secs_elapsed =
          (env_->NowMicros() - time_started + kMicrosInSecond / 2) /
          kMicrosInSecond;
      std::string report = ToString(secs_elapsed) + "," +
                           ToString(total_ops_done_snapshot - last_report_) +
                           "\n";
      auto s = report_file_->Append(report);
      if (s.ok()) {
        s = report_file_->Flush();
      }
      if (!s.ok()) {
        fprintf(stderr,
                "Can't write to report file (%s), stopping the reporting\n",
                s.ToString().c_str());
        break;
      }
      last_report_ = total_ops_done_snapshot;
    }
  }

  Env* env_;
  std::unique_ptr<WritableFile> report_file_;
  std::atomic<int64_t> total_ops_done_;
  int64_t last_report_;
  const uint64_t report_interval_secs_;
  rocksdb::port::Thread reporting_thread_;
  std::mutex mutex_;
  // will notify on stop
  std::condition_variable stop_cv_;
  bool stop_;
};

enum OperationType : unsigned char {
  kRead = 0,
  kWrite,
  kDelete,
  kSeek,
  kMerge,
  kUpdate,
  kReadModifyWrite,
  kCompress,
  kUncompress,
  kCrc,
  kHash,
  kOthers
};

static std::unordered_map<OperationType, std::string, std::hash<unsigned char>>
    OperationTypeString = {{kRead, "read"},         {kWrite, "write"},
                           {kDelete, "delete"},     {kSeek, "seek"},
                           {kMerge, "merge"},       {kUpdate, "update"},
                           {kCompress, "compress"}, {kCompress, "uncompress"},
                           {kCrc, "crc"},           {kHash, "hash"},
                           {kOthers, "op"},         {kReadModifyWrite, "readmodifywrite"}};

class Stats {
 private:
  int id_;
  uint64_t start_;
  uint64_t sine_interval_;
  uint64_t finish_;
  double seconds_;
  uint64_t done_;
  uint64_t last_report_done_;
  uint64_t next_report_;
  uint64_t bytes_;
  uint64_t last_op_finish_;
  uint64_t last_report_finish_;
  std::unordered_map<OperationType, std::shared_ptr<HistogramImpl>,
                     std::hash<unsigned char>>
      hist_;
  std::string message_;
  bool exclude_from_merge_;
  ReporterAgent* reporter_agent_;  // does not own

 public:
  Stats() { Start(-1); }

  void SetReporterAgent(ReporterAgent* reporter_agent) {
    reporter_agent_ = reporter_agent;
  }

  void Start(int id) {
    id_ = id;
    next_report_ = FLAGS_stats_interval ? FLAGS_stats_interval : 100;
    last_op_finish_ = start_;
    hist_.clear();
    done_ = 0;
    last_report_done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = FLAGS_env->NowMicros();
    sine_interval_ = FLAGS_env->NowMicros();
    finish_ = start_;
    last_report_finish_ = start_;
    message_.clear();
    // When set, stats from this thread won't be merged with others.
    exclude_from_merge_ = false;
  }

  void Merge(const Stats& other) {
    if (other.exclude_from_merge_) return;

    for (auto it = other.hist_.begin(); it != other.hist_.end(); ++it) {
      auto this_it = hist_.find(it->first);
      if (this_it != hist_.end()) {
        this_it->second->Merge(*(other.hist_.at(it->first)));
      } else {
        hist_.insert({it->first, it->second});
      }
    }

    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
    finish_ = FLAGS_env->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void AddMessage(Slice msg) { AppendWithSpace(&message_, msg); }

  void SetId(int id) { id_ = id; }
  void SetExcludeFromMerge() { exclude_from_merge_ = true; }

  void PrintThreadStatus() {
    std::vector<ThreadStatus> thread_list;
    FLAGS_env->GetThreadList(&thread_list);

    fprintf(stderr, "\n%18s %10s %12s %20s %13s %45s %12s %s\n", "ThreadID",
            "ThreadType", "cfName", "Operation", "ElapsedTime", "Stage",
            "State", "OperationProperties");

    int64_t current_time = 0;
    FLAGS_env->GetCurrentTime(&current_time);
    for (auto ts : thread_list) {
      fprintf(stderr, "%18" PRIu64 " %10s %12s %20s %13s %45s %12s",
              ts.thread_id,
              ThreadStatus::GetThreadTypeName(ts.thread_type).c_str(),
              ts.cf_name.c_str(),
              ThreadStatus::GetOperationName(ts.operation_type).c_str(),
              ThreadStatus::MicrosToString(ts.op_elapsed_micros).c_str(),
              ThreadStatus::GetOperationStageName(ts.operation_stage).c_str(),
              ThreadStatus::GetStateName(ts.state_type).c_str());

      auto op_properties = ThreadStatus::InterpretOperationProperties(
          ts.operation_type, ts.op_properties);
      for (const auto& op_prop : op_properties) {
        fprintf(stderr, " %s %" PRIu64 " |", op_prop.first.c_str(),
                op_prop.second);
      }
      fprintf(stderr, "\n");
    }
  }

  void ResetSineInterval() { sine_interval_ = FLAGS_env->NowMicros(); }

  uint64_t GetSineInterval() { return sine_interval_; }

  uint64_t GetStart() { return start_; }

  void ResetLastOpTime() {
    // Set to now to avoid latency from calls to SleepForMicroseconds
    last_op_finish_ = FLAGS_env->NowMicros();
  }

  void FinishedOps(DBWithColumnFamilies* db_with_cfh, DB* db, int64_t num_ops,
                   enum OperationType op_type = kOthers) {
    if (reporter_agent_) {
      reporter_agent_->ReportFinishedOps(num_ops);
    }
    if (FLAGS_histogram) {
      uint64_t now = FLAGS_env->NowMicros();
      uint64_t micros = now - last_op_finish_;

      if (hist_.find(op_type) == hist_.end()) {
        auto hist_temp = std::make_shared<HistogramImpl>();
        hist_.insert({op_type, std::move(hist_temp)});
      }
      hist_[op_type]->Add(micros);

      if (micros > 20000 && !FLAGS_stats_interval) {
        fprintf(stderr, "long op: %" PRIu64 " micros%30s\r", micros, "");
        fflush(stderr);
      }
      last_op_finish_ = now;
    }

    done_ += num_ops;
    if (done_ >= next_report_) {
      if (!FLAGS_stats_interval) {
        if (next_report_ < 1000)
          next_report_ += 100;
        else if (next_report_ < 5000)
          next_report_ += 500;
        else if (next_report_ < 10000)
          next_report_ += 1000;
        else if (next_report_ < 50000)
          next_report_ += 5000;
        else if (next_report_ < 100000)
          next_report_ += 10000;
        else if (next_report_ < 500000)
          next_report_ += 50000;
        else
          next_report_ += 100000;
        fprintf(stderr, "... finished %" PRIu64 " ops%30s\r", done_, "");
      } else {
        uint64_t now = FLAGS_env->NowMicros();
        int64_t usecs_since_last = now - last_report_finish_;

        // Determine whether to print status where interval is either
        // each N operations or each N seconds.

        if (FLAGS_stats_interval_seconds &&
            usecs_since_last < (FLAGS_stats_interval_seconds * 1000000)) {
          // Don't check again for this many operations
          next_report_ += FLAGS_stats_interval;

        } else {
          fprintf(stderr,
                  "%s ... thread %d: (%" PRIu64 ",%" PRIu64
                  ") ops and "
                  "(%.1f,%.1f) ops/second in (%.6f,%.6f) seconds\n",
                  FLAGS_env->TimeToString(now / 1000000).c_str(), id_,
                  done_ - last_report_done_, done_,
                  (done_ - last_report_done_) / (usecs_since_last / 1000000.0),
                  done_ / ((now - start_) / 1000000.0),
                  (now - last_report_finish_) / 1000000.0,
                  (now - start_) / 1000000.0);

          if (id_ == 0 && FLAGS_stats_per_interval) {
            std::string stats;

            if (db_with_cfh && db_with_cfh->num_created.load()) {
              for (size_t i = 0; i < db_with_cfh->num_created.load(); ++i) {
                if (db->GetProperty(db_with_cfh->cfh[i], "rocksdb.cfstats",
                                    &stats))
                  fprintf(stderr, "%s\n", stats.c_str());
                if (FLAGS_show_table_properties) {
                  for (int level = 0; level < FLAGS_num_levels; ++level) {
                    if (db->GetProperty(
                            db_with_cfh->cfh[i],
                            "rocksdb.aggregated-table-properties-at-level" +
                                ToString(level),
                            &stats)) {
                      if (stats.find("# entries=0") == std::string::npos) {
                        fprintf(stderr, "Level[%d]: %s\n", level,
                                stats.c_str());
                      }
                    }
                  }
                }
              }
            } else if (db) {
              if (db->GetProperty("rocksdb.stats", &stats)) {
                fprintf(stderr, "%s\n", stats.c_str());
              }
              if (FLAGS_show_table_properties) {
                for (int level = 0; level < FLAGS_num_levels; ++level) {
                  if (db->GetProperty(
                          "rocksdb.aggregated-table-properties-at-level" +
                              ToString(level),
                          &stats)) {
                    if (stats.find("# entries=0") == std::string::npos) {
                      fprintf(stderr, "Level[%d]: %s\n", level, stats.c_str());
                    }
                  }
                }
              }
            }
          }

          next_report_ += FLAGS_stats_interval;
          last_report_finish_ = now;
          last_report_done_ = done_;
        }
      }
      if (id_ == 0 && FLAGS_thread_status_per_interval) {
        PrintThreadStatus();
      }
      fflush(stderr);
    }
  }

  void AddBytes(int64_t n) { bytes_ += n; }

  void Report(const Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedOps().
    if (done_ < 1) done_ = 1;

    std::string extra;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      double elapsed = (finish_ - start_) * 1e-6;
      char rate[100];
      snprintf(rate, sizeof(rate), "%6.1f MB/s",
               (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }
    AppendWithSpace(&extra, message_);
    double elapsed = (finish_ - start_) * 1e-6;
    double throughput = (double)done_ / elapsed;

    fprintf(stdout, "%-12s : %11.3f micros/op %ld ops/sec;%s%s\n",
            name.ToString().c_str(), seconds_ * 1e6 / done_, (long)throughput,
            (extra.empty() ? "" : " "), extra.c_str());
    if (FLAGS_histogram) {
      for (auto it = hist_.begin(); it != hist_.end(); ++it) {
        fprintf(stdout, "Microseconds per %s:\n%s\n",
                OperationTypeString[it->first].c_str(),
                it->second->ToString().c_str());
      }
    }
    if (FLAGS_report_file_operations) {
      ReportFileOpEnv* env = static_cast<ReportFileOpEnv*>(FLAGS_env);
      ReportFileOpCounters* counters = env->counters();
      fprintf(stdout, "Num files opened: %d\n",
              counters->open_counter_.load(std::memory_order_relaxed));
      fprintf(stdout, "Num Read(): %d\n",
              counters->read_counter_.load(std::memory_order_relaxed));
      fprintf(stdout, "Num Append(): %d\n",
              counters->append_counter_.load(std::memory_order_relaxed));
      fprintf(stdout, "Num bytes read: %" PRIu64 "\n",
              counters->bytes_read_.load(std::memory_order_relaxed));
      fprintf(stdout, "Num bytes written: %" PRIu64 "\n",
              counters->bytes_written_.load(std::memory_order_relaxed));
      env->reset();
    }
    fflush(stdout);
  }
};

class TimestampEmulator {
 private:
  std::atomic<uint64_t> timestamp_;

 public:
  TimestampEmulator() : timestamp_(0) {}
  uint64_t Get() const { return timestamp_.load(); }
  void Inc() { timestamp_++; }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
  port::Mutex mu;
  port::CondVar cv;
  int total;
  int perf_level;

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  long num_initialized;
  long num_done;
  bool start;

  SharedState() : cv(&mu), perf_level(FLAGS_perf_level) {}
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  int tid;        // 0..n-1 when running in n threads
  Stats stats;
  SharedState* shared;
	int cfid;
  Workload *workload;

  /* implicit */ ThreadState(int index)
      : tid(index) {}
};

class Duration {
 public:
  Duration(uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage = 0) {
    max_seconds_ = max_seconds;
    max_ops_ = max_ops;
    ops_per_stage_ = (ops_per_stage > 0) ? ops_per_stage : max_ops;
    ops_ = 0;
    start_at_ = FLAGS_env->NowMicros();
  }

  int64_t GetStage() { return std::min(ops_, max_ops_ - 1) / ops_per_stage_; }

  bool Done(int64_t increment) {
    if (increment <= 0) increment = 1;  // avoid Done(0) and infinite loops
    ops_ += increment;

    if (max_seconds_) {
      // Recheck every appx 1000 ops (exact iff increment is factor of 1000)
      auto granularity = FLAGS_ops_between_duration_checks;
      if ((ops_ / granularity) != ((ops_ - increment) / granularity)) {
        uint64_t now = FLAGS_env->NowMicros();
        return ((now - start_at_) / 1000000) >= max_seconds_;
      } else {
        return false;
      }
    } else {
      return ops_ > max_ops_;
    }
  }

 private:
  uint64_t max_seconds_;
  int64_t max_ops_;
  int64_t ops_per_stage_;
  int64_t ops_;
  uint64_t start_at_;
};

class Benchmark {
 private:
  std::shared_ptr<Cache> cache_;
  std::shared_ptr<Cache> compressed_cache_;
  std::shared_ptr<const FilterPolicy> filter_policy_;
  DBWithColumnFamilies db_;
  std::vector<DBWithColumnFamilies> multi_dbs_;
  int64_t num_;
  int value_size_;
  int key_size_;
  WriteOptions write_options_;
  Options open_options_;  // keep options around to properly destroy db later
#ifndef ROCKSDB_LITE
  TraceOptions trace_options_;
  TraceOptions block_cache_trace_options_;
#endif
  bool report_file_operations_;
  bool use_blob_db_;
  std::vector<std::string> keys_;

  class ErrorHandlerListener : public EventListener {
   public:
#ifndef ROCKSDB_LITE
    ErrorHandlerListener()
        : mutex_(),
          cv_(&mutex_),
          no_auto_recovery_(false),
          recovery_complete_(false) {}

    ~ErrorHandlerListener() override {}

    void OnErrorRecoveryBegin(BackgroundErrorReason /*reason*/,
                              Status /*bg_error*/,
                              bool* auto_recovery) override {
      if (*auto_recovery && no_auto_recovery_) {
        *auto_recovery = false;
      }
    }

    void OnErrorRecoveryCompleted(Status /*old_bg_error*/) override {
      InstrumentedMutexLock l(&mutex_);
      recovery_complete_ = true;
      cv_.SignalAll();
    }

    bool WaitForRecovery(uint64_t abs_time_us) {
      InstrumentedMutexLock l(&mutex_);
      if (!recovery_complete_) {
        cv_.TimedWait(abs_time_us);
      }
      if (recovery_complete_) {
        recovery_complete_ = false;
        return true;
      }
      return false;
    }

    void EnableAutoRecovery(bool enable = true) { no_auto_recovery_ = !enable; }

   private:
    InstrumentedMutex mutex_;
    InstrumentedCondVar cv_;
    bool no_auto_recovery_;
    bool recovery_complete_;
#else   // ROCKSDB_LITE
    bool WaitForRecovery(uint64_t /*abs_time_us*/) { return true; }
    void EnableAutoRecovery(bool /*enable*/) {}
#endif  // ROCKSDB_LITE
  };

  std::shared_ptr<ErrorHandlerListener> listener_;

  bool SanityCheck() {
    if (FLAGS_compression_ratio > 1) {
      fprintf(stderr, "compression_ratio should be between 0 and 1\n");
      return false;
    }
    return true;
  }

  inline bool CompressSlice(const CompressionInfo& compression_info,
                            const Slice& input, std::string* compressed) {
    bool ok = true;
    switch (FLAGS_compression_type_e) {
      case rocksdb::kSnappyCompression:
        ok = Snappy_Compress(compression_info, input.data(), input.size(),
                             compressed);
        break;
      case rocksdb::kZlibCompression:
        ok = Zlib_Compress(compression_info, 2, input.data(), input.size(),
                           compressed);
        break;
      case rocksdb::kBZip2Compression:
        ok = BZip2_Compress(compression_info, 2, input.data(), input.size(),
                            compressed);
        break;
      case rocksdb::kLZ4Compression:
        ok = LZ4_Compress(compression_info, 2, input.data(), input.size(),
                          compressed);
        break;
      case rocksdb::kLZ4HCCompression:
        ok = LZ4HC_Compress(compression_info, 2, input.data(), input.size(),
                            compressed);
        break;
      case rocksdb::kXpressCompression:
        ok = XPRESS_Compress(input.data(), input.size(), compressed);
        break;
      case rocksdb::kZSTD:
        ok = ZSTD_Compress(compression_info, input.data(), input.size(),
                           compressed);
        break;
      default:
        ok = false;
    }
    return ok;
  }

  void PrintHeader() {
    PrintEnvironment();
    fprintf(stdout, "Keys:       %d bytes each\n", FLAGS_key_size);
    fprintf(stdout, "Values:     %d bytes each (%d bytes after compression)\n",
            FLAGS_value_size,
            static_cast<int>(FLAGS_value_size * FLAGS_compression_ratio + 0.5));
    fprintf(stdout, "Entries:    %" PRIu64 "\n", num_);
    fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
            ((static_cast<int64_t>(FLAGS_key_size + FLAGS_value_size) * num_) /
             1048576.0));
    fprintf(stdout, "FileSize:   %.1f MB (estimated)\n",
            (((FLAGS_key_size + FLAGS_value_size * FLAGS_compression_ratio) *
              num_) /
             1048576.0));
    if (FLAGS_enable_numa) {
      fprintf(stderr, "Running in NUMA enabled mode.\n");
#ifndef NUMA
      fprintf(stderr, "NUMA is not defined in the system.\n");
      exit(1);
#else
      if (numa_available() == -1) {
        fprintf(stderr, "NUMA is not supported by the system.\n");
        exit(1);
      }
#endif
    }

    auto compression = CompressionTypeToString(FLAGS_compression_type_e);
    fprintf(stdout, "Compression: %s\n", compression.c_str());
    fprintf(stdout, "Compression sampling rate: %" PRId64 "\n",
            FLAGS_sample_for_compression);

    switch (FLAGS_rep_factory) {
      case kPrefixHash:
        fprintf(stdout, "Memtablerep: prefix_hash\n");
        break;
      case kSkipList:
        fprintf(stdout, "Memtablerep: skip_list\n");
        break;
      case kVectorRep:
        fprintf(stdout, "Memtablerep: vector\n");
        break;
      case kHashLinkedList:
        fprintf(stdout, "Memtablerep: hash_linkedlist\n");
        break;
    }
    fprintf(stdout, "Perf Level: %d\n", FLAGS_perf_level);

    PrintWarnings(compression.c_str());
    fprintf(stdout, "------------------------------------------------\n");
  }

  void PrintWarnings(const char* compression) {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    fprintf(
        stdout,
        "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n");
#endif
#ifndef NDEBUG
    fprintf(stdout,
            "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif
    if (FLAGS_compression_type_e != rocksdb::kNoCompression) {
      // The test string should not be too small.
      const int len = FLAGS_block_size;
      std::string input_str(len, 'y');
      std::string compressed;
      CompressionOptions opts;
      CompressionContext context(FLAGS_compression_type_e);
      CompressionInfo info(opts, context, CompressionDict::GetEmptyDict(),
                           FLAGS_compression_type_e,
                           FLAGS_sample_for_compression);
      bool result = CompressSlice(info, Slice(input_str), &compressed);

      if (!result) {
        fprintf(stdout, "WARNING: %s compression is not enabled\n",
                compression);
      } else if (compressed.size() >= input_str.size()) {
        fprintf(stdout, "WARNING: %s compression is not effective\n",
                compression);
      }
    }
  }

// Current the following isn't equivalent to OS_LINUX.
#if defined(__linux)
  static Slice TrimSpace(Slice s) {
    unsigned int start = 0;
    while (start < s.size() && isspace(s[start])) {
      start++;
    }
    unsigned int limit = static_cast<unsigned int>(s.size());
    while (limit > start && isspace(s[limit - 1])) {
      limit--;
    }
    return Slice(s.data() + start, limit - start);
  }
#endif

  void PrintEnvironment() {
    fprintf(stderr, "RocksDB:    version %d.%d\n", kMajorVersion,
            kMinorVersion);

#if defined(__linux)
    time_t now = time(nullptr);
    char buf[52];
    // Lint complains about ctime() usage, so replace it with ctime_r(). The
    // requirement is to provide a buffer which is at least 26 bytes.
    fprintf(stderr, "Date:       %s",
            ctime_r(&now, buf));  // ctime_r() adds newline

    FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
    if (cpuinfo != nullptr) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != nullptr) {
        const char* sep = strchr(line, ':');
        if (sep == nullptr) {
          continue;
        }
        Slice key = TrimSpace(Slice(line, sep - 1 - line));
        Slice val = TrimSpace(Slice(sep + 1));
        if (key == "model name") {
          ++num_cpus;
          cpu_type = val.ToString();
        } else if (key == "cache size") {
          cache_size = val.ToString();
        }
      }
      fclose(cpuinfo);
      fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#endif
  }

  static bool KeyExpired(const TimestampEmulator* timestamp_emulator,
                         const Slice& key) {
    const char* pos = key.data();
    pos += 8;
    uint64_t timestamp = 0;
    if (port::kLittleEndian) {
      int bytes_to_fill = 8;
      for (int i = 0; i < bytes_to_fill; ++i) {
        timestamp |= (static_cast<uint64_t>(static_cast<unsigned char>(pos[i]))
                      << ((bytes_to_fill - i - 1) << 3));
      }
    } else {
      memcpy(&timestamp, pos, sizeof(timestamp));
    }
    return timestamp_emulator->Get() - timestamp > FLAGS_time_range;
  }

  class ExpiredTimeFilter : public CompactionFilter {
   public:
    explicit ExpiredTimeFilter(
        const std::shared_ptr<TimestampEmulator>& timestamp_emulator)
        : timestamp_emulator_(timestamp_emulator) {}
    bool Filter(int /*level*/, const Slice& key,
                const Slice& /*existing_value*/, std::string* /*new_value*/,
                bool* /*value_changed*/) const override {
      return KeyExpired(timestamp_emulator_.get(), key);
    }
    const char* Name() const override { return "ExpiredTimeFilter"; }

   private:
    std::shared_ptr<TimestampEmulator> timestamp_emulator_;
  };

  class KeepFilter : public CompactionFilter {
   public:
    bool Filter(int /*level*/, const Slice& /*key*/, const Slice& /*value*/,
                std::string* /*new_value*/,
                bool* /*value_changed*/) const override {
      return false;
    }

    const char* Name() const override { return "KeepFilter"; }
  };

  std::shared_ptr<Cache> NewCache(int64_t capacity) {
    if (capacity <= 0) {
      return nullptr;
    }
    if (FLAGS_use_clock_cache) {
      auto cache = NewClockCache(static_cast<size_t>(capacity),
                                 FLAGS_cache_numshardbits);
      if (!cache) {
        fprintf(stderr, "Clock cache not supported.");
        exit(1);
      }
      return cache;
    } else {
      return NewLRUCache(
          static_cast<size_t>(capacity), FLAGS_cache_numshardbits,
          false /*strict_capacity_limit*/, FLAGS_cache_high_pri_pool_ratio);
    }
  }

 public:
  Benchmark()
      : cache_(NewCache(FLAGS_cache_size)),
        compressed_cache_(NewCache(FLAGS_compressed_cache_size)),
        filter_policy_(FLAGS_bloom_bits >= 0
                           ? NewBloomFilterPolicy(FLAGS_bloom_bits,
                                                  FLAGS_use_block_based_filter)
                           : nullptr),
        num_(FLAGS_num),
        value_size_(FLAGS_value_size),
        key_size_(FLAGS_key_size),
#ifndef ROCKSDB_LITE
        use_blob_db_(FLAGS_use_blob_db)
#else
        use_blob_db_(false)
#endif  // !ROCKSDB_LITE
  {
    // use simcache instead of cache
    if (FLAGS_simcache_size >= 0) {
      if (FLAGS_cache_numshardbits >= 1) {
        cache_ =
            NewSimCache(cache_, FLAGS_simcache_size, FLAGS_cache_numshardbits);
      } else {
        cache_ = NewSimCache(cache_, FLAGS_simcache_size, 0);
      }
    }

    if (report_file_operations_) {
      if (!FLAGS_hdfs.empty()) {
        fprintf(stderr,
                "--hdfs and --report_file_operations cannot be enabled "
                "at the same time");
        exit(1);
      }
      FLAGS_env = new ReportFileOpEnv(FLAGS_env);
    }

    std::vector<std::string> files;
    FLAGS_env->GetChildren(FLAGS_db, &files);
    for (size_t i = 0; i < files.size(); i++) {
      if (Slice(files[i]).starts_with("heap-")) {
        FLAGS_env->DeleteFile(FLAGS_db + "/" + files[i]);
      }
    }
    if (!FLAGS_use_existing_db) {
      Options options;
      options.env = FLAGS_env;
      if (!FLAGS_wal_dir.empty()) {
        options.wal_dir = FLAGS_wal_dir;
      }
#ifndef ROCKSDB_LITE
      if (use_blob_db_) {
        blob_db::DestroyBlobDB(FLAGS_db, options, blob_db::BlobDBOptions());
      }
#endif  // !ROCKSDB_LITE
      DestroyDB(FLAGS_db, options);
      if (!FLAGS_wal_dir.empty()) {
        FLAGS_env->DeleteDir(FLAGS_wal_dir);
      }

      if (FLAGS_num_multi_db > 1) {
        FLAGS_env->CreateDir(FLAGS_db);
        if (!FLAGS_wal_dir.empty()) {
          FLAGS_env->CreateDir(FLAGS_wal_dir);
        }
      }
    }

    listener_.reset(new ErrorHandlerListener());
  }

  ~Benchmark() {
    db_.DeleteDBs();
    if (cache_.get() != nullptr) {
      // this will leak, but we're shutting down so nobody cares
      cache_->DisownData();
    }
  }

  std::string GetPathForMultiple(std::string base_name, size_t id) {
    if (!base_name.empty()) {
#ifndef OS_WIN
      if (base_name.back() != '/') {
        base_name += '/';
      }
#else
      if (base_name.back() != '\\') {
        base_name += '\\';
      }
#endif
    }
    return base_name + ToString(id);
  }

  void Run() {
    if (!SanityCheck()) {
      exit(1);
    }
    Open(&open_options_);
    PrintHeader();
    std::unique_ptr<ExpiredTimeFilter> filter;

    // Sanitize parameters
    num_ = FLAGS_num;
    value_size_ = FLAGS_value_size;
    key_size_ = FLAGS_key_size;
    write_options_ = WriteOptions();
    if (FLAGS_sync) {
      write_options_.sync = true;
    }
    write_options_.disableWAL = FLAGS_disable_wal;

    void (Benchmark::*method)(ThreadState*) = &Benchmark::YCSB_workload;

    int num_threads = FLAGS_threads;

    fprintf(stdout, "DB path: [%s]\n", FLAGS_db.c_str());

#ifndef ROCKSDB_LITE
    // A trace_file option can be provided both for trace and replay
    // operations. But db_bench does not support tracing and replaying at
    // the same time, for now. So, start tracing only when it is not a
    // replay.
    if (FLAGS_trace_file != "") {
      std::unique_ptr<TraceWriter> trace_writer;
      Status s = NewFileTraceWriter(FLAGS_env, EnvOptions(), FLAGS_trace_file,
                                    &trace_writer);
      if (!s.ok()) {
        fprintf(stderr, "Encountered an error starting a trace, %s\n",
                s.ToString().c_str());
        exit(1);
      }
      s = db_.db->StartTrace(trace_options_, std::move(trace_writer));
      if (!s.ok()) {
        fprintf(stderr, "Encountered an error starting a trace, %s\n",
                s.ToString().c_str());
        exit(1);
      }
      fprintf(stdout, "Tracing the workload to: [%s]\n",
              FLAGS_trace_file.c_str());
    }
    // Start block cache tracing.
    if (!FLAGS_block_cache_trace_file.empty()) {
      // Sanity checks.
      if (FLAGS_block_cache_trace_sampling_frequency <= 0) {
        fprintf(stderr,
                "Block cache trace sampling frequency must be higher than "
                "0.\n");
        exit(1);
      }
      if (FLAGS_block_cache_trace_max_trace_file_size_in_bytes <= 0) {
        fprintf(stderr,
                "The maximum file size for block cache tracing must be "
                "higher than 0.\n");
        exit(1);
      }
      block_cache_trace_options_.max_trace_file_size =
          FLAGS_block_cache_trace_max_trace_file_size_in_bytes;
      block_cache_trace_options_.sampling_frequency =
          FLAGS_block_cache_trace_sampling_frequency;
      std::unique_ptr<TraceWriter> block_cache_trace_writer;
      Status s = NewFileTraceWriter(FLAGS_env, EnvOptions(),
                                    FLAGS_block_cache_trace_file,
                                    &block_cache_trace_writer);
      if (!s.ok()) {
        fprintf(stderr, "Encountered an error when creating trace writer, %s\n",
                s.ToString().c_str());
        exit(1);
      }
      s = db_.db->StartBlockCacheTrace(block_cache_trace_options_,
                                       std::move(block_cache_trace_writer));
      if (!s.ok()) {
        fprintf(stderr,
                "Encountered an error when starting block cache tracing, %s\n",
                s.ToString().c_str());
        exit(1);
      }
      fprintf(stdout, "Tracing block cache accesses to: [%s]\n",
              FLAGS_block_cache_trace_file.c_str());
    }
#endif  // ROCKSDB_LITE

    Stats stats = RunBenchmark(num_threads, "ycsb", method);

    if (secondary_update_thread_) {
      secondary_update_stopped_.store(1, std::memory_order_relaxed);
      secondary_update_thread_->join();
      secondary_update_thread_.reset();
    }

#ifndef ROCKSDB_LITE
    if (FLAGS_trace_file != "") {
      Status s = db_.db->EndTrace();
      if (!s.ok()) {
        fprintf(stderr, "Encountered an error ending the trace, %s\n",
                s.ToString().c_str());
      }
    }
    if (!FLAGS_block_cache_trace_file.empty()) {
      Status s = db_.db->EndBlockCacheTrace();
      if (!s.ok()) {
        fprintf(stderr,
                "Encountered an error ending the block cache tracing, %s\n",
                s.ToString().c_str());
      }
    }
#endif  // ROCKSDB_LITE

    if (FLAGS_statistics) {
      fprintf(stdout, "STATISTICS:\n%s\n", dbstats->ToString().c_str());
    }
    if (FLAGS_simcache_size >= 0) {
      fprintf(stdout, "SIMULATOR CACHE STATISTICS:\n%s\n",
              static_cast_with_check<SimCache, Cache>(cache_.get())
                  ->ToString()
                  .c_str());
    }

#ifndef ROCKSDB_LITE
    if (FLAGS_use_secondary_db) {
      fprintf(stdout, "Secondary instance updated  %" PRIu64 " times.\n",
              secondary_db_updates_);
    }
#endif  // ROCKSDB_LITE
  }

 private:
  std::shared_ptr<TimestampEmulator> timestamp_emulator_;
  std::unique_ptr<port::Thread> secondary_update_thread_;
  std::atomic<int> secondary_update_stopped_{0};
#ifndef ROCKSDB_LITE
  uint64_t secondary_db_updates_ = 0;
#endif  // ROCKSDB_LITE
  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
    Workload *workload;
    void (Benchmark::*method)(ThreadState*);
  };

  static void ThreadBody(void* v) {
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    {
      MutexLock l(&shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->cv.SignalAll();
      }
      while (!shared->start) {
        shared->cv.Wait();
      }
    }

    SetPerfLevel(static_cast<PerfLevel>(shared->perf_level));
    perf_context.EnablePerLevelPerfContext();
    thread->stats.Start(thread->tid);
    (arg->bm->*(arg->method))(thread);
    thread->stats.Stop();

    {
      MutexLock l(&shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->cv.SignalAll();
      }
    }
  }

  Stats RunBenchmark(int n, Slice name,
                     void (Benchmark::*method)(ThreadState*)) {
    SharedState shared;
    shared.total = n;
    shared.num_initialized = 0;
    shared.num_done = 0;
    shared.start = false;

    std::unique_ptr<ReporterAgent> reporter_agent;
    if (FLAGS_report_interval_seconds > 0) {
      reporter_agent.reset(new ReporterAgent(FLAGS_env, FLAGS_report_file,
                                             FLAGS_report_interval_seconds));
    }

    ThreadArg* arg = new ThreadArg[n];
    Workload *tmp_workload = NULL;
    DBWithColumnFamilies* db_with_cfh = SelectDBWithCfh((uint64_t)0);

    WorkloadProperties p;
    p.key_size_ = key_size_;
    p.value_size_ = value_size_;
    p.lb_ = FLAGS_lower_bound;
    p.insert_start_ = FLAGS_insert_start;
    p.min_scan_len_ = FLAGS_min_scan_len;
    p.max_scan_len_ = FLAGS_max_scan_len;
    p.read_proportion_ = FLAGS_read_proportion;
    p.update_proportion_ = FLAGS_update_proportion;
    p.insert_proportion_ = FLAGS_insert_proportion;
    p.scan_proportion_ = FLAGS_scan_proportion;
    p.readmodifywrite_proportion_ = FLAGS_readmodifywrite_proportion;
    p.write_options_ = write_options_;
    p.request_distribution_ = static_cast<Distribution>(FLAGS_request_distribution);
    p.scan_length_distribution_ = static_cast<Distribution>(FLAGS_scan_length_distribution);

    int32_t subtable_count = 0;
    for (int i = 0; i < n; i++) {
#ifdef NUMA
      if (FLAGS_enable_numa) {
        // Performs a local allocation of memory to threads in numa node.
        int n_nodes = numa_num_task_nodes();  // Number of nodes in NUMA.
        numa_exit_on_error = 1;
        int numa_node = i % n_nodes;
        bitmask* nodes = numa_allocate_nodemask();
        numa_bitmask_clearall(nodes);
        numa_bitmask_setbit(nodes, numa_node);
        // numa_bind() call binds the process to the node and these
        // properties are passed on to the thread that is created in
        // StartThread method called later in the loop.
        numa_bind(nodes);
        numa_set_strict(1);
        numa_free_nodemask(nodes);
      }
#endif
      if (i % FLAGS_thread_per_subtable == 0) {
        tmp_workload = new Workload();
        tmp_workload->init(p, db_with_cfh->GetCfhByID(subtable_count));
        subtable_count++;
      }
      
      arg[i].workload = tmp_workload;
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      arg[i].thread = new ThreadState(i);
			arg[i].thread->cfid = i;
      arg[i].thread->stats.SetReporterAgent(reporter_agent.get());
      arg[i].thread->shared = &shared;
      arg[i].thread->workload = arg[i].workload;
      FLAGS_env->StartThread(ThreadBody, &arg[i]);
    }

    shared.mu.Lock();
    while (shared.num_initialized < n) {
      shared.cv.Wait();
    }

    shared.start = true;
    shared.cv.SignalAll();
    while (shared.num_done < n) {
      shared.cv.Wait();
    }
    shared.mu.Unlock();

    // Stats for some threads can be excluded.
    Stats merge_stats;
    for (int i = 0; i < n; i++) {
      merge_stats.Merge(arg[i].thread->stats);
    }
    merge_stats.Report(name);

    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }
    delete[] arg;

    return merge_stats;
  }

  void YCSB_workload(ThreadState *thread) {
    Duration duration(FLAGS_duration, 0);
    int64_t writes_done = 0;
    int64_t reads_done = 0;
    int64_t scans_done = 0;
    int64_t updates_done = 0;
    int64_t readmodifywrites_done = 0;

    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      DBOperation op;
      Status s = thread->workload->do_transaction(db, op);
      if (!s.ok() && !s.IsNotFound()) {
        fprintf(stderr, "error: %s\n", s.ToString().c_str());
      }
      switch (op) {
        case DBOperation::INSERT:
          thread->stats.FinishedOps(nullptr, db, 1, kWrite);
          writes_done++;
          break;
        case DBOperation::READ:
          thread->stats.FinishedOps(nullptr, db, 1, kRead);
          reads_done++;
          break;
        case DBOperation::SCAN:
          thread->stats.FinishedOps(nullptr, db, 1, kSeek);
          scans_done++;
          break;
        case DBOperation::UPDATE:
          thread->stats.FinishedOps(nullptr, db, 1, kUpdate);
          updates_done++;
          break;
        case DBOperation::READMODIFYWRITE:
          thread->stats.FinishedOps(nullptr, db, 1, kReadModifyWrite);
          readmodifywrites_done++;
          break;
      }
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 " scans:%" PRIu64
                               " updates:%" PRIu64 " readmodifywrites:%" PRIu64 ")",
             reads_done, writes_done, scans_done, updates_done, readmodifywrites_done);
    thread->stats.AddMessage(msg);
  }

  // Returns true if the options is initialized from the specified
  // options file.
  bool InitializeOptionsFromFile(Options* opts) {
#ifndef ROCKSDB_LITE
    printf("Initializing RocksDB Options from the specified file\n");
    DBOptions db_opts;
    std::vector<ColumnFamilyDescriptor> cf_descs;
    if (FLAGS_options_file != "") {
      auto s = LoadOptionsFromFile(FLAGS_options_file, FLAGS_env, &db_opts,
                                   &cf_descs);
      db_opts.env = FLAGS_env;
      if (s.ok()) {
        *opts = Options(db_opts, cf_descs[0].options);
        return true;
      }
      fprintf(stderr, "Unable to load options file %s --- %s\n",
              FLAGS_options_file.c_str(), s.ToString().c_str());
      exit(1);
    }
#else
    (void)opts;
#endif
    return false;
  }

  void InitializeOptionsFromFlags(Options* opts) {
    printf("Initializing RocksDB Options from command-line flags\n");
    Options& options = *opts;

    assert(db_.db == nullptr);

    options.env = FLAGS_env;
    options.max_open_files = FLAGS_open_files;
    if (FLAGS_cost_write_buffer_to_cache || FLAGS_db_write_buffer_size != 0) {
      options.write_buffer_manager.reset(
          new WriteBufferManager(FLAGS_db_write_buffer_size, cache_));
    }
    options.write_buffer_size = FLAGS_write_buffer_size;
    options.max_write_buffer_number = FLAGS_max_write_buffer_number;
    options.min_write_buffer_number_to_merge =
        FLAGS_min_write_buffer_number_to_merge;
    options.max_write_buffer_number_to_maintain =
        FLAGS_max_write_buffer_number_to_maintain;
    options.max_write_buffer_size_to_maintain =
        FLAGS_max_write_buffer_size_to_maintain;
    options.max_background_jobs = FLAGS_max_background_jobs;
    options.max_background_compactions = FLAGS_max_background_compactions;
    options.max_subcompactions = static_cast<uint32_t>(FLAGS_subcompactions);
    options.max_background_flushes = FLAGS_max_background_flushes;
    options.compaction_style = FLAGS_compaction_style_e;
    options.compaction_pri = FLAGS_compaction_pri_e;
    options.allow_mmap_reads = FLAGS_mmap_read;
    options.allow_mmap_writes = FLAGS_mmap_write;
    options.use_direct_reads = FLAGS_use_direct_reads;
    options.use_direct_io_for_flush_and_compaction =
        FLAGS_use_direct_io_for_flush_and_compaction;
#ifndef ROCKSDB_LITE
    options.ttl = FLAGS_fifo_compaction_ttl;
    options.compaction_options_fifo = CompactionOptionsFIFO(
        FLAGS_fifo_compaction_max_table_files_size_mb * 1024 * 1024,
        FLAGS_fifo_compaction_allow_compaction);
#endif  // ROCKSDB_LITE
    if (FLAGS_use_uint64_comparator) {
      options.comparator = test::Uint64Comparator();
      if (FLAGS_key_size != 8) {
        fprintf(stderr, "Using Uint64 comparator but key size is not 8.\n");
        exit(1);
      }
    }
    if (FLAGS_use_stderr_info_logger) {
      options.info_log.reset(new StderrLogger());
    }
    options.memtable_huge_page_size = FLAGS_memtable_use_huge_page ? 2048 : 0;
    options.memtable_prefix_bloom_size_ratio = FLAGS_memtable_bloom_size_ratio;
    options.memtable_whole_key_filtering = FLAGS_memtable_whole_key_filtering;
    options.bloom_locality = FLAGS_bloom_locality;
    options.max_file_opening_threads = FLAGS_file_opening_threads;
    options.new_table_reader_for_compaction_inputs =
        FLAGS_new_table_reader_for_compaction_inputs;
    options.compaction_readahead_size = FLAGS_compaction_readahead_size;
    options.random_access_max_buffer_size = FLAGS_random_access_max_buffer_size;
    options.writable_file_max_buffer_size = FLAGS_writable_file_max_buffer_size;
    options.use_fsync = FLAGS_use_fsync;
    options.num_levels = FLAGS_num_levels;
    options.target_file_size_base = FLAGS_target_file_size_base;
    options.target_file_size_multiplier = FLAGS_target_file_size_multiplier;
    options.max_bytes_for_level_base = FLAGS_max_bytes_for_level_base;
    options.level_compaction_dynamic_level_bytes =
        FLAGS_level_compaction_dynamic_level_bytes;
    options.max_bytes_for_level_multiplier =
        FLAGS_max_bytes_for_level_multiplier;
    if (FLAGS_rep_factory == kPrefixHash || FLAGS_rep_factory == kHashLinkedList) {
      fprintf(stderr,
              "prefix_size should be non-zero if PrefixHash or "
              "HashLinkedList memtablerep is used\n");
      exit(1);
    }
    switch (FLAGS_rep_factory) {
      case kSkipList:
        options.memtable_factory.reset(
            new SkipListFactory(FLAGS_skip_list_lookahead));
        break;
#ifndef ROCKSDB_LITE
      case kPrefixHash:
        options.memtable_factory.reset(
            NewHashSkipListRepFactory(FLAGS_hash_bucket_count));
        break;
      case kHashLinkedList:
        options.memtable_factory.reset(
            NewHashLinkListRepFactory(FLAGS_hash_bucket_count));
        break;
      case kVectorRep:
        options.memtable_factory.reset(new VectorRepFactory);
        break;
#else
      default:
        fprintf(stderr, "Only skip list is supported in lite mode\n");
        exit(1);
#endif  // ROCKSDB_LITE
    }
    if (FLAGS_use_plain_table) {
#ifndef ROCKSDB_LITE
      if (FLAGS_rep_factory != kPrefixHash &&
          FLAGS_rep_factory != kHashLinkedList) {
        fprintf(stderr, "Waring: plain table is used with skipList\n");
      }

      int bloom_bits_per_key = FLAGS_bloom_bits;
      if (bloom_bits_per_key < 0) {
        bloom_bits_per_key = 0;
      }

      PlainTableOptions plain_table_options;
      plain_table_options.user_key_len = FLAGS_key_size;
      plain_table_options.bloom_bits_per_key = bloom_bits_per_key;
      plain_table_options.hash_table_ratio = 0.75;
      options.table_factory = std::shared_ptr<TableFactory>(
          NewPlainTableFactory(plain_table_options));
#else
      fprintf(stderr, "Plain table is not supported in lite mode\n");
      exit(1);
#endif  // ROCKSDB_LITE
    } else if (FLAGS_use_cuckoo_table) {
#ifndef ROCKSDB_LITE
      if (FLAGS_cuckoo_hash_ratio > 1 || FLAGS_cuckoo_hash_ratio < 0) {
        fprintf(stderr, "Invalid cuckoo_hash_ratio\n");
        exit(1);
      }

      if (!FLAGS_mmap_read) {
        fprintf(stderr, "cuckoo table format requires mmap read to operate\n");
        exit(1);
      }

      rocksdb::CuckooTableOptions table_options;
      table_options.hash_table_ratio = FLAGS_cuckoo_hash_ratio;
      table_options.identity_as_first_hash = FLAGS_identity_as_first_hash;
      options.table_factory =
          std::shared_ptr<TableFactory>(NewCuckooTableFactory(table_options));
#else
      fprintf(stderr, "Cuckoo table is not supported in lite mode\n");
      exit(1);
#endif  // ROCKSDB_LITE
    } else {
      BlockBasedTableOptions block_based_options;
      if (FLAGS_use_hash_search) {
        fprintf(stderr,
                  "prefix_size not assigned when enable use_hash_search \n");
          exit(1);
        block_based_options.index_type = BlockBasedTableOptions::kHashSearch;
      } else {
        block_based_options.index_type = BlockBasedTableOptions::kBinarySearch;
      }
      if (FLAGS_partition_index_and_filters || FLAGS_partition_index) {
        if (FLAGS_use_hash_search) {
          fprintf(stderr,
                  "use_hash_search is incompatible with "
                  "partition index and is ignored");
        }
        block_based_options.index_type =
            BlockBasedTableOptions::kTwoLevelIndexSearch;
        block_based_options.metadata_block_size = FLAGS_metadata_block_size;
        if (FLAGS_partition_index_and_filters) {
          block_based_options.partition_filters = true;
        }
      }
      if (cache_ == nullptr) {
        block_based_options.no_block_cache = true;
      }
      block_based_options.cache_index_and_filter_blocks =
          FLAGS_cache_index_and_filter_blocks;
      block_based_options.pin_l0_filter_and_index_blocks_in_cache =
          FLAGS_pin_l0_filter_and_index_blocks_in_cache;
      block_based_options.pin_top_level_index_and_filter =
          FLAGS_pin_top_level_index_and_filter;
      if (FLAGS_cache_high_pri_pool_ratio > 1e-6) {  // > 0.0 + eps
        block_based_options.cache_index_and_filter_blocks_with_high_priority =
            true;
      }
      block_based_options.block_cache = cache_;
      block_based_options.block_cache_compressed = compressed_cache_;
      block_based_options.block_size = FLAGS_block_size;
      block_based_options.block_restart_interval = FLAGS_block_restart_interval;
      block_based_options.index_block_restart_interval =
          FLAGS_index_block_restart_interval;
      block_based_options.filter_policy = filter_policy_;
      block_based_options.format_version =
          static_cast<uint32_t>(FLAGS_format_version);
      block_based_options.read_amp_bytes_per_bit = FLAGS_read_amp_bytes_per_bit;
      block_based_options.enable_index_compression =
          FLAGS_enable_index_compression;
      block_based_options.block_align = FLAGS_block_align;
      if (FLAGS_use_data_block_hash_index) {
        block_based_options.data_block_index_type =
            rocksdb::BlockBasedTableOptions::kDataBlockBinaryAndHash;
      } else {
        block_based_options.data_block_index_type =
            rocksdb::BlockBasedTableOptions::kDataBlockBinarySearch;
      }
      block_based_options.data_block_hash_table_util_ratio =
          FLAGS_data_block_hash_table_util_ratio;
      if (FLAGS_read_cache_path != "") {
#ifndef ROCKSDB_LITE
        Status rc_status;

        // Read cache need to be provided with a the Logger, we will put all
        // reac cache logs in the read cache path in a file named rc_LOG
        rc_status = FLAGS_env->CreateDirIfMissing(FLAGS_read_cache_path);
        std::shared_ptr<Logger> read_cache_logger;
        if (rc_status.ok()) {
          rc_status = FLAGS_env->NewLogger(FLAGS_read_cache_path + "/rc_LOG",
                                           &read_cache_logger);
        }

        if (rc_status.ok()) {
          PersistentCacheConfig rc_cfg(FLAGS_env, FLAGS_read_cache_path,
                                       FLAGS_read_cache_size,
                                       read_cache_logger);

          rc_cfg.enable_direct_reads = FLAGS_read_cache_direct_read;
          rc_cfg.enable_direct_writes = FLAGS_read_cache_direct_write;
          rc_cfg.writer_qdepth = 4;
          rc_cfg.writer_dispatch_size = 4 * 1024;

          auto pcache = std::make_shared<BlockCacheTier>(rc_cfg);
          block_based_options.persistent_cache = pcache;
          rc_status = pcache->Open();
        }

        if (!rc_status.ok()) {
          fprintf(stderr, "Error initializing read cache, %s\n",
                  rc_status.ToString().c_str());
          exit(1);
        }
#else
        fprintf(stderr, "Read cache is not supported in LITE\n");
        exit(1);

#endif
      }
      options.table_factory.reset(
          NewBlockBasedTableFactory(block_based_options));
    }
    if (FLAGS_max_bytes_for_level_multiplier_additional_v.size() > 0) {
      if (FLAGS_max_bytes_for_level_multiplier_additional_v.size() !=
          static_cast<unsigned int>(FLAGS_num_levels)) {
        fprintf(stderr, "Insufficient number of fanouts specified %d\n",
                static_cast<int>(
                    FLAGS_max_bytes_for_level_multiplier_additional_v.size()));
        exit(1);
      }
      options.max_bytes_for_level_multiplier_additional =
          FLAGS_max_bytes_for_level_multiplier_additional_v;
    }
    options.level0_stop_writes_trigger = FLAGS_level0_stop_writes_trigger;
    options.level0_file_num_compaction_trigger =
        FLAGS_level0_file_num_compaction_trigger;
    options.level0_slowdown_writes_trigger =
        FLAGS_level0_slowdown_writes_trigger;
    options.compression = FLAGS_compression_type_e;
    options.sample_for_compression = FLAGS_sample_for_compression;
    options.WAL_ttl_seconds = FLAGS_wal_ttl_seconds;
    options.WAL_size_limit_MB = FLAGS_wal_size_limit_MB;
    options.max_total_wal_size = FLAGS_max_total_wal_size;

    if (FLAGS_min_level_to_compress >= 0) {
      assert(FLAGS_min_level_to_compress <= FLAGS_num_levels);
      options.compression_per_level.resize(FLAGS_num_levels);
      for (int i = 0; i < FLAGS_min_level_to_compress; i++) {
        options.compression_per_level[i] = kNoCompression;
      }
      for (int i = FLAGS_min_level_to_compress; i < FLAGS_num_levels; i++) {
        options.compression_per_level[i] = FLAGS_compression_type_e;
      }
    }
    options.soft_rate_limit = FLAGS_soft_rate_limit;
    options.hard_rate_limit = FLAGS_hard_rate_limit;
    options.soft_pending_compaction_bytes_limit =
        FLAGS_soft_pending_compaction_bytes_limit;
    options.hard_pending_compaction_bytes_limit =
        FLAGS_hard_pending_compaction_bytes_limit;
    options.delayed_write_rate = FLAGS_delayed_write_rate;
    options.allow_concurrent_memtable_write =
        FLAGS_allow_concurrent_memtable_write;
    options.inplace_update_support = FLAGS_inplace_update_support;
    options.inplace_update_num_locks = FLAGS_inplace_update_num_locks;
    options.enable_write_thread_adaptive_yield =
        FLAGS_enable_write_thread_adaptive_yield;
    options.enable_pipelined_write = FLAGS_enable_pipelined_write;
    options.unordered_write = FLAGS_unordered_write;
    options.write_thread_max_yield_usec = FLAGS_write_thread_max_yield_usec;
    options.write_thread_slow_yield_usec = FLAGS_write_thread_slow_yield_usec;
    options.rate_limit_delay_max_milliseconds =
        FLAGS_rate_limit_delay_max_milliseconds;
    options.table_cache_numshardbits = FLAGS_table_cache_numshardbits;
    options.max_compaction_bytes = FLAGS_max_compaction_bytes;
    options.disable_auto_compactions = FLAGS_disable_auto_compactions;
    options.optimize_filters_for_hits = FLAGS_optimize_filters_for_hits;

    // fill storage options
    options.advise_random_on_open = FLAGS_advise_random_on_open;
    options.access_hint_on_compaction_start = FLAGS_compaction_fadvice_e;
    options.use_adaptive_mutex = FLAGS_use_adaptive_mutex;
    options.bytes_per_sync = FLAGS_bytes_per_sync;
    options.wal_bytes_per_sync = FLAGS_wal_bytes_per_sync;

    // merge operator options
    options.merge_operator =
        MergeOperators::CreateFromStringId(FLAGS_merge_operator);
    if (options.merge_operator == nullptr && !FLAGS_merge_operator.empty()) {
      fprintf(stderr, "invalid merge operator: %s\n",
              FLAGS_merge_operator.c_str());
      exit(1);
    }
    options.max_successive_merges = FLAGS_max_successive_merges;
    options.report_bg_io_stats = FLAGS_report_bg_io_stats;

    // set universal style compaction configurations, if applicable
    if (FLAGS_universal_size_ratio != 0) {
      options.compaction_options_universal.size_ratio =
          FLAGS_universal_size_ratio;
    }
    if (FLAGS_universal_min_merge_width != 0) {
      options.compaction_options_universal.min_merge_width =
          FLAGS_universal_min_merge_width;
    }
    if (FLAGS_universal_max_merge_width != 0) {
      options.compaction_options_universal.max_merge_width =
          FLAGS_universal_max_merge_width;
    }
    if (FLAGS_universal_max_size_amplification_percent != 0) {
      options.compaction_options_universal.max_size_amplification_percent =
          FLAGS_universal_max_size_amplification_percent;
    }
    if (FLAGS_universal_compression_size_percent != -1) {
      options.compaction_options_universal.compression_size_percent =
          FLAGS_universal_compression_size_percent;
    }
    options.compaction_options_universal.allow_trivial_move =
        FLAGS_universal_allow_trivial_move;
    if (FLAGS_thread_status_per_interval > 0) {
      options.enable_thread_tracking = true;
    }

#ifndef ROCKSDB_LITE
    if (FLAGS_readonly && FLAGS_transaction_db) {
      fprintf(stderr, "Cannot use readonly flag with transaction_db\n");
      exit(1);
    }
    if (FLAGS_use_secondary_db &&
        (FLAGS_transaction_db || FLAGS_optimistic_transaction_db)) {
      fprintf(stderr, "Cannot use use_secondary_db flag with transaction_db\n");
      exit(1);
    }
#endif  // ROCKSDB_LITE
  }

  void InitializeOptionsGeneral(Options* opts) {
    Options& options = *opts;

    options.create_missing_column_families = FLAGS_num_column_families > 1;
    options.statistics = dbstats;
    options.wal_dir = FLAGS_wal_dir;
    options.create_if_missing = !FLAGS_use_existing_db;
    options.dump_malloc_stats = FLAGS_dump_malloc_stats;
    options.stats_dump_period_sec =
        static_cast<unsigned int>(FLAGS_stats_dump_period_sec);
    options.stats_persist_period_sec =
        static_cast<unsigned int>(FLAGS_stats_persist_period_sec);
    options.persist_stats_to_disk = FLAGS_persist_stats_to_disk;
    options.stats_history_buffer_size =
        static_cast<size_t>(FLAGS_stats_history_buffer_size);

    options.compression_opts.level = FLAGS_compression_level;
    options.compression_opts.max_dict_bytes = FLAGS_compression_max_dict_bytes;
    options.compression_opts.zstd_max_train_bytes =
        FLAGS_compression_zstd_max_train_bytes;
    // If this is a block based table, set some related options
    if (options.table_factory->Name() == BlockBasedTableFactory::kName &&
        options.table_factory->GetOptions() != nullptr) {
      BlockBasedTableOptions* table_options =
          reinterpret_cast<BlockBasedTableOptions*>(
              options.table_factory->GetOptions());
      if (FLAGS_cache_size) {
        table_options->block_cache = cache_;
      }
      if (FLAGS_bloom_bits >= 0) {
        table_options->filter_policy.reset(NewBloomFilterPolicy(
            FLAGS_bloom_bits, FLAGS_use_block_based_filter));
      }
    }
    if (FLAGS_row_cache_size) {
      if (FLAGS_cache_numshardbits >= 1) {
        options.row_cache =
            NewLRUCache(FLAGS_row_cache_size, FLAGS_cache_numshardbits);
      } else {
        options.row_cache = NewLRUCache(FLAGS_row_cache_size);
      }
    }
    if (FLAGS_enable_io_prio) {
      FLAGS_env->LowerThreadPoolIOPriority(Env::LOW);
      FLAGS_env->LowerThreadPoolIOPriority(Env::HIGH);
    }
    if (FLAGS_enable_cpu_prio) {
      FLAGS_env->LowerThreadPoolCPUPriority(Env::LOW);
      FLAGS_env->LowerThreadPoolCPUPriority(Env::HIGH);
    }
    options.env = FLAGS_env;

    if (FLAGS_rate_limiter_bytes_per_sec > 0) {
      if (FLAGS_rate_limit_bg_reads &&
          !FLAGS_new_table_reader_for_compaction_inputs) {
        fprintf(stderr,
                "rate limit compaction reads must have "
                "new_table_reader_for_compaction_inputs set\n");
        exit(1);
      }
      options.rate_limiter.reset(NewGenericRateLimiter(
          FLAGS_rate_limiter_bytes_per_sec, 100 * 1000 /* refill_period_us */,
          10 /* fairness */,
          FLAGS_rate_limit_bg_reads ? RateLimiter::Mode::kReadsOnly
                                    : RateLimiter::Mode::kWritesOnly,
          FLAGS_rate_limiter_auto_tuned));
    }

    options.listeners.emplace_back(listener_);
    if (FLAGS_num_multi_db <= 1) {
      OpenDb(options, FLAGS_db, &db_);
    } else {
      multi_dbs_.clear();
      multi_dbs_.resize(FLAGS_num_multi_db);
      auto wal_dir = options.wal_dir;
      for (int i = 0; i < FLAGS_num_multi_db; i++) {
        if (!wal_dir.empty()) {
          options.wal_dir = GetPathForMultiple(wal_dir, i);
        }
        OpenDb(options, GetPathForMultiple(FLAGS_db, i), &multi_dbs_[i]);
      }
      options.wal_dir = wal_dir;
    }

    // KeepFilter is a noop filter, this can be used to test compaction filter
    if (FLAGS_use_keep_filter) {
      options.compaction_filter = new KeepFilter();
      fprintf(stdout, "A noop compaction filter is used\n");
    }
  }

  void Open(Options* opts) {
    if (!InitializeOptionsFromFile(opts)) {
      InitializeOptionsFromFlags(opts);
    }

    InitializeOptionsGeneral(opts);
  }

  void OpenDb(Options options, const std::string& db_name,
              DBWithColumnFamilies* db) {
    Status s;
    // Open with column families if necessary.
    if (FLAGS_num_column_families > 1) {
      size_t num_hot = FLAGS_num_column_families;
      if (FLAGS_num_hot_column_families > 0 &&
          FLAGS_num_hot_column_families < FLAGS_num_column_families) {
        num_hot = FLAGS_num_hot_column_families;
      } else {
        FLAGS_num_hot_column_families = FLAGS_num_column_families;
      }
      std::vector<ColumnFamilyDescriptor> column_families;
      for (size_t i = 0; i < num_hot; i++) {
        column_families.push_back(ColumnFamilyDescriptor(
            ColumnFamilyName(i), ColumnFamilyOptions(options)));
      }
      std::vector<int> cfh_idx_to_prob;
      if (!FLAGS_column_family_distribution.empty()) {
        std::stringstream cf_prob_stream(FLAGS_column_family_distribution);
        std::string cf_prob;
        int sum = 0;
        while (std::getline(cf_prob_stream, cf_prob, ',')) {
          cfh_idx_to_prob.push_back(std::stoi(cf_prob));
          sum += cfh_idx_to_prob.back();
        }
        if (sum != 100) {
          fprintf(stderr, "column_family_distribution items must sum to 100\n");
          exit(1);
        }
        if (cfh_idx_to_prob.size() != num_hot) {
          fprintf(stderr,
                  "got %" ROCKSDB_PRIszt
                  " column_family_distribution items; expected "
                  "%" ROCKSDB_PRIszt "\n",
                  cfh_idx_to_prob.size(), num_hot);
          exit(1);
        }
      }
#ifndef ROCKSDB_LITE
      if (FLAGS_readonly) {
        s = DB::OpenForReadOnly(options, db_name, column_families, &db->cfh,
                                &db->db);
      } else if (FLAGS_optimistic_transaction_db) {
        s = OptimisticTransactionDB::Open(options, db_name, column_families,
                                          &db->cfh, &db->opt_txn_db);
        if (s.ok()) {
          db->db = db->opt_txn_db->GetBaseDB();
        }
      } else if (FLAGS_transaction_db) {
        TransactionDB* ptr;
        TransactionDBOptions txn_db_options;
        if (options.unordered_write) {
          options.two_write_queues = true;
          txn_db_options.skip_concurrency_control = true;
          txn_db_options.write_policy = WRITE_PREPARED;
        }
        s = TransactionDB::Open(options, txn_db_options, db_name,
                                column_families, &db->cfh, &ptr);
        if (s.ok()) {
          db->db = ptr;
        }
      } else {
        s = DB::Open(options, db_name, column_families, &db->cfh, &db->db);
      }
#else
      s = DB::Open(options, db_name, column_families, &db->cfh, &db->db);
#endif  // ROCKSDB_LITE
      db->cfh.resize(FLAGS_num_column_families);
      db->num_created = num_hot;
      db->num_hot = num_hot;
      db->cfh_idx_to_prob = std::move(cfh_idx_to_prob);
#ifndef ROCKSDB_LITE
    } else if (FLAGS_readonly) {
      s = DB::OpenForReadOnly(options, db_name, &db->db);
    } else if (FLAGS_optimistic_transaction_db) {
      s = OptimisticTransactionDB::Open(options, db_name, &db->opt_txn_db);
      if (s.ok()) {
        db->db = db->opt_txn_db->GetBaseDB();
      }
    } else if (FLAGS_transaction_db) {
      TransactionDB* ptr = nullptr;
      TransactionDBOptions txn_db_options;
      if (options.unordered_write) {
        options.two_write_queues = true;
        txn_db_options.skip_concurrency_control = true;
        txn_db_options.write_policy = WRITE_PREPARED;
      }
      s = CreateLoggerFromOptions(db_name, options, &options.info_log);
      if (s.ok()) {
        s = TransactionDB::Open(options, txn_db_options, db_name, &ptr);
      }
      if (s.ok()) {
        db->db = ptr;
      }
    } else if (FLAGS_use_blob_db) {
      blob_db::BlobDBOptions blob_db_options;
      blob_db_options.enable_garbage_collection = FLAGS_blob_db_enable_gc;
      blob_db_options.garbage_collection_cutoff = FLAGS_blob_db_gc_cutoff;
      blob_db_options.is_fifo = FLAGS_blob_db_is_fifo;
      blob_db_options.max_db_size = FLAGS_blob_db_max_db_size;
      blob_db_options.ttl_range_secs = FLAGS_blob_db_ttl_range_secs;
      blob_db_options.min_blob_size = FLAGS_blob_db_min_blob_size;
      blob_db_options.bytes_per_sync = FLAGS_blob_db_bytes_per_sync;
      blob_db_options.blob_file_size = FLAGS_blob_db_file_size;
      blob_db::BlobDB* ptr = nullptr;
      s = blob_db::BlobDB::Open(options, blob_db_options, db_name, &ptr);
      if (s.ok()) {
        db->db = ptr;
      }
    } else if (FLAGS_use_secondary_db) {
      if (FLAGS_secondary_path.empty()) {
        std::string default_secondary_path;
        FLAGS_env->GetTestDirectory(&default_secondary_path);
        default_secondary_path += "/dbbench_secondary";
        FLAGS_secondary_path = default_secondary_path;
      }
      s = DB::OpenAsSecondary(options, db_name, FLAGS_secondary_path, &db->db);
      if (s.ok() && FLAGS_secondary_update_interval > 0) {
        secondary_update_thread_.reset(new port::Thread(
            [this](int interval, DBWithColumnFamilies* _db) {
              while (0 == secondary_update_stopped_.load(
                              std::memory_order_relaxed)) {
                Status secondary_update_status =
                    _db->db->TryCatchUpWithPrimary();
                if (!secondary_update_status.ok()) {
                  fprintf(stderr, "Failed to catch up with primary: %s\n",
                          secondary_update_status.ToString().c_str());
                  break;
                }
                ++secondary_db_updates_;
                FLAGS_env->SleepForMicroseconds(interval * 1000000);
              }
            },
            FLAGS_secondary_update_interval, db));
      }
#endif  // ROCKSDB_LITE
    } else {
      s = DB::Open(options, db_name, &db->db);
    }
    if (!s.ok()) {
      fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      exit(1);
    }
  }

  DB* SelectDB(ThreadState* thread) { return SelectDBWithCfh(thread)->db; }

  DBWithColumnFamilies* SelectDBWithCfh(ThreadState* thread) {
    return SelectDBWithCfh(0UL);
  }

  DBWithColumnFamilies* SelectDBWithCfh(uint64_t rand_int) {
    if (db_.db != nullptr) {
      return &db_;
    } else {
      return &multi_dbs_[rand_int % multi_dbs_.size()];
    }
  }
};

int ycsb_tool(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  static bool initialized = false;
  if (!initialized) {
    SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                    " [OPTIONS]...");
    initialized = true;
  }
  ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_compaction_style_e = (rocksdb::CompactionStyle)FLAGS_compaction_style;
#ifndef ROCKSDB_LITE
  if (FLAGS_statistics && !FLAGS_statistics_string.empty()) {
    fprintf(stderr,
            "Cannot provide both --statistics and --statistics_string.\n");
    exit(1);
  }
  if (!FLAGS_statistics_string.empty()) {
    Status s = ObjectRegistry::NewInstance()->NewSharedObject<Statistics>(
        FLAGS_statistics_string, &dbstats);
    if (dbstats == nullptr) {
      fprintf(stderr,
              "No Statistics registered matching string: %s status=%s\n",
              FLAGS_statistics_string.c_str(), s.ToString().c_str());
      exit(1);
    }
  }
#endif  // ROCKSDB_LITE
  if (FLAGS_statistics) {
    dbstats = rocksdb::CreateDBStatistics();
  }
  if (dbstats) {
    dbstats->set_stats_level(static_cast<StatsLevel>(FLAGS_stats_level));
  }
  FLAGS_compaction_pri_e = (rocksdb::CompactionPri)FLAGS_compaction_pri;

  std::vector<std::string> fanout = rocksdb::StringSplit(
      FLAGS_max_bytes_for_level_multiplier_additional, ',');
  for (size_t j = 0; j < fanout.size(); j++) {
    FLAGS_max_bytes_for_level_multiplier_additional_v.push_back(
#ifndef CYGWIN
        std::stoi(fanout[j]));
#else
        stoi(fanout[j]));
#endif
  }

  FLAGS_compression_type_e =
      StringToCompressionType(FLAGS_compression_type.c_str());

#ifndef ROCKSDB_LITE
  if (!FLAGS_hdfs.empty() && !FLAGS_env_uri.empty()) {
    fprintf(stderr, "Cannot provide both --hdfs and --env_uri.\n");
    exit(1);
  } else if (!FLAGS_env_uri.empty()) {
    Status s = Env::LoadEnv(FLAGS_env_uri, &FLAGS_env, &env_guard);
    if (FLAGS_env == nullptr) {
      fprintf(stderr, "No Env registered for URI: %s\n", FLAGS_env_uri.c_str());
      exit(1);
    }
  }
#endif  // ROCKSDB_LITE

  if (!FLAGS_hdfs.empty()) {
    FLAGS_env = new rocksdb::HdfsEnv(FLAGS_hdfs);
  }

  if (!strcasecmp(FLAGS_compaction_fadvice.c_str(), "NONE"))
    FLAGS_compaction_fadvice_e = rocksdb::Options::NONE;
  else if (!strcasecmp(FLAGS_compaction_fadvice.c_str(), "NORMAL"))
    FLAGS_compaction_fadvice_e = rocksdb::Options::NORMAL;
  else if (!strcasecmp(FLAGS_compaction_fadvice.c_str(), "SEQUENTIAL"))
    FLAGS_compaction_fadvice_e = rocksdb::Options::SEQUENTIAL;
  else if (!strcasecmp(FLAGS_compaction_fadvice.c_str(), "WILLNEED"))
    FLAGS_compaction_fadvice_e = rocksdb::Options::WILLNEED;
  else {
    fprintf(stdout, "Unknown compaction fadvice:%s\n",
            FLAGS_compaction_fadvice.c_str());
  }

  FLAGS_rep_factory = StringToRepFactory(FLAGS_memtablerep.c_str());

  // Note options sanitization may increase thread pool sizes according to
  // max_background_flushes/max_background_compactions/max_background_jobs
  FLAGS_env->SetBackgroundThreads(FLAGS_num_high_pri_threads,
                                  rocksdb::Env::Priority::HIGH);
  FLAGS_env->SetBackgroundThreads(FLAGS_num_bottom_pri_threads,
                                  rocksdb::Env::Priority::BOTTOM);
  FLAGS_env->SetBackgroundThreads(FLAGS_num_low_pri_threads,
                                  rocksdb::Env::Priority::LOW);

  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db.empty()) {
    std::string default_db_path;
    FLAGS_env->GetTestDirectory(&default_db_path);
    default_db_path += "/ycsb";
    FLAGS_db = default_db_path;
  }

  if (FLAGS_stats_interval_seconds > 0) {
    // When both are set then FLAGS_stats_interval determines the frequency
    // at which the timer is checked for FLAGS_stats_interval_seconds
    FLAGS_stats_interval = 1000;
  }

  rocksdb::ycsb::Benchmark benchmark;
  benchmark.Run();

#ifndef ROCKSDB_LITE
  if (FLAGS_print_malloc_stats) {
    std::string stats_string;
    rocksdb::DumpMallocStats(&stats_string);
    fprintf(stdout, "Malloc stats:\n%s\n", stats_string.c_str());
  }
#endif  // ROCKSDB_LITE

  return 0;
}
}  // namespace ycsb
}  // namespace rocksdb
#endif
