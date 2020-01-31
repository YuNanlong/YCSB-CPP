#pragma once
#include <string>
#include <time.h>

#include "generator.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/slice.h"
#include "port/port.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/options.h"
#include "rocksdb/iterator.h"

using namespace rocksdb;

namespace rocksdb {
namespace util {

enum class Distribution: int32_t {
  ZIPFIAN,
  UNIFORM,
  LATEST
};

struct WorkloadProperties {
  uint32_t key_size_ = 0;
  uint32_t value_size_ = 0;
  uint64_t lb_ = 0;
  uint64_t insert_start_ = 0;
  uint64_t min_scan_len_ = 0;
  uint64_t max_scan_len_ = 0;
  double insert_proportion_ = 0.0;
  double read_proportion_ = 0.0;
  double scan_proportion_ = 0.0;
  double update_proportion_ = 0.0;
  double readmodifywrite_proportion_ = 0.0;
  Distribution request_distribution_ = Distribution::ZIPFIAN;
  Distribution scan_length_distribution_ = Distribution::UNIFORM;
  WriteOptions write_options_;
};

class Workload {
 private:
  WorkloadProperties workload_properties_;
  DiscreteGenerator *operation_chooser_;
  Generator *key_chooser_;
  AcknowledgedCounterGenerator *transaction_insert_keysequence_;
  Generator *scan_length_chooser_;

  ColumnFamilyHandle* cfh_;

  void GenerateKeyFromInt(uint64_t v, Slice* key);

  Slice GenerateValue(uint64_t len);

  Slice AllocateKey(std::unique_ptr<const char[]>* key_guard);

 public:
  Workload() {}

  ~Workload() {
    delete operation_chooser_;
    delete key_chooser_;
    delete transaction_insert_keysequence_;
    delete scan_length_chooser_;
  }

  void init(WorkloadProperties p, ColumnFamilyHandle *cfh);

  Status do_transaction(DB *db, DBOperation &op);

  Status do_insert(DB *db);

  Status do_read(DB *db);

  Status do_scan(DB *db);

  Status do_update(DB *db);

  Status do_readmodifywrite(DB *db);
};

} // namespace util
} // namespace rocksdb
