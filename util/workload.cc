#include "workload.h"

namespace rocksdb {
namespace util {
void Workload::init(WorkloadProperties p, ColumnFamilyHandle *cfh) {
  workload_properties_ = p;
  cfh_ = cfh;

  operation_chooser_ = new DiscreteGenerator((uint64_t)time(NULL));
  transaction_insert_keysequence_ = new AcknowledgedCounterGenerator(p.insert_start_);
  if (p.request_distribution_ == Distribution::ZIPFIAN) {
    key_chooser_ = new ScrambledZipfianGenerator((uint64_t)time(NULL), transaction_insert_keysequence_, p.insert_start_ - 1, p.lb_);
  } else if (p.request_distribution_ == Distribution::UNIFORM) {
    key_chooser_ = new UniformGenerator((uint64_t)time(NULL), transaction_insert_keysequence_, p.insert_start_ - 1, p.lb_);
  } else if (p.request_distribution_ == Distribution::LATEST) {
    key_chooser_ = new SkewedLatestGenerator((uint64_t)time(NULL), transaction_insert_keysequence_, p.insert_start_ - 1, p.lb_);
  }
  if (p.scan_length_distribution_ == Distribution::ZIPFIAN) {
    scan_length_chooser_ = new ZipfianGenerator((uint64_t)time(NULL), NULL, p.max_scan_len_, p.min_scan_len_);
  } else if (p.scan_length_distribution_ == Distribution::UNIFORM) {
    scan_length_chooser_ = new UniformGenerator((uint64_t)time(NULL), NULL, p.max_scan_len_, p.min_scan_len_);
  }

  if (p.insert_proportion_ > 0) {
    operation_chooser_->add_value(DBOperation::INSERT, p.insert_proportion_);
  }
  if (p.read_proportion_ > 0) {
    operation_chooser_->add_value(DBOperation::READ, p.read_proportion_);
  }
  if (p.scan_proportion_ > 0) {
    operation_chooser_->add_value(DBOperation::SCAN, p.scan_proportion_);
  }
  if (p.update_proportion_ > 0) {
    operation_chooser_->add_value(DBOperation::UPDATE, p.update_proportion_);
  }
  if (p.readmodifywrite_proportion_ > 0) {
    operation_chooser_->add_value(DBOperation::READMODIFYWRITE, p.readmodifywrite_proportion_);
  }
}

Status Workload::do_transaction(DB *db, DBOperation &op) {
  op = static_cast<DBOperation>(operation_chooser_->next_val());
  switch (op) {
    case DBOperation::INSERT:
      return do_insert(db);
    case DBOperation::READ:
      return do_read(db);
    case DBOperation::SCAN:
      return do_scan(db);
    case DBOperation::UPDATE:
      return do_update(db);
    case DBOperation::READMODIFYWRITE:
      return do_readmodifywrite(db);
    default:
      throw std::runtime_error("Should not reach here.");
  }
}

Status Workload::do_insert(DB *db) {
  std::unique_ptr<const char[]> key_guard;
  Slice key = AllocateKey(&key_guard);
  uint64_t keynum = transaction_insert_keysequence_->next_val();
  GenerateKeyFromInt(keynum, &key);

  Status s = db->Put(workload_properties_.write_options_, key, GenerateValue(workload_properties_.value_size_));
  transaction_insert_keysequence_->acknowledge(keynum);
  return s;
}

Status Workload::do_read(DB *db) {
  std::unique_ptr<const char[]> key_guard;
  Slice key = AllocateKey(&key_guard);
  uint64_t keynum = key_chooser_->next_val();
  GenerateKeyFromInt(keynum, &key);
  std::string value;

  ReadOptions options(false, true);
  Status s = db->Get(options, cfh_, key, &value);
  return s;
}

Status Workload::do_scan(DB *db) {
  std::unique_ptr<const char[]> key_guard;
  Slice key = AllocateKey(&key_guard);
  uint64_t keynum = key_chooser_->next_val();
  GenerateKeyFromInt(keynum, &key);

  ReadOptions options(false, true);
  Iterator *iter = db->NewIterator(options, cfh_);
  uint64_t scan_len = scan_length_chooser_->next_val();
  iter->Seek(key);
  for (uint64_t i = 0; i < scan_len && iter->Valid(); i++, iter->Next()) {
    iter->key();
    iter->value();
  }
  delete iter;
  Status s;
  return s;
}

Status Workload::do_update(DB *db) {
  std::unique_ptr<const char[]> key_guard;
  Slice key = AllocateKey(&key_guard);
  uint64_t keynum = key_chooser_->next_val();
  GenerateKeyFromInt(keynum, &key);

  Status s = db->Put(workload_properties_.write_options_, key, GenerateValue(workload_properties_.value_size_));
  return s;
}

Status Workload::do_readmodifywrite(DB *db) {
  std::unique_ptr<const char[]> key_guard;
  Slice key = AllocateKey(&key_guard);
  uint64_t keynum = key_chooser_->next_val();
  GenerateKeyFromInt(keynum, &key);
  std::string value;

  Status s;
  ReadOptions options(false, true);
  s = db->Get(options, cfh_, key, &value);
  s = db->Put(workload_properties_.write_options_, key, GenerateValue(workload_properties_.value_size_));
  return s;
}

void Workload::GenerateKeyFromInt(uint64_t v, Slice* key) {
  char* start = const_cast<char*>(key->data());
  char* pos = start;

  int bytes_to_fill = std::min(workload_properties_.key_size_ - static_cast<int>(pos - start), (uint32_t)8);
  if (port::kLittleEndian) {
    for (int i = 0; i < bytes_to_fill; ++i) {
      pos[i] = (v >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
    }
  } else {
    memcpy(pos, static_cast<void*>(&v), bytes_to_fill);
  }
  pos += bytes_to_fill;
  if (workload_properties_.key_size_ > pos - start) {
    memset(pos, '0', workload_properties_.key_size_ - (pos - start));
  }
}

Slice Workload::GenerateValue(uint64_t len) {
  UniformGenerator gen((uint64_t)time(NULL), NULL, 25);
  std::string str = "";
  for (uint64_t i = 0; i < len; i++) {
    str += 'A' + gen.next_val();
  }
  return Slice(str.data(), len);
}

Slice Workload::AllocateKey(std::unique_ptr<const char[]>* key_guard) {
  char* data = new char[workload_properties_.key_size_];
  const char* const_data = data;
  key_guard->reset(const_data);
  return Slice(key_guard->get(), workload_properties_.key_size_);
}

} // namespace util
} // namespace rocksdb