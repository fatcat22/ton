#include "fast-shard-account-db.hpp"
#include "tddb/td/db/RocksDb.h"
#include "tdutils/td/utils/port/path.h"

FastShardAccountDB::FastShardAccountDB(std::string db_root_path, bool must_success) {
  td::RocksDbOptions db_options;
  db_options.use_direct_reads = true;

  const auto db_path = db_root_path + "/fastshardaccount";
  const auto dir_status = td::mkdir(db_path);
  if (dir_status.is_error()) {
    LOG(ERROR) << "create fast shard account db path " << dir_status << " failed: " << dir_status.to_string();
  }

  auto r_rocks_db = td::RocksDb::open(db_path, std::move(db_options));
  if (must_success || r_rocks_db.is_ok()) {
    db_ = std::make_shared<td::RocksDb>(r_rocks_db.move_as_ok());
  }
}

td::Result<> FastShardAccountDB::store(ton::BlockId id, std::string data) {
  // for write, db_ must not be null if the function is called.
  CHECK(nullptr != db_);

  std::array<char, 64> key;
  const auto key_len = format_block_id(id, key);

  auto status = db_->set(td::Slice(key.data(), key_len), td::Slice(data));
  if (status.is_error()) {
    td::StringBuilder buf;
    status.print(buf);
    LOG(ERROR) << "write fast shard account for " << id.to_str() << " error:" << buf.as_cslice().c_str();
    return std::move(status);
  }

  return {};
}

td::Result<std::string> FastShardAccountDB::read(const ton::BlockId& id) {
  if (nullptr == db_) {
    return td::Status::Error("fast shard account db doesn't exist");
  }

  std::array<char, 64> key;
  const auto key_len = format_block_id(id, key);

  std::string data;
  auto r_res = db_->get(td::Slice(key.data(), key_len), data);
  if (r_res.is_error()) {
    LOG(ERROR) << "read fast shard account " << id.to_str() << " error:" << r_res.error().to_string();
    return r_res.move_as_error();
  }
  const auto status = r_res.move_as_ok();
  if (status == td::KeyValueReader::GetStatus::NotFound) {
    td::StringBuilder buf;
    buf << "not found " << id.to_str() << " in fast shard account db";
    return td::Status::Error(buf.as_cslice());
  }

  return std::move(data);
}

void FastShardAccountDB::erase(ton::BlockId id) {
  if (nullptr == db_) {
    LOG(ERROR) << "fast shard account db is null";
    return;
  }

  std::array<char, 64> key;
  const auto key_len = format_block_id(id, key);

  const auto res = db_->erase(td::Slice(key.data(), key_len));
  if (res.is_error()) {
    LOG(ERROR) << "erase " << id.to_str() << " from fast shard account db failed: " << res.to_string();
  }
}

size_t FastShardAccountDB::format_block_id(const ton::BlockId& id, std::array<char, 64>& buf) {
  const auto l =
      snprintf(buf.data(), 63, "%d-%016llx-%u", id.workchain, static_cast<unsigned long long>(id.shard), id.seqno);
  buf[l] = '\0';
  return l;
}