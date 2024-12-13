#include "fast-shard-account-db-file.hpp"
#include "tddb/td/db/RocksDb.h"
#include "tdutils/td/utils/port/path.h"
#include "tdutils/td/utils/port/FileFd.h"

FastShardAccountDBFile::FastShardAccountDBFile(std::string db_root_path, bool must_success) {
  db_path_ = db_root_path + "/fastshardaccount/";
}

td::Result<> FastShardAccountDBFile::store(ton::BlockId id, std::string data) {
  LOG(DEBUG) << "FastShardAccountDBFile::store store " << id.to_str();

  const auto file_path = gen_file_path(id);

  auto r_fd = td::FileFd::open(file_path, td::FileFd::Write | td::FileFd::CreateNew | td::FileFd::Truncate);
  if (r_fd.is_error()) {
    LOG(ERROR) << "open fast shard account file " << file_path << " error: " << r_fd.error().to_string();
    return r_fd.move_as_error();
  }
  auto fd = r_fd.move_as_ok();

  auto r_w = fd.write(data);
  if (r_w.is_error()) {
    LOG(ERROR) << "write fast shard account file " << file_path << " error: " << r_w.error().to_string();
    return r_w.move_as_error();
  }
  const auto write_len = r_w.move_as_ok();

  if (write_len != data.length()) {
    LOG(ERROR) << "invalid write fast shard account file " << file_path << " . expect write lenght " << data.length()
               << "but writen length " << write_len;
    return td::Status::Error("write length invalid");
  }

  return {};
}

td::Result<std::string> FastShardAccountDBFile::read(const ton::BlockId& id) {
  const auto file_path = gen_file_path(id);

  auto r_fd = td::FileFd::open(file_path, td::FileFd::Read);
  if (r_fd.is_error()) {
    LOG(ERROR) << "open fast shard account file " << file_path << " error: " << r_fd.error().to_string();
    return r_fd.move_as_error();
  }
  auto fd = r_fd.move_as_ok();

  std::string data;
  auto r = fd.read(data);
  if (r.is_error()) {
    LOG(ERROR) << "read fast shard account " << file_path << " failed: " << r.error().to_string();
    return r.move_as_error();
  }

  return std::move(data);
}

void FastShardAccountDBFile::erase(ton::BlockId id) {
  const auto file_path = gen_file_path(id);
  const auto r = td::unlink(file_path);
  if (r.is_error()) {
    // just log a debug message, for the file may not exist, then it's normal to remove failed.
    LOG(DEBUG) << "remove file " << file_path << " failed: " << r.error().to_string();
  }
}

std::string FastShardAccountDBFile::gen_file_path(const ton::BlockId& id) {
  std::array<char, 64> key;
  const auto key_len = format_block_id(id, key);

  std::string file_path;
  file_path.reserve(db_path_.length() + key_len);
  file_path = db_path_;
  file_path.append(key.data(), key_len);
}

size_t FastShardAccountDBFile::format_block_id(const ton::BlockId& id, std::array<char, 64>& buf) {
  const auto l =
      snprintf(buf.data(), 63, "%d-%016llx-%u", id.workchain, static_cast<unsigned long long>(id.shard), id.seqno);
  buf[l] = '\0';
  return l;
}