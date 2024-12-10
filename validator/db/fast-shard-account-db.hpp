#pragma once

#include "td/actor/actor.h"
#include "ton/ton-types.h"
#include "block.h"
#include <memory>
#include <array>
#include "tddb/td/db/KeyValue.h"

class FastShardAccountDB : public td::actor::Actor {
 public:
  FastShardAccountDB(std::string db_root_path, bool must_success);

  td::Result<> store(ton::BlockId id, std::string data);
  td::Result<std::string> read(const ton::BlockId& id);
  void erase(ton::BlockId id);

 private:
  size_t format_block_id(const ton::BlockId& id, std::array<char, 64>& buf);

 private:
  std::string db_root_path_;

 private:
  std::shared_ptr<td::KeyValue> db_;
};