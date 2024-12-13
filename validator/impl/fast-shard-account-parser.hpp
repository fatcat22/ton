#pragma once

#include "td/actor/actor.h"
#include "ton/ton-types.h"
#include "block/block-auto.h"
#include "interfaces/block.h"
#include "crypto/common/refcnt.hpp"
#include "tdutils/td/utils/optional.h"
#include "db/fast-shard-account-db-file.hpp"

class FastShardAccountParser : public td::actor::Actor {
 public:
  FastShardAccountParser() = default;

  void parse(ton::BlockId id, block::gen::ShardStateUnsplit::Record state_record,
             td::Ref<ton::validator::BlockData> block);
  void set_db(td::actor::ActorId<FastShardAccountDBFile> db);

 private:
  void try_store();

 private:
  td::Result<std::set<td::Bits256>> extract_addresses(const ton::BlockId& id,
                                                      const td::Ref<ton::validator::BlockData>& block);
  td::Result<std::map<td::Bits256, block::gen::ShardAccount::Record>> extract_accounts(
      std::set<td::Bits256> addresses, const block::gen::ShardStateUnsplit::Record& state_record);

 private:
  bool stored_ = false;
  ton::BlockId id_;
  td::optional<std::string> serialized_data_;
  td::optional<td::actor::ActorId<FastShardAccountDBFile>> db_;
};