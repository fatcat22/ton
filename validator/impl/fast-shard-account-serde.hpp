#pragma once

#include "ton/ton-types.h"
#include "block/block-auto.h"
#include "crypto/common/refcnt.hpp"
#include <map>

namespace shard_account {

td::Result<std::string> serialize(const std::map<td::Bits256, block::gen::ShardAccount::Record>& accounts);
td::Result<std::map<td::Bits256, block::gen::ShardAccount::Record>> deserialize(td::Slice data);

}  // namespace shard_account
