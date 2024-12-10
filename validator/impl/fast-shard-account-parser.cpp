#include "fast-shard-account-parser.hpp"
#include "fast-shard-account-serde.hpp"
#include "crypto/block/block-parse.h"
#include <sstream>
#include <map>

void FastShardAccountParser::parse(ton::BlockId id, block::gen::ShardStateUnsplit::Record state_record,
                                   td::Ref<ton::validator::BlockData> block) {
  // this class is not designed for parse more than one block or more than once.
  CHECK(!stored_ && false == bool(serialized_data_));

  auto r_addresses = extract_addresses(id, block);
  if (r_addresses.is_error()) {
    return;
  }
  const auto addresses = r_addresses.move_as_ok();

  auto r_accounts = extract_accounts(std::move(addresses), state_record);
  if (r_accounts.is_error()) {
    return;
  }
  const auto accounts = r_accounts.move_as_ok();

  auto r_ser = shard_account::serialize(accounts);
  if (r_ser.is_error()) {
    return;
  }

  id_ = id;
  serialized_data_ = r_ser.move_as_ok();

  try_store();
}

void FastShardAccountParser::set_db(td::actor::ActorId<FastShardAccountDB> db) {
  if (false == bool(db_)) {
    db_ = db;
  }

  try_store();
}

void FastShardAccountParser::try_store() {
  if (stored_ || !serialized_data_ || !db_) {
    return;
  }

  td::actor::send_closure(db_.value(), &FastShardAccountDB::store, id_, serialized_data_.unwrap());
  stored_ = true;
}

td::Result<std::set<td::Bits256>> FastShardAccountParser::extract_addresses(
    const ton::BlockId& id, const td::Ref<ton::validator::BlockData>& block) {
  std::set<td::Bits256> addresses;

  block::gen::Block::Record blk;
  block::gen::BlockExtra::Record extra;
  if (!(tlb::unpack_cell(block->root_cell(), blk) && tlb::unpack_cell(blk.extra, extra))) {
    LOG(ERROR) << "block data info extra unpack failed";
    return td::Status::Error();
  }

  try {
    vm::AugmentedDictionary acc_dict{vm::load_cell_slice_ref(extra.account_blocks), 256,
                                     block::tlb::aug_ShardAccountBlocks};

    td::Bits256 cur_addr = td::Bits256::zero();
    bool eof = false;
    bool allow_same = true;
    while (!eof) {
      td::Ref<vm::CellSlice> value;
      try {
        value = acc_dict.extract_value(
            acc_dict.vm::DictionaryFixed::lookup_nearest_key(cur_addr.bits(), 256, true, allow_same));
      } catch (vm::VmError err) {
        LOG(ERROR) << "error while traversing account block dictionary: " << err.get_msg();
      }

      if (value.is_null()) {
        LOG(ERROR) << id.to_str() << "value is null, exist. address count: " << addresses.size();
        eof = true;
        break;
      }
      allow_same = false;
      block::gen::AccountBlock::Record acc_blk;
      if (!(tlb::csr_unpack(std::move(value), acc_blk) && acc_blk.account_addr == cur_addr)) {
        LOG(ERROR) << "invalid AccountBlock for account " + cur_addr.to_hex();
        continue;
      }
      addresses.insert(cur_addr);
    }
  } catch (vm::VmError err) {
    LOG(ERROR) << "error while parsing AccountBlocks : " << err.get_msg();
    return td::Status::Error();
  }

  return std::move(addresses);
}

td::Result<std::map<td::Bits256, block::gen::ShardAccount::Record>> FastShardAccountParser::extract_accounts(
    std::set<td::Bits256> addresses, const block::gen::ShardStateUnsplit::Record& state_record) {
  std::map<td::Bits256, block::gen::ShardAccount::Record> accounts;

  vm::AugmentedDictionary accounts_dict{vm::load_cell_slice_ref(state_record.accounts), 256,
                                        block::tlb::aug_ShardAccounts};
  for (auto entry : addresses) {
    auto shard_account_csr = accounts_dict.lookup(entry);
    if (shard_account_csr.is_null()) {
      // TODO:
      LOG(ERROR) << "lookup_ref return null";
    } else {
      block::gen::ShardAccount::Record acc_info;
      CHECK(tlb::csr_unpack(shard_account_csr, acc_info));

      accounts.emplace(std::move(entry), std::move(acc_info));
    }
  }

  return std::move(accounts);
}