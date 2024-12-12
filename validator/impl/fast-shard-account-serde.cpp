#include "fast-shard-account-serde.hpp"
#include "crypto/vm/boc.h"
#include "tdutils/td/utils/StringBuilder.h"
#include "tdutils/td/utils/int_types.h"
#include "ton/ton-types.h"
#include "td/utils/misc.h"

namespace helper {
struct SerdeError {
  td::StringBuilder msg;
};

struct Account {
  td::BufferSlice boc;
  td::BitArray<256> last_trans_hash;
  td::uint64 last_trans_lt;
};
}  // namespace helper

namespace td {
template <class StorerT>
void store(const Bits256& x, StorerT& storer) {
  storer.store_binary(x);
}
template <class ParserT>
void parse(Bits256& x, ParserT& parser) {
  x = parser.template fetch_binary<Bits256>();
}

template <class StorerT>
void store(const helper::Account& acc, StorerT& storer) {
  store(acc.boc, storer);
  store(acc.last_trans_hash, storer);
  store(acc.last_trans_lt, storer);
}

template <class ParserT>
void parse(helper::Account& acc, ParserT& parser) {
  parse(acc.boc, parser);
  parse(acc.last_trans_hash, parser);
  parse(acc.last_trans_lt, parser);
}

/// NOTE: put this function in a try/catch code block.
template <class StorerT>
void store(const block::gen::ShardAccount::Record& acc, StorerT& storer) {
  auto r_res = vm::std_boc_serialize(acc.account, 0);
  if (r_res.is_error()) {
    td::StringBuilder buf;
    buf << "vm::std_boc_serialize error: ";
    r_res.error().print(buf);
    throw helper::SerdeError{std::move(buf)};
  }
  store(helper::Account{r_res.move_as_ok(), acc.last_trans_hash, acc.last_trans_lt}, storer);
}

/// NOTE: put this function in a try/catch code block.
template <class ParserT>
void parse(block::gen::ShardAccount::Record& record, ParserT& parser) {
  helper::Account acc;
  parse(acc, parser);

  auto r_boc = vm::std_boc_deserialize(acc.boc.as_slice());
  if (r_boc.is_error()) {
    td::StringBuilder buf;
    buf << "vm::std_boc_deserialize error: ";
    r_boc.error().print(buf);
    throw helper::SerdeError{std::move(buf)};
  }

  record.account = r_boc.move_as_ok();
  record.last_trans_hash = acc.last_trans_hash;
  record.last_trans_lt = acc.last_trans_lt;
}

template <class Key, class T, class Compare, class Allocator, class StorerT>
void store(const std::map<Key, T, Compare, Allocator>& s, StorerT& storer) {
  storer.store_binary(narrow_cast<int32>(s.size()));
  for (const auto& val : s) {
    store(val.first, storer);
    store(val.second, storer);
  }
}

template <class Key, class T, class Compare, class Allocator, class ParserT>
void parse(std::map<Key, T, Compare, Allocator>& s, ParserT& parser) {
  uint32 size = parser.fetch_int();
  if (parser.get_left_len() < size) {
    parser.set_error("Wrong set length");
    return;
  }
  s.clear();
  for (uint32 i = 0; i < size; i++) {
    Key k;
    parse(k, parser);
    T v;
    parse(v, parser);
    s.emplace(std::move(k), std::move(v));
  }
}
}  // namespace td

// inclusion of tl_helpers.h must after those store and parse functions below.
#include "tdutils/td/utils/tl_helpers.h"
namespace shard_account {

td::Result<std::string> serialize(const std::map<td::Bits256, block::gen::ShardAccount::Record>& accounts) {
  try {
    return td::serialize(accounts);
  } catch (helper::SerdeError& e) {
    LOG(ERROR) << e.msg.as_cslice();
    return td::Status::Error();
  }
}

td::Result<std::map<td::Bits256, block::gen::ShardAccount::Record>> deserialize(td::Slice data) {
  std::map<td::Bits256, block::gen::ShardAccount::Record> accounts;
  try {
    td::unserialize(accounts, data);
  } catch (helper::SerdeError& e) {
    LOG(ERROR) << e.msg.as_cslice();
    return td::Status::Error();
  }

  return std::move(accounts);
}

}  // namespace shard_account