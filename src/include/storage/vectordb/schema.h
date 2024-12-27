
#pragma once
#include "leanstore/leanstore.h"
#include "share_headers/db_types.h"

namespace leanstore::storage {
namespace vector {

struct VectorRecord {
  static constexpr int TYPE_ID = 42;

  struct Key {
    Integer my_key;
  };

  leanstore::BlobState blobState;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return blobState.MallocSize(); }

  static auto FoldKey(uint8_t *out, const Key &key) -> uint16_t {
    auto pos = Fold(out, key.my_key);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, Key &key) -> uint16_t {
    auto pos = Unfold(in, key.my_key);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::my_key); }
};
}
} // namespace