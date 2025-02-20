#pragma once
#include "leanstore/leanstore.h"
#include "schema.h"

namespace leanstore::storage::vector {

using FoundVectorFunc = std::function<bool(const VectorRecord::Key &, const VectorRecord &)>;
using AccessVectorFunc = std::function<void(const VectorRecord &)>;
using ModifyVectorFunc = std::function<void(VectorRecord &)>;

struct BlobAdapter {
private:

  leanstore::LeanStore *db_;

public:
  explicit BlobAdapter(leanstore::LeanStore &db) : db_(&db) {}
  ~BlobAdapter() = default;


  auto RegisterBlob(std::span<const u8> blob_payload) -> const BlobState *;
  auto UpdateBlob(std::span<const u8> blob_payload, leanstore::BlobState *prev_blob) -> const BlobState *;
  void LoadBlob(const BlobState *state, const std::function<void(std::span<const u8>)> &read_cb);
  void RemoveBlob(leanstore::BlobState *state);
  auto GetFloatVectorFromBlobState(const BlobState *blob_state) -> std::vector<float>;
};


} // namespace leanstore::storage::vector