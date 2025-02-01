#pragma once

#include "leanstore/kv_interface.h"
#include "leanstore/leanstore.h"
#include "schema.h"
#include <typeindex>

namespace leanstore::storage::vector {

using FoundVectorFunc = std::function<bool(const VectorRecord::Key &, const VectorRecord &)>;
using AccessVectorFunc = std::function<void(const VectorRecord &)>;
using ModifyVectorFunc = std::function<void(VectorRecord &)>;

static constexpr uint64_t GLOBAL_BLOCK_SIZE = 4096;

struct CentroidType {};

struct VectorAdapter {
private:
  std::type_index relation_;
  std::type_index centroid_relation_;
  leanstore::LeanStore *db_;
  leanstore::KVInterface *tree_;
  leanstore::KVInterface *centroid_tree_;

  void ScanImpl(const VectorRecord::Key &r_key, const FoundVectorFunc &found_record_cb, bool scan_ascending);
  void ScanCentroidsImpl(const VectorRecord::Key &r_key, const FoundVectorFunc &found_record_cb, bool scan_ascending);

public:
  explicit VectorAdapter(leanstore::LeanStore &db);
  ~VectorAdapter() = default;

  // -------------------------------------------------------------------------------------
  void Scan(const VectorRecord::Key &key, const FoundVectorFunc &found_db);
  void ScanDesc(const VectorRecord::Key &key, const FoundVectorFunc &found_record_cb);
  void ScanCentroids(const VectorRecord::Key &key, const FoundVectorFunc &found_db);
  void ScanCentroidsDesc(const VectorRecord::Key &key, const FoundVectorFunc &found_record_cb);
  void InsertVectorRecordIntoMain(const VectorRecord::Key &r_key, const VectorRecord &record);
  void InsertVectorRecordIntoCentroids(const VectorRecord::Key &r_key, const VectorRecord &record);
  void UpdateCentroid(const VectorRecord::Key &key, std::span<u8> updated_data);
  // void UpdateCentroidInPlace(const VectorRecord::Key& key, const ModifyVectorFunc &fn);
  auto LookUpMain(const VectorRecord::Key &r_key, const AccessVectorFunc &fn) -> bool;
  auto LookUpCentroids(const VectorRecord::Key &r_key, const AccessVectorFunc &fn) -> bool;
  auto GetFloatVectorFromMain(const VectorRecord::Key &key) -> std::vector<float>;
  auto GetFloatVectorFromCentroids(const VectorRecord::Key &key) -> std::vector<float>;
  auto GetFloatVectorFromBlobState(const BlobState *blob_state) -> std::vector<float>;
  auto CountMain() -> u64;
  auto CountCentroids() -> u64;

  // -------------------------------------------------------------------------------------
  auto RegisterBlob(std::span<const u8> blob_payload) -> const BlobState *;
  auto UpdateBlob(std::span<const u8> blob_payload, leanstore::BlobState *prev_blob) -> const BlobState *;
  void LoadBlob(const BlobState *state, const std::function<void(std::span<const u8>)> &read_cb, bool partial_load);
  void RemoveBlob(leanstore::BlobState *state);
};

static_assert(GLOBAL_BLOCK_SIZE == leanstore::BLK_BLOCK_SIZE);

} // namespace leanstore::storage::vector