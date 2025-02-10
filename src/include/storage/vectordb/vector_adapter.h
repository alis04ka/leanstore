#pragma once
#include "leanstore/kv_interface.h"
#include "leanstore/leanstore.h"
#include "schema.h"
#include "storage/vectordb/blob_adapter.h"
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
  leanstore::LeanStore *db_;
  leanstore::KVInterface *tree_;
  BlobAdapter blob_adapter;

  void ScanImpl(const VectorRecord::Key &r_key, const FoundVectorFunc &found_record_cb, bool scan_ascending);
  void ScanCentroidsImpl(const VectorRecord::Key &r_key, const FoundVectorFunc &found_record_cb, bool scan_ascending);

public:
  template<class T>
  static VectorAdapter CreateVectorAdapter(leanstore::LeanStore &db) {
    auto adapter = VectorAdapter(db, static_cast<std::type_index>(typeid(T)));
    return adapter;
  }

  explicit VectorAdapter(leanstore::LeanStore &db, std::type_index idx)
      : relation_(idx),
        db_(&db), blob_adapter(db) {
    db_->RegisterTable(relation_);
    tree_ = db_->RetrieveIndex(relation_);
}
  ~VectorAdapter() = default;

  // -------------------------------------------------------------------------------------
  void Scan(const VectorRecord::Key &key, const FoundVectorFunc &found_db);
  void ScanDesc(const VectorRecord::Key &key, const FoundVectorFunc &found_record_cb);
  void InsertVectorRecord(const VectorRecord::Key &r_key, const VectorRecord &record);
  void Update(const VectorRecord::Key &key, std::span<u8> updated_data);
  auto LookUp(const VectorRecord::Key &r_key, const AccessVectorFunc &fn) -> bool;
  auto GetFloatVector(const VectorRecord::Key &key) -> std::vector<float>;
  auto GetFloatVectorFromBlobState(const BlobState *blob_state) -> std::vector<float>;
  auto Count() -> u64;

};

static_assert(GLOBAL_BLOCK_SIZE == leanstore::BLK_BLOCK_SIZE);

} // namespace leanstore::storage::vector