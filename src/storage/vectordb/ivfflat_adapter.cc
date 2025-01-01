#include "storage/vectordb/ivfflat_adapter.h"

namespace leanstore::storage::vector {

VectorAdapter::VectorAdapter(leanstore::LeanStore &db)
    : relation_(static_cast<std::type_index>(typeid(VectorRecord))),
      centroid_relation_(static_cast<std::type_index>(typeid(CentroidType))),
      db_(&db) {
  db_->RegisterTable(relation_);
  db_->RegisterTable(centroid_relation_);
  tree_ = db_->RetrieveIndex(relation_);
  centroid_tree_ = db.RetrieveIndex(centroid_relation_);
}

void VectorAdapter::ScanImpl(
  const VectorRecord::Key &r_key, const FoundVectorFunc &found_record_cb, bool scan_ascending) {
  u8 key[VectorRecord::MaxFoldLength()];
  auto len = VectorRecord::FoldKey(key, r_key);

  auto read_cb = [&](std::span<u8> key, std::span<u8> payload) -> bool {
    VectorRecord::Key typed_key;
    VectorRecord::UnfoldKey(key.data(), typed_key);
    return found_record_cb(typed_key, *reinterpret_cast<const VectorRecord *>(payload.data()));
  };

  if (scan_ascending) {
    tree_->ScanAscending({key, len}, read_cb);
  } else {
    tree_->ScanDescending({key, len}, read_cb);
  }
}

void VectorAdapter::Scan(const VectorRecord::Key &key, const FoundVectorFunc &found_record_cb) {
  ScanImpl(key, found_record_cb, true);
}

void VectorAdapter::ScanDesc(const VectorRecord::Key &key, const FoundVectorFunc &found_record_cb) {
  ScanImpl(key, found_record_cb, false);
}

void VectorAdapter::ScanCentroidsImpl(
  const VectorRecord::Key &r_key, const FoundVectorFunc &found_record_cb, bool scan_ascending) {
  u8 key[VectorRecord::MaxFoldLength()];
  auto len = VectorRecord::FoldKey(key, r_key);

  auto read_cb = [&](std::span<u8> key, std::span<u8> payload) -> bool {
    VectorRecord::Key typed_key;
    VectorRecord::UnfoldKey(key.data(), typed_key);
    return found_record_cb(typed_key, *reinterpret_cast<const VectorRecord *>(payload.data()));
  };

  if (scan_ascending) {
    centroid_tree_->ScanAscending({key, len}, read_cb);
  } else {
    centroid_tree_->ScanDescending({key, len}, read_cb);
  }
}

void VectorAdapter::ScanCentroids(const VectorRecord::Key &key, const FoundVectorFunc &found_record_cb) {
  ScanCentroidsImpl(key, found_record_cb, true);
}

void VectorAdapter::ScanCentroidsDesc(const VectorRecord::Key &key, const FoundVectorFunc &found_record_cb) {
  ScanCentroidsImpl(key, found_record_cb, false);
}

void VectorAdapter::InsertVectorRecordIntoMain(const VectorRecord::Key &r_key, const VectorRecord &record) {
  u8 key[VectorRecord::MaxFoldLength()];
  auto len = VectorRecord::FoldKey(key, r_key);
  auto data = reinterpret_cast<const u8 *>(&record);
  auto span = std::span<const u8>(data, record.PayloadSize());
  tree_->Insert({key, len}, span);
}

void VectorAdapter::InsertVectorRecordIntoCentroids(const VectorRecord::Key &r_key, const VectorRecord &record) {
  u8 key[VectorRecord::MaxFoldLength()];
  auto len = VectorRecord::FoldKey(key, r_key);
  auto data = reinterpret_cast<const u8 *>(&record);
  auto span = std::span<const u8>(data, record.PayloadSize());
  centroid_tree_->Insert({key, len}, span);
}

void VectorAdapter::UpdateCentroid(const VectorRecord::Key &r_key, std::span<u8> updated_data) {
  u8 key[VectorRecord::MaxFoldLength()];
  auto len = VectorRecord::FoldKey(key, r_key);
  LookUpCentroids(r_key, [&](const VectorRecord &record) { RemoveBlob((leanstore::BlobState *)&record.blobState); });
  const leanstore::BlobState *updated_state = RegisterBlob(updated_data);
  const VectorRecord *record = reinterpret_cast<const VectorRecord *>(updated_state);
  centroid_tree_->Update({key, len}, {reinterpret_cast<const u8 *>(record), record->PayloadSize()}, {});
}

auto VectorAdapter::LookUpMain(const VectorRecord::Key &r_key, const AccessVectorFunc &fn) -> bool {
  u8 key[VectorRecord::MaxFoldLength()];
  auto len = VectorRecord::FoldKey(key, r_key);
  bool success =
    tree_->LookUp({key, len}, [&](std::span<u8> pl) { fn(*reinterpret_cast<VectorRecord *>(pl.data())); });
  return success;
}

auto VectorAdapter::LookUpCentroids(const VectorRecord::Key &r_key, const AccessVectorFunc &fn) -> bool {
  u8 key[VectorRecord::MaxFoldLength()];
  auto len = VectorRecord::FoldKey(key, r_key);
  bool success =
    centroid_tree_->LookUp({key, len}, [&](std::span<u8> pl) { fn(*reinterpret_cast<VectorRecord *>(pl.data())); });
  return success;
}

auto VectorAdapter::GetFloatVectorFromMain(const VectorRecord::Key &key) -> std::vector<float> {
  std::vector<float> vec_to_return;
  LookUpMain(key, [&](const VectorRecord &record) {
    LoadBlob(&record.blobState, [&](std::span<const u8> blob) {
       std::span<const float> span(reinterpret_cast<const float *>(blob.data()), blob.size() / sizeof(float));
        vec_to_return.assign(span.begin(), span.end()); }, true);
  });
  return vec_to_return;
}

auto VectorAdapter::GetFloatVectorFromCentroids(const VectorRecord::Key &key) -> std::vector<float> {
  std::vector<float> vec_to_return;
  LookUpCentroids(key, [&](const VectorRecord &record) {
    LoadBlob(&record.blobState, [&](std::span<const u8> blob) {
             std::span<const float> span(reinterpret_cast<const float *>(blob.data()), blob.size() / sizeof(float));
             vec_to_return.assign(span.begin(), span.end()); }, true);
  });
  return vec_to_return;
}

auto VectorAdapter::GetFloatVectorFromBlobState(const BlobState *blob_state) -> std::vector<float> {
  std::vector<float> vec_to_return;
  LoadBlob(
    blob_state,
    [&](std::span<const u8> blob) {
      std::span<const float> span(reinterpret_cast<const float *>(blob.data()), blob.size() / sizeof(float));
      vec_to_return.assign(span.begin(), span.end());
    },
    false);
  return vec_to_return;
}

auto VectorAdapter::CountMain() -> u64 {
  return tree_->CountEntries();
}

auto VectorAdapter::CountCentroids() -> u64 {
  return centroid_tree_->CountEntries();
}

auto VectorAdapter::RegisterBlob(std::span<const u8> blob_payload) -> const BlobState * {
  auto prev_btup = nullptr;
  auto res = db_->CreateNewBlob(blob_payload, prev_btup, false);
  return reinterpret_cast<const BlobState *>(res.data());
}

auto VectorAdapter::UpdateBlob(std::span<const u8> blob_payload, leanstore::BlobState *prev_blob) -> const BlobState * {
  assert(prev_blob != nullptr);
  auto res = db_->CreateNewBlob(blob_payload, prev_blob, false);
  return reinterpret_cast<const BlobState *>(res.data());
}

void VectorAdapter::LoadBlob(
  const BlobState *state, const std::function<void(std::span<const u8>)> &read_cb, bool partial_load) {
  // std::cout << "partial load: " << partial_load << std::endl;
  Ensure(
    (leanstore::BlobState::MIN_MALLOC_SIZE <= state->MallocSize()) &&
    (leanstore::BlobState::MAX_MALLOC_SIZE >= state->MallocSize()));
  db_->LoadBlob(state, read_cb, partial_load);
}

void VectorAdapter::RemoveBlob(leanstore::BlobState *state) {
  db_->RemoveBlob(state);
}

} // namespace leanstore::storage::vector
