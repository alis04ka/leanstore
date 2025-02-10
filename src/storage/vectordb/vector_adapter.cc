#include "storage/vectordb/vector_adapter.h"
#include "storage/vectordb/blob_adapter.h"

namespace leanstore::storage::vector {

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


void VectorAdapter::InsertVectorRecord(const VectorRecord::Key &r_key, const VectorRecord &record) {
  u8 key[VectorRecord::MaxFoldLength()];
  auto len = VectorRecord::FoldKey(key, r_key);
  auto data = reinterpret_cast<const u8 *>(&record);
  auto span = std::span<const u8>(data, record.PayloadSize());
  tree_->Insert({key, len}, span);
}


auto VectorAdapter::LookUp(const VectorRecord::Key &r_key, const AccessVectorFunc &fn) -> bool {
  u8 key[VectorRecord::MaxFoldLength()];
  auto len = VectorRecord::FoldKey(key, r_key);
  bool success =
    tree_->LookUp({key, len}, [&](std::span<u8> pl) { fn(*reinterpret_cast<VectorRecord *>(pl.data())); });
  return success;
}

void VectorAdapter::Update(const VectorRecord::Key &r_key, std::span<u8> updated_data) {
  u8 key[VectorRecord::MaxFoldLength()];
  auto len = VectorRecord::FoldKey(key, r_key);
  LookUp(r_key, [&](const VectorRecord &record) {
    blob_adapter.RemoveBlob((leanstore::BlobState *)&record.blobState);
  });
  const leanstore::BlobState *updated_state = blob_adapter.RegisterBlob(updated_data);
  const VectorRecord *record = reinterpret_cast<const VectorRecord *>(updated_state);
  tree_->Update({key, len}, {reinterpret_cast<const u8 *>(record), record->PayloadSize()}, {});
}


auto VectorAdapter::GetFloatVector(const VectorRecord::Key &key) -> std::vector<float> {
  std::vector<float> vec_to_return;
  LookUp(key, [&](const VectorRecord &record) {
    blob_adapter.LoadBlob(&record.blobState, [&](std::span<const u8> blob) {
       std::span<const float> span(reinterpret_cast<const float *>(blob.data()), blob.size() / sizeof(float));
        vec_to_return.assign(span.begin(), span.end()); }, true);
  });
  return vec_to_return;
}

auto VectorAdapter::GetFloatVectorFromBlobState(const BlobState *blob_state) -> std::vector<float> {
  std::vector<float> vec_to_return;
  blob_adapter.LoadBlob(
    blob_state,
    [&](std::span<const u8> blob) {
      std::span<const float> span(reinterpret_cast<const float *>(blob.data()), blob.size() / sizeof(float));
      vec_to_return.assign(span.begin(), span.end());
    },
    false);
  return vec_to_return;
}

auto VectorAdapter::Count() -> u64 {
  return tree_->CountEntries();
}

} // namespace leanstore::storage::vector
