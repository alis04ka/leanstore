#include "storage/vectordb/blob_adapter.h"
#include "tracy/Tracy.hpp"

namespace leanstore::storage::vector {


auto BlobAdapter::RegisterBlob(std::span<const u8> blob_payload) -> const BlobState * {
  auto prev_blob = nullptr;
  auto res = db_->CreateNewBlob(blob_payload, prev_blob, true);
  return reinterpret_cast<const BlobState *>(res.data());
}

auto BlobAdapter::UpdateBlob(std::span<const u8> blob_payload, leanstore::BlobState *prev_blob) -> const BlobState * {
  assert(prev_blob != nullptr);
  auto res = db_->CreateNewBlob(blob_payload, prev_blob, true);
  return reinterpret_cast<const BlobState *>(res.data());
}

void BlobAdapter::LoadBlob(
  const BlobState *state, const std::function<void(std::span<const u8>)> &read_cb) {
  ZoneScoped;
  Ensure(
    (leanstore::BlobState::MIN_MALLOC_SIZE <= state->MallocSize()) &&
    (leanstore::BlobState::MAX_MALLOC_SIZE >= state->MallocSize()));
  db_->LoadBlob(state, read_cb, false);
}

void BlobAdapter::RemoveBlob(leanstore::BlobState *state) {
  db_->RemoveBlob(state);
}

auto BlobAdapter::GetFloatVectorFromBlobState(const BlobState *blob_state) -> std::vector<float> {
  ZoneScoped;
  std::vector<float> vec_to_return;
  LoadBlob(
    blob_state,
    [&](std::span<const u8> blob) {
      ZoneScopedN("copy blob to float vec");
      std::span<const float> span(reinterpret_cast<const float *>(blob.data()), blob.size() / sizeof(float));
      vec_to_return.assign(span.begin(), span.end());
    });
  return vec_to_return;
}

} // namespace leanstore::storage::vector
