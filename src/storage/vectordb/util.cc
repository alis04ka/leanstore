#include "storage/vectordb/util.h"

namespace leanstore::storage::vector {


float distance_vec(std::span<const float> span1, std::span<const float> span2) {
  assert(span1.size() == span2.size());

  float sum = 0.0;
  for (size_t i = 0; i < span1.size(); ++i) {
    float diff = span1[i] - span2[i];
    sum += diff * diff;
  }

  return std::sqrt(sum);
}

float distance_blob(VectorAdapter &db, const BlobState *blob_state_1, const BlobState *blob_state_2) {
  assert(blob_state_1->blob_size == blob_state_2->blob_size);
  float distance = 0.0;
  db.LoadBlob(
    blob_state_1,
    [&](std::span<const uint8_t> blob1) {
      std::span<const float> span1(reinterpret_cast<const float *>(blob1.data()), blob1.size() / sizeof(float));
      db.LoadBlob(
        blob_state_2,
        [&](std::span<const uint8_t> blob2) {
          std::span<const float> span2(reinterpret_cast<const float *>(blob2.data()), blob2.size() / sizeof(float));
          distance = distance_vec(span1, span2);
        },
        false);
    },
    false);

  return distance;
}

float distance_vec_blob(VectorAdapter &db, std::span<const float> input_span, const BlobState *blob_state) {
  float distance = 0.0;
  db.LoadBlob(
    blob_state,
    [&](std::span<const uint8_t> blob1) {
      std::span<const float> span1(reinterpret_cast<const float *>(blob1.data()), blob1.size() / sizeof(float));
      distance = distance_vec(span1, input_span);
    },
    false);

  return distance;
}

}