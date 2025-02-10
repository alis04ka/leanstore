#include "storage/vectordb/util.h"
#include "storage/vectordb/timer.h"

namespace leanstore::storage::vector {

float distance_vec(std::span<const float> span1, std::span<const float> span2) {
  assert(span1.size() == span2.size());

  size_t size = span1.size();
  size_t simd_width = 8; 
  size_t simd_end = size - (size % simd_width);

  __m256 sum_vec = _mm256_setzero_ps();

  for (size_t i = 0; i < simd_end; i += simd_width) {
    __m256 vec1 = _mm256_loadu_ps(&span1[i]);
    __m256 vec2 = _mm256_loadu_ps(&span2[i]);
    __m256 diff = _mm256_sub_ps(vec1, vec2);
    __m256 sq_diff = _mm256_mul_ps(diff, diff);
    sum_vec = _mm256_add_ps(sum_vec, sq_diff);
  }

  float sum_array[8];
  _mm256_storeu_ps(sum_array, sum_vec);
  float sum = 0.0f;
  for (float v : sum_array) {
    sum += v;
  }

  for (size_t i = simd_end; i < size; ++i) {
    float diff = span1[i] - span2[i];
    sum += diff * diff;
  }

  return std::sqrt(sum);
}

float distance_blob(BlobAdapter &db, const BlobState *blob_state_1, const BlobState *blob_state_2) {
  assert(blob_state_1->blob_size == blob_state_2->blob_size);
  if (blob_state_1 == blob_state_2) {
    return 0.0;
  }
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

float distance_vec_blob(BlobAdapter &db, std::span<const float> input_span, const BlobState *blob_state) {
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