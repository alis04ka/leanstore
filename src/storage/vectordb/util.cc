#include "storage/vectordb/util.h"

namespace leanstore::storage::vector {

float distance_vec(std::span<const float> span1, std::span<const float> span2) {
  //std::cout << "distance vec" <<  std::endl;
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
  if (blob_state_1->BlobID() == blob_state_2->BlobID()) {
    return 0.0;
  }


  // blob_state_1->print_float();
  // blob_state_2->print_float();
  float distance = 0.0;
  // std::vector<float> blob1_data;
  // db.LoadBlob(blob_state_1, [&](std::span<const uint8_t> blob1) {
  //     std::span<const float> span1(reinterpret_cast<const float *>(blob1.data()), blob1.size() / sizeof(float));
  //     blob1_data.resize(span1.size());
  //     std::copy(span1.begin(), span1.end(), blob1_data.begin()); }, false);
  // db.LoadBlob(
  //   blob_state_2,
  //   [&](std::span<const uint8_t> blob2) {
  //     std::span<const float> span2(reinterpret_cast<const float *>(blob2.data()), blob2.size() / sizeof(float));
  //     distance = distance_vec(blob1_data, span2);
  //   },
  //   false);

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
  //std::cout << "distance vec blob" <<  std::endl;
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

std::vector<size_t> knn(const std::vector<float> &query, const std::vector<std::vector<float>> &data, size_t k) {
  std::vector<std::pair<float, size_t>> distIdx;
  distIdx.reserve(data.size());

  for (size_t i = 0; i < data.size(); ++i) {
    float dist = distance_vec(query, data[i]);
    distIdx.emplace_back(dist, i);
  }

  std::sort(distIdx.begin(), distIdx.end(), [](auto &l, auto &r) { return l.first < r.first; });

  std::vector<size_t> neighbors;
  neighbors.reserve(k);
  for (size_t i = 0; i < k && i < distIdx.size(); ++i) {
    neighbors.push_back(distIdx[i].second);
  }
  return neighbors;
}


float knn_hnsw_error(BlobAdapter &db, const std::vector<float> &query, const std::vector<size_t> &knn_res, const std::vector<const BlobState *> &hnsw_res, const std::vector<std::vector<float>> &data) {
  assert(knn_res.size() == hnsw_res.size());

  float error_knn = 0.0f;
  for (size_t idx : knn_res) {
    error_knn += distance_vec(query, data[idx]);
  }
  error_knn /= (float)knn_res.size();
  std::cout << "KNN absolute error: " << error_knn << std::endl;

  float error_hnsw = 0.0f;
  for (auto state : hnsw_res) {
    error_hnsw += distance_vec(query, db.GetFloatVectorFromBlobState(state));
  }
  error_hnsw /= (float)hnsw_res.size();
  std::cout << "HNSW absolute error: " << error_hnsw << std::endl;

  float res_error = error_hnsw - error_knn;
  std::cout << "Result error:" << res_error << std::endl;
  return res_error;
}

} // namespace leanstore::storage::vector
