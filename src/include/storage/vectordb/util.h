#pragma once
#include "storage/vectordb/blob_adapter.h"

namespace leanstore::storage::vector {

float distance_vec(std::span<const float> span1, std::span<const float> span2);
float distance_blob(BlobAdapter &adapter, const BlobState *blob1, const BlobState *blob2);
float distance_vec_blob(BlobAdapter &adapter, std::span<const float> input_span, const BlobState *blob_state);
std::vector<size_t> knn(const std::vector<float> &query, const std::vector<std::vector<float>> &data, size_t k);
float knn_hnsw_error(const std::vector<float> &query, const std::vector<size_t> &knn_res, const std::vector<size_t> &hnsw_res, const std::vector<std::vector<float>> &data);
}