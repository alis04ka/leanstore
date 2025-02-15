#pragma once
#include "schema.h"
#include "storage/vectordb/vector_adapter.h"

namespace leanstore::storage::vector {

class BaseVectorIndex {
public:
  BaseVectorIndex() = default;

  virtual void build_index_vec() = 0;
  virtual std::vector<std::span<float>> find_n_closest_vectors_vec(const std::vector<float> &input_vec, size_t n) = 0;
};

} // namespace leanstore::storage::vector
