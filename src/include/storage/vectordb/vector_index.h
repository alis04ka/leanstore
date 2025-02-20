#pragma once
#include "leanstore/leanstore.h"
#include <vector>
namespace leanstore::storage::vector {

class VectorIndex {
public:
  VectorIndex() = default;

  virtual void build_index() = 0;
  virtual std::vector<const BlobState *> find_n_closest_vectors(const std::vector<float> &input_vec, size_t n) = 0;
};

} // namespace leanstore::storage::vector
