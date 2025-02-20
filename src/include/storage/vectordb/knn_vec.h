#pragma once
#include "storage/vectordb/vector_index_base.h"

#define TIME_INDEX

namespace leanstore::storage::vector {
namespace  vec {

class KnnIndexVec : public BaseVectorIndex {
public:
  KnnIndexVec(size_t vector_size, std::vector<std::vector<float>> &&vectors);

  void build_index_vec() override;
  std::vector<std::span<float>> find_n_closest_vectors_vec(const std::vector<float> &input_vec, size_t n) override;

private:
  size_t vector_size;
  std::vector<std::vector<float>> vectors_;
};

};
} // namespace leanstore::storage::vector
