#include "storage/vectordb/knn_vec.h"
#include "storage/vectordb/util.h"

namespace leanstore::storage::vector::vec {


KnnIndexVec::KnnIndexVec(size_t vector_size, std::vector<std::vector<float>> &&vectors) : vector_size(vector_size), vectors_(std::move(vectors))  {}

void KnnIndexVec::build_index_vec() {}

std::vector<std::span<float>> KnnIndexVec::find_n_closest_vectors_vec(const std::vector<float> &input_vec, size_t n) {
std::vector<size_t> indices_res = knn(input_vec, vectors_, n);
std::vector<std::span<float>> vectors_res;
  for (size_t i = 0; i < indices_res.size(); i++) {
    vectors_res.push_back(vectors_[indices_res[i]]);
  }
  return vectors_res;
}

} // namespace leanstore::storage::vector
