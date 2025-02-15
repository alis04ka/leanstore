#include "storage/vectordb/hnsw_vec.h"
#include "gtest/gtest.h"

using namespace leanstore::storage::vector::vec;

TEST(HNSWVec, BuildIndex) {

  int num_vec = 10000;
  size_t vector_size = 1000;

  std::vector<std::vector<float>> vectors;
  for (int i = 0; i < num_vec; ++i) {
    std::vector<float> vector(vector_size, static_cast<float>(i));
    vectors.push_back(vector);
  }

  HNSWIndex index(std::move(vectors),  200, 100, 10);
  index.build_index_vec();
  std::vector<float> search_vector(vector_size, 38.6);
  std::vector<size_t> res = index.scan_vector_entry_vec(search_vector, 7);
  std::cout << res.size() << std::endl;
  for (size_t i = 0; i < res.size(); i++) {
    std::cout << res[i] << std::endl;
  }

  std::cout << "Search time: " << get_search_time_hnsw_vec() << " μs" << std::endl;
}
