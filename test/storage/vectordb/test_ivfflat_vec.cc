#include "storage/vectordb/ivfflat_vec.h"
#include "gtest/gtest.h"

using namespace leanstore::storage::vector;

TEST(IVFFlatVec, BuildIndexAndLookup) {

  int num_vec = 10000;
  int num_centroids = calculate_num_centroids_vec(num_vec);
  int num_probe_centroids = calculate_num_probe_centroids_vec(num_centroids);
  size_t vector_size = 1000;

  std::vector<std::vector<float>> vectors;
  for (int i = 0; i < num_vec; ++i) {
    std::vector<float> vector(vector_size, static_cast<float>(i));
    vectors.push_back(vector);
  }

  IVFFlatIndex index(num_centroids, num_probe_centroids, vector_size, std::move(vectors));
  index.build_index_vec();
  std::vector<Centroid> centroids = index.get_centroids();

  std::vector<float> input_vec(vector_size, 30.6);
  std::vector<std::span<float>> results = index.find_n_closest_vectors_vec(input_vec, 8);
  std::cout << "Search time: " << get_search_time_ivfflat_vec() << " μs" << std::endl;

  for (const auto &span : results) {
    if (!span.empty()) {
      std::cout << span.front() << std::endl;
    } else {
      std::cout << "Empty span" << std::endl;
    }
  }
  // std::vector<float> expected_results = {31.0, 30.0, 32.0, 29.0, 33.0, 28.0, 34.0, 27.0};
  // ASSERT_EQ(states.size(), expected_results.size());

  // for (size_t i = 0; i < states.size(); i++) {
  //   std::vector<float> res = db.GetFloatVectorFromBlobState(states[i]);
  //   ASSERT_EQ(res[0], expected_results[i]) << "Mismatch at index " << i;
  //   std::cout << res[0] << std::endl;
  // }
}
