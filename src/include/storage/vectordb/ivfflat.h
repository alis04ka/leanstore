#pragma once
#include "schema.h"
#include "storage/vectordb/vector_adapter.h"
#include "storage/vectordb/vector_index.h"
#include <iostream>

namespace leanstore::storage::vector {

template <typename T>
void print_vec(T vec) {
  for (auto elem : vec) {
    std::cout << elem << ", ";
  }
  std::cout << "\n";
}

struct Centroid {
  std::vector<const BlobState *> bucket;
  std::vector<float> sum_vec;
};

int calculate_num_centroids(int num_vec);
int calculate_num_probe_centroids(int num_centroids);
double get_search_time_ivfflat();

int find_bucket(VectorAdapter &adapter_centroids, BlobAdapter &blob_adapter, const BlobState *input_vec, std::vector<Centroid>& centroids);
std::vector<int> find_k_closest_centroids(VectorAdapter &adapter_centroids, BlobAdapter &blob_adapter, const std::vector<float> &input_vec, size_t k);
void initialize_centroids(VectorAdapter &adapter_centroids, VectorAdapter &adapter_main, BlobAdapter &blob_adapter, size_t num_centroids);
void update_one_centroid(VectorAdapter &adapter_centroids, BlobAdapter &blob_adapter, Centroid& centroid, const VectorRecord::Key &key, size_t vector_size);

class IVFFlatIndex : public VectorIndex {
public:
  IVFFlatIndex(VectorAdapter adapter_main, VectorAdapter adapter_centroids, BlobAdapter &blob_adapter, size_t num_centroids, size_t num_probe_centroids, size_t vector_size, size_t num_iter);

  void assign_vectors_to_centroids();
  void update_centroids();
  void build_index() override;
  std::vector<const BlobState *> find_n_closest_vectors(const std::vector<float> &input_vec, size_t n) override;

private:
  VectorAdapter adapter_main;
  VectorAdapter adapter_centroids;
  BlobAdapter &blob_adapter;
  size_t num_centroids;
  size_t num_probe_centroids;
  size_t vector_size;
  size_t num_iter;
  std::vector<Centroid> centroids;
  std::vector<const BlobState *> vectors;
  std::vector<std::array<uint8_t, BlobState::MAX_MALLOC_SIZE>> vectors_storage;
};

} // namespace leanstore::storage::vector
