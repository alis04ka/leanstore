#include "schema.h"
#include "storage/vectordb/ivfflat_adapter.h"
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
};

float distance_vec(std::span<const float> span1, std::span<const float> span2);
float distance_blob(VectorAdapter &db, const BlobState *blob1, const BlobState *blob2);
float distance_vec_blob(VectorAdapter &db, std::span<const float> input_span, const BlobState *blob_state);

int calculate_num_centroids(int num_vec);
int calculate_num_probe_centroids(int num_centroids);

int find_bucket(VectorAdapter &db, const BlobState *input_vec);
std::vector<int> find_k_closest_centroids(VectorAdapter &db, const std::vector<float> &input_vec, size_t k);
void initialize_centroids(VectorAdapter &db, size_t num_centroids);
void update_one_centroid(VectorAdapter &db, std::vector<const BlobState *> bucket, const VectorRecord::Key &key, size_t vector_size);

class IVFFlatNoCopyIndex {
public:
  IVFFlatNoCopyIndex(VectorAdapter db, size_t num_centroids, size_t num_probe_centroids, size_t vector_size);

  void assign_vectors_to_centroids();
  void update_centroids();
  void BuildIndex();
  std::vector<const BlobState *> find_n_closest_vectors(const std::vector<float> &input_vec, size_t n);

private:
  VectorAdapter db;
  size_t num_centroids;
  size_t num_probe_centroids;
  size_t vector_size;
  std::vector<Centroid> centroids;
  std::vector<const BlobState *> vectors;
  std::vector<std::array<uint8_t, BlobState::MAX_MALLOC_SIZE>> vectors_storage;
};

} // namespace leanstore::storage::vector
