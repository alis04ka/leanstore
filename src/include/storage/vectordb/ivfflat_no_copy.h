#include <iostream>
#include "benchmark/adapters/leanstore_adapter.h"
#include "schema.h"
#include "storage/vectordb/ivfflat_adapter.h"

namespace leanstore::storage::vector {

template <typename T>
void print_vec(T vec) {
  for (auto elem : vec) { std::cout << elem << ", "; }
  std::cout << "\n";
}

struct Centroid {
  std::vector<const BlobState*> bucket;
};

float distance_vec(std::span<const float> span1, std::span<const float> span2);
float distance_blob(VectorAdapter &db, const BlobState *blob1, const BlobState *blob2);
int find_bucket(VectorAdapter &db, const BlobState *input_vec);
void initialize_centroids(VectorAdapter &db, size_t num_centroids);
void update_one_centroid(VectorAdapter &db, std::vector<const BlobState *> bucket, const VectorRecord::Key &key);

class IVFFlatNoCopyIndex {
 public:
  IVFFlatNoCopyIndex(VectorAdapter db, size_t num_centroids, size_t num_probe_centroids, size_t vector_size);

  void load_vectors();
  void assign_vectors_to_centroids();
  void update_centroids();
  void BuildIndex();
  void InsertVectorEntry(VectorRecord &record);
  std::vector<std::pair<float, const BlobState*>> lookup_k_nearest(BlobState* query_blob, size_t k);

 private:
  VectorAdapter db;
  size_t num_centroids;
  size_t num_probe_centroids;
  size_t vector_size;
  std::vector<Centroid> centroids;
  std::vector<const BlobState *> vectors;
  std::vector<std::array<uint8_t, BlobState::MAX_MALLOC_SIZE>> vectors_storage;
  
};

}  // namespace leanstore::storage::vector
