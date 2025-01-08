#include "storage/vectordb/ivfflat_adapter.h"

namespace leanstore::storage::vector {

class NSWIndex {
public:  
  NSWIndex(VectorAdapter& db, std::vector<const BlobState *>& vectors, float probability, size_t max_number_edges);
  std::vector<size_t> build_index(std::vector<size_t> available_indices);
  std::vector<size_t> choose_indices(std::vector<size_t> available_indices);
  void insert_edges_one_vec(size_t vec_id);
  void insert_or_replace_edge(size_t from, size_t to, float dist);
  std::vector<size_t> search(const std::vector<float> &query_vec, size_t k, size_t entry_point_id);
  
  VectorAdapter& db;
  std::vector<const BlobState *>& vectors;
  float probability;
  size_t max_number_edges;
  std::vector<size_t> indices;
  std::unordered_map<size_t, std::vector<size_t>> edges;
};

class HNSWIndex {
public:  
  VectorAdapter db;
  size_t num_layers = 3;
  std::vector<float> prob_per_layer = {1.0, 0.25, 0.25};
  std::vector<int> num_edges_per_layer = {6, 5, 4};
  std::vector<NSWIndex> layers;
  std::vector<const BlobState *> vectors;
  std::vector<std::array<uint8_t, BlobState::MAX_MALLOC_SIZE>> vectors_storage;

  HNSWIndex(VectorAdapter db);
  void build_index();
  std::vector<size_t> search(const std::vector<float> &query_vec, size_t k);
};

}