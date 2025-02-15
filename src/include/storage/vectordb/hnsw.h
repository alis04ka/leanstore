#pragma once
#include "storage/vectordb/vector_adapter.h"
#include "storage/vectordb/vector_index.h"
#include <random>

#define TIME_INDEX

namespace leanstore::storage::vector {

double get_search_time_hnsw();

class NSWIndex {
private:
  std::vector<const BlobState *> &vertices_;
  std::unordered_map<size_t, std::vector<size_t>> edges_{};
  std::vector<size_t> in_vertices_{};

  template <typename VectorType, typename DistanceFunc>
  std::vector<size_t> search_layer_template(BlobAdapter &adapter, const VectorType &base_vector, size_t limit, const std::vector<size_t> &entry_points, DistanceFunc distance_func);

public:
  NSWIndex(std::vector<const BlobState *> &vertices);
  std::vector<size_t> search_layer(BlobAdapter &adapter, const BlobState *input_vector, size_t limit, const std::vector<size_t> &entry_points);
  std::vector<size_t> search_layer(BlobAdapter &adapter, const std::vector<float> &base_vector, size_t limit, const std::vector<size_t> &entry_points);
  void insert(VectorAdapter &db, const BlobState *vec, size_t vertex_id, size_t ef_construction, size_t m);

  void connect(size_t vertex_a, size_t vertex_b);
  auto add_vertex(size_t vertex_id);
  auto default_entry_point() -> size_t { return in_vertices_[0]; }

  friend class HNSWIndex;
};

class HNSWIndex : public VectorIndex {
public:
  VectorAdapter db;
  BlobAdapter blob_adapter;
  std::vector<NSWIndex> layers_;
  std::vector<const BlobState *> vectors;
  std::vector<const BlobState *> vertices_;
  std::vector<std::array<uint8_t, BlobState::MAX_MALLOC_SIZE>> vectors_storage;

  size_t vector_size;

  // number of neighbors to search when inserting
  size_t ef_construction_;
  // number of neighbors to search when lookup
  size_t ef_search_;
  // maximum number of edges in all layers
  size_t m_max_;
  // random number generator
  std::mt19937 generator_;
  // level normalization factor
  double m_l_;

  HNSWIndex(VectorAdapter db, BlobAdapter adapter, size_t ef_construction, size_t ef_search, size_t m_max);
  void build_index() override;
  std::vector<size_t> scan_vector_entry(const std::vector<float> &base_vector, size_t limit);
  void insert_vector_entry(const BlobState *vec);
  std::vector<const BlobState *> find_n_closest_vectors(const std::vector<float> &input_vec, size_t n) override;

  size_t add_vertex(const BlobState *vec);
};

} // namespace leanstore::storage::vector
