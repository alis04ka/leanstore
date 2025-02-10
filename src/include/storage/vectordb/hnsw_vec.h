#pragma once
#include <random>
#include <span>
#include <unordered_map>

#define TIME_INDEX

namespace leanstore::storage::vector {

double get_search_time_hnsw_vec();

class NSWIndex {
private:
  std::vector<std::span<float>> &vertices_;
  size_t m_max_ = 5;
  std::unordered_map<size_t, std::vector<size_t>> edges_{};
  std::vector<size_t> in_vertices_{};

public:
  NSWIndex(std::vector<std::span<float>> &vertices);
  std::vector<size_t> search_layer_vec(const std::span<float> base_vector, size_t limit, const std::vector<size_t> &entry_points);
  void insert_vec(const std::span<float> vec, size_t vertex_id, size_t ef_construction, size_t m);

  void connect_vec(size_t vertex_a, size_t vertex_b);
  auto add_vertex_vec(size_t vertex_id);
  auto default_entry_point_vec() -> size_t { return in_vertices_[0]; }

  friend class HNSWIndex;
};

class HNSWIndex {
public:
  std::vector<NSWIndex> layers_;
  std::vector<std::vector<float>> vectors;
  std::vector<std::span<float>> vertices_;

  // number of edges to create each time a vertex is inserted
  size_t m_ = 5;
  // number of neighbors to search when inserting
  size_t ef_construction_ = 4;
  // number of neighbors to search when lookup
  size_t ef_search_ = 5;
  // maximum number of edges in all layers except layer 0
  size_t m_max_ = 5;
  // maximum number of edges in layer 0
  size_t m_max_0_ = 5;
  // random number generator
  std::mt19937 generator_;
  // level normalization factor
  double m_l_ = 1.0 / std::log(m_);

  HNSWIndex(std::vector<std::vector<float>> &&vectors);
  void build_index_vec();
  std::vector<size_t> scan_vector_entry_vec(const std::span<float> base_vector, size_t limit);
  void insert_vector_entry_vec(const std::span<float> vector);

  size_t add_vertex_vec(std::span<float> vector);
};

} // namespace leanstore::storage::vector