#include "storage/vectordb/ivfflat_adapter.h"
#include <random>

namespace leanstore::storage::vector {

class NSWIndex {
public:
  std::vector<const BlobState *>& vertices_;
  size_t m_max_;
  std::unordered_map<size_t, std::vector<size_t>> edges_{};
  std::vector<size_t> in_vertices_{};

  NSWIndex(std::vector<const BlobState *> &vertices);
  std::vector<size_t> search_layer(VectorAdapter &db, const BlobState *input_vector, size_t limit, const std::vector<size_t> &entry_points);
  std::vector<size_t> search_layer(VectorAdapter &db, const std::vector<float> &base_vector, size_t limit, const std::vector<size_t> &entry_points);
  void insert(VectorAdapter &db, const BlobState* vec, size_t vertex_id, size_t ef_construction, size_t m);
  void connect(size_t vertex_a, size_t vertex_b);
  auto add_vertex(size_t vertex_id);
  auto default_entry_point() -> size_t { return in_vertices_[0]; }
};

std::vector<size_t> select_neighbors(VectorAdapter &db, const BlobState *input_vec, const std::vector<size_t> &vertex_ids, const std::vector<const BlobState *> &vertices, size_t m);
std::vector<size_t> select_neighbors(VectorAdapter &db, const BlobState *input_vec, const std::vector<size_t> &vertex_ids,
                                               const std::vector<const BlobState *> &vertices, size_t m);

class HNSWIndex {
public:  
  VectorAdapter db;
  std::vector<NSWIndex> layers_;
  std::vector<const BlobState *> vectors;
  std::vector<const BlobState *> vertices_;
  std::vector<std::array<uint8_t, BlobState::MAX_MALLOC_SIZE>> vectors_storage;

  size_t vertex_id;
  // number of edges to create each time a vertex is inserted
  size_t m_ = 5;
  // number of neighbors to search when inserting
  size_t ef_construction_ = 2;
  // number of neighbors to search when lookup
  size_t ef_search_ = 2;
  // maximum number of edges in all layers except layer 0
  size_t m_max_ = 5;
  // maximum number of edges in layer 0
  size_t m_max_0_ = 5;
    // random number generator
  std::mt19937 generator_;
  // level normalization factor
  double m_l_ = 1.0 / std::log(m_);


  HNSWIndex(VectorAdapter db);
  void build_index();
  //std::vector<size_t> scan_vector_entry(const BlobState *base_vector, size_t limit);
  std::vector<size_t> scan_vector_entry( const std::vector<float> &base_vector, size_t limit);
  void insert_vector_entry(const BlobState *vec);

  size_t add_vertex(const BlobState *vec);
};

}