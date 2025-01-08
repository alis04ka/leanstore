#include "storage/vectordb/ivfflat_adapter.h"

namespace leanstore::storage::vector {

static constexpr int DEFAULT_ENTRY_POINT_ID = 0;

class NSWIndex {

public:
  VectorAdapter db;
  float probability;
  size_t max_number_edges = 4;
  std::vector<std::vector<size_t>> edges;
  std::vector<const BlobState *> vectors;
  std::vector<std::array<uint8_t, BlobState::MAX_MALLOC_SIZE>> vectors_storage;

  NSWIndex(VectorAdapter db);
  void build_index();
  void insert_or_replace_edge(size_t from, size_t to, float dist);
  void insert_edges_one_vec(size_t vec_id);
  void insert_edges();
  std::vector<size_t> search(const std::vector<float> &query_vec, size_t k);
  std::vector<size_t> search_optimized(const std::vector<float> &query_vec, size_t k);
  std::vector<size_t> search_multiple_entries(const std::vector<float> &query_vec, size_t k, const std::vector<size_t>& entry_points);

private:
  // move here
};


} // namespace leanstore::storage::vector