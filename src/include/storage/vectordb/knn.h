#pragma once
#include "storage/vectordb/vector_adapter.h"
#include "storage/vectordb/vector_index.h"

#define TIME_INDEX

namespace leanstore::storage::vector {

class KnnIndex : public VectorIndex {
public:
  KnnIndex(VectorAdapter adapter_main, BlobAdapter &blob_adapter, size_t vector_size);

  void build_index() override;
  std::vector<const BlobState *> find_n_closest_vectors(const std::vector<float> &input_vec, size_t k) override;

private:
  VectorAdapter adapter_main;
  BlobAdapter &blob_adapter;

  std::vector<const BlobState *> vectors;
  std::vector<std::array<uint8_t, BlobState::MAX_MALLOC_SIZE>> vectors_storage;
  size_t vector_size;
};

} // namespace leanstore::storage::vector
