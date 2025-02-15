#include "storage/vectordb/knn.h"
#include "storage/vectordb/util.h"

namespace leanstore::storage::vector {


KnnIndex::KnnIndex(VectorAdapter adapter_main, BlobAdapter &blob_adapter, size_t vector_size)
    : adapter_main(adapter_main), blob_adapter(blob_adapter), vector_size(vector_size) {
}

void KnnIndex::build_index() {
  vectors_storage.resize(adapter_main.Count());
  vectors.resize(adapter_main.Count());
  int idx = 0;
  adapter_main.Scan({0},
    [&](const VectorRecord::Key &key, const VectorRecord &record) {
      uint8_t *raw_data = (uint8_t *)&record.blobState;
      std::memcpy(vectors_storage[idx].data(), raw_data, record.blobState.MallocSize());
      vectors[idx] = reinterpret_cast<const BlobState *>(vectors_storage[idx].data());
      idx++;
      return true;
  });
  std::cout << "count: " << adapter_main.Count() << std::endl;
}

std::vector<const BlobState *> KnnIndex::find_n_closest_vectors(const std::vector<float> &input_vec, size_t k) {
  std::vector<std::pair<float, const BlobState *>> distIdx;
  distIdx.reserve(vectors.size());

  for (size_t i = 0; i < vectors.size(); ++i) {
    float dist = distance_vec_blob(blob_adapter, input_vec, vectors[i]);
    distIdx.emplace_back(dist, vectors[i]);
  }

  std::sort(distIdx.begin(), distIdx.end(), [](auto &l, auto &r) { return l.first < r.first; });

  std::vector<const BlobState *> neighbors;
  neighbors.reserve(k);
  for (size_t i = 0; i < k && i < distIdx.size(); ++i) {
    neighbors.push_back(distIdx[i].second);
  }
  return neighbors;
}

} // namespace leanstore::storage::vector
