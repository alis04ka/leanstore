#include "storage/vectordb/hnsw.h"
#include "storage/vectordb/ivfflat_adapter.h"
#include "gtest/gtest.h"

using namespace leanstore::storage::vector;

TEST(NSW, BuildIndex) {
  auto leanstore = std::make_unique<leanstore::LeanStore>();
  VectorAdapter db = VectorAdapter(*leanstore);

  leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
    leanstore->StartTransaction();

    int num_vec = 1000;
    size_t vector_size = 1000;

    for (int id = 0; id < num_vec; ++id) {
      std::vector<float> vector(vector_size, static_cast<float>(id));
      std::span<u8> data(reinterpret_cast<u8 *>(vector.data()), vector.size() * sizeof(float));
      const leanstore::BlobState *state = db.RegisterBlob(data);
      db.InsertVectorRecordIntoMain({id}, *reinterpret_cast<const VectorRecord *>(state));
    }

    std::cout << "insereted: " << db.CountMain() << std::endl;

    HNSWIndex index(db);
    index.build_index();
    std::vector<float> search_vector(vector_size, 38.6);
    std::vector<size_t> res = index.scan_vector_entry(search_vector, 7);
    std::cout << res.size() << std::endl;
    for (size_t i = 0; i < res.size(); i++) {
      std::cout << res[i] << std::endl;
    }

    leanstore->CommitTransaction();
  });

  leanstore->Shutdown();
}
