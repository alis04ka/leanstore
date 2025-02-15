#include "storage/vectordb/knn.h"
#include "storage/vectordb/vector_adapter.h"
#include "gtest/gtest.h"

using namespace leanstore::storage::vector;

TEST(Knn, BuildIndexAndLookup) {
  auto leanstore = std::make_unique<leanstore::LeanStore>();
  VectorAdapter adapter_main = VectorAdapter::CreateVectorAdapter<VectorRecord>(*leanstore);
  BlobAdapter blob_adapter(*leanstore);

  leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
    leanstore->StartTransaction();

    int num_vec = 100000;
    size_t vector_size = 10000;

    for (int i = 0; i < num_vec; ++i) {
      std::vector<float> vector(vector_size, static_cast<float>(i));
      std::span<u8> data(reinterpret_cast<u8 *>(vector.data()), vector.size() * sizeof(float));
      const leanstore::BlobState *state = blob_adapter.RegisterBlob(data);
      adapter_main.InsertVectorRecord({i}, *reinterpret_cast<const VectorRecord *>(state));
    }

    assert((int)adapter_main.Count() == num_vec);

    KnnIndex index(adapter_main, blob_adapter, vector_size);
    index.build_index();

    std::vector<float> input_vec(vector_size, 30.6);
    std::vector<const leanstore::BlobState *> states = index.find_n_closest_vectors(input_vec, 8);

    std::vector<float> expected_results = {31.0, 30.0, 32.0, 29.0, 33.0, 28.0, 34.0, 27.0};
    ASSERT_EQ(states.size(), expected_results.size());

    for (size_t i = 0; i < states.size(); i++) {
      std::vector<float> res = blob_adapter.GetFloatVectorFromBlobState(states[i]);
      ASSERT_EQ(res[0], expected_results[i]) << "Mismatch at index " << i;
      std::cout << res[0] << std::endl;
    }


    leanstore->CommitTransaction();
  });

  leanstore->Shutdown();
}
