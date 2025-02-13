#include "storage/vectordb/hnsw.h"
#include "storage/vectordb/util.h"
#include "gtest/gtest.h"

using namespace leanstore::storage::vector;

// TEST(HNSW, BuildIndex) {
//   auto leanstore = std::make_unique<leanstore::LeanStore>();
//   VectorAdapter db = VectorAdapter(*leanstore);

//   leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
//     leanstore->StartTransaction();

//     int num_vec = 10000;
//     size_t vector_size = 1000;

//     for (int id = 0; id < num_vec; ++id) {
//       std::vector<float> vector(vector_size, static_cast<float>(id));
//       std::span<u8> data(reinterpret_cast<u8 *>(vector.data()), vector.size() * sizeof(float));
//       const leanstore::BlobState *state = db.RegisterBlob(data);
//       db.InsertVectorRecordIntoMain({id}, *reinterpret_cast<const VectorRecord *>(state));
//     }

//     std::cout << "insereted: " << db.CountMain() << std::endl;

//     HNSWIndex index(db);
//     index.build_index();
//     std::vector<float> search_vector(vector_size, 1333.6);
//     std::vector<size_t> res = index.scan_vector_entry(search_vector, 7);
//     ASSERT_EQ(res.size(), 7);
//     for (size_t i = 0; i < res.size(); i++) {
//       std::cout << res[i] << std::endl;
//     }

//     std::cout << "Search time: " << get_search_time_hnsw() << " μs" << std::endl;

//     leanstore->CommitTransaction();
//   });

//   leanstore->Shutdown();
// }




TEST(HNSW, BuildIndexNonTrivial) {
  auto leanstore = std::make_unique<leanstore::LeanStore>();
  VectorAdapter db = VectorAdapter::CreateVectorAdapter<VectorRecord>(*leanstore);
  BlobAdapter blob_adapter(*leanstore);

  leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
    leanstore->StartTransaction();

    int num_vec = 400;
    size_t vector_size = 2000;

    std::mt19937 rng(std::random_device{}());
    std::uniform_real_distribution<float> dist(0.0f, 10.0f);
    std::vector<std::vector<float>> all_vecs;
    all_vecs.reserve(num_vec);

    for (int id = 0; id < num_vec; ++id) {
      std::vector<float> vec(vector_size);
      for (size_t d = 0; d < vector_size; ++d) {
        vec[d] = dist(rng);
      }
      all_vecs.push_back(vec);
      std::span<u8> data(reinterpret_cast<u8 *>(vec.data()), vec.size() * sizeof(float));
      const leanstore::BlobState *state = blob_adapter.RegisterBlob(data);
      db.InsertVectorRecord({id}, *reinterpret_cast<const VectorRecord *>(state));
    }

    std::cout << "Inserted: " << db.Count() << std::endl;

    HNSWIndex index(db, blob_adapter, 200, 100, 10);
    index.build_index();

    std::vector<float> search_vector(vector_size);
    for (size_t d = 0; d < vector_size; ++d) {
      search_vector[d] = dist(rng);
    }

    std::cout << "\nQuery vector: ";
    for (int i = 0; i < 5; ++i) {
      std::cout << search_vector[i] << " ";
    }
    std::cout << std::endl;

    std::vector<size_t> res_hnsw = index.scan_vector_entry(search_vector, 10);
    // std::vector<size_t> res_knn = knn(search_vector, all_vecs, 10);

    // std::cout << "\nResult hnsw: " << std::endl;
    // for (auto neighbor_id : res_hnsw) {
    //   const leanstore::BlobState *state = index.vectors[neighbor_id];
    //   std::vector<float> vec = blob_adapter.GetFloatVectorFromBlobState(state);
    //   for (int i = 0; i < 5; ++i) {
    //     std::cout << vec[i] << " ";
    //   }
    //   std::cout << std::endl;
    // }

    // std::cout << "Result knn: " << std::endl;
    // for (auto neighbor_id : res_knn) {
    //   for (int i = 0; i < 5; ++i) {
    //     std::cout << all_vecs[neighbor_id][i] << " ";
    //   }
    //   std::cout << std::endl;
    // }

    // std::cout << "\nSearch time hnsw: " << get_search_time_hnsw() << " μs" << std::endl;
    // float error = knn_hnsw_error(search_vector, res_knn, res_hnsw, all_vecs);

    leanstore->CommitTransaction();
  });

  leanstore->Shutdown();
}
