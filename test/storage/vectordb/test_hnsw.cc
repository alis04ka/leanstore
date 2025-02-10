#include "storage/vectordb/hnsw.h"
#include "storage/vectordb/util.h"
//#include "storage/vectordb/vector_adapter.h"
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

std::vector<size_t> knn(const std::vector<float> &query,
                        const std::vector<std::vector<float>> &data,
                        size_t k)
{
    std::vector<std::pair<float, size_t>> distIdx;
    distIdx.reserve(data.size());

    for(size_t i = 0; i < data.size(); ++i) {
        float dist = distance_vec(query, data[i]);
        distIdx.emplace_back(dist, i);
    }

    std::sort(distIdx.begin(), distIdx.end(),
              [](auto &l, auto &r){ return l.first < r.first; });

    std::vector<size_t> neighbors;
    neighbors.reserve(k);
    for(size_t i = 0; i < k && i < distIdx.size(); ++i) {
        neighbors.push_back(distIdx[i].second);
    }
    return neighbors;
}


TEST(HNSW, BuildIndexNonTrivial) {
  auto leanstore = std::make_unique<leanstore::LeanStore>();
  VectorAdapter db = VectorAdapter::CreateVectorAdapter<VectorRecord>(*leanstore);
  BlobAdapter blob_adapter(*leanstore);

  leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
    leanstore->StartTransaction();

    int num_vec = 1000;
    size_t vector_size = 100;

    std::mt19937 rng(24);
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
    std::vector<size_t> res_knn = knn(search_vector, all_vecs, 10);

    std::cout << "\nResult hnsw: " << std::endl;
    for (auto neighbor_id : res_hnsw) {
      const leanstore::BlobState *state = index.vectors[neighbor_id];
      std::vector<float> vec = db.GetFloatVectorFromBlobState(state);
      for (int i = 0; i < 5; ++i) {
        std::cout << vec[i] << " ";
      }
      std::cout << std::endl;
    }

    std::cout << "Result knn: " << std::endl;
    for (auto neighbor_id : res_knn) {
      for (int i = 0; i < 5; ++i) {
        std::cout << all_vecs[neighbor_id][i] << " ";
      }
      std::cout << std::endl;
    }


    std::cout << "\nSearch time hnsw: " << get_search_time_hnsw() << " μs" << std::endl;

    leanstore->CommitTransaction();
  });

  leanstore->Shutdown();
}
