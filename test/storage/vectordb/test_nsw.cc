#include "storage/vectordb/nsw.h"
#include "storage/vectordb/ivfflat_adapter.h"
#include "gtest/gtest.h"

using namespace leanstore::storage::vector;

TEST(NSW, BuildIndex) {
  auto leanstore = std::make_unique<leanstore::LeanStore>();
  VectorAdapter db = VectorAdapter(*leanstore);

  leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
    leanstore->StartTransaction();

    int num_vec = 10;
    size_t vector_size = 10;

    for (int id = 0; id < num_vec; ++id) {
      std::vector<float> vector(vector_size, static_cast<float>(id));
      std::span<u8> data(reinterpret_cast<u8 *>(vector.data()), vector.size() * sizeof(float));
      const leanstore::BlobState *state = db.RegisterBlob(data);
      db.InsertVectorRecordIntoMain({id}, *reinterpret_cast<const VectorRecord *>(state));
    }

    std::cout << "insereted: " << db.CountMain() << std::endl;

    NSWIndex index(db);
    index.build_index();

    std::vector<std::vector<int>> expected_edges = {
      {1, 2, 3, 4},
      {0, 2, 3, 4},
      {0, 1, 3, 4},
      {5, 1, 2, 4},
      {5, 6, 2, 3},
      {4, 6, 7, 3},
      {4, 5, 7, 8},
      {9, 5, 6, 8},
      {9, 5, 6, 7},
      {8, 5, 6, 7}};

    ASSERT_EQ(index.edges.size(), expected_edges.size());

    for (size_t i = 0; i < index.edges.size(); i++) {
      ASSERT_EQ(index.edges[i].size(), expected_edges[i].size());
      for (size_t j = 0; j < index.edges[i].size(); j++) {
        ASSERT_EQ(index.edges[i][j], expected_edges[i][j]) << "Mismatch at edge (" << i << ", " << j << ")";
      }
    }

    leanstore->CommitTransaction();
  });

  leanstore->Shutdown();
}

TEST(NSW, BuildIndexAndSearchOneEntryPoint) {
  auto leanstore = std::make_unique<leanstore::LeanStore>();
  VectorAdapter db = VectorAdapter(*leanstore);

  leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
    leanstore->StartTransaction();

    int num_vec = 500;
    size_t vector_size = 1000;

    for (int id = 0; id < num_vec; ++id) {
      std::vector<float> vector(vector_size, static_cast<float>(id));
      std::span<u8> data(reinterpret_cast<u8 *>(vector.data()), vector.size() * sizeof(float));
      const leanstore::BlobState *state = db.RegisterBlob(data);
      db.InsertVectorRecordIntoMain({id}, *reinterpret_cast<const VectorRecord *>(state));
    }

    std::cout << "Inserted: " << db.CountMain() << " vectors" << std::endl;

    NSWIndex index(db);
    auto start_build = std::chrono::high_resolution_clock::now();
    index.build_index();
    auto end_build = std::chrono::high_resolution_clock::now();
    auto build_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_build - start_build);
    std::cout << "Index build time: " << build_duration.count() << " ms" << std::endl;

    std::vector<float> query(vector_size, 34.9);
    size_t k = 5;

    auto start_search = std::chrono::high_resolution_clock::now();
    auto results = index.search(query, k);
    auto end_search = std::chrono::high_resolution_clock::now();
    auto search_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_search - start_search);
    std::cout << "Search time: " << search_duration.count() << " µs" << std::endl;

    std::vector<int> expected = {35, 34, 36, 33, 37};
    ASSERT_EQ(results.size(), expected.size()) << "Result size mismatch!";

    for (size_t i = 0; i < expected.size(); ++i) {
      ASSERT_EQ(results[i], expected[i]) << "Mismatch at position " << i << ": Expected "
                                         << expected[i] << " but got " << results[i];
    }

    leanstore->CommitTransaction();
  });

  leanstore->Shutdown();
}

TEST(NSW, BuildIndexAndSearchOneEntryPoint_Optimized) {
  auto leanstore = std::make_unique<leanstore::LeanStore>();
  VectorAdapter db = VectorAdapter(*leanstore);

  leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
    leanstore->StartTransaction();

    int num_vec = 500;
    size_t vector_size = 1000;

    for (int id = 0; id < num_vec; ++id) {
      std::vector<float> vector(vector_size, static_cast<float>(id));
      std::span<u8> data(reinterpret_cast<u8 *>(vector.data()), vector.size() * sizeof(float));
      const leanstore::BlobState *state = db.RegisterBlob(data);
      db.InsertVectorRecordIntoMain({id}, *reinterpret_cast<const VectorRecord *>(state));
    }

    std::cout << "Inserted: " << db.CountMain() << " vectors" << std::endl;

    NSWIndex index(db);
    auto start_build = std::chrono::high_resolution_clock::now();
    index.build_index();
    auto end_build = std::chrono::high_resolution_clock::now();
    auto build_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_build - start_build);
    std::cout << "Index build time: " << build_duration.count() << " ms" << std::endl;

    std::vector<float> query(vector_size, 34.9);
    size_t k = 5;

    auto start_search = std::chrono::high_resolution_clock::now();
    auto results = index.search_optimized(query, k);
    auto end_search = std::chrono::high_resolution_clock::now();
    auto search_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_search - start_search);
    std::cout << "Optimized search time: " << search_duration.count() << " ms" << std::endl;

    std::vector<int> expected = {35, 34, 36, 33, 37};
    ASSERT_EQ(results.size(), expected.size()) << "Result size mismatch!";

    for (size_t i = 0; i < expected.size(); ++i) {
      ASSERT_EQ(results[i], expected[i]) << "Mismatch at position " << i << ": Expected "
                                         << expected[i] << " but got " << results[i];
    }

    std::cout << std::endl;
    std::cout << "Multile entries----------------" << std::endl;
    std::vector<size_t> enrties = {0, 190, 35, 250, 300, 172};
    auto start_search2 = std::chrono::high_resolution_clock::now();
    auto results2 = index.search_multiple_entries(query, k, enrties);
    auto end_search2 = std::chrono::high_resolution_clock::now();
    auto search_duration2 = std::chrono::duration_cast<std::chrono::milliseconds>(end_search2 - start_search2);
    std::cout << "Multile entries search time: " << search_duration2.count() << " ms" << std::endl;

    ASSERT_EQ(results2.size(), expected.size()) << "Result size mismatch!";

    for (size_t i = 0; i < results2.size(); ++i) {
      std::cout << results2[i] << ",";
       ASSERT_EQ(results[i], expected[i]) << "Mismatch at position " << i << ": Expected "
                                         << expected[i] << " but got " << results[i];
    }

    leanstore->CommitTransaction();
  });

  leanstore->Shutdown();
}

