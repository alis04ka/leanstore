#include "storage/vectordb/ivfflat_adapter.h"
#include "storage/vectordb/ivfflat.h"
#include "storage/vectordb/util.h"
#include "gtest/gtest.h"

using namespace leanstore::storage::vector;

// TEST(IVFFlat, DistanceBlobMainAndMain) {
//   auto leanstore = std::make_unique<leanstore::LeanStore>();
//   VectorAdapter db = VectorAdapter(*leanstore);

//   leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
//     leanstore->StartTransaction();

//     std::vector<float> vector1(1000, 1.0);
//     std::span<u8> data1(reinterpret_cast<u8 *>(vector1.data()), vector1.size() * sizeof(float));
//     const leanstore::BlobState *state1 = db.RegisterBlob(data1);
//     db.InsertVectorRecordIntoMain({1}, *reinterpret_cast<const VectorRecord *>(state1));

//     std::vector<float> vector2(1000, 2.0);
//     std::span<u8> data2(reinterpret_cast<u8 *>(vector2.data()), vector2.size() * sizeof(float));
//     const leanstore::BlobState *state2 = db.RegisterBlob(data2);
//     db.InsertVectorRecordIntoMain({2}, *reinterpret_cast<const VectorRecord *>(state2));

//     const leanstore::BlobState *loaded1;
//     const leanstore::BlobState *loaded2;

//     db.LookUpMain({1}, [&](const VectorRecord &record) { loaded1 = &record.blobState; });
//     db.LookUpMain({2}, [&](const VectorRecord &record) { loaded2 = &record.blobState; });

//     loaded1->print_float();
//     loaded2->print_float();

//     float distance = distance_blob(db, loaded1, loaded2);
//     std::cout << "Distance between vector1 and vector2: " << distance << std::endl;
//     EXPECT_NEAR(distance, std::sqrt(1000.0), 1e-3);

//     leanstore->CommitTransaction();
//   });

//   leanstore->Shutdown();
// }

// TEST(IVFFlat, DistanceBetweenMainAndCentroids) {
//   auto leanstore = std::make_unique<leanstore::LeanStore>();
//   VectorAdapter db = VectorAdapter(*leanstore);

//   leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
//     leanstore->StartTransaction();

//     std::vector<float> vector_main(1000, 1.0);
//     std::span<u8> data_main(reinterpret_cast<u8 *>(vector_main.data()), vector_main.size() * sizeof(float));
//     const leanstore::BlobState *state_main = db.RegisterBlob(data_main);
//     db.InsertVectorRecordIntoMain({1}, *reinterpret_cast<const VectorRecord *>(state_main));

//     std::vector<float> vector_centroid(1000, 2.0);
//     std::span<u8> data_centroid(
//       reinterpret_cast<u8 *>(vector_centroid.data()), vector_centroid.size() * sizeof(float));
//     const leanstore::BlobState *state_centroid = db.RegisterBlob(data_centroid);
//     db.InsertVectorRecordIntoCentroids({1}, *reinterpret_cast<const VectorRecord *>(state_centroid));

//     const leanstore::BlobState *loaded_main;
//     const leanstore::BlobState *loaded_centroid;

//     db.LookUpMain({1}, [&](const VectorRecord &record) { loaded_main = &record.blobState; });
//     db.LookUpCentroids({1}, [&](const VectorRecord &record) { loaded_centroid = &record.blobState; });

//     loaded_main->print_float();
//     loaded_centroid->print_float();

//     float distance = distance_blob(db, loaded_main, loaded_centroid);
//     std::cout << "Distance between main and centroid vector: " << distance << std::endl;
//     EXPECT_NEAR(distance, std::sqrt(1000.0), 1e-3);

//     leanstore->CommitTransaction();
//   });

//   leanstore->Shutdown();
// }

// TEST(IVFFlat, FindBucketTest1) {
//   auto leanstore = std::make_unique<leanstore::LeanStore>();
//   VectorAdapter db = VectorAdapter(*leanstore);

//   leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
//     leanstore->StartTransaction();
//     std::vector<float> vec_main(1000, 1.5);
//     std::span<u8> data_main(reinterpret_cast<u8 *>(vec_main.data()), vec_main.size() * sizeof(float));
//     const leanstore::BlobState *state_main = db.RegisterBlob(data_main);
//     db.InsertVectorRecordIntoMain({2}, *reinterpret_cast<const VectorRecord *>(state_main));

//     std::vector<float> vec_centroid1(1000, 1.0);
//     std::span<u8> data_centroid1(
//       reinterpret_cast<u8 *>(vec_centroid1.data()), vec_centroid1.size() * sizeof(float));
//     const leanstore::BlobState *state_centroid1 = db.RegisterBlob(data_centroid1);
//     db.InsertVectorRecordIntoCentroids({0}, *reinterpret_cast<const VectorRecord *>(state_centroid1));

//     std::vector<float> vec_centroid2(1000, 3.0);
//     std::span<u8> data_centroid2(
//       reinterpret_cast<u8 *>(vec_centroid2.data()), vec_centroid2.size() * sizeof(float));
//     const leanstore::BlobState *state_centroid2 = db.RegisterBlob(data_centroid2);
//     db.InsertVectorRecordIntoCentroids({1}, *reinterpret_cast<const VectorRecord *>(state_centroid2));

//     const leanstore::BlobState *loaded_main;
//     db.LookUpMain({2}, [&](const VectorRecord &record) { loaded_main = &record.blobState; });

//     int bucket_index = find_bucket(db, loaded_main);
//     std::cout << "Chosen bucket index: " << bucket_index << std::endl;

//     EXPECT_EQ(bucket_index, 0);

//     leanstore->CommitTransaction();
//   });

//   leanstore->Shutdown();
// }

// TEST(IVFFlat, FindKClosestTest) {
//   auto leanstore = std::make_unique<leanstore::LeanStore>();
//   VectorAdapter db = VectorAdapter(*leanstore);

//   leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
//     leanstore->StartTransaction();
//     std::vector<float> vec_main(1000, 1.5);

//     std::vector<std::vector<float>> centroids = {
//       std::vector<float>(1000, 1.0),
//       std::vector<float>(1000, 3.2),
//       std::vector<float>(1000, 2.1),
//       std::vector<float>(1000, 4.0),
//       std::vector<float>(1000, 0.5),
//       std::vector<float>(1000, 1.7),
//       std::vector<float>(1000, 2.5)};

//     for (size_t i = 0; i < centroids.size(); ++i) {
//       std::span<u8> data_centroid(reinterpret_cast<u8 *>(centroids[i].data()), centroids[i].size() * sizeof(float));
//       const leanstore::BlobState *state_centroid = db.RegisterBlob(data_centroid);
//       db.InsertVectorRecordIntoCentroids({(int)i}, *reinterpret_cast<const VectorRecord *>(state_centroid));
//     }

//     int k = 3;
//     std::vector<int> closest_indices = find_k_closest_centroids(db, vec_main, k);
//     EXPECT_EQ(closest_indices.size(), k);

//     std::vector<int> expected_indices = {5, 0, 2};
//     for (int j = 0; j < k; ++j) {
//       EXPECT_EQ(closest_indices[j], expected_indices[j]);
//     }

//     leanstore->CommitTransaction();
//   });

//   leanstore->Shutdown();
// }

// TEST(IVFFlat, FindBucketTestMultiple) {
//   auto leanstore = std::make_unique<leanstore::LeanStore>();
//   VectorAdapter db = VectorAdapter(*leanstore);

//   leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
//     leanstore->StartTransaction();

//     std::vector<const leanstore::BlobState *> loaded_vecs;

//     for (int i = 0; i <= 10; ++i) {
//       std::vector<float> vec(1000, static_cast<float>(i));
//       std::span<u8> data(reinterpret_cast<u8 *>(vec.data()), vec.size() * sizeof(float));
//       const leanstore::BlobState *state = db.RegisterBlob(data);
//       db.InsertVectorRecordIntoMain({i}, *reinterpret_cast<const VectorRecord *>(state));

//       const leanstore::BlobState *loaded;
//       db.LookUpMain({i}, [&](const VectorRecord &record) { loaded = &record.blobState; });
//       loaded_vecs.push_back(loaded);
//     }

//     std::vector<float> vec_centroid1(1000, 2.0);
//     std::span<u8> data_centroid1(
//       reinterpret_cast<u8 *>(vec_centroid1.data()), vec_centroid1.size() * sizeof(float));
//     const leanstore::BlobState *state_centroid1 = db.RegisterBlob(data_centroid1);
//     db.InsertVectorRecordIntoCentroids({0}, *reinterpret_cast<const VectorRecord *>(state_centroid1));

//     std::vector<float> vec_centroid2(1000, 4.0);
//     std::span<u8> data_centroid2(
//       reinterpret_cast<u8 *>(vec_centroid2.data()), vec_centroid2.size() * sizeof(float));
//     const leanstore::BlobState *state_centroid2 = db.RegisterBlob(data_centroid2);
//     db.InsertVectorRecordIntoCentroids({1}, *reinterpret_cast<const VectorRecord *>(state_centroid2));

//     std::vector<float> vec_centroid3(1000, 8.0);
//     std::span<u8> data_centroid3(
//       reinterpret_cast<u8 *>(vec_centroid3.data()), vec_centroid3.size() * sizeof(float));
//     const leanstore::BlobState *state_centroid3 = db.RegisterBlob(data_centroid3);
//     db.InsertVectorRecordIntoCentroids({2}, *reinterpret_cast<const VectorRecord *>(state_centroid3));

//     std::vector<int> expected_buckets = {0, 0, 0, 0, 1, 1, 1, 2, 2, 2, 2};

//     for (int i = 0; i <= 10; ++i) {
//       int bucket_index = find_bucket(db, loaded_vecs[i]);
//       std::cout << "Vector " << i << " chose bucket index: " << bucket_index << std::endl;
//       EXPECT_EQ(bucket_index, expected_buckets[i]);
//     }

//     leanstore->CommitTransaction();
//   });

//   leanstore->Shutdown();
// }

// TEST(IVFFlat, FindBucketTestLarge) {
//   auto leanstore = std::make_unique<leanstore::LeanStore>();
//   VectorAdapter db = VectorAdapter(*leanstore);

//   leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
//     leanstore->StartTransaction();

//     std::vector<const leanstore::BlobState *> loaded_vecs;
//     int num_vec = 1000;

//     for (int i = 0; i < num_vec; ++i) {
//       std::vector<float> vec(1000, static_cast<float>(i));
//       std::span<u8> data(reinterpret_cast<u8 *>(vec.data()), vec.size() * sizeof(float));
//       const leanstore::BlobState *state = db.RegisterBlob(data);
//       db.InsertVectorRecordIntoMain({i}, *reinterpret_cast<const VectorRecord *>(state));
//     }

//     for (int i = 0; i < num_vec; ++i) {
//       const leanstore::BlobState *loaded;
//       db.LookUpMain({i}, [&](const VectorRecord &record) { loaded = &record.blobState; });
//       loaded_vecs.push_back(loaded);
//     }

//     for (int i = 0; i < num_vec / 5; ++i) {
//       std::vector<float> vec_centroid(1000, static_cast<float>(i * 5));
//       std::span<u8> data_centroid(reinterpret_cast<u8 *>(vec_centroid.data()), vec_centroid.size() * sizeof(float));
//       const leanstore::BlobState *state_centroid = db.RegisterBlob(data_centroid);
//       db.InsertVectorRecordIntoCentroids({i}, *reinterpret_cast<const VectorRecord *>(state_centroid));
//     }

//     for (int i = 0; i < num_vec; ++i) {
//       int bucket_index = find_bucket(db, loaded_vecs[i]);
//       // std::cout << "Vector " << i << " chosen bucket index: " << bucket_index << std::endl;
//     }

//     leanstore->CommitTransaction();
//   });

//   leanstore->Shutdown();
// }

// TEST(IVFFlat, InitializeCentroidsTest1) {
//   auto leanstore = std::make_unique<leanstore::LeanStore>();
//   VectorAdapter db = VectorAdapter(*leanstore);

//   leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
//     leanstore->StartTransaction();

//     std::vector<float> vec_main1(1000, 1.5);
//     std::span<u8> data_main1(reinterpret_cast<u8 *>(vec_main1.data()), vec_main1.size() * sizeof(float));
//     const leanstore::BlobState *state_main1 = db.RegisterBlob(data_main1);
//     db.InsertVectorRecordIntoMain({0}, *reinterpret_cast<const VectorRecord *>(state_main1));

//     std::vector<float> vec_main2(1000, 2.5);
//     std::span<u8> data_main2(reinterpret_cast<u8 *>(vec_main2.data()), vec_main2.size() * sizeof(float));
//     const leanstore::BlobState *state_main2 = db.RegisterBlob(data_main2);
//     db.InsertVectorRecordIntoMain({1}, *reinterpret_cast<const VectorRecord *>(state_main2));

//     size_t num_centroids = 2;
//     initialize_centroids(db, num_centroids);

//     int size = db.CountCentroids();
//     ASSERT_EQ(size, num_centroids);

//     auto vec1 = db.GetFloatVectorFromCentroids({0});
//     ASSERT_EQ(vec1[0], 1.5);

//     auto vec2 = db.GetFloatVectorFromCentroids({1});
//     ASSERT_EQ(vec2[0], 2.5);

//     leanstore->CommitTransaction();
//   });

//   leanstore->Shutdown();
// }

// TEST(IVFFlat, InitializeCentroidsTest2) {
//   auto leanstore = std::make_unique<leanstore::LeanStore>();
//   VectorAdapter db = VectorAdapter(*leanstore);

//   leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
//     leanstore->StartTransaction();

//     std::vector<std::vector<float>> vectors;
//     for (int i = 0; i < 1000; ++i) {
//       vectors.emplace_back(1000, static_cast<float>(i));
//       std::span<u8> data(reinterpret_cast<u8 *>(vectors[i].data()), vectors[i].size() * sizeof(float));
//       const leanstore::BlobState *state = db.RegisterBlob(data);
//       db.InsertVectorRecordIntoMain({i}, *reinterpret_cast<const VectorRecord *>(state));
//     }

//     int num_centroids = 1000 / 5;
//     initialize_centroids(db, num_centroids);

//     int size = db.CountCentroids();
//     ASSERT_EQ(size, num_centroids);

//     leanstore->CommitTransaction();
//   });

//   leanstore->Shutdown();
// }

// TEST(IVFFlat, UpdateOneCentroid) {
//   auto leanstore = std::make_unique<leanstore::LeanStore>();
//   VectorAdapter db = VectorAdapter(*leanstore);

//   const int num_vec = 10;
//   std::vector<const leanstore::BlobState *> bucket;

//   leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
//     leanstore->StartTransaction();

//     for (int id = 1; id <= num_vec; ++id) {
//       std::vector<float> vector(1000, static_cast<float>(id));
//       std::span<u8> data(reinterpret_cast<u8 *>(vector.data()), vector.size() * sizeof(float));
//       const leanstore::BlobState *state = db.RegisterBlob(data);
//       db.InsertVectorRecordIntoMain({id}, *reinterpret_cast<const VectorRecord *>(state));
//     }

//     for (int id = 1; id <= num_vec; ++id) {
//       const leanstore::BlobState *state;
//       db.LookUpMain({id}, [&](const VectorRecord &record) {
//         state = &record.blobState;
//         bucket.push_back(state);
//       });
//     }

//     std::vector<float> vector(1000, 1.0);
//     std::span<u8> data(reinterpret_cast<u8 *>(vector.data()), vector.size() * sizeof(float));
//     const leanstore::BlobState *state = db.RegisterBlob(data);
//     db.InsertVectorRecordIntoCentroids({1}, *reinterpret_cast<const VectorRecord *>(state));

//     update_one_centroid(db, bucket, {1}, 1000);

//     auto vec = db.GetFloatVectorFromCentroids({1});
//     ASSERT_EQ(vec[0], 5.5);

//     leanstore->CommitTransaction();
//   });

//   leanstore->Shutdown();
// }

// TEST(IVFFlat, BuildIndex1) {
//   auto leanstore = std::make_unique<leanstore::LeanStore>();
//   VectorAdapter db = VectorAdapter(*leanstore);

//   leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
//     leanstore->StartTransaction();

//     int num_vec = 1000;
//     int num_centroids = 3;
//     size_t vector_size = 1000;

//     for (int id = 0; id < num_vec; ++id) {
//       std::vector<float> vector(vector_size, static_cast<float>(id));
//       std::span<u8> data(reinterpret_cast<u8 *>(vector.data()), vector.size() * sizeof(float));
//       const leanstore::BlobState *state = db.RegisterBlob(data);
//       db.InsertVectorRecordIntoMain({id}, *reinterpret_cast<const VectorRecord *>(state));
//     }

//     IVFFlatIndex index(db, num_centroids, num_centroids, vector_size);
//     index.build_index();

//     leanstore->CommitTransaction();
//   });

//   leanstore->Shutdown();
// }

// TEST(IVFFlat, AssertSize) {
//   auto leanstore = std::make_unique<leanstore::LeanStore>();
//   VectorAdapter db = VectorAdapter(*leanstore);
//   int num_vec = 3;
//   size_t vector_size = 10000;

//   leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
//     leanstore->StartTransaction();

//     for (int i = 0; i < num_vec; ++i) {
//       std::vector<float> vector(vector_size, static_cast<float>(i));
//       std::span<u8> data(reinterpret_cast<u8 *>(vector.data()), vector.size() * sizeof(float));
//       const leanstore::BlobState *state = db.RegisterBlob(data);
//       EXPECT_EQ(state->blob_size, vector_size * sizeof(float));
//       db.InsertVectorRecordIntoMain({i}, *reinterpret_cast<const VectorRecord *>(state));
//     }

//     assert((int)db.CountMain() == num_vec);

//     db.Scan({0},
//       [&](const VectorRecord::Key &key, const VectorRecord &record) {
//         EXPECT_EQ(record.blobState.blob_size, vector_size * sizeof(float));
//         EXPECT_GT(record.blobState.PageCount(), 1);
//         db.LoadBlob(
//           &record.blobState,
//           [&](std::span<const uint8_t> span) {
//             EXPECT_EQ(span.size(), vector_size * sizeof(float));
//           },
//           false);

//         return false;
//       });

//     leanstore->CommitTransaction();
//   });

//   leanstore->Shutdown();
// }

TEST(IVFFlat, BuildIndexAndLookup) {
  auto leanstore = std::make_unique<leanstore::LeanStore>();
  VectorAdapter db = VectorAdapter(*leanstore);

  leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
    leanstore->StartTransaction();

    int num_vec = 1000;
    int num_centroids = calculate_num_centroids(num_vec);
    int num_probe_centroids = calculate_num_probe_centroids(num_centroids);
    size_t vector_size = 1000;

    for (int i = 0; i < num_vec; ++i) {
      std::vector<float> vector(vector_size, static_cast<float>(i));
      std::span<u8> data(reinterpret_cast<u8 *>(vector.data()), vector.size() * sizeof(float));
      const leanstore::BlobState *state = db.RegisterBlob(data);
      db.InsertVectorRecordIntoMain({i}, *reinterpret_cast<const VectorRecord *>(state));
    }

    assert((int)db.CountMain() == num_vec);

    IVFFlatIndex index(db, num_centroids, num_probe_centroids, vector_size);
    auto build_start_time = std::chrono::high_resolution_clock::now();
    index.build_index();
    auto build_end_time = std::chrono::high_resolution_clock::now();
    auto build_duration = std::chrono::duration_cast<std::chrono::milliseconds>(build_end_time - build_start_time);

    std::vector<float> input_vec(vector_size, 30.6);
    auto search_start_time = std::chrono::high_resolution_clock::now();
    std::vector<const leanstore::BlobState *> states = index.find_n_closest_vectors(input_vec, 8);
    auto search_end_time = std::chrono::high_resolution_clock::now();
    auto search_duration = std::chrono::duration_cast<std::chrono::milliseconds>(search_end_time - search_start_time);

    std::vector<float> expected_results = {31.0, 30.0, 32.0, 29.0, 33.0, 28.0, 34.0, 27.0};
    ASSERT_EQ(states.size(), expected_results.size());

    for (size_t i = 0; i < states.size(); i++) {
      std::vector<float> res = db.GetFloatVectorFromBlobState(states[i]);
      ASSERT_EQ(res[0], expected_results[i]) << "Mismatch at index " << i;
      std::cout << res[0] << std::endl;
    }

    std::cout << "Index Build Time: " << build_duration.count() << " ms" << std::endl;
    std::cout << "Search Time: " << search_duration.count() << " ms" << std::endl;

    leanstore->CommitTransaction();
  });

  leanstore->Shutdown();
}

// TEST(IVFFlat, BuildIndexAndLookupBigVecSize) {
//   auto leanstore = std::make_unique<leanstore::LeanStore>();
//   VectorAdapter db = VectorAdapter(*leanstore);

//   leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
//     leanstore->StartTransaction();

//     int num_vec = 200;
//     int num_centroids = calculate_num_centroids(num_vec);
//     int num_probe_centroids = calculate_num_probe_centroids(num_centroids);
//     size_t vector_size = 10000;

//     for (int i = 0; i < num_vec; ++i) {
//       std::vector<float> vector(vector_size, static_cast<float>(i));
//       std::span<u8> data(reinterpret_cast<u8 *>(vector.data()), vector.size() * sizeof(float));
//       const leanstore::BlobState *state = db.RegisterBlob(data);
//       db.InsertVectorRecordIntoMain({i}, *reinterpret_cast<const VectorRecord *>(state));
//     }

//     assert((int)db.CountMain() == num_vec);

//     IVFFlatIndex index(db, num_centroids, num_probe_centroids, vector_size);
//     auto build_start_time = std::chrono::high_resolution_clock::now();
//     index.build_index();
//     auto build_end_time = std::chrono::high_resolution_clock::now();
//     auto build_duration = std::chrono::duration_cast<std::chrono::milliseconds>(build_end_time - build_start_time);

//     std::vector<float> input_vec(vector_size, 30.6);
//     auto search_start_time = std::chrono::high_resolution_clock::now();
//     std::vector<const leanstore::BlobState *> states = index.find_n_closest_vectors(input_vec, 8);
//     auto search_end_time = std::chrono::high_resolution_clock::now();
//     auto search_duration = std::chrono::duration_cast<std::chrono::milliseconds>(search_end_time - search_start_time);

//     std::vector<float> expected_results = {31.0, 30.0, 32.0, 29.0, 33.0, 28.0, 34.0, 27.0};
//     ASSERT_EQ(states.size(), expected_results.size());

//     for (size_t i = 0; i < states.size(); i++) {
//       std::vector<float> res = db.GetFloatVectorFromBlobState(states[i]);
//       ASSERT_EQ(res[0], expected_results[i]) << "Mismatch at index " << i;
//       std::cout << res[0] << std::endl;
//     }

//     std::cout << "Index Build Time: " << build_duration.count() << " ms" << std::endl;
//     std::cout << "Search Time: " << search_duration.count() << " ms" << std::endl;

//     leanstore->CommitTransaction();
//   });

//   leanstore->Shutdown();
// }
