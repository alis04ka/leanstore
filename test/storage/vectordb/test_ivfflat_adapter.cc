#include "storage/vectordb/ivfflat.h"
#include "storage/vectordb/vector_adapter.h"
#include "gtest/gtest.h"

using namespace leanstore::storage::vector;

// TEST(Adapter, Basic) {
//   auto leanstore = std::make_unique<leanstore::LeanStore>();
//   VectorAdapter db = VectorAdapter(*leanstore);

//   leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
//     leanstore->StartTransaction();

//     EXPECT_EQ(db.CountCentroids(), 0);
//     EXPECT_EQ(db.CountMain(), 0);

//     std::vector<u8> vector1(1000, 1);
//     const leanstore::BlobState *state = db.RegisterBlob(vector1);
//     db.InsertVectorRecordIntoMain({0}, *reinterpret_cast<const VectorRecord *>(state));
//     db.InsertVectorRecordIntoCentroids({0}, *reinterpret_cast<const VectorRecord *>(state));

//     db.LookUpMain({0}, [&](const VectorRecord &record) {
//       db.LoadBlob(&record.blobState, [](std::span<const u8> data) { EXPECT_EQ(data[0], 1); }, true);
//     });

//     db.LookUpCentroids({0}, [&](const VectorRecord &record) {
//       db.LoadBlob(&record.blobState, [](std::span<const u8> data) { EXPECT_EQ(data[0], 1); }, true);
//     });

//     leanstore->CommitTransaction();
//   });

//   leanstore->Shutdown();
// }

// TEST(Adapter, BulkInsertAndLookup) {
//   auto leanstore = std::make_unique<leanstore::LeanStore>();
//   VectorAdapter db = VectorAdapter(*leanstore);

//   const size_t num_records = 10000;

//   leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
//     leanstore->StartTransaction();

//     EXPECT_EQ(db.CountCentroids(), 0);
//     EXPECT_EQ(db.CountMain(), 0);

//     std::vector<std::vector<u8>> vectors;
//     vectors.reserve(num_records);

//     for (size_t i = 0; i < num_records; ++i) {
//       vectors.emplace_back(1000, static_cast<u8>(i % 256));
//       const leanstore::BlobState *state = db.RegisterBlob(vectors[i]);
//       db.InsertVectorRecordIntoMain({static_cast<Integer>(i)}, *reinterpret_cast<const VectorRecord *>(state));
//       db.InsertVectorRecordIntoCentroids(
//         {static_cast<Integer>(i)}, *reinterpret_cast<const VectorRecord *>(state));
//     }

//     EXPECT_EQ(db.CountCentroids(), num_records);
//     EXPECT_EQ(db.CountMain(), num_records);

//     for (size_t i = 0; i < num_records; ++i) {
//       db.LookUpMain({static_cast<Integer>(i)}, [&](const VectorRecord &record) {
//         db.LoadBlob(
//           &record.blobState, [&](std::span<const u8> data) { EXPECT_EQ(data[0], static_cast<u8>(i % 256)); }, true);
//       });
//     }

//     for (size_t i = 0; i < num_records; ++i) {
//       db.LookUpCentroids({static_cast<Integer>(i)}, [&](const VectorRecord &record) {
//         db.LoadBlob(
//           &record.blobState, [&](std::span<const u8> data) { EXPECT_EQ(data[0], static_cast<u8>(i % 256)); }, true);
//       });
//     }

//     leanstore->CommitTransaction();
//   });

//   leanstore->Shutdown();
// }

// TEST(Adapter, BlobStateValidAfterCopy) {
//   auto leanstore = std::make_unique<leanstore::LeanStore>();
//   VectorAdapter db = VectorAdapter(*leanstore);

//   const size_t num_records = 100000;

//   leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
//     leanstore->StartTransaction();

//     EXPECT_EQ(db.CountCentroids(), 0);
//     EXPECT_EQ(db.CountMain(), 0);

//     std::vector<std::array<uint8_t, leanstore::BlobState::MAX_MALLOC_SIZE>> states(num_records);
//     for (size_t i = 0; i < num_records; ++i) {
//       std::vector<u8> vec(1000, static_cast<u8>(i % 256));
//       const leanstore::BlobState *state = db.RegisterBlob(vec);
//       const u8 *raw_state = reinterpret_cast<const u8 *>(state);
//       std::memcpy(states[i].data(), raw_state, state->MallocSize());
//       db.InsertVectorRecordIntoMain({static_cast<Integer>(i)}, *reinterpret_cast<const VectorRecord *>(state));
//     }

//     EXPECT_EQ(db.CountMain(), num_records);

//     EXPECT_EQ(states.size(), db.CountMain());

//     for (const auto &array : states) {
//       db.LoadBlob(reinterpret_cast<const leanstore::BlobState *>(array.data()), [](std::span<const u8> data) {
//         // std::cout << ((float*)data.data())[0] << std::endl;
//       },
//         true);
//     }
//     leanstore->CommitTransaction();
//   });

//   leanstore->Shutdown();
// }

// TEST(Adapter, UpdateCentroidFloatVec1) {
//   auto leanstore = std::make_unique<leanstore::LeanStore>();
//   VectorAdapter db = VectorAdapter(*leanstore);

//   leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
//     leanstore->StartTransaction();

//     std::vector<float> vector1(1000, 1.0);
//     std::span<u8> data1(reinterpret_cast<u8 *>(vector1.data()), vector1.size() * sizeof(float));
//     const leanstore::BlobState *state1 = db.RegisterBlob(data1);
//     db.InsertVectorRecordIntoCentroids({1}, *reinterpret_cast<const VectorRecord *>(state1));

//     std::vector<float> updated_vector(1000, 2.0);
//     std::span<u8> updated_data(
//       reinterpret_cast<u8 *>(updated_vector.data()), updated_vector.size() * sizeof(float));
//     db.UpdateCentroid({1}, updated_data);

//     db.LookUpCentroids({1}, [&](const VectorRecord &record) {
//       db.LoadBlob(
//         &record.blobState,
//         [&](std::span<const u8> data) {
//           auto updated_vec = reinterpret_cast<const float *>(data.data());
//           for (size_t i = 0; i < 1000; ++i) {
//             EXPECT_FLOAT_EQ(updated_vec[i], 2.0);
//           }
//         },
//         true);
//     });

//     leanstore->CommitTransaction();
//   });

//   leanstore->Shutdown();
// }

// TEST(Adapter, UpdateCentroidFloatVec) {
//   auto leanstore = std::make_unique<leanstore::LeanStore>();
//   VectorAdapter db = VectorAdapter(*leanstore);

//   const int num_vec = 10;

//   leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
//     leanstore->StartTransaction();

//     for (int id = 1; id <= num_vec; ++id) {
//       std::vector<float> vector(1000, static_cast<float>(id));
//       std::span<u8> data(reinterpret_cast<u8 *>(vector.data()), vector.size() * sizeof(float));
//       const leanstore::BlobState *state = db.RegisterBlob(data);
//       db.InsertVectorRecordIntoCentroids({id}, *reinterpret_cast<const VectorRecord *>(state));
//     }

//     for (int id = 1; id <= num_vec; ++id) {
//       std::vector<float> updated_vector(1000, static_cast<float>(3 * id));
//       std::span<u8> updated_data(
//         reinterpret_cast<u8 *>(updated_vector.data()), updated_vector.size() * sizeof(float));
//       db.UpdateCentroid({id}, updated_data);
//     }

//     for (int id = 1; id <= num_vec; ++id) {
//       db.LookUpCentroids({id}, [&](const VectorRecord &record) {
//         db.LoadBlob(
//           &record.blobState,
//           [&](std::span<const u8> data) {
//             auto updated_vec = reinterpret_cast<const float *>(data.data());
//             for (size_t i = 0; i < 1000; ++i) {
//               EXPECT_FLOAT_EQ(updated_vec[i], static_cast<float>(3 * id));
//             }
//           },
//           true);
//       });
//     }

//     leanstore->CommitTransaction();
//   });

//   leanstore->Shutdown();
// }