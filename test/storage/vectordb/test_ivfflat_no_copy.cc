#include "gtest/gtest.h"
#include "storage/vectordb/ivfflat_adapter.h"
#include "storage/vectordb/ivfflat_no_copy.h"


using namespace leanstore::storage::vector;

TEST(IVFFlatNoCopy, DistanceBlobMainAndMain) {
    auto leanstore   = std::make_unique<leanstore::LeanStore>();
    VectorAdapter db = VectorAdapter(*leanstore);

    leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
        leanstore->StartTransaction();

        std::vector<float> vector1(1000, 1.0);
        std::span<u8> data1(reinterpret_cast<u8 *>(vector1.data()), vector1.size() * sizeof(float));
        const leanstore::BlobState *state1 = db.RegisterBlob(data1);
        db.InsertVectorRecordIntoMain({1}, *reinterpret_cast<const VectorRecord *>(state1));

        std::vector<float> vector2(1000, 2.0);
        std::span<u8> data2(reinterpret_cast<u8 *>(vector2.data()), vector2.size() * sizeof(float));
        const leanstore::BlobState *state2 = db.RegisterBlob(data2);
        db.InsertVectorRecordIntoMain({2}, *reinterpret_cast<const VectorRecord *>(state2));

        const leanstore::BlobState *loaded1;
        const leanstore::BlobState *loaded2;

        db.LookUpMain({1}, [&](const VectorRecord &record) { loaded1 = &record.blobState; });
        db.LookUpMain({2}, [&](const VectorRecord &record) { loaded2 = &record.blobState; });

        loaded1->print_float();
        loaded2->print_float();

        float distance = distance_blob(db, loaded1, loaded2);
        std::cout << "Distance between vector1 and vector2: " << distance << std::endl;
        EXPECT_NEAR(distance, std::sqrt(1000.0), 1e-3);

        leanstore->CommitTransaction();
    });

    leanstore->Shutdown();
}

TEST(IVFFlatNoCopy, DistanceBetweenMainAndCentroids) {
    auto leanstore   = std::make_unique<leanstore::LeanStore>();
    VectorAdapter db = VectorAdapter(*leanstore);

    leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
        leanstore->StartTransaction();

        std::vector<float> vector_main(1000, 1.0);
        std::span<u8> data_main(reinterpret_cast<u8 *>(vector_main.data()), vector_main.size() * sizeof(float));
        const leanstore::BlobState *state_main = db.RegisterBlob(data_main);
        db.InsertVectorRecordIntoMain({1}, *reinterpret_cast<const VectorRecord *>(state_main));

        std::vector<float> vector_centroid(1000, 2.0);
        std::span<u8> data_centroid(
            reinterpret_cast<u8 *>(vector_centroid.data()), vector_centroid.size() * sizeof(float));
        const leanstore::BlobState *state_centroid = db.RegisterBlob(data_centroid);
        db.InsertVectorRecordIntoCentroids({1}, *reinterpret_cast<const VectorRecord *>(state_centroid));

        const leanstore::BlobState *loaded_main;
        const leanstore::BlobState *loaded_centroid;

        db.LookUpMain({1}, [&](const VectorRecord &record) { loaded_main = &record.blobState; });
        db.LookUpCentroids({1}, [&](const VectorRecord &record) { loaded_centroid = &record.blobState; });

        loaded_main->print_float();
        loaded_centroid->print_float();

        float distance = distance_blob(db, loaded_main, loaded_centroid);
        std::cout << "Distance between main and centroid vector: " << distance << std::endl;
        EXPECT_NEAR(distance, std::sqrt(1000.0), 1e-3);

        leanstore->CommitTransaction();
    });

    leanstore->Shutdown();
}

TEST(IVFFlatNoCopy, FindBucketTest1) {
    auto leanstore   = std::make_unique<leanstore::LeanStore>();
    VectorAdapter db = VectorAdapter(*leanstore);

    leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
        leanstore->StartTransaction();
        std::vector<float> vec_main(1000, 1.5);
        std::span<u8> data_main(reinterpret_cast<u8 *>(vec_main.data()), vec_main.size() * sizeof(float));
        const leanstore::BlobState *state_main = db.RegisterBlob(data_main);
        db.InsertVectorRecordIntoMain({2}, *reinterpret_cast<const VectorRecord *>(state_main));

        std::vector<float> vec_centroid1(1000, 1.0);
        std::span<u8> data_centroid1(
            reinterpret_cast<u8 *>(vec_centroid1.data()), vec_centroid1.size() * sizeof(float));
        const leanstore::BlobState *state_centroid1 = db.RegisterBlob(data_centroid1);
        db.InsertVectorRecordIntoCentroids({0}, *reinterpret_cast<const VectorRecord *>(state_centroid1));

        std::vector<float> vec_centroid2(1000, 3.0);
        std::span<u8> data_centroid2(
            reinterpret_cast<u8 *>(vec_centroid2.data()), vec_centroid2.size() * sizeof(float));
        const leanstore::BlobState *state_centroid2 = db.RegisterBlob(data_centroid2);
        db.InsertVectorRecordIntoCentroids({1}, *reinterpret_cast<const VectorRecord *>(state_centroid2));

        const leanstore::BlobState *loaded_main;
        db.LookUpMain({2}, [&](const VectorRecord &record) { loaded_main = &record.blobState; });

        int bucket_index = find_bucket(db, loaded_main);
        std::cout << "Chosen bucket index: " << bucket_index << std::endl;

        EXPECT_EQ(bucket_index, 0);

        leanstore->CommitTransaction();
    });

    leanstore->Shutdown();
}

TEST(IVFFlatNoCopy, FindBucketTestMultiple) {
    auto leanstore   = std::make_unique<leanstore::LeanStore>();
    VectorAdapter db = VectorAdapter(*leanstore);

    leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
        leanstore->StartTransaction();

        std::vector<const leanstore::BlobState *> loaded_vecs;

        for (int i = 0; i <= 10; ++i) {
            std::vector<float> vec(1000, static_cast<float>(i));
            std::span<u8> data(reinterpret_cast<u8 *>(vec.data()), vec.size() * sizeof(float));
            const leanstore::BlobState *state = db.RegisterBlob(data);
            db.InsertVectorRecordIntoMain({i}, *reinterpret_cast<const VectorRecord *>(state));

            const leanstore::BlobState *loaded;
            db.LookUpMain({i}, [&](const VectorRecord &record) { loaded = &record.blobState; });
            loaded_vecs.push_back(loaded);
        }

        std::vector<float> vec_centroid1(1000, 2.0);
        std::span<u8> data_centroid1(
            reinterpret_cast<u8 *>(vec_centroid1.data()), vec_centroid1.size() * sizeof(float));
        const leanstore::BlobState *state_centroid1 = db.RegisterBlob(data_centroid1);
        db.InsertVectorRecordIntoCentroids({0}, *reinterpret_cast<const VectorRecord *>(state_centroid1));

        std::vector<float> vec_centroid2(1000, 4.0);
        std::span<u8> data_centroid2(
            reinterpret_cast<u8 *>(vec_centroid2.data()), vec_centroid2.size() * sizeof(float));
        const leanstore::BlobState *state_centroid2 = db.RegisterBlob(data_centroid2);
        db.InsertVectorRecordIntoCentroids({1}, *reinterpret_cast<const VectorRecord *>(state_centroid2));

        std::vector<float> vec_centroid3(1000, 8.0);
        std::span<u8> data_centroid3(
            reinterpret_cast<u8 *>(vec_centroid3.data()), vec_centroid3.size() * sizeof(float));
        const leanstore::BlobState *state_centroid3 = db.RegisterBlob(data_centroid3);
        db.InsertVectorRecordIntoCentroids({2}, *reinterpret_cast<const VectorRecord *>(state_centroid3));

        std::vector<int> expected_buckets = {0, 0, 0, 0, 1, 1, 1, 2, 2, 2, 2};

        for (int i = 0; i <= 10; ++i) {
            int bucket_index = find_bucket(db, loaded_vecs[i]);
            std::cout << "Vector " << i << " chose bucket index: " << bucket_index << std::endl;
            EXPECT_EQ(bucket_index, expected_buckets[i]);
        }

        leanstore->CommitTransaction();
    });

    leanstore->Shutdown();
}

TEST(IVFFlatNoCopy, FindBucketTestLarge) {
    auto leanstore = std::make_unique<leanstore::LeanStore>();
    VectorAdapter db = VectorAdapter(*leanstore);

    leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
        leanstore->StartTransaction();

        std::vector<const leanstore::BlobState*> loaded_vecs;
        int num_vec = 1000;

        for (int i = 0; i < num_vec; ++i) {
            std::vector<float> vec(1000, static_cast<float>(i));
            std::span<u8> data(reinterpret_cast<u8*>(vec.data()), vec.size() * sizeof(float));
            const leanstore::BlobState* state = db.RegisterBlob(data);
            db.InsertVectorRecordIntoMain({i}, *reinterpret_cast<const VectorRecord*>(state));
        }

        for (int i = 0; i < num_vec; ++i) {
            const leanstore::BlobState* loaded;
            db.LookUpMain({i}, [&](const VectorRecord& record) { loaded = &record.blobState; });
            loaded_vecs.push_back(loaded);
        }

        for (int i = 0; i < num_vec / 5; ++i) {
            std::vector<float> vec_centroid(1000, static_cast<float>(i * 5));
            std::span<u8> data_centroid(reinterpret_cast<u8*>(vec_centroid.data()), vec_centroid.size() *
            sizeof(float)); const leanstore::BlobState* state_centroid = db.RegisterBlob(data_centroid);
            db.InsertVectorRecordIntoCentroids({i}, *reinterpret_cast<const VectorRecord*>(state_centroid));
        }

        for (int i = 0; i < num_vec; ++i) {
            int bucket_index = find_bucket(db, loaded_vecs[i]);
           // std::cout << "Vector " << i << " chosen bucket index: " << bucket_index << std::endl;
        }

        leanstore->CommitTransaction();
    });

    leanstore->Shutdown();
}

TEST(IVFFlatNoCopy, InitializeCentroidsTest1) {
    auto leanstore   = std::make_unique<leanstore::LeanStore>();
    VectorAdapter db = VectorAdapter(*leanstore);

    leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
        leanstore->StartTransaction();

        std::vector<float> vec_main1(1000, 1.5);
        std::span<u8> data_main1(reinterpret_cast<u8 *>(vec_main1.data()), vec_main1.size() * sizeof(float));
        const leanstore::BlobState *state_main1 = db.RegisterBlob(data_main1);
        db.InsertVectorRecordIntoMain({0}, *reinterpret_cast<const VectorRecord *>(state_main1));

        std::vector<float> vec_main2(1000, 2.5);
        std::span<u8> data_main2(reinterpret_cast<u8 *>(vec_main2.data()), vec_main2.size() * sizeof(float));
        const leanstore::BlobState *state_main2 = db.RegisterBlob(data_main2);
        db.InsertVectorRecordIntoMain({1}, *reinterpret_cast<const VectorRecord *>(state_main2));

        size_t num_centroids = 2;
        initialize_centroids(db, num_centroids);

        int size = db.CountCentroids();
        ASSERT_EQ(size, num_centroids);

        auto vec1 = db.GetFloatVectorFromCentroids({0});
        ASSERT_EQ(vec1[0], 1.5);

        auto vec2 = db.GetFloatVectorFromCentroids({1});
        ASSERT_EQ(vec2[0], 2.5);

        leanstore->CommitTransaction();
    });

    leanstore->Shutdown();
}

TEST(IVFFlatNoCopy, InitializeCentroidsTest2) {
    auto leanstore   = std::make_unique<leanstore::LeanStore>();
    VectorAdapter db = VectorAdapter(*leanstore);

    leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
        leanstore->StartTransaction();

        std::vector<std::vector<float>> vectors;
        for (int i = 0; i < 1000; ++i) {
            vectors.emplace_back(1000, static_cast<float>(i));
            std::span<u8> data(reinterpret_cast<u8 *>(vectors[i].data()), vectors[i].size() * sizeof(float));
            const leanstore::BlobState *state = db.RegisterBlob(data);
            db.InsertVectorRecordIntoMain({i}, *reinterpret_cast<const VectorRecord *>(state));
        }

        int num_centroids = 1000 / 5;
        initialize_centroids(db, num_centroids);

        int size = db.CountCentroids();
        ASSERT_EQ(size, num_centroids);

        leanstore->CommitTransaction();
    });

    leanstore->Shutdown();
}

TEST(IVFFlatNoCopy, UpdateOneCentroid) {
    auto leanstore   = std::make_unique<leanstore::LeanStore>();
    VectorAdapter db = VectorAdapter(*leanstore);

    const int num_vec = 10;
    std::vector<const leanstore::BlobState *> bucket;

    leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
        leanstore->StartTransaction();

        for (int id = 1; id <= num_vec; ++id) {
            std::vector<float> vector(1000, static_cast<float>(id));
            std::span<u8> data(reinterpret_cast<u8 *>(vector.data()), vector.size() * sizeof(float));
            const leanstore::BlobState *state = db.RegisterBlob(data);
            db.InsertVectorRecordIntoMain({id}, *reinterpret_cast<const VectorRecord *>(state));
        }

        for (int id = 1; id <= num_vec; ++id) {
            const leanstore::BlobState *state;
            db.LookUpMain({id}, [&](const VectorRecord &record) {
                state = &record.blobState;
                bucket.push_back(state);
            });
        }

        std::vector<float> vector(1000, 1.0);
        std::span<u8> data(reinterpret_cast<u8 *>(vector.data()), vector.size() * sizeof(float));
        const leanstore::BlobState *state = db.RegisterBlob(data);
        db.InsertVectorRecordIntoCentroids({1}, *reinterpret_cast<const VectorRecord *>(state));

        update_one_centroid(db, bucket, {1});

        auto vec = db.GetFloatVectorFromCentroids({1});
        ASSERT_EQ(vec[0], 5.5);

        leanstore->CommitTransaction();
    });

    leanstore->Shutdown();
}

TEST(IVFFlatNoCopy, BuildIndex1) {
    auto leanstore   = std::make_unique<leanstore::LeanStore>();
    VectorAdapter db = VectorAdapter(*leanstore);

    leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
        leanstore->StartTransaction();

        int num_vec = 1000;
        int num_centroids = 3;
        size_t vector_size = 1000;

        for (int id = 0; id < num_vec; ++id) {
            std::vector<float> vector(vector_size, static_cast<float>(id));
            std::span<u8> data(reinterpret_cast<u8 *>(vector.data()), vector.size() * sizeof(float));
            const leanstore::BlobState *state = db.RegisterBlob(data);
            db.InsertVectorRecordIntoMain({id}, *reinterpret_cast<const VectorRecord *>(state));
        }

        IVFFlatNoCopyIndex index(db, num_centroids, num_centroids, vector_size);
        index.BuildIndex(); 

        leanstore->CommitTransaction();
    });

    leanstore->Shutdown();
}