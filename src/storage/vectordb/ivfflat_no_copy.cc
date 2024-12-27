#include "storage/vectordb/ivfflat_no_copy.h"
#include <random>
#include "storage/vectordb/ivfflat_adapter.h"

namespace leanstore::storage::vector {

int vector_size = 1000;

float distance_vec(std::span<const float> span1, std::span<const float> span2) {
    assert(span1.size() == span2.size());

    float sum = 0.0;
    for (size_t i = 0; i < span1.size(); ++i) {
        float diff = span1[i] - span2[i];
        sum += diff * diff;
    }

    return std::sqrt(sum);
}

float distance_blob(VectorAdapter &db, const BlobState *blob_state_1, const BlobState *blob_state_2) {
    assert(blob_state_1->blob_size == blob_state_2->blob_size);
    float distance = 0.0;
    db.LoadBlob(
        blob_state_1,
        [&](std::span<const uint8_t> blob1) {
            std::span<const float> span1(reinterpret_cast<const float *>(blob1.data()), blob1.size() / sizeof(float));
            db.LoadBlob(
                blob_state_2,
                [&](std::span<const uint8_t> blob2) {
                    std::span<const float> span2(
                        reinterpret_cast<const float *>(blob2.data()), blob2.size() / sizeof(float));
                    distance = distance_vec(span1, span2);
                },
                true);
        },
        true);

    return distance;
}

int find_bucket(VectorAdapter &db, const BlobState *input_vec) {
    float min_dist = MAXFLOAT;
    int min_index  = -1;
    int i          = 0;

    db.ScanCentroids({0}, [&](const VectorRecord::Key &key, const VectorRecord &record) {
        float cur_dist = distance_blob(db, input_vec, &record.blobState);
        if (cur_dist < min_dist) {
            min_dist  = cur_dist;
            min_index = i;
        }
        i++;
        return true;
    });

    return min_index;
}

void initialize_centroids(VectorAdapter &db, size_t num_centroids) {
    //std::cout << "Initialize centroids ---------------  " << std::endl;
    size_t num_vectors   = db.CountMain();
    size_t num_to_assign = std::min(num_centroids, num_vectors);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::unordered_set<size_t> random_indices;
    std::uniform_int_distribution<size_t> dist(0, num_vectors - 1);

    while (random_indices.size() < num_to_assign) {
        size_t random_index = dist(gen);
        if (random_indices.insert(random_index).second) {
            std::cout << "Random index chosen: " << random_index << std::endl;
        }
    }

    int centroid_id = 0;
    db.Scan({0}, [&](const VectorRecord::Key &key, const VectorRecord &record) {
        if (random_indices.contains(key.my_key)) {
            db.LoadBlob(
                &record.blobState,
                [&](std::span<const uint8_t> span) {
                    const leanstore::BlobState *state_span = db.RegisterBlob(span);
                    db.InsertVectorRecordIntoCentroids(
                        {centroid_id++}, *reinterpret_cast<const VectorRecord *>(state_span));
                },
                true);
        }
        return true;
    });

    std::cout << "Centroid initialization complete. Total assigned: " << num_to_assign << std::endl;
}

void update_one_centroid(VectorAdapter &db, std::vector<const BlobState *> bucket, const VectorRecord::Key &key) {
    if (bucket.empty()) return;
    int num_vec = bucket.size();

    std::vector<float> sum_vec(vector_size, 0.0);

    for (int i = 0; i < num_vec; i++) {
        db.LoadBlob(
            bucket[i],
            [&](std::span<const uint8_t> blob) {
                assert(vector_size == blob.size() / sizeof(float));
                std::span<const float> temp(reinterpret_cast<const float *>(blob.data()), blob.size() / sizeof(float));
                for (int j = 0; j < vector_size; j++) {
                    sum_vec[j] += temp[j];
                }
            },
            true);
    }

    for (int i = 0; i < vector_size; i++) {
        sum_vec[i] /= num_vec;
    }

    std::span<u8> new_centroid_data(reinterpret_cast<u8 *>(sum_vec.data()), sum_vec.size() * sizeof(float));
    std::cout << "New centroid " << sum_vec[0] << std::endl;
    db.UpdateCentroid({key}, new_centroid_data);
}

IVFFlatNoCopyIndex::IVFFlatNoCopyIndex(VectorAdapter db, size_t num_centroids, size_t num_probe_centroids, size_t vector_size)
    : db(db), num_centroids(num_centroids), num_probe_centroids(num_probe_centroids), vector_size(vector_size) {
    centroids.resize(num_centroids);
}

void IVFFlatNoCopyIndex::BuildIndex() {
    vectors_storage.resize(db.CountMain());
    vectors.resize(db.CountMain());
    int idx = 0;
    db.Scan({0}, [&](const VectorRecord::Key &key, const VectorRecord &record) {
        uint8_t *raw_data = (uint8_t *)&record.blobState;
        std::memcpy(vectors_storage[idx].data(), raw_data, record.blobState.MallocSize());
        vectors[idx] = reinterpret_cast<const BlobState*>(vectors_storage[idx].data());
        idx++;
        return true;
    });
    initialize_centroids(db, num_centroids);
    assign_vectors_to_centroids();
}

void IVFFlatNoCopyIndex::update_centroids() {
    std::cout << "Update centroids ---------------  " << std::endl;
    for (size_t i = 0; i < centroids.size(); i++) {
        update_one_centroid(db, centroids[i].bucket, {(int)i});
    }
}

void IVFFlatNoCopyIndex::assign_vectors_to_centroids() {
    int max_iterations = 5;
    for (int i = 0; i < max_iterations; i++) {
        std::cout << "Assign vectors to centroids. Iteration " << i << std::endl;

        for (size_t i = 0; i < centroids.size(); i++) {
            centroids[i].bucket.clear();
        }

        for (int i = 0; i < vectors.size(); i++) {
            int bucket_num = find_bucket(db, vectors[i]);
            centroids[bucket_num].bucket.push_back(vectors[i]);
        }

        update_centroids();
    }
}

}  // namespace leanstore::storage::vector