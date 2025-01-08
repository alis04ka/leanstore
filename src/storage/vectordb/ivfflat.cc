#include "storage/vectordb/ivfflat.h"
#include "storage/vectordb/ivfflat_adapter.h"
#include "storage/vectordb/util.h"
#include <random>

namespace leanstore::storage::vector {

int calculate_num_centroids(int num_vec) {
  if (num_vec < 3)
    return num_vec;
  return std::max(3, static_cast<int>(std::sqrt(num_vec)));
}

int calculate_num_probe_centroids(int num_centroids) {
  if (num_centroids < 3)
    return num_centroids;
  return std::max(3, static_cast<int>(num_centroids * 0.1));
}

int find_bucket(VectorAdapter &db, const BlobState *input_vec) {
  float min_dist = MAXFLOAT;
  int min_index = -1;
  int i = 0;

  db.ScanCentroids({0},
    [&](const VectorRecord::Key &key, const VectorRecord &record) {
      float cur_dist = distance_blob(db, input_vec, &record.blobState);
      if (cur_dist < min_dist) {
        min_dist = cur_dist;
        min_index = i;
      }
      i++;
      return true;
    });

  return min_index;
}

std::vector<int> find_k_closest_centroids(VectorAdapter &db, const std::vector<float> &input_vec, size_t k) {
  std::vector<std::pair<float, int>> distances;

  int i = 0;
  db.ScanCentroids({0},
    [&](const VectorRecord::Key &key, const VectorRecord &record) {
      float cur_dist = distance_vec_blob(db, input_vec, &record.blobState);
      distances.emplace_back(cur_dist, i);
      i++;
      return true;
    });

  std::sort(distances.begin(), distances.end());

  std::vector<int> closest_indices;
  for (size_t j = 0; j < k && j < distances.size(); j++) {
    closest_indices.push_back(distances[j].second);
  }

  return closest_indices;
}

void initialize_centroids(VectorAdapter &db, size_t num_centroids) {
  size_t num_vectors = db.CountMain();
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
  db.Scan({0},
    [&](const VectorRecord::Key &key, const VectorRecord &record) {
      if (random_indices.contains(key.my_key)) {
        db.LoadBlob(
          &record.blobState,
          [&](std::span<const uint8_t> span) {
            const leanstore::BlobState *state_span = db.RegisterBlob(span);
            db.InsertVectorRecordIntoCentroids({centroid_id++},
              *reinterpret_cast<const VectorRecord *>(state_span));
          },
          false);
      }
      return true;
    });

  std::cout << "Centroid initialization complete. Total assigned: " << num_to_assign << std::endl;
}

void update_one_centroid(VectorAdapter &db, std::vector<const BlobState *> bucket, const VectorRecord::Key &key, size_t vector_size) {
  if (bucket.empty())
    return;
  int num_vec = bucket.size();

  std::vector<float> sum_vec(vector_size, 0.0);

  for (int i = 0; i < num_vec; i++) {
    db.LoadBlob(
      bucket[i],
      [&](std::span<const uint8_t> blob) {
        assert(vector_size == blob.size() / sizeof(float));
        std::span<const float> temp(reinterpret_cast<const float *>(blob.data()), blob.size() / sizeof(float));
        for (size_t j = 0; j < vector_size; j++) {
          sum_vec[j] += temp[j];
        }
      },
      false);
  }

  for (size_t i = 0; i < vector_size; i++) {
    sum_vec[i] /= num_vec;
  }

  std::span<u8> new_centroid_data(reinterpret_cast<u8 *>(sum_vec.data()), sum_vec.size() * sizeof(float));
  // std::cout << "New centroid " << sum_vec[0] << std::endl;
  db.UpdateCentroid({key}, new_centroid_data);
}

IVFFlatIndex::IVFFlatIndex(VectorAdapter db, size_t num_centroids, size_t num_probe_centroids, size_t vector_size)
    : db(db), num_centroids(num_centroids), num_probe_centroids(num_probe_centroids), vector_size(vector_size) {
  std::cout << "Number of centroids: " << num_centroids << std::endl;
  std::cout << "Number of probe centroids: " << num_probe_centroids << std::endl;
  std::cout << "Vector size: " << vector_size << std::endl;
  centroids.resize(num_centroids);
}

void IVFFlatIndex::build_index() {
  vectors_storage.resize(db.CountMain());
  vectors.resize(db.CountMain());
  int idx = 0;
  db.Scan({0},
    [&](const VectorRecord::Key &key, const VectorRecord &record) {
      uint8_t *raw_data = (uint8_t *)&record.blobState;
      std::memcpy(vectors_storage[idx].data(), raw_data, record.blobState.MallocSize());
      vectors[idx] = reinterpret_cast<const BlobState *>(vectors_storage[idx].data());
      idx++;
      return true;
    });
  initialize_centroids(db, num_centroids);
  assign_vectors_to_centroids();
}

void IVFFlatIndex::update_centroids() {
  for (size_t i = 0; i < centroids.size(); i++) {
    update_one_centroid(db, centroids[i].bucket, {(int)i}, vector_size);
  }
}

void IVFFlatIndex::assign_vectors_to_centroids() {
  int max_iterations = 5;
  for (int i = 0; i < max_iterations; i++) {
    for (size_t i = 0; i < centroids.size(); i++) {
      centroids[i].bucket.clear();
    }

    for (size_t i = 0; i < vectors.size(); i++) {
      int bucket_num = find_bucket(db, vectors[i]);
      centroids[bucket_num].bucket.push_back(vectors[i]);
    }

    update_centroids();
  }
}

std::vector<const BlobState *> IVFFlatIndex::find_n_closest_vectors(const std::vector<float> &input_vec, size_t n) {
  std::vector<int> indices = find_k_closest_centroids(db, input_vec, num_probe_centroids);
  std::vector<const BlobState *> relevant_vector_states;
  for (size_t i = 0; i < indices.size(); i++) {
    std::vector<const BlobState *> bucket = centroids[i].bucket;
    for (size_t j = 0; j < bucket.size(); j++) {
      relevant_vector_states.push_back(bucket[j]);
    }
  }

  std::vector<std::pair<float, const BlobState *>> result_vec_records;
  for (size_t i = 0; i < relevant_vector_states.size(); i++) {
    float distance = distance_vec_blob(db, input_vec, relevant_vector_states[i]);
    result_vec_records.push_back({distance, relevant_vector_states[i]});
  }

  std::sort(result_vec_records.begin(), result_vec_records.end(), [](const std::pair<float, const BlobState *> &a, const std::pair<float, const BlobState *> &b) {
    return a.first < b.first;
  });

  std::vector<const BlobState *> closest_vectors;
  for (size_t i = 0; i < n && i < result_vec_records.size(); i++) {
    closest_vectors.push_back(result_vec_records[i].second);
  }

  return closest_vectors;
}

} // namespace leanstore::storage::vector