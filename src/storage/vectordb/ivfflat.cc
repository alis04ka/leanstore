#include "storage/vectordb/ivfflat.h"
#include "storage/vectordb/timer.h"
#include "storage/vectordb/util.h"
#include <random>

namespace leanstore::storage::vector {

#ifdef TIME_INDEX
static i64 build_index_time = 0;
static i64 update_centroids_time = 0;
static i64 assign_vectros_time = 0;
static i64 find_bucket_time = 0;
static i64 find_k_closest_time = 0;
static i64 initialize_centroids_time = 0;
static i64 search_time = 0;
static i64 distance_blob_time = 0;
#endif

#ifdef TIME_INDEX
static void report_timing() {
  std::cout << "Reporting timing....." << std::endl;
  std::cout << "Build_index time: " << static_cast<double>(build_index_time) / 1000.0f << "ms\n";
  std::cout << "---Initialize_centroids time: " << static_cast<double>(initialize_centroids_time) / 1000.0f << "ms\n";
  std::cout << "---Assign_vectors time: " << static_cast<double>(assign_vectros_time) / 1000.0f << "ms\n";
  std::cout << "------Find_bucket time: " << static_cast<double>(find_bucket_time) / 1000.0f << "ms\n";
  std::cout << "---------Distance_blob time: " << static_cast<double>(distance_blob_time) / 1000.0f << "ms\n";
  std::cout << "------Update_centroids time: " << static_cast<double>(update_centroids_time) / 1000.0f << "ms\n";
}
#endif

double get_search_time_ivfflat() {
  return static_cast<double>(search_time);
}

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

int find_bucket(VectorAdapter &adapter_centroids, BlobAdapter &blob_adapter, const BlobState *input_vec) {
  START_TIMER(t);
  float min_dist = MAXFLOAT;
  int min_index = -1;
  int i = 0;

  adapter_centroids.Scan({0},
    [&](const VectorRecord::Key &key, const VectorRecord &record) {
      START_TIMER(t1);
      float cur_dist = distance_blob(blob_adapter, input_vec, &record.blobState);
      END_TIMER(t1, distance_blob_time);
      if (cur_dist < min_dist) {
        min_dist = cur_dist;
        min_index = i;
      }
      i++;
      return true;
    });

  END_TIMER(t, find_bucket_time);
  assert(min_index >= 0);
  return min_index;
}

std::vector<int> find_k_closest_centroids(VectorAdapter &adapter_centroids, BlobAdapter &blob_adapter, const std::vector<float> &input_vec, size_t k) {
  START_TIMER(t);
  std::vector<std::pair<float, int>> distances;

  int i = 0;
  adapter_centroids.Scan({0},
    [&](const VectorRecord::Key &key, const VectorRecord &record) {
      float cur_dist = distance_vec_blob(blob_adapter, input_vec, &record.blobState);
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
  END_TIMER(t, find_k_closest_time);
}

void initialize_centroids(VectorAdapter &adapter_centroids, VectorAdapter &adapter_main, BlobAdapter &blob_adapter, size_t num_centroids) {
  START_TIMER(t);
  size_t num_vectors = adapter_main.Count();
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
  adapter_main.Scan({0},
    [&](const VectorRecord::Key &key, const VectorRecord &record) {
      if (random_indices.contains(key.my_key)) {
        blob_adapter.LoadBlob(
          &record.blobState,
          [&](std::span<const uint8_t> span) {
            const leanstore::BlobState *state_span = blob_adapter.RegisterBlob(span);
            adapter_centroids.InsertVectorRecord({centroid_id++},
              *reinterpret_cast<const VectorRecord *>(state_span));
          },
          false);
      }
      return true;
    });

  END_TIMER(t, initialize_centroids_time);
  std::cout << "Centroid initialization complete. Total assigned: " << num_to_assign << std::endl;
}

float update_one_centroid(VectorAdapter &adapter_centroids, BlobAdapter &blob_adapter, std::vector<const BlobState *> bucket, const VectorRecord::Key &key, size_t vector_size) {
  if (bucket.empty()) {
    return 0.0;
  }
  int num_vec = bucket.size();

  std::vector<float> sum_vec(vector_size, 0.0);

  for (int i = 0; i < num_vec; i++) {
    blob_adapter.LoadBlob(
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
  std::span<float> new_centroid_data_float(sum_vec.data(), sum_vec.size());
  float dist = 0.0;

  adapter_centroids.LookUp({key}, [&](const VectorRecord &record) {
    blob_adapter.LoadBlob(&record.blobState, [&](std::span<const u8> old_data) {
        std::span<const float> old_data_span(reinterpret_cast<const float *>(old_data.data()), old_data.size() / sizeof(float));
        dist = distance_vec(new_centroid_data_float, old_data_span);
        }, false);
  });

  // std::cout << "New centroid " << sum_vec[0] << std::endl;
  adapter_centroids.Update({key}, new_centroid_data);
  return dist;
}

IVFFlatIndex::IVFFlatIndex(VectorAdapter adapter_main, VectorAdapter adapter_centroids, BlobAdapter &blob_adapter, size_t num_centroids, size_t num_probe_centroids, size_t vector_size)
    : adapter_main(adapter_main), adapter_centroids(adapter_centroids), blob_adapter(blob_adapter),num_centroids(num_centroids), num_probe_centroids(num_probe_centroids), vector_size(vector_size) {
  std::cout << "Number of centroids: " << num_centroids << std::endl;
  std::cout << "Number of probe centroids: " << num_probe_centroids << std::endl;
  std::cout << "Vector size: " << vector_size << std::endl;
  centroids.resize(num_centroids);
  vectors_storage.resize(adapter_main.Count());
  vectors.resize(adapter_main.Count());
  int idx = 0;
  adapter_main.Scan({0},
    [&](const VectorRecord::Key &key, const VectorRecord &record) {
      uint8_t *raw_data = (uint8_t *)&record.blobState;
      std::memcpy(vectors_storage[idx].data(), raw_data, record.blobState.MallocSize());
      vectors[idx] = reinterpret_cast<const BlobState *>(vectors_storage[idx].data());
      idx++;
      return true;
    });
}

void IVFFlatIndex::build_index() {
  START_TIMER(t);
  initialize_centroids(adapter_centroids, adapter_main, blob_adapter, num_centroids);
  assign_vectors_to_centroids();
  END_TIMER(t, build_index_time);
#ifdef TIME_INDEX
  report_timing();
#endif
}

bool IVFFlatIndex::update_centroids() {
  bool converged = true;
  START_TIMER(t);
  for (size_t i = 0; i < centroids.size(); i++) {
    float dist = update_one_centroid(adapter_centroids, blob_adapter, centroids[i].bucket, {(int)i}, vector_size);
    if (dist > 5 * sqrt(vector_size)) {
      converged = false;
    }
  }
  END_TIMER(t, update_centroids_time);
  return converged;
}

void IVFFlatIndex::assign_vectors_to_centroids() {
  START_TIMER(t);
  int max_iterations = 5;
  for (int i = 0; i < max_iterations; i++) {
    std::cout << "i = " << i << std::endl;
    for (size_t i = 0; i < centroids.size(); i++) {
      centroids[i].bucket.clear();
    }

    for (size_t i = 0; i < vectors.size(); i++) {
      int bucket_num = find_bucket(adapter_centroids, blob_adapter, vectors[i]);
      centroids[bucket_num].bucket.push_back(vectors[i]);
    }

    bool converged = update_centroids();
    if (converged)
      break;
  }
  END_TIMER(t, assign_vectros_time);
}

std::vector<const BlobState *> IVFFlatIndex::find_n_closest_vectors(const std::vector<float> &input_vec, size_t n) {
  START_TIMER(t);
  std::vector<int> indices = find_k_closest_centroids(adapter_centroids, blob_adapter, input_vec, num_probe_centroids);
  std::vector<const BlobState *> relevant_vector_states;
  for (size_t i = 0; i < indices.size(); i++) {
    std::vector<const BlobState *> bucket = centroids[i].bucket;
    for (size_t j = 0; j < bucket.size(); j++) {
      relevant_vector_states.push_back(bucket[j]);
    }
  }

  std::vector<std::pair<float, const BlobState *>> result_vec_records;
  for (size_t i = 0; i < relevant_vector_states.size(); i++) {
    float distance = distance_vec_blob(blob_adapter, input_vec, relevant_vector_states[i]);
    result_vec_records.push_back({distance, relevant_vector_states[i]});
  }

  std::sort(result_vec_records.begin(), result_vec_records.end(), [](const std::pair<float, const BlobState *> &a, const std::pair<float, const BlobState *> &b) {
    return a.first < b.first;
  });

  std::vector<const BlobState *> closest_vectors;
  for (size_t i = 0; i < n && i < result_vec_records.size(); i++) {
    closest_vectors.push_back(result_vec_records[i].second);
  }

  END_TIMER(t, search_time);
  return closest_vectors;
}

} // namespace leanstore::storage::vector