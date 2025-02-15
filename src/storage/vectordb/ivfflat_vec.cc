#include "storage/vectordb/ivfflat_vec.h"
#include "storage/vectordb/timer.h"
#include "storage/vectordb/util.h"
#include <random>

namespace leanstore::storage::vector::vec {

#ifdef TIME_INDEX
static i64 build_index_time = 0;
static i64 update_centroids_time = 0;
static i64 assign_vectros_time = 0;
static i64 find_bucket_time = 0;
static i64 initialize_centroids_time = 0;
static i64 search_time = 0;
static i64 distance_vec_time = 0;
#endif

#ifdef TIME_INDEX
static void report_timing() {
  std::cout << "Reporting timing....." << std::endl;
  std::cout << "Build_index time: " << static_cast<double>(build_index_time) / 1000.0f << "ms\n";
  std::cout << "---Initialize_centroids time: " << static_cast<double>(initialize_centroids_time) / 1000.0f << "ms\n";
  std::cout << "---Assign_vectors time: " << static_cast<double>(assign_vectros_time) / 1000.0f << "ms\n";
  std::cout << "------Find_bucket time: " << static_cast<double>(find_bucket_time) / 1000.0f << "ms\n";
  std::cout << "---------Distance_vec time: " << static_cast<double>(distance_vec_time) / 1000.0f << "ms\n";
  std::cout << "------Update_centroids time: " << static_cast<double>(update_centroids_time) / 1000.0f << "ms\n";
}
#endif

double get_search_time_ivfflat_vec() {
  return static_cast<double>(search_time);
}

int calculate_num_centroids_vec(int num_vec) {
  if (num_vec < 3)
    return num_vec;
  return std::max(3, static_cast<int>(std::sqrt(num_vec)));
}

int calculate_num_probe_centroids_vec(int num_centroids) {
  if (num_centroids < 3)
    return num_centroids;
  return std::max(3, static_cast<int>(num_centroids * 0.1));
}

int find_bucket_vec(const std::vector<float> &input_vec, std::vector<Centroid> &centroids) {
  START_TIMER(t);
  float min_dist = MAXFLOAT;
  int min_index = -1;
  int i = 0;

  for (const auto &centroid : centroids) {
    START_TIMER(t1);
    float cur_dist = distance_vec(input_vec, centroid.value);
    END_TIMER(t1, distance_vec_time);
    // std::cout << "Dist: " << cur_dist << std::endl;
    if (cur_dist < min_dist) {
      min_dist = cur_dist;
      min_index = i;
    }
    i++;
  }

  END_TIMER(t, find_bucket_time);
  return min_index;
}

std::vector<int> find_k_closest_centroids_vec(const std::vector<float> &input_vec, const std::vector<Centroid> &centroids, size_t k) {
  START_TIMER(t);
  std::vector<std::pair<float, int>> distances;

  for (size_t i = 0; i < centroids.size(); i++) {
    float cur_dist = distance_vec(input_vec, centroids[i].value);
    distances.emplace_back(cur_dist, i);
  }

  std::sort(distances.begin(), distances.end());

  std::vector<int> closest_indices;
  for (size_t j = 0; j < k && j < distances.size(); j++) {
    closest_indices.push_back(distances[j].second);
  }

  return closest_indices;
}

void initialize_centroids_vec(const std::vector<std::vector<float>> &vectors, std::vector<Centroid> &centroids, size_t num_centroids) {
  START_TIMER(t);
  size_t num_vectors = vectors.size();
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

  for (size_t i = 0; i < vectors.size(); i++) {
    if (random_indices.contains(i)) {
      centroids.push_back({vectors[i], {}});
    }
  }

  END_TIMER(t, initialize_centroids_time);
  std::cout << "Centroid initialization complete. Total assigned: " << num_to_assign << std::endl;
}

void update_one_centroid_vec(Centroid &centroid, size_t vector_size) {
  if (centroid.bucket.empty()) {
    return;
  }
  int num_vec = centroid.bucket.size();

  std::vector<float> sum_vec(vector_size, 0.0);

  for (int i = 0; i < num_vec; i++) {
    for (size_t j = 0; j < vector_size; j++) {
      sum_vec[j] += centroid.bucket[i][j];
    }
  }

  for (size_t j = 0; j < vector_size; j++) {
    sum_vec[j] /= num_vec;
  }
  centroid.value = sum_vec;
}

IVFFlatIndexVec::IVFFlatIndexVec(size_t num_centroids, size_t num_probe_centroids, size_t vector_size, size_t num_iter, std::vector<std::vector<float>> &&vectors)
    : num_centroids(num_centroids), num_probe_centroids(num_probe_centroids), vector_size(vector_size),
      num_iter(num_iter), vectors(std::move(vectors)) {
}

void IVFFlatIndexVec::build_index_vec() {
  START_TIMER(t);
  centroids.reserve(num_centroids);
  initialize_centroids_vec(vectors, centroids, num_centroids);
  assign_vectors_to_centroids_vec();
  END_TIMER(t, build_index_time);
#ifdef TIME_INDEX
  report_timing();
#endif
}

std::vector<Centroid> IVFFlatIndexVec::get_centroids() {
  return centroids;
}

void IVFFlatIndexVec::update_centroids_vec() {
  START_TIMER(t);
  for (size_t i = 0; i < centroids.size(); i++) {
    update_one_centroid_vec(centroids[i], vector_size);
  }
  END_TIMER(t, update_centroids_time);
}

void IVFFlatIndexVec::assign_vectors_to_centroids_vec() {
  START_TIMER(t);
  for (size_t i = 0; i < num_iter; i++) {
    std::cout << "i = " << i << std::endl;
    for (size_t i = 0; i < centroids.size(); i++) {
      centroids[i].bucket.clear();
    }
    for (size_t i = 0; i < vectors.size(); i++) {
      int bucket_num = find_bucket_vec(vectors[i], centroids);
      // std::cout << "Bucket number: " << bucket_num << std::endl;
      centroids[bucket_num].bucket.push_back(vectors[i]);
    }

    update_centroids_vec();
  }
  END_TIMER(t, assign_vectros_time);
}

std::vector<std::span<float>> IVFFlatIndexVec::find_n_closest_vectors_vec(const std::vector<float> &input_vec, size_t n) {
  START_TIMER(t);
  std::vector<int> indices = find_k_closest_centroids_vec(input_vec, centroids, num_probe_centroids);
  std::vector<std::span<float>> relevant_vectors;
  for (size_t i = 0; i < indices.size(); i++) {
    std::vector<std::span<float>> bucket = centroids[i].bucket;
    for (size_t j = 0; j < bucket.size(); j++) {
      relevant_vectors.push_back(bucket[j]);
    }
  }

  std::vector<std::pair<float, std::span<float>>> result_vec_records;
  for (size_t i = 0; i < relevant_vectors.size(); i++) {
    float distance = distance_vec(input_vec, relevant_vectors[i]);
    result_vec_records.push_back({distance, relevant_vectors[i]});
  }
  std::sort(result_vec_records.begin(), result_vec_records.end(), [](const auto &a, const auto &b) {
    return a.first < b.first;
  });
  std::vector<std::span<float>> closest_vectors;
  for (size_t i = 0; i < n && i < result_vec_records.size(); i++) {
    closest_vectors.push_back(result_vec_records[i].second);
  }

  END_TIMER(t, search_time);
  return closest_vectors;
}

} // namespace leanstore::storage::vector
