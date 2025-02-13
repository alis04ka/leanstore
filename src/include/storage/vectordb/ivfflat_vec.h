#pragma once
#include <span>
#include <vector>
#define TIME_INDEX

namespace leanstore::storage::vector {

namespace vec{

struct Centroid {
  std::vector<float> value;
  std::vector<std::span<float>> bucket;
};

int calculate_num_centroids_vec(int num_vec);
int calculate_num_probe_centroids_vec(int num_centroids);
double get_search_time_ivfflat_vec();

int find_bucket_vec(const std::vector<float> &input_vec, std::vector<Centroid> &centroids);
std::vector<int> find_k_closest_centroids_vec(const std::vector<float> &input_vec, const std::vector<Centroid> &centroids, size_t k);
void initialize_centroids_vec(const std::vector<std::vector<float>> &vectors, std::vector<Centroid> &centroids, size_t num_centroids);
void update_one_centroid_vec(Centroid &centroid, size_t vector_size);

class IVFFlatIndexVec {
public:
  IVFFlatIndexVec(size_t num_centroids, size_t num_probe_centroids, size_t vector_size, std::vector<std::vector<float>> &&vectors);

  void assign_vectors_to_centroids_vec();
  void update_centroids_vec();
  void build_index_vec();
  std::vector<std::span<float>> find_n_closest_vectors_vec(const std::vector<float> &input_vec, size_t n);
  std::vector<Centroid> get_centroids();

private:
  size_t num_centroids;
  size_t num_probe_centroids;
  size_t vector_size;
  std::vector<Centroid> centroids;
  std::vector<std::vector<float>> vectors;
};

}

} // namespace leanstore::storage::vector
