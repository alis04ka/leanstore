#include "storage/vectordb/hnsw.h"
#include "storage/vectordb/timer.h"
#include "storage/vectordb/util.h"
#include <random>
#include <tracy/Tracy.hpp>

namespace leanstore::storage::vector {

#ifdef TIME_INDEX
static i64 build_index_time = 0;
static i64 search_time = 0;
#endif

#ifdef TIME_INDEX
static void report_timing() {
  std::cout << "Reporting timing....." << std::endl;
  std::cout << "Build_index time: " << static_cast<double>(build_index_time) / 1000.0f << "ms\n";
}

double get_search_time_hnsw() {
  return static_cast<double>(search_time);
}

#endif



NSWIndex::NSWIndex(std::vector<const BlobState *> &vertices)
    : vertices_(vertices) {}

template <typename DistanceFunc>
std::vector<size_t> select_neighbors_generic(DistanceFunc distance_func, const std::vector<size_t> &vertex_ids, size_t m) {
  ZoneScoped;
  std::vector<std::pair<float, size_t>> distances;
  distances.reserve(vertex_ids.size());

  for (const auto vert : vertex_ids) {
    distances.emplace_back(distance_func(vert), vert);
  }

  std::sort(distances.begin(), distances.end());

  std::vector<size_t> selected_vs;
  selected_vs.reserve(std::min(m, distances.size()));
  for (size_t i = 0; i < m && i < distances.size(); i++) {
    selected_vs.emplace_back(distances[i].second);
  }

  return selected_vs;
}

std::vector<size_t> select_neighbors_float(BlobAdapter &adapter, const std::vector<float> &input_vec, const std::vector<size_t> &vertex_ids, const std::vector<const BlobState *> &vertices, size_t m) {
  ZoneScoped;
  auto distance_func = [&](size_t vert) {
    return distance_vec_blob(adapter, input_vec, vertices[vert]);
  };
  return select_neighbors_generic(distance_func, vertex_ids, m);
}

std::vector<size_t> select_neighbors_blob(BlobAdapter &adapter, const BlobState *input_vec, const std::vector<size_t> &vertex_ids, const std::vector<const BlobState *> &vertices, size_t m) {
  ZoneScoped;
  auto distance_func = [&](size_t vert) {
    return distance_blob(adapter, input_vec, vertices[vert]);
  };
  return select_neighbors_generic(distance_func, vertex_ids, m);
}

template <typename VectorType, typename DistanceFunc>
std::vector<size_t> NSWIndex::search_layer_template(BlobAdapter &adapter, const VectorType &base_vector, size_t limit, const std::vector<size_t> &entry_points, DistanceFunc distance_func) {
  ZoneScoped;
  assert(limit > 0);
  std::vector<size_t> candidates;
  std::unordered_set<size_t> visited;
  std::priority_queue<std::pair<float, size_t>, std::vector<std::pair<float, size_t>>, std::greater<>> explore_q;
  std::priority_queue<std::pair<float, size_t>, std::vector<std::pair<float, size_t>>, std::less<>> result_set;

  for (const auto entry_point : entry_points) {
    float dist = distance_func(adapter, base_vector, vertices_[entry_point]);
    explore_q.emplace(dist, entry_point);
    result_set.emplace(dist, entry_point);
    visited.emplace(entry_point);
  }

  while (!explore_q.empty()) {
    auto [dist, vertex] = explore_q.top();
    explore_q.pop();

    if (dist > result_set.top().first) {
      break;
    }

    assert(in_vertices_.size() <= 1 || !edges_[vertex].empty());

    for (const auto &neighbor : edges_[vertex]) {
      if (visited.find(neighbor) == visited.end()) {
        visited.emplace(neighbor);
        auto dist = distance_func(adapter, base_vector, vertices_[neighbor]);
        explore_q.emplace(dist, neighbor);
        result_set.emplace(dist, neighbor);

        while (result_set.size() > limit) {
          result_set.pop();
        }
      }
    }
  }

  while (!result_set.empty()) {
    candidates.push_back(result_set.top().second);
    result_set.pop();
  }

  std::reverse(candidates.begin(), candidates.end());
  return candidates;
}

std::vector<size_t> NSWIndex::search_layer(BlobAdapter &adapter, const std::vector<float> &base_vector, size_t limit, const std::vector<size_t> &entry_points) {
  ZoneScoped;
  return search_layer_template(adapter, base_vector, limit, entry_points, [](BlobAdapter &adpt, const auto &base_vector, const auto &vertex) {
    return distance_vec_blob(adpt, base_vector, vertex);
  });
}

std::vector<size_t> NSWIndex::search_layer(BlobAdapter &adapter, const BlobState *base_vector, size_t limit, const std::vector<size_t> &entry_points) {
  ZoneScoped;
  return search_layer_template(adapter, base_vector, limit, entry_points, [](BlobAdapter &adpt, const auto &base_vector, const auto &vertex) {
    return distance_blob(adpt, vertex, base_vector);
  });
}

auto NSWIndex::add_vertex(size_t vertex_id) {
  in_vertices_.push_back(vertex_id);
}

void NSWIndex::connect(size_t vertex_a, size_t vertex_b) {
  edges_[vertex_a].push_back(vertex_b);
  edges_[vertex_b].push_back(vertex_a);
}

inline size_t compute_m_max(size_t N, size_t m_min = 4, size_t alpha = 2) {
  ZoneScoped;
  if (N < 2) return m_min;
  return std::max(m_min, static_cast<size_t>(std::floor(alpha * std::log(static_cast<double>(N)))));
}

HNSWIndex::HNSWIndex(VectorAdapter db, BlobAdapter blob_adapter, size_t ef_construction, size_t ef_search, size_t m_max)
    : db(db), blob_adapter(blob_adapter), ef_construction_(ef_construction), ef_search_(ef_search), m_max_(m_max) {

  m_l_ = 1.0 / std::log(m_max);
  std::random_device rand_dev;
  generator_ = std::mt19937(rand_dev());
}

void HNSWIndex::build_index() {
  ZoneScoped;
  START_TIMER(t);
  vector_size = db.Count();
  vectors_storage.resize(vector_size);
  vectors.resize(vector_size);
  int idx = 0;
  db.Scan({0},
    [&](const VectorRecord::Key &key, const VectorRecord &record) {
      uint8_t *raw_data = (uint8_t *)&record.blobState;
      std::memcpy(vectors_storage[idx].data(), raw_data, record.blobState.MallocSize());
      vectors[idx] = reinterpret_cast<const BlobState *>(vectors_storage[idx].data());
      idx++;
      return true;
    });
  layers_.reserve(100);
  layers_.emplace_back(vertices_);
  for (const BlobState *vertex : vectors) {
    insert_vector_entry(vertex);
  }
  END_TIMER(t, build_index_time);
  #ifdef TIME_INDEX
    report_timing();
  #endif
}

std::vector<size_t> HNSWIndex::scan_vector_entry(const std::vector<float> &base_vector, size_t limit) {
  ZoneScoped;
  std::vector<size_t> entry_points{layers_[layers_.size() - 1].default_entry_point()};
  for (int level = layers_.size() - 1; level >= 1; level--) {
    auto nearest_elements = layers_[level].search_layer(blob_adapter, base_vector, ef_search_, entry_points);
    nearest_elements = select_neighbors_float(blob_adapter, base_vector, nearest_elements, vertices_, 1);
    entry_points = {nearest_elements[0]};
  }
  auto neighbors = layers_[0].search_layer(blob_adapter, base_vector, limit > ef_search_ ? limit : ef_search_, entry_points);
  neighbors = select_neighbors_float(blob_adapter, base_vector, neighbors, vertices_, limit);
  return neighbors;
}

std::vector<const BlobState *> HNSWIndex::find_n_closest_vectors(const std::vector<float> &input_vec, size_t n) {
  ZoneScoped;
  START_TIMER(t);
  std::vector<size_t> neighbors = scan_vector_entry(input_vec, n);
  std::vector<const BlobState *> states_res;
  for (size_t i = 0; i < neighbors.size(); i++) {
    states_res.push_back(vectors[i]);
  }
  END_TIMER(t, search_time);
  return states_res;
}

size_t HNSWIndex::add_vertex(const BlobState *vec) {
  auto id = vertices_.size();
  vertices_.push_back(vec);
  return id;
}

void HNSWIndex::insert_vector_entry(const BlobState *key) {
  ZoneScoped;
  std::uniform_real_distribution<double> level_dist(0.0, 1.0);
  auto vertex_id = add_vertex(key);
  int target_level = static_cast<int>(std::floor(-std::log(level_dist(generator_)) * m_l_));
  assert(target_level >= 0);
  std::vector<size_t> nearest_elements;
  if (!layers_[0].in_vertices_.empty()) {
    std::vector<size_t> entry_points{layers_[layers_.size() - 1].default_entry_point()};
    int level = layers_.size() - 1;
    for (; level > target_level; level--) {
      // std::cout << "level " << level << std::endl;
      nearest_elements = layers_[level].search_layer(blob_adapter, key, ef_search_, entry_points);
      nearest_elements = select_neighbors_blob(blob_adapter, key, nearest_elements, vertices_, 1);
      entry_points = {nearest_elements[0]};
    }
    for (; level >= 0; level--) {
      auto &layer = layers_[level];
      // std::cout << "level " << level << std::endl;
      nearest_elements = layer.search_layer(blob_adapter, key, ef_construction_, entry_points);
      auto neighbors = select_neighbors_blob(blob_adapter, key, nearest_elements, vertices_, m_max_);
      layer.add_vertex(vertex_id);
      for (const auto neighbor : neighbors) {
        layer.connect(vertex_id, neighbor);
      }
      for (const auto neighbor : neighbors) {
        auto &edges = layer.edges_[neighbor];
        if (edges.size() > m_max_) {
          auto new_neighbors = select_neighbors_blob(blob_adapter, vertices_[neighbor], edges, vertices_, m_max_);
          edges = new_neighbors;
        }
      }
      entry_points = nearest_elements;
    }
  } else {
    layers_[0].add_vertex(vertex_id);
  }
  while (static_cast<int>(layers_.size()) <= target_level) {
    layers_.emplace_back(vertices_);
    layers_.back().add_vertex(vertex_id);
  }
}

} // namespace leanstore::storage::vector
