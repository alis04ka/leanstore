#include "storage/vectordb/hnsw_vec.h"
#include "storage/vectordb/timer.h"
#include "storage/vectordb/util.h"
#include <random>

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
#endif

double get_search_time_hnsw_vec() {
  return static_cast<double>(search_time);
}

NSWIndex::NSWIndex(std::vector<std::span<float>> &vertices)
    : vertices_(vertices) {}

std::vector<size_t> select_neighbors_vec(const std::span<float> input_vec, const std::vector<size_t> &vertex_ids, std::vector<std::span<float>> &vertices, size_t m) {
  std::vector<std::pair<float, size_t>> distances;
  distances.reserve(vertex_ids.size());

  for (const auto vert : vertex_ids) {
    distances.emplace_back(distance_vec(input_vec, vertices[vert]), vert);
  }

  std::sort(distances.begin(), distances.end());
  std::vector<size_t> selected_vs;
  selected_vs.reserve(std::min(m, distances.size()));
  for (size_t i = 0; i < m && i < distances.size(); i++) {
    selected_vs.emplace_back(distances[i].second);
  }

  assert(!selected_vs.empty());
  return selected_vs;
}

std::vector<size_t> NSWIndex::search_layer_vec(const std::span<float> base_vector, size_t limit, const std::vector<size_t> &entry_points) {
  // for (const auto &[key, value] : edges_) {
  //   std::cout << "Vertex " << key << " has " << value.size() << " edges." << std::endl;
  // }
  assert(limit > 0);
  std::vector<size_t> candidates;
  std::unordered_set<size_t> visited;
  std::priority_queue<std::pair<float, size_t>, std::vector<std::pair<float, size_t>>, std::greater<>> explore_q;
  std::priority_queue<std::pair<float, size_t>, std::vector<std::pair<float, size_t>>, std::less<>> result_set;

  for (const auto entry_point : entry_points) {
    float dist = distance_vec(base_vector, vertices_[entry_point]);
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
        float dist = distance_vec(base_vector, vertices_[neighbor]);
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

auto NSWIndex::add_vertex_vec(size_t vertex_id) {
  in_vertices_.push_back(vertex_id);
}

void NSWIndex::connect_vec(size_t vertex_a, size_t vertex_b) {
  edges_[vertex_a].push_back(vertex_b);
  edges_[vertex_b].push_back(vertex_a);
}

HNSWIndex::HNSWIndex(std::vector<std::vector<float>> &&vectors)
    : vectors(std::move(vectors)) {
  std::random_device rand_dev;
  generator_ = std::mt19937(rand_dev());
  layers_.reserve(100);
  layers_.emplace_back(vertices_);
}

void HNSWIndex::build_index_vec() {
  START_TIMER(t);
  for (std::vector<float> &vertex : vectors) {
    insert_vector_entry_vec(vertex);
  }
  END_TIMER(t, build_index_time);
#ifdef TIME_INDEX
  report_timing();
#endif
}

std::vector<size_t> HNSWIndex::scan_vector_entry_vec(const std::span<float> base_vector, size_t limit) {
  START_TIMER(t);
  std::vector<size_t> entry_points{layers_[layers_.size() - 1].default_entry_point_vec()};
  for (int level = layers_.size() - 1; level >= 1; level--) {
    auto nearest_elements = layers_[level].search_layer_vec(base_vector, ef_search_, entry_points);
    nearest_elements = select_neighbors_vec(base_vector, nearest_elements, vertices_, 1);
    entry_points = {nearest_elements[0]};
  }
  auto neighbors = layers_[0].search_layer_vec(base_vector, limit > ef_search_ ? limit : ef_search_, entry_points);
  neighbors = select_neighbors_vec(base_vector, neighbors, vertices_, limit);
  END_TIMER(t, search_time);
  return neighbors;
}

size_t HNSWIndex::add_vertex_vec(std::span<float> vec) {
  auto id = vertices_.size();
  vertices_.emplace_back(vec);
  return id;
}

void HNSWIndex::insert_vector_entry_vec(const std::span<float> key) {
  std::uniform_real_distribution<double> level_dist(0.0, 1.0);
  auto vertex_id = add_vertex_vec(key);
  int target_level = static_cast<int>(std::floor(-std::log(level_dist(generator_)) * m_l_));
  assert(target_level >= 0);
  std::vector<size_t> nearest_elements;
  if (!layers_[0].in_vertices_.empty()) {
    std::vector<size_t> entry_points{layers_[layers_.size() - 1].default_entry_point_vec()};
    int level = layers_.size() - 1;
    for (; level > target_level; level--) {
      // std::cout << "level " << level << std::endl;
      nearest_elements = layers_[level].search_layer_vec(key, ef_search_, entry_points);
      nearest_elements = select_neighbors_vec(key, nearest_elements, vertices_, 1);
      entry_points = {nearest_elements[0]};
    }
    for (; level >= 0; level--) {
      auto &layer = layers_[level];
      // std::cout << "Level " << level << std::endl;
      nearest_elements = layer.search_layer_vec(key, ef_construction_, entry_points);
      auto neighbors = select_neighbors_vec(key, nearest_elements, vertices_, m_);
      layer.add_vertex_vec(vertex_id);
      for (const auto neighbor : neighbors) {
        layer.connect_vec(vertex_id, neighbor);
      }
      for (const auto neighbor : neighbors) {
        auto &edges = layer.edges_[neighbor];
        if (edges.size() > m_max_) {
          std::vector<size_t> new_neighbors = select_neighbors_vec(vertices_[neighbor], edges, vertices_, layer.m_max_);
          edges = new_neighbors;
        }
      }
      entry_points = nearest_elements;
    }
  } else {
    layers_[0].add_vertex_vec(vertex_id);
  }
  while (static_cast<int>(layers_.size()) <= target_level) {
    layers_.emplace_back(vertices_);
    layers_.back().add_vertex_vec(vertex_id);
  }
}

} // namespace leanstore::storage::vector