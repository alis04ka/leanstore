#include "storage/vectordb/hnsw.h"
#include "storage/vectordb/ivfflat_adapter.h"
#include "storage/vectordb/util.h"
#include <random>

namespace leanstore::storage::vector {

NSWIndex::NSWIndex(std::vector<const BlobState *> &vertices)
    : vertices_(vertices) {}

std::vector<size_t> select_neighbors(VectorAdapter &db, const std::vector<float> &input_vec, const std::vector<size_t> &vertex_ids, const std::vector<const BlobState *> &vertices, size_t m) {
  std::vector<std::pair<float, size_t>> distances;
  distances.reserve(vertex_ids.size());
  for (const auto vert : vertex_ids) {
    distances.emplace_back(distance_vec_blob(db, input_vec, vertices[vert]), vert);
  }
  std::sort(distances.begin(), distances.end());
  std::vector<size_t> selected_vs;
  selected_vs.reserve(vertex_ids.size());
  for (size_t i = 0; i < m && i < distances.size(); i++) {
    selected_vs.emplace_back(distances[i].second);
  }
  return selected_vs;
}

std::vector<size_t> select_neighbors(VectorAdapter &db, const BlobState *input_vec, const std::vector<size_t> &vertex_ids, const std::vector<const BlobState *> &vertices, size_t m) {
  std::vector<std::pair<float, size_t>> distances;
  distances.reserve(vertex_ids.size());
  for (const auto vert : vertex_ids) {
    distances.emplace_back(distance_blob(db, input_vec, vertices[vert]), vert);
  }
  std::sort(distances.begin(), distances.end());
  std::vector<size_t> selected_vs;
  selected_vs.reserve(vertex_ids.size());
  for (size_t i = 0; i < m && i < distances.size(); i++) {
    selected_vs.emplace_back(distances[i].second);
  }
  return selected_vs;
}

std::vector<size_t> NSWIndex::search_layer(VectorAdapter &db, const std::vector<float> &base_vector, size_t limit, const std::vector<size_t> &entry_points) {
  assert(limit > 0);
  std::vector<size_t> candidates;
  std::unordered_set<size_t> visited;
  std::priority_queue<std::pair<float, size_t>, std::vector<std::pair<float, size_t>>, std::greater<>> explore_q;
  std::priority_queue<std::pair<float, size_t>, std::vector<std::pair<float, size_t>>, std::less<>> result_set;
  for (const auto entry_point : entry_points) {
    float dist = distance_vec_blob(db,  base_vector, vertices_[entry_point]);
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
    // std::cout <<  "vertices size " << in_vertices_.size() << std::endl;
    // std::cout <<  "edges size " << edges_[vertex].size() << std::endl;
    // std::cout << "-----------"<< std::endl;
    assert(in_vertices_.size() <= 1 || !edges_[vertex].empty());
    for (const auto &neighbor : edges_[vertex]) {
      if (visited.find(neighbor) == visited.end()) {
        visited.emplace(neighbor);
        auto dist = distance_vec_blob(db,  base_vector, vertices_[neighbor]);
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

std::vector<size_t> NSWIndex::search_layer(VectorAdapter &db, const BlobState *base_vector, size_t limit, const std::vector<size_t> &entry_points) {
  assert(limit > 0);
  std::vector<size_t> candidates;
  std::unordered_set<size_t> visited;
  std::priority_queue<std::pair<float, size_t>, std::vector<std::pair<float, size_t>>, std::greater<>> explore_q;
  std::priority_queue<std::pair<float, size_t>, std::vector<std::pair<float, size_t>>, std::less<>> result_set;
  for (const auto entry_point : entry_points) {
    float dist = distance_blob(db, vertices_[entry_point], base_vector);
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
    // std::cout <<  "vertices size " << in_vertices_.size() << std::endl;
    // std::cout <<  "edges size " << edges_[vertex].size() << std::endl;
    // std::cout << "-----------"<< std::endl;
    assert(in_vertices_.size() <= 1 || !edges_[vertex].empty());
    for (const auto &neighbor : edges_[vertex]) {
      if (visited.find(neighbor) == visited.end()) {
        visited.emplace(neighbor);
        auto dist = distance_blob(db, vertices_[neighbor], base_vector);
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

auto NSWIndex::add_vertex(size_t vertex_id) {
  in_vertices_.push_back(vertex_id);
}


void NSWIndex::connect(size_t vertex_a, size_t vertex_b) {
  edges_[vertex_a].push_back(vertex_b);
  edges_[vertex_b].push_back(vertex_a);
}

HNSWIndex::HNSWIndex(VectorAdapter db)
    : db(db) {
  std::random_device rand_dev;
  generator_ = std::mt19937(rand_dev());
  vertex_id = 0;
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
  layers_.reserve(100);
  layers_.emplace_back(vertices_);
}

void HNSWIndex::build_index() {
  int i = 0;
  for (const BlobState *vertex : vectors) {
    // std::cout << "insert vector entry "<< i << std::endl;
    insert_vector_entry(vertex);
    i++;
  }
}

std::vector<size_t> HNSWIndex::scan_vector_entry(const std::vector<float> &base_vector, size_t limit) {
  std::vector<size_t> entry_points{layers_[layers_.size() - 1].default_entry_point()};
  for (int level = layers_.size() - 1; level >= 1; level--) {
    auto nearest_elements = layers_[level].search_layer(db, base_vector, ef_search_, entry_points);
    nearest_elements = select_neighbors(db, base_vector, nearest_elements, vertices_, 1);
    entry_points = {nearest_elements[0]};
  }
  auto neighbors = layers_[0].search_layer(db, base_vector, limit > ef_search_ ? limit : ef_search_, entry_points);
  neighbors = select_neighbors(db, base_vector, neighbors, vertices_, limit);
  return neighbors;
}

size_t HNSWIndex::add_vertex(const BlobState *vec) {
  auto id = vertices_.size();
  vertices_.push_back(vec);
  return id;
}

void HNSWIndex::insert_vector_entry(const BlobState *key) {
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
      nearest_elements = layers_[level].search_layer(db, key, ef_search_, entry_points);
      nearest_elements = select_neighbors(db, key, nearest_elements, vertices_, 1);
      entry_points = {nearest_elements[0]};
    }
    for (; level >= 0; level--) {
      auto &layer = layers_[level];
      // std::cout << "level " << level << std::endl;
      nearest_elements = layer.search_layer(db, key, ef_construction_, entry_points);
      auto neighbors = select_neighbors(db, key, nearest_elements, vertices_, m_);
      layer.add_vertex(vertex_id);
      for (const auto neighbor : neighbors) {
        layer.connect(vertex_id, neighbor);
      }
      for (const auto neighbor : neighbors) {
        auto &edges = layer.edges_[neighbor];
        if (edges.size() > m_max_) {
          auto new_neighbors = select_neighbors(db, vertices_[neighbor], edges, vertices_, layer.m_max_);
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