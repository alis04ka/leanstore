#include "storage/vectordb/hnsw.h"
#include "storage/vectordb/ivfflat_adapter.h"
#include "storage/vectordb/util.h"
#include <random>

namespace leanstore::storage::vector {

NSWIndex::NSWIndex(VectorAdapter &db, std::vector<const BlobState *> &vectors, float probability, size_t max_number_edges)
    : db(db), vectors(vectors), probability(probability), max_number_edges(max_number_edges) {}

std::vector<size_t> NSWIndex::build_index(std::vector<size_t> available_indices) {
  indices = choose_indices(available_indices);
  for (size_t i = 0; i < indices.size(); i++) {
    insert_edges_one_vec(indices[i]);
  }
  assert(edges.size() == indices.size());
  return indices;
}

std::vector<size_t> NSWIndex::choose_indices(std::vector<size_t> available_indices) {
    std::vector<size_t> chosen_indices;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::bernoulli_distribution d(probability);

    std::cout << "available " << available_indices.size() << std::endl;
    for (size_t index : available_indices) {
        if (d(gen)) {
            chosen_indices.push_back(index);
        }
    }
    std::cout << "chosen " << chosen_indices.size() << std::endl;
    return chosen_indices;
}

void NSWIndex::insert_edges_one_vec(size_t vec_id) {
   std::priority_queue<std::pair<float, size_t>> p_queue;

  // Compute distances to all other vectors
  for (size_t j = 0; j < indices.size(); ++j) {
    if (vec_id == indices[j]) {
      continue;
    }
    float dist = distance_blob(db, vectors[vec_id], vectors[j]);
    p_queue.emplace(-dist, indices[j]); // Negative distance for min-heap behavior
  }

  // Process nearest neighbors
  while (!p_queue.empty()) {
    float dist = -p_queue.top().first; // Revert negative distance
    size_t neighbor = p_queue.top().second;
    p_queue.pop();

    // Insert or replace edges for both directions
    insert_or_replace_edge(vec_id, neighbor, dist);
    insert_or_replace_edge(neighbor, vec_id, dist);
  }
}

void NSWIndex::insert_or_replace_edge(size_t from, size_t to, float dist) {
  auto &edges_list = edges[from];

  // Check if 'to' already exists in the edge list
  for (auto &edge : edges_list) {
    if (edge == to) {
      return;
    }
  }

  // If space is available, add directly
  if (edges_list.size() < max_number_edges) {
    edges_list.push_back(to);
  } else {
    // Replace the farthest edge if distance is shorter
    float max_dist = 0.0f;
    size_t replace_idx = 0;
    for (size_t i = 0; i < edges_list.size(); ++i) {
      float d = distance_blob(db, vectors[from], vectors[edges_list[i]]);
      if (d > max_dist) {
        max_dist = d;
        replace_idx = i;
      }
    }
    if (dist < max_dist) { // Replace only if the new distance is shorter
      edges_list[replace_idx] = to;
    }
  }
}

std::vector<size_t> NSWIndex::search(const std::vector<float> &query_vec, size_t k, size_t entry_point_id) {
  std::priority_queue<std::pair<float, size_t>, std::vector<std::pair<float, size_t>>, std::greater<>> candidates;
  std::unordered_set<size_t> visited;
  std::priority_queue<std::pair<float, size_t>> results;

  // start from entry point 
  candidates.push({distance_vec_blob(db, query_vec, vectors[entry_point_id]), entry_point_id});

  while (!candidates.empty()) {
    auto current = candidates.top();
    candidates.pop();

    if (visited.find(current.second) != visited.end()) { // skip visited
      continue;
    }
    visited.insert(current.second);

    results.push(current);
    if (results.size() > k)
      results.pop();

    // stopping condition
    if (!candidates.empty() && candidates.top().first > results.top().first) {
      break;
    }

    for (size_t neighbor : edges[current.second]) {
      if (visited.find(neighbor) == visited.end()) {
        float dist = distance_vec_blob(db, query_vec, vectors[neighbor]);
        candidates.push({dist, neighbor});
      }
    }
  }

  std::vector<size_t> nearest_neighbors;
  while (!results.empty()) {
    nearest_neighbors.push_back(results.top().second);
    results.pop();
  }

  std::reverse(nearest_neighbors.begin(), nearest_neighbors.end());

  return nearest_neighbors;
}


//------------------------------------------------------- HNSW -------------------------------------

HNSWIndex::HNSWIndex(VectorAdapter db)
    : db(db) {}

void HNSWIndex::build_index() {
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

  assert(prob_per_layer.size() == num_layers);
  assert(num_edges_per_layer.size() == num_layers);
  for (size_t i = 0; i < num_layers; i++) {
    NSWIndex index(db, vectors, prob_per_layer[i], num_edges_per_layer[i]);
    layers.push_back(index);
  }

  std::vector<size_t> available_indices(vectors.size());
  std::iota(available_indices.begin(), available_indices.end(), 0);

  for (size_t i = 0; i < num_layers; i++) {
    std::vector<size_t> next_indices = layers[i].build_index(available_indices);
    available_indices = std::move(next_indices);
  }
}

std::vector<size_t> HNSWIndex::search(const std::vector<float> &query_vec, size_t k) {
  size_t entry_point_id = layers[num_layers - 1].indices[0];
  std::vector<size_t> res;
  for (int i = layers.size() - 1; i >= 0; i--) {
     std::cout << "entry point:" << entry_point_id << std::endl;
    if (i == 0) {
      res = layers[i].search(query_vec, k, entry_point_id);
    }
    res = layers[i].search(query_vec, k, entry_point_id);
    assert(res.size() > 0);
    std::cout << "res size:" << res.size() << std::endl;
    entry_point_id = res[0];
  }
  //assert(res.size() == k);
  return res;
}

} // namespace leanstore::storage::vector