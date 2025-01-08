#include "storage/vectordb/nsw.h"
#include "storage/vectordb/util.h"

namespace leanstore::storage::vector {

NSWIndex::NSWIndex(VectorAdapter db)
    : db(db) {}

void NSWIndex::build_index() {
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

  insert_edges();
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

void NSWIndex::insert_edges_one_vec(size_t vec_id) {
  std::priority_queue<std::pair<float, size_t>> p_queue;

  // Compute distances to all other vectors
  for (size_t j = 0; j < vectors.size(); ++j) {
    if (vec_id == j)
      continue;
    float dist = distance_blob(db, vectors[vec_id], vectors[j]);
    p_queue.emplace(-dist, j); // Negative distance for min-heap behavior
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

void NSWIndex::insert_edges() {
  edges.resize(db.CountMain());
  for (size_t i = 0; i < vectors.size(); i++) {
    insert_edges_one_vec(i);
  }
}

std::vector<size_t> NSWIndex::search(const std::vector<float> &query_vec, size_t k) {
  std::priority_queue<std::pair<float, size_t>, std::vector<std::pair<float, size_t>>, std::greater<>> candidates;
  std::unordered_set<size_t> visited;
  std::priority_queue<std::pair<float, size_t>> results;

  // start from entry point 
  candidates.push({distance_vec_blob(db, query_vec, vectors[DEFAULT_ENTRY_POINT_ID]), DEFAULT_ENTRY_POINT_ID});

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

  for (size_t i = 0; i < std::min(k, nearest_neighbors.size()); i++) {
    vectors[nearest_neighbors[i]]->print_float();
  }

  return nearest_neighbors;
}

std::vector<size_t> NSWIndex::search_optimized(const std::vector<float> &query_vec, size_t k) {
  std::priority_queue<std::pair<float, size_t>, std::vector<std::pair<float, size_t>>, std::greater<>> candidates;
  std::unordered_set<size_t> visited;
  std::priority_queue<std::pair<float, size_t>> results;

  // start from entry point 
  candidates.push({distance_vec_blob(db, query_vec, vectors[DEFAULT_ENTRY_POINT_ID]), DEFAULT_ENTRY_POINT_ID});

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

  for (size_t i = 0; i < std::min(k, nearest_neighbors.size()); i++) {
    vectors[nearest_neighbors[i]]->print_float();
  }

  return nearest_neighbors;
}

std::vector<size_t> NSWIndex::search_multiple_entries(const std::vector<float> &query_vec, size_t k, const std::vector<size_t>& entry_points = {DEFAULT_ENTRY_POINT_ID}) {
  std::priority_queue<std::pair<float, size_t>, std::vector<std::pair<float, size_t>>, std::greater<>> candidates;
  std::unordered_set<size_t> visited;
  std::priority_queue<std::pair<float, size_t>> results;

  // Initialize candidates with multiple entry points
  for (size_t entry : entry_points) {
    float dist = distance_vec_blob(db, query_vec, vectors[entry]);
    candidates.push({dist, entry});
  }

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

    // Early stopping condition
    if (!candidates.empty() && candidates.top().first >= results.top().first && results.size() >= k) {
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

  for (size_t i = 0; i < std::min(k, nearest_neighbors.size()); i++) {
    vectors[nearest_neighbors[i]]->print_float();
  }

  return nearest_neighbors;
}



} // namespace leanstore::storage::vector