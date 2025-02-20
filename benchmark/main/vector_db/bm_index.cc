#include "share_headers/perf_event.h"
#include "storage/vectordb/hnsw.h"
#include "storage/vectordb/hnsw_vec.h"
#include "storage/vectordb/ivfflat.h"
#include "storage/vectordb/ivfflat_vec.h"
#include "storage/vectordb/knn.h"
#include "storage/vectordb/knn_vec.h"
#include "storage/vectordb/vector_adapter.h"
#include <random>
#include <tracy/Tracy.hpp>

using namespace leanstore::storage::vector;

enum class IndexType {
  IVFFlat,
  HNSW,
  KNN,
};

IndexType parse_index_type(const std::string &type) {
  if (type == "ivfflat")
    return IndexType::IVFFlat;
  if (type == "hnsw")
    return IndexType::HNSW;
  else
    return IndexType::KNN;
}

DEFINE_string(index_type, "ivfflat", "what index to use for benchmarks");

// IVFFlat flags
DEFINE_uint64(num_centroids, 128, "Number of centroids");
DEFINE_uint64(num_probe_centroids, 10, "Number of centroids to probe");
DEFINE_uint64(num_iterations, 10, "number of iterations for ivfflat");

// HNSW flags
DEFINE_uint64(ef_construction, 200, "ef_construction ?");
DEFINE_uint64(ef_search, 100, "ef_search ?");
DEFINE_uint64(m_max, 10, "m_max ?");

// Data generation flags
DEFINE_uint64(vector_size, 3000, "Vector size for benchmarking");
DEFINE_uint64(num_vectors, 1000, "number of Vectors for benchmarking");
DEFINE_double(std_dev, 5.0, "standart deviation for data generation");

// Benchmark flags
DEFINE_bool(benchmark_baseline, true, "whether to also run the benchmarks for the baseline implementation");

// Search flags
DEFINE_bool(benchmark_lookup_perf, true, "wheter to run lookup benchmarks");
DEFINE_uint64(num_query_vectors, 1000, "how many lookup requests to send");
DEFINE_uint64(num_result_vectors, 10, "how many results the lookup should return");

static unsigned int seed = std::chrono::steady_clock::now().time_since_epoch().count();
static std::mt19937 gen(seed);

// Utility function to print timing
void print_timing(const std::string &stage,
  const std::chrono::high_resolution_clock::time_point &start,
  const std::chrono::high_resolution_clock::time_point &end) {
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::cout << stage << " took " << duration.count() << " ms\n";
}

void print_timing_one_query(const std::string &stage,
  const std::chrono::high_resolution_clock::time_point &start,
  const std::chrono::high_resolution_clock::time_point &end) {
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::cout << stage << " took " << duration.count() / FLAGS_num_query_vectors << " ms\n";
}

template <class T>
std::vector<float> create_random_vector(T &dist) {
  std::vector<float> vector;
  vector.reserve(FLAGS_vector_size);

  for (uint64_t j = 0; j < FLAGS_vector_size; j++) {
    vector.push_back(dist(gen));
  }

  return vector;
}

int main(int argc, char **argv) {
  gflags::SetUsageMessage("Vector Index Benchmark");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  IndexType indexType = parse_index_type(FLAGS_index_type);
  auto leanstore = std::make_unique<leanstore::LeanStore>();
  VectorAdapter adapter_main = VectorAdapter::CreateVectorAdapter<VectorRecord>(*leanstore);
  VectorAdapter adapter_centroids = VectorAdapter::CreateVectorAdapter<CentroidType>(*leanstore);
  BlobAdapter blob_adapter(*leanstore);

  std::normal_distribution<float> dist(0.0, FLAGS_std_dev);

  std::vector<std::vector<float>> embedding_vectors;
  embedding_vectors.reserve(FLAGS_num_vectors);

  std::vector<std::vector<float>> query_vectors;
  query_vectors.reserve(FLAGS_num_query_vectors);

  std::cout << "Starting generating benchmark Data for" + FLAGS_index_type + "\n";
  std::cout << "Vector size: " << FLAGS_vector_size << "\n";
  std::cout << "Number of vectors: " << FLAGS_num_vectors << "\n";
  std::cout << "stddev: " << FLAGS_std_dev << "\n";

  auto data_gen_start = std::chrono::high_resolution_clock::now();

  leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
    ZoneScopedN("Data initialization");
    leanstore->StartTransaction();

    for (uint64_t i = 0; i < FLAGS_num_vectors; i++) {
      auto vector = create_random_vector(dist);
      std::span<u8> data(reinterpret_cast<u8 *>(vector.data()), vector.size() * sizeof(float));
      const leanstore::BlobState *state = blob_adapter.RegisterBlob(data);
      adapter_main.InsertVectorRecord({static_cast<int>(i)}, *reinterpret_cast<const VectorRecord *>(state));

      if (FLAGS_benchmark_baseline) {
        embedding_vectors.push_back(std::move(vector));
      }
    }

    if (FLAGS_benchmark_lookup_perf) {
      for (uint64_t i = 0; i < FLAGS_num_query_vectors; i++) {
        auto vector = create_random_vector(dist);
        query_vectors.push_back(std::move(vector));
      }
    }

    assert((int)adapter_main.Count() == FLAGS_num_vectors);
    leanstore->CommitTransaction();
  });

  auto data_gen_end = std::chrono::high_resolution_clock::now();
  print_timing("Data generation", data_gen_start, data_gen_end);

  auto create_blob_index = [&]() -> std::unique_ptr<VectorIndex> {
    switch (indexType) {
    case IndexType::IVFFlat: {
      std::cout << "Bulding IVFFlat Blob Index:\n";
      std::cout << "num_centroids: " << FLAGS_num_centroids << "\n";
      std::cout << "num_probe_centroids: " << FLAGS_num_probe_centroids << "\n";
      std::cout << "vector_size: " << FLAGS_vector_size << "\n";
      std::cout << "num_iterations: " << FLAGS_num_iterations << "\n";
      return std::make_unique<IVFFlatIndex>(adapter_main, adapter_centroids, blob_adapter, FLAGS_num_centroids, FLAGS_num_probe_centroids, FLAGS_vector_size, FLAGS_num_iterations);
    }
    case IndexType::HNSW: {
      std::cout << "Bulding HNSW Blob Index:\n";
      std::cout << "vector_size: " << FLAGS_vector_size << "\n";
      std::cout << "ef_construction: " << FLAGS_ef_construction << "\n";
      std::cout << "ef_search: " << FLAGS_ef_search << "\n";
      std::cout << "m_max: " << FLAGS_m_max << "\n";
      return std::make_unique<HNSWIndex>(adapter_main, blob_adapter, FLAGS_ef_construction, FLAGS_ef_search, FLAGS_m_max);
    }
    case IndexType::KNN: {
      std::cout << "Bulding KNN Blob Index..\n";
      return std::make_unique<KnnIndex>(adapter_main, blob_adapter, FLAGS_vector_size);
    }
    default:
      return nullptr;
    }
  };

  auto create_base_index = [&]() -> std::unique_ptr<BaseVectorIndex> {
    switch (indexType) {
    case IndexType::IVFFlat: {
      std::cout << "Bulding IVFFlat Base Index:\n";
      std::cout << "num_centroids: " << FLAGS_num_centroids << "\n";
      std::cout << "num_probe_centroids: " << FLAGS_num_probe_centroids << "\n";
      std::cout << "vector_size: " << FLAGS_vector_size << "\n";
      std::cout << "num_iterations: " << FLAGS_num_iterations << "\n";
      return std::make_unique<vec::IVFFlatIndexVec>(FLAGS_num_centroids, FLAGS_num_probe_centroids, FLAGS_vector_size, FLAGS_num_iterations, std::move(embedding_vectors));
    }
    case IndexType::HNSW: {
      std::cout << "Bulding HNSW Base Index:\n";
      std::cout << "vector_size: " << FLAGS_vector_size << "\n";
      std::cout << "ef_construction: " << FLAGS_ef_construction << "\n";
      std::cout << "ef_search: " << FLAGS_ef_search << "\n";
      std::cout << "m_max: " << FLAGS_m_max << "\n";
      return std::make_unique<vec::HNSWIndex>(std::move(embedding_vectors), FLAGS_ef_construction, FLAGS_ef_search, FLAGS_m_max);
    }
    case IndexType::KNN: {
      std::cout << "Bulding KNN Base Index..\n";
      return std::make_unique<vec::KnnIndexVec>(FLAGS_vector_size, std::move(embedding_vectors));
    }
    default:
      return nullptr;
    }
  };

  std::unique_ptr<VectorIndex> blob_index = create_blob_index();
  std::unique_ptr<BaseVectorIndex> base_index = create_base_index();

  auto blob_index_start = std::chrono::high_resolution_clock::now();

  leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
    ZoneScopedN("Build Blob Index");
    leanstore->StartTransaction();
    assert(adapter_main.Count() == FLAGS_num_vectors);
    blob_index->build_index();
    leanstore->CommitTransaction();
  });

  auto blob_index_end = std::chrono::high_resolution_clock::now();
  print_timing("Blob " + FLAGS_index_type + "  building", blob_index_start, blob_index_end);

  if (FLAGS_benchmark_baseline) {
    ZoneScopedN("Build Vector Index");
    auto baseline_index_start = std::chrono::high_resolution_clock::now();
    base_index->build_index_vec();
    auto baseline_index_end = std::chrono::high_resolution_clock::now();
    print_timing("Baseline " + FLAGS_index_type + " building", baseline_index_start, baseline_index_end);
  }

  if (FLAGS_benchmark_lookup_perf) {
    std::cout << "Starting lookup benchmark \n";
    std::cout << "num_query_vectors: " << FLAGS_num_query_vectors << "\n";
    std::cout << "num_result_vectors: " << FLAGS_num_result_vectors << "\n";

    std::cout << "Starting lookup benchmark for Blob Index\n";
    auto blob_lookup_start = std::chrono::high_resolution_clock::now();

    leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
    ZoneScopedN("Lookup Blob Index");
      leanstore->StartTransaction();
      for (uint64_t i = 0; i < FLAGS_num_query_vectors; i++) {
        auto states = blob_index->find_n_closest_vectors(query_vectors[i], FLAGS_num_result_vectors);
      }
      leanstore->CommitTransaction();
    });

    auto blob_lookup_end = std::chrono::high_resolution_clock::now();
    print_timing("Blob " + FLAGS_index_type + " lookup", blob_lookup_start, blob_lookup_end);
    print_timing_one_query("Blob " + FLAGS_index_type + " lookup 1 query", blob_lookup_start, blob_lookup_end);

    if (FLAGS_benchmark_baseline) {
      ZoneScopedN("Lookup Vector Index");
      auto baseline_lookup_start = std::chrono::high_resolution_clock::now();
      for (uint64_t i = 0; i < FLAGS_num_query_vectors; i++) {
        auto states = base_index->find_n_closest_vectors_vec(query_vectors[i], FLAGS_num_result_vectors);
      }
      auto baseline_lookup_end = std::chrono::high_resolution_clock::now();
      print_timing("Baseline lookup", baseline_lookup_start, baseline_lookup_end);
      print_timing_one_query("Baseline lookup 1 query", baseline_lookup_start, baseline_lookup_end);
    }
  }
  return 0;
}
