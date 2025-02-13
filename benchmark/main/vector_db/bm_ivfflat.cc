#include "share_headers/perf_event.h"
#include "storage/vectordb/ivfflat.h"
#include "storage/vectordb/ivfflat_vec.h"
#include "storage/vectordb/vector_adapter.h"
#include <random>

using namespace leanstore::storage::vector;

DEFINE_uint64(num_centroids, 128, "Number of centroids");
DEFINE_uint64(num_probe_centroids, 10, "Number of centroids to probe");
DEFINE_uint64(vector_size, 3000, "Vector size for benchmarking");
DEFINE_uint64(num_vectors, 1000, "number of Vectors for benchmarking");
DEFINE_bool(normalize_vectors, false, "normalize the vectors to unit length");
DEFINE_bool(benchmark_baseline, true, "whether to also run the benchmarks for the baseline implementation");
DEFINE_bool(benchmark_lookup_perf, false, "wheter to run lookup benchmarks");
DEFINE_uint64(num_query_vectors, 1000, "how many lookup requests to send");
DEFINE_uint64(num_result_vectors, 10, "how many results the lookup should return");

static const unsigned int seed = 42;
static std::mt19937 gen(seed);

template <class T>
std::vector<float> create_random_vector(T &dist) {
  std::vector<float> vector;
  vector.reserve(FLAGS_vector_size);

  for (uint64_t j = 0; j < FLAGS_vector_size; j++) {
    vector.push_back(dist(gen));
  }

  if (FLAGS_normalize_vectors) {
    float sum = 0.0;
    for (const auto &val : vector) {
      sum += val * val;
    }
    float norm = std::sqrt(sum);
    for (auto &val : vector) {
      val /= norm;
    }
  }
  return vector;
}

int main(int argc, char **argv) {
  gflags::SetUsageMessage("IVFFlat Benchmark");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto leanstore = std::make_unique<leanstore::LeanStore>();
  VectorAdapter adapter_main = VectorAdapter::CreateVectorAdapter<VectorRecord>(*leanstore);
  VectorAdapter adapter_centroids = VectorAdapter::CreateVectorAdapter<CentroidType>(*leanstore);
  BlobAdapter blob_adapter(*leanstore);

  float stddev = 1.0f / std::sqrt(static_cast<float>(FLAGS_vector_size));
  std::normal_distribution<float> dist(0.0, stddev);

  std::vector<std::vector<float>> embedding_vectors;
  embedding_vectors.reserve(FLAGS_num_vectors);

  std::vector<std::vector<float>> query_vectors;
  query_vectors.reserve(FLAGS_num_query_vectors);

  std::cout << "Starting generating benchmark Data\n";
  std::cout << "Vector_size: " << FLAGS_vector_size << "\n";
  std::cout << "stddev: " << stddev << "\n";

  leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
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

    assert((int)adapter_main.Count() == Flags_num_vectors);
    leanstore->CommitTransaction();
  });

  std::cout << "Finished generating Data\n";

  IVFFlatIndex index(adapter_main, adapter_centroids, blob_adapter, FLAGS_num_centroids, FLAGS_num_probe_centroids, FLAGS_vector_size);
  vec::IVFFlatIndexVec index_vec(FLAGS_num_centroids, FLAGS_num_probe_centroids, FLAGS_vector_size, std::move(embedding_vectors));

  leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
    leanstore->StartTransaction();
    assert(adapter_main.Count() == Flags_num_vectors);
    index.build_index();
    leanstore->CommitTransaction();
  });

  std::cout << "Finished building Blob Index\n";

  // if (FLAGS_benchmark_baseline) {
  //   vec::IVFFlatIndexVec index(FLAGS_num_centroids, FLAGS_num_probe_centroids, FLAGS_vector_size, std::move(embedding_vectors));
  //   index.build_index_vec();
  //   std::cout << "Finished baseline Index\n";
  // }
  // if (FLAGS_benchmark_lookup_perf) {
  //   std::cout << "Starting lookup benchmark for Blob Index\n";
  //   leanstore->worker_pool.ScheduleSyncJob(0, [&]() {
  //     leanstore->StartTransaction();
  //     for (uint64_t i = 0; i < FLAGS_num_query_vectors; i++) {
  //       auto states = index.find_n_closest_vectors(query_vectors[i], FLAGS_num_result_vectors);
  //     }
  //     leanstore->CommitTransaction();
  //   });

  //   if (FLAGS_benchmark_baseline) {
  //     for (uint64_t i = 0; i < FLAGS_num_query_vectors; i++) {
  //       auto states = index_vec.find_n_closest_vectors_vec(query_vectors[i], FLAGS_num_result_vectors);
  //     }
  //   }
  // }

  return 0;
}
