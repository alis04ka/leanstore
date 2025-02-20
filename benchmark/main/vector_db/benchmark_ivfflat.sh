./build/benchmark/VectorIndexBM  -worker_count=1 \
                            -bm_virtual_gb=16 \
                            -bm_physical_gb=4 \
                            -db_path=/dev/nvme0n1p3 \
                            -blob_logging_variant=1 \
                            --index_type="ivfflat" \
                            --benchmark_baseline=true \
                            --vector_size=5000 \
                            --num_vectors=1000 \
                            --std_dev=5.0 \
                            --num_centroids=12 \
                            --num_probe_centroids=12 \
                            --num_iterations=5 \
                            --benchmark_lookup_perf=true \
                            --num_query_vectors=100 \
                            --num_result_vectors=10 \
