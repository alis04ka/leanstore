#pragma once
#include "storage/vectordb/ivfflat_adapter.h"

namespace leanstore::storage::vector {

float distance_vec(std::span<const float> span1, std::span<const float> span2);
float distance_blob(VectorAdapter &db, const BlobState *blob1, const BlobState *blob2);
float distance_vec_blob(VectorAdapter &db, std::span<const float> input_span, const BlobState *blob_state);

}