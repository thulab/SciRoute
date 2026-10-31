#pragma once

#include "iotdbfs/query/training/TrainingQueryModel.h"

namespace iotdbfs {

// One fixed Arrow IPC stream schema for a bounded sample group. Each row is a
// (sample, role, step) position. The opposite role's value columns are null;
// nulls in the active role carry the training validity mask. Only the first
// row of each sample carries its ID and complete descriptor/provenance; other
// rows use sample_index, including across record-batch boundaries. Schema
// metadata records the view's identity or standardization transform and its
// complete per-channel training statistics.
QueryResult<BinaryValue> encode_window_batch_ipc(const TrainingViewSpec& view,
  const WindowBatch& batch, std::uint64_t max_ipc_bytes);

} // namespace iotdbfs
