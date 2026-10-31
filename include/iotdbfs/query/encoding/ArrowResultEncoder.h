#pragma once

#include "iotdbfs/query/core/QueryModel.h"

#include <cstdint>
#include <vector>

namespace iotdbfs {

class QueryBatchCursor;

// Produces one Arrow IPC stream. The schema is fixed for the entire stream;
// each record batch carries its own domain and source provenance in IPC
// custom metadata. No partial stream is returned after a failure.
class ArrowResultEncoder {
public:
  static constexpr std::size_t max_metadata_entries = 128;
  static constexpr std::size_t max_metadata_key_bytes = 256;
  static constexpr std::uint64_t max_metadata_bytes = 64ULL * 1024;

  // Schema descriptors are scoped by the caller. Reject duplicate or reserved
  // keys before allocating Arrow metadata; the stream cap also includes them.
  static QueryStatus validate_metadata(const QueryResultMetadata& metadata,
    std::uint64_t max_ipc_bytes);

  static QueryResult<BinaryValue> encode_query_batch(
    const QueryBatch& batch, std::uint64_t max_ipc_bytes,
    const QueryResultMetadata& metadata = {});

  // An empty vector writes a schema-only stream. The supplied schema is also
  // checked against every batch, including display metadata and nullability.
  static QueryResult<BinaryValue> encode_query_batches(
    const LogicalSchema& schema, const std::vector<QueryBatch>& batches,
    std::uint64_t max_ipc_bytes, const QueryResultMetadata& metadata = {});

  // Pulls batches incrementally and enforces the cap across the entire stream.
  // The cursor is cancelled on any error, including an encoding failure.
  static QueryResult<BinaryValue> encode_query_cursor(
    const LogicalSchema& schema, QueryBatchCursor& cursor,
    std::uint64_t max_ipc_bytes, const QueryResultMetadata& metadata = {});
};

} // namespace iotdbfs
