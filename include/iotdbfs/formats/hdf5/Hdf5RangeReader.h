#pragma once

#include "iotdbfs/query/core/QueryPlanning.h"
#include "iotdbfs/query/core/DecodedFragmentCache.h"

namespace iotdbfs {

struct Hdf5RangeReadConfig {
  std::size_t max_parallel_reads{1}; // 1, 2, or 4 independent source calls
  std::uint64_t max_bytes_per_call{kMaxPayloadBytesPerBatch};
  std::size_t max_ranges_per_call{kMaxPayloadRangesPerBatch};
  // Bounds split-range/call control descriptors independently of byte buffers.
  std::size_t max_transport_ranges{65536};
};

// Conservative owned byte peak: all per-span outputs plus one bounded wave of
// transport responses. Overlapping input spans have distinct output buffers.
// Small split-range/call descriptors are bounded by max_transport_ranges.
QueryResult<std::uint64_t> hdf5_range_read_temporary_bytes(
  const StructuredBlobPayloadRanges&, Hdf5RangeReadConfig = {});

// Merges adjacent/overlapping spans, splits oversized ranges, and batches
// disjoint ranges when the source advertises support. Results preserve input
// order. Uses only producer cancellation/service I/O accounting: callers must
// pass shared_source, never a request-budgeted wrapper. Started calls are
// drained before errors/cancellation return; no background operation outlives
// this function or captures a request's token/limits.
QueryResult<std::vector<BinaryValue>> read_hdf5_payload_ranges(
  const std::shared_ptr<const QuerySource>& shared_source,
  const StructuredBlobPayloadRanges&, const FragmentLoadContext&,
  Hdf5RangeReadConfig = {});

} // namespace iotdbfs
