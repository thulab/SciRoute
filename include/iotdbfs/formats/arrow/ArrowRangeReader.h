#pragma once

#include "iotdbfs/formats/arrow/ArrowLayout.h"
#include "iotdbfs/query/core/QueryPlanning.h"

#include "iotdbfs/query/core/DecodedFragmentCache.h"

namespace iotdbfs {

struct ArrowDecodeConfig {
  std::uint64_t tile_elements{4096};
  std::uint64_t max_buffer_bytes{64 * 1024 * 1024};
};

struct ArrowReadMetrics {
  std::uint64_t payload_bytes{0}, read_ranges{0}, decoded_bytes{0};
  std::uint64_t decompressions{0};
};

struct ArrowDecodeMetrics {
  std::uint64_t logical_bytes{0}, logical_ranges{0}, output_bytes{0};
};

// One execution's bounded physical reader. Calls are sequential except cancel(),
// which may run concurrently. A source must outlive this reader.
class ArrowRangeReader {
public:
  ArrowRangeReader(const QuerySource& source, QueryExecutionOptions options = {},
    ArrowDecodeConfig config = {}, CancellationToken cancellation = {},
    const FragmentLoadContext* service = nullptr);
  QueryResult<BinaryValue> read(ArrowBufferSpan span);
  // IPC buffers may contain trailing native bytes. The codec must produce
  // exactly its declared length, which must cover minimum_decoded_bytes.
  QueryResult<BinaryValue> decode(ArrowBufferSpan span,
    ArrowBufferCompression compression, std::uint64_t minimum_decoded_bytes);
  // Decode bytes already fetched by the training adapter's bounded multi-range
  // request. This shares codec validation with the scalar reader without
  // charging or issuing another physical read.
  QueryResult<BinaryValue> decode_buffer(BinaryValue bytes,
    ArrowBufferCompression compression, std::uint64_t minimum_decoded_bytes);
  void cancel();
  bool cancelled() const;
  const ArrowReadMetrics& metrics() const { return metrics_; }

private:
  const QuerySource& source_;
  QueryExecutionOptions options_;
  ArrowDecodeConfig config_;
  CancellationToken cancellation_;
  const FragmentLoadContext* service_;
  std::uint64_t reserved_bytes_{0}, reserved_ranges_{0};
  ArrowReadMetrics metrics_;
};

} // namespace iotdbfs
