#pragma once

#include "iotdbfs/formats/hdf5/Hdf5Layout.h"
#include "iotdbfs/query/core/DecodedFragmentCache.h"
#include <functional>
#include <memory>

namespace iotdbfs {
struct Hdf5ChunkDecoderConfig {
  std::uint64_t max_decoded_bytes{64ULL * 1024 * 1024};
  // Hard cap for all simultaneously live zlib allocations, including our
  // allocation headers. No native HDF5 filter or dynamic plugin is invoked.
  std::uint64_t max_zlib_scratch_bytes{1024ULL * 1024};
};

// Immutable and safe to reuse across chunks/requests. Compiles the canonical
// subsequences of shuffle -> DEFLATE -> Fletcher32, plus the GWOSC native
// Fletcher32 -> DEFLATE pipeline, once per dataset. Mask bits
// refer to positions in the declared pipeline, including skipped filters.
class Hdf5ChunkDecoder {
public:
  static QueryResult<std::shared_ptr<const Hdf5ChunkDecoder>> compile(
    const Hdf5DatasetLayout&, Hdf5ChunkDecoderConfig = {});
  std::uint64_t decoded_bytes() const { return decoded_bytes_; }
  // Peak owned buffer + codec allocation capacity. Pass encoded.capacity()
  // after reading; size is sufficient for pre-admission of an exact buffer.
  QueryResult<std::uint64_t> working_set_bytes(
    std::uint64_t encoded_capacity, std::uint32_t filter_mask) const;
  // Consumes the source buffer and returns native-endian full-chunk bytes.
  // Cancellation is checked between bounded processing blocks. A callback
  // exception becomes Internal; no exception crosses this boundary.
  QueryResult<BinaryValue> decode(BinaryValue encoded, std::uint32_t filter_mask,
    const std::function<bool()>& cancelled = {},
    FragmentDecodeTimings* timings = nullptr) const;
private:
  Hdf5ChunkDecoder() = default;
  std::uint64_t inflated_bytes(std::uint32_t filter_mask) const;
  std::uint64_t decoded_bytes_{0};
  std::size_t element_bytes_{0};
  bool swap_endian_{false};
  Hdf5ChunkDecoderConfig config_;
  std::vector<std::uint32_t> filters_;
};
} // namespace iotdbfs
