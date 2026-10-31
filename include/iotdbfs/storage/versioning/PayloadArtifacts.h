#pragma once

#include "iotdbfs/storage/versioning/VersionDirectory.h"
#include "iotdbfs/StructuredBlobStorage.h"
#include <functional>

namespace iotdbfs::versioning {
inline constexpr std::uint64_t kPayloadRequestBytes = 1ULL << 30;
inline constexpr std::uint64_t kPayloadRequestExtents = 65536;
inline constexpr std::size_t kPayloadMapEncodedBytes = 93;

struct PayloadExtent {
  std::uint64_t logical_offset{0}, length{0}, content_offset{0}, content_size{0};
  ContentId content_id;
};
struct PayloadChild {
  std::uint64_t begin{0}, end{0}, extent_count{0}, encoded_size{0};
  ContentId page;
};
struct ExtentPage {
  std::uint8_t level{0};
  std::uint64_t begin{0}, end{0}, extent_count{0};
  std::vector<PayloadExtent> extents;
  std::vector<PayloadChild> children;
};
struct PayloadMap {
  std::uint64_t payload_size{0}, extent_count{0};
  std::uint8_t height{0};
  std::uint64_t page_bytes{0};
  ContentId page;
};
Bytes encode_extent_page(const ExtentPage&);
ExtentPage decode_extent_page(const Bytes&);
Bytes encode_payload_map(const PayloadMap&);
PayloadMap decode_payload_map(const Bytes&);

// Immutable complete fixed-block maps. No previous-version links, payload
// cache or authorization state. Owner owns the bounded metadata page cache.
class PayloadArtifacts {
public:
  explicit PayloadArtifacts(std::shared_ptr<Owner>);
  ContentId build(Transaction&, std::uint64_t size,
    const std::function<Bytes(std::uint64_t, std::uint64_t)>& staging_reader);
  void verify(const ContentId&, std::uint64_t size, bool deep = false,
    std::uint64_t* shared_metadata_bytes = nullptr,
    const std::string& logical_source = {}) const;
  StructuredBlobPayloadBatch read_ranges(const ContentId&, std::uint64_t size,
    const StructuredBlobPayloadRanges&, const std::string& logical_source = {}) const;
private:
  std::shared_ptr<Owner> owner_;
};
} // namespace iotdbfs::versioning
