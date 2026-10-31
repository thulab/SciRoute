#pragma once

#include "iotdbfs/formats/arrow/ArrowRangeReader.h"
#include "iotdbfs/query/core/QueryRuntime.h"

#include <optional>

namespace iotdbfs {

struct ArrowDecodedFragment final : DecodedFragment {
  std::uint64_t begin{0}, end{0};
  std::vector<float> values;
  BinaryValue validity;
  std::uint64_t resident_bytes() const override {
    return values.capacity() * sizeof(float) + validity.capacity();
  }
};

struct ArrowFragmentDemand {
  std::uint64_t batch{0};
  std::uint32_t field{0};
  std::uint64_t leaf_position{0};
  std::uint64_t demand_elements{1};
};

struct ArrowFragmentBatchItem {
  std::shared_ptr<const ArrowDecodedFragment> fragment;
  std::string key_hex;
};

// Immutable physical fragments, shared through a runtime. The source must be an
// owned, unbudgeted exact-version capability; serialized L must have been checked
// against that source. Namespace/source/version/layout/decode identity form the
// key. Windows, views and selected channel sets do not. Metadata dependencies
// are cached separately and pinned for this provider's lifetime.
// get/metrics/account_output are sequential; cancel only signals this waiter.
class ArrowFragmentProvider {
public:
  ArrowFragmentProvider(std::shared_ptr<const QuerySource> source,
    std::shared_ptr<const BinaryValue> layout,
    std::shared_ptr<QueryRuntime> runtime,
    QueryExecutionOptions options = {}, ArrowDecodeConfig config = {},
    std::optional<CancellationToken> cancellation = std::nullopt);
  ~ArrowFragmentProvider();
  ArrowFragmentProvider(const ArrowFragmentProvider&) = delete;
  ArrowFragmentProvider& operator=(const ArrowFragmentProvider&) = delete;
  QueryResult<std::shared_ptr<const ArrowDecodedFragment>> get(
    std::uint64_t batch, std::uint32_t field, std::uint64_t leaf_position,
    std::uint64_t demand_elements = 1);
  // Training-only bounded demand batch; returns results in demand order.
  // Cache identities and decoded fragments are identical to get().
  QueryResult<std::vector<ArrowFragmentBatchItem>> get_batch(
    const std::vector<ArrowFragmentDemand>& demands);
  void cancel();
  void account_output(std::uint64_t bytes);
  const ArrowDecodeMetrics& metrics() const;
  // Stable opaque identity of the fragment returned by the latest successful
  // get(). Calls to get()/this accessor are sequential within one provider.
  const std::string& last_fragment_key() const;
private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace iotdbfs
