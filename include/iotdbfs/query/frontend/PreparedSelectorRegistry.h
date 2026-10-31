#pragma once

#include "iotdbfs/HotQueryCache.h"
#include "iotdbfs/query/core/QueryModel.h"
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <sys/types.h>

namespace iotdbfs {

// Format-neutral, daemon-local immutable templates. A lease grants ownership,
// never authorization: execution must still validate its current principal/head.
class PreparedSelectorRegistry final {
public:
  using Clock = std::chrono::steady_clock;
  using Payload = std::shared_ptr<const void>;
  using Builder = std::function<QueryResult<Payload>()>;
  static constexpr std::uint64_t entry_overhead_bytes = 1024;
  static constexpr std::uint64_t registry_overhead_bytes = 4096;
  struct Config {
    std::size_t max_entries{64};
    std::size_t max_entries_per_uid{16};
    std::uint64_t max_bytes{8ULL * 1024 * 1024};
    std::uint64_t max_item_bytes{1024ULL * 1024};
    std::chrono::milliseconds ttl{std::chrono::minutes(5)};
    std::function<Clock::time_point()> clock{Clock::now};
    // Empty uses RAND_bytes(24), encoded as 48 lowercase hexadecimal bytes.
    // Injection is for deterministic collision/failure tests only.
    std::function<std::string()> token;
  };
  struct Stats {
    std::size_t resident_entries{0};
    std::uint64_t resident_bytes{0};
  };
  explicit PreparedSelectorRegistry(std::shared_ptr<HotQueryCache> pool);
  PreparedSelectorRegistry(std::shared_ptr<HotQueryCache> pool, Config config);
  ~PreparedSelectorRegistry();
  PreparedSelectorRegistry(const PreparedSelectorRegistry&) = delete;
  PreparedSelectorRegistry& operator=(const PreparedSelectorRegistry&) = delete;

  // reserved_bytes includes entry_overhead_bytes plus a conservative upper bound
  // on every retained payload allocation. Reserve before parsing/compiling in
  // builder. Builders must return uniquely accounted, immutable allocations.
  QueryResult<std::string> insert(uid_t uid, std::uint64_t reserved_bytes,
    Builder builder);
  struct ObservationChanged {};
  QueryResult<Payload> acquire(uid_t uid, const std::string& id,
    const std::weak_ptr<const void>* expected = nullptr);
  // Scheduling observation only: no TTL renewal, expiry removal, counters or
  // authorization. Execution must still acquire and validate the live handle.
  Payload inspect(uid_t uid, const std::string& id) const;
  bool erase(uid_t uid, const std::string& id);
  // Retire only the exact lease acquired and checked by the caller. Comparing
  // both object and shared ownership prevents a retired/reused ID (including
  // an address reused by another allocation) from erasing its replacement.
  // A null/mismatched expected lease leaves the entry and its TTL unchanged.
  bool erase(uid_t uid, const std::string& id, const Payload& expected);
  void reap();
  // Retires registrations, including pending builders. Existing leases survive.
  void clear();
  Stats stats() const;
  std::uint64_t ttl_ms() const { return static_cast<std::uint64_t>(config_.ttl.count()); }

private:
  struct Accounting;
  struct Lease;
  struct Entry { uid_t uid; Payload payload; Clock::time_point expires; };
  Config config_;
  std::shared_ptr<HotQueryCache> pool_;
  std::shared_ptr<Accounting> accounting_;
  mutable std::mutex mutex_;
  std::map<std::string, Entry> entries_;
  std::uint64_t generation_{0};
  bool generation_exhausted_{false};
};
} // namespace iotdbfs
