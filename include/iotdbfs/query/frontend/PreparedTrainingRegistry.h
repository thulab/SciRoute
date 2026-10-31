#pragma once

#include "iotdbfs/query/training/arrow/TrainingQueryModule.h"
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <sys/types.h>

namespace iotdbfs {

// Prepared metadata has a separate budget from decoded fragments and IPC bytes.
// Charges survive ID retirement while an executing request or route retains a lease.
class PreparedTrainingRegistry {
public:
  using Clock = std::chrono::steady_clock;
  struct Config {
    std::size_t max_entries{64};
    std::uint64_t max_bytes{256ULL * 1024 * 1024};
    std::uint64_t max_item_bytes{64ULL * 1024 * 1024};
    std::size_t max_entries_per_uid{16};
    std::size_t max_routes_per_entry{1024};
    std::chrono::milliseconds ttl{std::chrono::minutes(5)};
    std::function<Clock::time_point()> clock{Clock::now};
  };
  struct Stats { std::size_t resident_entries{0}; std::uint64_t resident_bytes{0}; };
  using RetireRoute = std::function<void(const std::string&)>;
  PreparedTrainingRegistry(Config config, RetireRoute retire);
  ~PreparedTrainingRegistry();
  QueryResult<std::string> insert(uid_t uid, std::shared_ptr<const TrainingQueryModule> module);
  QueryResult<std::shared_ptr<const TrainingQueryModule>> acquire(const std::string& id, uid_t uid);
  QueryStatus track_route(const std::string& id, uid_t uid, const std::string& route);
  bool release_result(const std::string& id, uid_t uid, const std::string& route);
  bool erase(const std::string& id, uid_t uid);
  void reap();
  void clear();
  Stats stats() const;
  std::uint64_t ttl_ms() const { return static_cast<std::uint64_t>(config_.ttl.count()); }
private:
  struct Accounting;
  struct Lease;
  struct Entry {
    uid_t uid;
    std::shared_ptr<const TrainingQueryModule> module;
    Clock::time_point expires;
    std::unordered_set<std::string> routes;
  };
  Config config_;
  RetireRoute retire_;
  std::shared_ptr<Accounting> accounting_;
  std::mutex mutex_;
  std::unordered_map<std::string, Entry> entries_;
};
} // namespace iotdbfs
