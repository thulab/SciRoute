#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <limits>
#include <string>

namespace iotdbfs {

// Optional synchronous-call profiling. Durations of nested stages overlap;
// cache worker activity is measured separately by QueryRuntimeStats. A caller
// owns the record and must outlive all timers in its scope.
struct QueryProfileRecord {
  std::map<std::string, std::uint64_t> nanoseconds;
  // Synchronous observations only. Worker metrics live in QueryRuntimeStats.
  std::map<std::string, std::uint64_t> counters;
};

inline thread_local QueryProfileRecord* active_query_profile = nullptr;

// Diagnostic counters saturate rather than wrap and never affect execution.
inline void query_profile_count(const char* name, std::uint64_t amount = 1) noexcept {
  if (!active_query_profile) return;
  try {
    auto& value = active_query_profile->counters[name];
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    value = amount > maximum - value ? maximum : value + amount;
  } catch (...) {}
}


class ScopedQueryProfile {
public:
  explicit ScopedQueryProfile(QueryProfileRecord* record) noexcept
    : previous_(active_query_profile) { active_query_profile = record; }
  ~ScopedQueryProfile() { active_query_profile = previous_; }
  ScopedQueryProfile(const ScopedQueryProfile&) = delete;
  ScopedQueryProfile& operator=(const ScopedQueryProfile&) = delete;
private:
  QueryProfileRecord* previous_;
};

class QueryProfileTimer {
public:
  explicit QueryProfileTimer(const char* stage) noexcept
    : record_(active_query_profile), stage_(stage),
      start_(record_ ? Clock::now() : Clock::time_point{}) {}
  ~QueryProfileTimer() noexcept {
    if (!record_) return;
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
      Clock::now() - start_).count();
    try { record_->nanoseconds[stage_] += static_cast<std::uint64_t>(elapsed); }
    catch (...) {} // Diagnostics must not change query success or failure.
  }
  QueryProfileTimer(const QueryProfileTimer&) = delete;
  QueryProfileTimer& operator=(const QueryProfileTimer&) = delete;
private:
  using Clock = std::chrono::steady_clock;
  QueryProfileRecord* record_;
  const char* stage_;
  Clock::time_point start_;
};

} // namespace iotdbfs
