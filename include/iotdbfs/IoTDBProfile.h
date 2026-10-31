#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

namespace iotdbfs {
// Daemon-wide counters across every IoTDBClient/backend instance. These are
// client API observations, never database server disk or internal RPC metrics.
#define IOTDBFS_IOTDB_PROFILE_COUNTERS(X) \
  X(session_acquire_calls) X(session_acquire_timeouts) X(session_acquire_nanoseconds) \
  X(query_calls) X(query_failures) X(query_nanoseconds) \
  X(metadata_query_calls) X(metadata_query_nanoseconds) \
  X(version_store_query_calls) X(version_store_batch_query_calls) X(version_store_head_reads) \
  X(version_store_object_reads) X(version_store_blob_bytes) \
  X(payload_calls) X(payload_ranges) X(payload_requested_bytes) X(payload_returned_bytes) \
  X(payload_failures) X(payload_nanoseconds) \
  X(layout_calls) X(layout_requested_bytes) X(layout_returned_bytes) \
  X(layout_failures) X(layout_nanoseconds)

struct IoTDBProfileSnapshot {
#define FIELD(name) std::uint64_t name{};
  IOTDBFS_IOTDB_PROFILE_COUNTERS(FIELD)
  FIELD(session_leases_active) FIELD(session_leases_peak)
#undef FIELD
};
struct IoTDBProfileCounters {
#define FIELD(name) std::atomic<std::uint64_t> name{0};
  IOTDBFS_IOTDB_PROFILE_COUNTERS(FIELD)
  FIELD(session_leases_active) FIELD(session_leases_peak)
#undef FIELD
  std::atomic<bool> timing_enabled{false};
};
inline IoTDBProfileCounters iotdb_profile_counters;
inline IoTDBProfileSnapshot iotdb_profile_snapshot() noexcept {
  IoTDBProfileSnapshot result;
#define READ(name) result.name = iotdb_profile_counters.name.load(std::memory_order_relaxed);
  IOTDBFS_IOTDB_PROFILE_COUNTERS(READ)
  READ(session_leases_active) READ(session_leases_peak)
#undef READ
  return result;
}
inline void iotdb_profile_lease_acquired() noexcept {
  auto& counters = iotdb_profile_counters;
  const auto active = counters.session_leases_active.fetch_add(1, std::memory_order_relaxed) + 1;
  auto peak = counters.session_leases_peak.load(std::memory_order_relaxed);
  while (peak < active && !counters.session_leases_peak.compare_exchange_weak(
      peak, active, std::memory_order_relaxed)) {}
}
class IoTDBProfileTimer {
public:
  explicit IoTDBProfileTimer(std::atomic<std::uint64_t>& destination) noexcept
    : destination_(iotdb_profile_counters.timing_enabled.load(std::memory_order_relaxed)
        ? &destination : nullptr), start_(destination_ ? Clock::now() : Clock::time_point{}) {}
  ~IoTDBProfileTimer() noexcept {
    if (destination_) destination_->fetch_add(static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start_).count()),
      std::memory_order_relaxed);
  }
private:
  using Clock = std::chrono::steady_clock;
  std::atomic<std::uint64_t>* destination_;
  Clock::time_point start_;
};
class IoTDBReadObservation {
public:
  IoTDBReadObservation(bool payload, std::uint64_t ranges, std::uint64_t bytes) noexcept
    : returned_(payload ? iotdb_profile_counters.payload_returned_bytes : iotdb_profile_counters.layout_returned_bytes),
      failed_(payload ? iotdb_profile_counters.payload_failures : iotdb_profile_counters.layout_failures),
      timer_(payload ? iotdb_profile_counters.payload_nanoseconds : iotdb_profile_counters.layout_nanoseconds) {
    (payload ? iotdb_profile_counters.payload_calls : iotdb_profile_counters.layout_calls).fetch_add(1, std::memory_order_relaxed);
    (payload ? iotdb_profile_counters.payload_requested_bytes : iotdb_profile_counters.layout_requested_bytes).fetch_add(bytes, std::memory_order_relaxed);
    if (payload) iotdb_profile_counters.payload_ranges.fetch_add(ranges, std::memory_order_relaxed);
  }
  ~IoTDBReadObservation() noexcept { if (!success_) failed_.fetch_add(1, std::memory_order_relaxed); }
  void success(std::uint64_t returned) noexcept { returned_.fetch_add(returned, std::memory_order_relaxed); success_ = true; }
private:
  std::atomic<std::uint64_t>& returned_;
  std::atomic<std::uint64_t>& failed_;
  IoTDBProfileTimer timer_;
  bool success_{false};
};
} // namespace iotdbfs
