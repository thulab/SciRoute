#pragma once

#include "iotdbfs/query/core/QueryModel.h"

#include <atomic>
#include <array>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace iotdbfs {

// Copies share the same signal. Cancellation never takes a cache/source lock.
class CancellationToken {
public:
  CancellationToken() : flag_(std::make_shared<std::atomic<bool>>(false)) {}
  void cancel() const { flag_->store(true, std::memory_order_release); }
  bool cancelled() const { return flag_->load(std::memory_order_acquire); }
private:
  std::shared_ptr<std::atomic<bool>> flag_;
};

struct QueryTaskSchedulerConfig {
  std::size_t worker_threads{4};
  std::size_t max_queued_tasks{64};
  std::size_t max_batch_size{4};
};

struct QueryRuntimeConfig {
  // Whole, independent FS requests. Separate from fragment decode workers.
  QueryTaskSchedulerConfig query_tasks;
  std::uint64_t memory_budget_bytes{1024ULL * 1024 * 1024};
  std::size_t max_entries{1024};
  std::size_t max_inflight{64};
  std::size_t worker_threads{4};
  std::uint64_t wait_timeout_ms{2000};
  std::uint64_t max_read_bytes_per_load{1024ULL * 1024 * 1024};
  std::uint64_t max_read_ranges_per_load{65536};
  // Generic fair work admission. Training queries use one turn per bounded
  // window group while sharing the decoded fragment service above.
  std::size_t max_active_work_groups{4};
  std::size_t max_requests_per_workload{2};
  std::uint64_t max_output_bytes_per_workload{512ULL * 1024 * 1024};
  std::size_t max_queued_work_groups{1024};
  // Fixed-size detailed timers; ScopedQueryProfile also enables them per job.
  bool enable_detailed_profile{false};
};

// Fixed two-dimensional histogram of actual payload calls. Range buckets are
// <=1, <=4, <=16, <=32, >32; byte buckets are <=16 KiB, <=64 KiB,
// <=256 KiB, <=1 MiB, >1 MiB. These are observations, not I/O reservations.
struct PayloadReadShapeStats {
  std::uint64_t calls{0}, nanoseconds{0}, bytes{0}, ranges{0};
};

struct QueryRuntimeStats {
  std::uint64_t hits{0}, misses{0}, waits{0}, producer_count{0};
  std::uint64_t decode_count{0}, decoded_bytes{0}, decompressions{0};
  // Optional codec-only observations, excluding transport and admission. Wall
  // durations may overlap across producers. CPU time is sampled per decoding
  // thread; samples distinguishes unavailable clocks from a measured zero.
  // Producers that do not instrument decoding leave these counters unchanged.
  std::uint64_t decode_nanoseconds{0}, decode_cpu_nanoseconds{0}, decode_cpu_samples{0};
  std::uint64_t read_bytes{0}, read_ranges{0}, copy_bytes{0};
  // Runtime-wide cumulative wall durations, not CPU time. Loader duration
  // includes its reads/decoding but excludes admission and queue waits. Payload
  // calls count actual source read attempts (including failures), unlike the
  // read_bytes/read_ranges budget reservations. Concurrent loads can overlap.
  std::uint64_t payload_read_calls{0}, payload_read_nanoseconds{0};
  // Actual attempted transport shapes, including failed calls; not successful
  // response bytes or physical disk I/O. Unlike record_io these exclude reads
  // reserved but never issued.
  std::uint64_t payload_read_bytes{0}, payload_read_ranges{0};
  // Optional cumulative waits. Queue counts once per producer job; admission
  // counts caller waiting for an inflight slot; memory wait counts worker
  // backpressure and retired-memory release. Concurrent durations overlap.
  std::uint64_t fragment_queue_nanoseconds{0}, fragment_admission_nanoseconds{0};
  std::uint64_t fragment_memory_wait_nanoseconds{0};
  // Filter total is the sum of its three children, not an additional stage.
  // Includes failed/cancelled codec attempts; decode_count remains successes.
  std::uint64_t decode_filter_nanoseconds{0}, decode_inflate_nanoseconds{0};
  std::uint64_t decode_shuffle_nanoseconds{0}, decode_checksum_nanoseconds{0};
  std::uint64_t decode_conversion_nanoseconds{0};
  // Gauges: calls currently inside source I/O, and maximum simultaneous calls
  // during this runtime's lifetime. Never subtract/add peaks as counters.
  std::uint64_t payload_reads_active{0}, payload_reads_peak{0};
  std::array<PayloadReadShapeStats, 25> payload_read_shape{};
  std::uint64_t fragment_load_nanoseconds{0};
  std::uint64_t evictions{0}, redecodes{0};
  // pinned_bytes overlaps cache_owned_bytes. live_bytes charges each resident
  // allocation once, plus in-flight output reservations and temporary buffers.
  std::uint64_t cache_owned_bytes{0}, pinned_bytes{0}, inflight_bytes{0};
  std::uint64_t temporary_bytes{0}, live_bytes{0}, peak_bytes{0};
};

class DecodedFragment {
public:
  virtual ~DecodedFragment() = default;
  virtual std::uint64_t resident_bytes() const = 0;
};

struct FragmentMemory {
  std::uint64_t resident_bytes{0};
  std::uint64_t temporary_bytes{0};
};

struct FragmentBatchItem {
  std::string key;
  FragmentMemory memory;
};

struct FragmentDecodeTimings {
  std::uint64_t inflate_nanoseconds{0}, shuffle_nanoseconds{0};
  std::uint64_t checksum_nanoseconds{0}, conversion_nanoseconds{0};
};

class FragmentLoadContext {
public:
  const CancellationToken& cancellation() const { return cancellation_; }
  bool cancelled() const { return cancellation_.cancelled(); }
  // Reserve/count actual I/O before reading. A failure is sticky even if a
  // loader accidentally ignores the returned status.
  QueryStatus record_io(std::uint64_t bytes, std::uint64_t ranges = 1) const;
  // Pair one begin with record_payload_read on every exit, including failure.
  void begin_payload_read() const;
  void record_payload_read(std::uint64_t bytes, std::uint64_t ranges,
    std::uint64_t elapsed_nanoseconds) const;
  void record_decode(std::uint64_t bytes, std::uint64_t decompressions = 0,
    std::uint64_t elapsed_nanoseconds = 0, std::uint64_t cpu_nanoseconds = 0,
    std::uint64_t cpu_samples = 0) const;
  void record_copy(std::uint64_t bytes) const;
  bool detailed_profile_enabled() const noexcept;
  void record_decode_stages(const FragmentDecodeTimings&) const noexcept;
private:
  friend class DecodedFragmentCache;
  struct Impl;
  FragmentLoadContext(CancellationToken cancellation, std::shared_ptr<Impl> impl)
    : cancellation_(std::move(cancellation)), impl_(std::move(impl)) {}
  CancellationToken cancellation_;
  std::shared_ptr<Impl> impl_;
};

// Copying either this lease or shared() keeps the underlying allocation charged
// and pinned, including after eviction or destruction of the owning runtime.
class FragmentLease {
public:
  FragmentLease() = default;
  std::shared_ptr<const DecodedFragment> shared() const { return fragment_; }
  template<class T> std::shared_ptr<const T> as() const {
    return std::dynamic_pointer_cast<const T>(fragment_);
  }
  explicit operator bool() const { return static_cast<bool>(fragment_); }
private:
  friend class DecodedFragmentCache;
  explicit FragmentLease(std::shared_ptr<const DecodedFragment> fragment)
    : fragment_(std::move(fragment)) {}
  std::shared_ptr<const DecodedFragment> fragment_;
};

// The budget covers loader-declared resident and temporary data buffers plus
// metadata fee traces collected by shared producers. Each waiter replays fees
// into its own request ledger, including on cache hits; request budget failure
// does not poison a shared entry. Batch trace memory is charged per member.
// An admitted producer also pins the caller's execution lease until its loader
// and captured sources are released, even after all waiters cancel. Completed
// cache entries and fragment leases do not retain the execution token.
// Small
// control blocks are bounded by max_entries/max_inflight, not byte-accounted.
// Loaders must honor their declared upper bound and not retain untracked copies
// of the returned allocation. They must not synchronously recurse into get().
class DecodedFragmentCache {
public:
  using Loader = std::function<QueryResult<std::shared_ptr<const DecodedFragment>>(
    const FragmentLoadContext&)>;
  // A batch loader receives only the item indexes this call claimed as misses.
  // It returns one fragment per index, in that order. Cached and in-flight keys
  // are joined through the same single-flight entries used by get().
  using BatchLoader = std::function<QueryResult<std::vector<std::shared_ptr<const DecodedFragment>>>(
    const std::vector<std::size_t>&, const FragmentLoadContext&)>;
  explicit DecodedFragmentCache(QueryRuntimeConfig config = {});
  ~DecodedFragmentCache();
  DecodedFragmentCache(const DecodedFragmentCache&) = delete;
  DecodedFragmentCache& operator=(const DecodedFragmentCache&) = delete;
  QueryResult<FragmentLease> get(const std::string& key, FragmentMemory memory,
    Loader loader, CancellationToken waiter = {});
  QueryResult<std::vector<FragmentLease>> get_batch(const std::vector<FragmentBatchItem>& items,
    std::uint64_t shared_temporary_bytes, BatchLoader loader, CancellationToken waiter = {});
  QueryRuntimeStats stats() const;
  void record_copy(std::uint64_t bytes);
  const QueryRuntimeConfig& config() const;
  void evict_all();
private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace iotdbfs
