#include "iotdbfs/query/core/DecodedFragmentCache.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "iotdbfs/query/core/QueryReadContext.h"
#include "iotdbfs/query/core/QueryStorageReadControl.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace iotdbfs {
namespace {
using Clock = std::chrono::steady_clock;
using BatchFragmentResult = QueryResult<std::vector<std::shared_ptr<const DecodedFragment>>>;
class LoadTimer {
public:
  explicit LoadTimer(std::uint64_t& elapsed) : elapsed_(elapsed), start_(Clock::now()) {}
  ~LoadTimer() {
    elapsed_ = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start_).count());
  }
private:
  std::uint64_t& elapsed_;
  Clock::time_point start_;
};
QueryStatus failure(QueryError code, const char* text) {
  return QueryStatus::failure(code, QueryPhase::Execution, text);
}

struct ActiveWaiter {
  Clock::time_point deadline;
  ActiveWaiter* previous{nullptr};
  ActiveWaiter* next{nullptr};
};

struct BatchJob;

struct CacheEntry {
  std::string key;
  FragmentMemory reservation;
  std::weak_ptr<BatchJob> job;
  CancellationToken cancellation;
  // The latest deadline among current waiters, updated under CacheState::mutex.
  Clock::time_point deadline{Clock::time_point::min()};
  ActiveWaiter* waiter_head{nullptr};
  std::shared_ptr<const DecodedFragment> fragment;
  // Batch members may share a trace. Its full size is conservatively charged
  // to every member so eviction never undercounts a surviving shared trace.
  std::shared_ptr<const QueryMetadataTrace> metadata_fees;
  std::shared_ptr<CacheEntry> retired_next;
  QueryStatus status;
  std::uint64_t bytes{0}, stamp{0};
  std::size_t waiters{0}, leases{0};
  bool done{false}, owned{false};

  void add_waiter(ActiveWaiter& waiter) noexcept {
    waiter.next = waiter_head;
    if (waiter_head) waiter_head->previous = &waiter;
    waiter_head = &waiter;
    ++waiters;
    deadline = std::max(deadline, waiter.deadline);
  }

  void remove_waiter(ActiveWaiter& waiter) noexcept {
    if (waiter.previous) waiter.previous->next = waiter.next;
    else waiter_head = waiter.next;
    if (waiter.next) waiter.next->previous = waiter.previous;
    --waiters;
    if (waiter.deadline >= deadline) {
      deadline = Clock::time_point::min();
      for (auto* current = waiter_head; current; current = current->next)
        deadline = std::max(deadline, current->deadline);
    }
  }
};

struct BatchJob {
  std::vector<std::shared_ptr<CacheEntry>> entries;
  std::vector<std::size_t> indices;
  // Pins execution only until producer I/O and captured source cleanup end.
  // Neither completed entries nor public fragment leases retain this token.
  // Declare before loader so exceptional job destruction releases captures first.
  std::shared_ptr<QueryExecutionLease> execution_lease;
  DecodedFragmentCache::BatchLoader loader;
  FragmentMemory reservation;
  CancellationToken cancellation;
  bool detailed_profile{false};
  Clock::time_point queued_at{};
};

struct CacheState {
  explicit CacheState(QueryRuntimeConfig value) : config(std::move(value)) {}
  QueryRuntimeConfig config;
  mutable std::mutex mutex;
  std::condition_variable cv;
  std::unordered_map<std::string, std::shared_ptr<CacheEntry>> entries;
  std::deque<std::shared_ptr<BatchJob>> queue;
  // Detached entries retain their charge until their potentially dependency-
  // releasing destructors have run outside the cache mutex.
  std::shared_ptr<CacheEntry> retired;
  bool retired_releasing{false};
  // Bounded recent eviction history: redecodes is a lower bound after history
  // rolls over, rather than keeping an unbounded set of every historical key.
  std::deque<std::string> evicted_order;
  std::unordered_set<std::string> evicted;
  QueryRuntimeStats stats;
  std::uint64_t clock{0};
  std::size_t inflight{0}, owned_entries{0}, reserved_entries{0};
  bool stopping{false};
  std::array<std::weak_ptr<StorageReadOptionalCache>,64> storage_caches;

  bool register_storage_cache(const std::shared_ptr<StorageReadOptionalCache>& cache) {
    std::lock_guard<std::mutex> lock(mutex);
    std::weak_ptr<StorageReadOptionalCache>* free=nullptr;
    for(auto& slot:storage_caches) {
      if(!slot.owner_before(cache)&&!cache.owner_before(slot))return true;
      if(!free&&slot.expired())free=&slot;
    }
    if(!free)return false;
    *free=cache;return true;
  }
  // Snapshot only weak references while locked. Cache/pin destructors can
  // reenter this budget and must run with the runtime mutex released.
  void reclaim_storage_caches(std::unique_lock<std::mutex>& lock) {
    const auto caches=storage_caches;
    lock.unlock();
    for(const auto& slot:caches)if(auto cache=slot.lock())cache->reclaim();
    lock.lock();
  }

  // A cancelled producer can outlive its last waiter. A replacement with the
  // same key may already exist by the time that producer finishes.
  void erase_if_current(const std::shared_ptr<CacheEntry>& entry) {
    auto found = entries.find(entry->key);
    if (found != entries.end() && found->second == entry) entries.erase(found);
  }

  void remember_eviction(const std::string& key) noexcept {
    try {
      if (evicted.insert(key).second) evicted_order.push_back(key);
      while (evicted_order.size() > config.max_entries) {
        evicted.erase(evicted_order.front());
        evicted_order.pop_front();
      }
    } catch (const std::bad_alloc&) {
      // Diagnostics must never turn a memory-pressure eviction into a worker
      // failure. Clear both structures if only one insertion succeeded.
      evicted.clear();
      evicted_order.clear();
    }
  }

  void detach(const std::shared_ptr<CacheEntry>& entry) {
    if (!entry->owned) return;
    entry->owned = false;
    --owned_entries;
    stats.cache_owned_bytes -= entry->bytes;
    ++stats.evictions;
    remember_eviction(entry->key);
    entries.erase(entry->key);
    if (!entry->leases) {
      entry->retired_next = std::move(retired);
      retired = entry;
    }
  }

  bool evict_one(bool allow_pinned) {
    std::shared_ptr<CacheEntry> oldest;
    for (const auto& pair : entries) {
      const auto& entry = pair.second;
      // Waiters must receive the completed result before it becomes evictable.
      if (!entry->owned || entry->waiters || (!allow_pinned && entry->leases)) continue;
      if (!oldest || entry->stamp < oldest->stamp) oldest = entry;
    }
    if (!oldest) return false;
    detach(oldest);
    return true;
  }
};

void release_retired(const std::shared_ptr<CacheState>& state,
    std::unique_lock<std::mutex>& lock) {
  // A different worker may already be destroying detached fragments. Wait for
  // its actual memory release before making another admission or eviction
  // decision, without holding the cache mutex while it runs destructors.
  state->cv.wait(lock, [&] { return !state->retired_releasing; });
  auto retired = std::move(state->retired);
  if (!retired) return;
  state->retired_releasing = true;
  lock.unlock();
  std::uint64_t released = 0;
  while (retired) {
    auto entry = std::move(retired);
    retired = std::move(entry->retired_next);
    entry->fragment.reset();
    entry->metadata_fees.reset();
    released += entry->bytes;
    entry->bytes = 0;
  }
  lock.lock();
  state->stats.live_bytes -= released;
  state->retired_releasing = false;
  state->cv.notify_all();
}

// An aliasing shared_ptr to this owner is the only public resident-data handle.
struct LeaseOwner {
  std::shared_ptr<CacheState> state;
  std::shared_ptr<CacheEntry> entry;
  ~LeaseOwner() {
    std::unique_lock<std::mutex> lock(state->mutex);
    if (--entry->leases == 0) {
      if (!entry->owned) {
        const auto bytes = entry->bytes;
        auto fragment = std::move(entry->fragment);
        auto fees = std::move(entry->metadata_fees);
        entry->bytes = 0;
        lock.unlock();
        fragment.reset();
        fees.reset();
        lock.lock();
        state->stats.live_bytes -= bytes;
        state->stats.pinned_bytes -= bytes;
      } else {
        state->stats.pinned_bytes -= entry->bytes;
      }
    }
    state->cv.notify_all();
  }
};

// Storage amplification buffers are additional to a codec's declared memory.
// A token may outlive the loader, so it retains the accounting state itself.
struct StorageTemporaryReservation {
  explicit StorageTemporaryReservation(std::shared_ptr<CacheState> value) : state(std::move(value)) {}
  std::shared_ptr<CacheState> state;
  std::uint64_t bytes{0};
  ~StorageTemporaryReservation() {
    if(!bytes)return;
    {std::lock_guard<std::mutex> lock(state->mutex);
      state->stats.temporary_bytes-=bytes;state->stats.live_bytes-=bytes;}
    state->cv.notify_all();
  }
};

// This stack node is linked only while get() holds or waits on the cache mutex.
// It is removed before the caller's stack frame can disappear, including when
// an exception unwinds get().
struct WaiterRegistration {
  CacheState& state;
  const std::shared_ptr<CacheEntry>& entry;
  ActiveWaiter waiter;

  WaiterRegistration(CacheState& state, const std::shared_ptr<CacheEntry>& entry,
    Clock::time_point deadline) noexcept
    : state(state), entry(entry), waiter{deadline} {
    entry->add_waiter(waiter);
  }

  ~WaiterRegistration() {
    entry->remove_waiter(waiter);
    if (!entry->done && entry->waiters == 0) {
      // A batch producer remains useful while any of its keys has a waiter,
      // including a scalar get() that joined after batch admission.
      const auto job = entry->job.lock();
      const bool any_waiter = job && std::any_of(job->entries.begin(), job->entries.end(),
        [](const auto& member) { return member->waiters != 0; });
      if (!any_waiter) {
        if (job) job->cancellation.cancel();
        for (const auto& member : job->entries) {
          member->cancellation.cancel();
          state.erase_if_current(member);
        }
      }
    }
    state.cv.notify_all();
  }
};

thread_local bool in_fragment_loader = false;

Clock::time_point deadline_after(std::uint64_t timeout_ms) {
  // Config validation limits this conversion to a representable duration.
  return Clock::now() + std::chrono::milliseconds(timeout_ms);
}

void poll_wait(CacheState& state, std::unique_lock<std::mutex>& lock,
  Clock::time_point deadline) {
  state.cv.wait_until(lock, std::min(deadline, Clock::now() + std::chrono::milliseconds(10)));
}
} // namespace

struct FragmentLoadContext::Impl {
  std::shared_ptr<CacheState> state;
  std::uint64_t bytes{0}, ranges{0};
  QueryStatus status;
  bool detailed_profile{false};
};

QueryStatus FragmentLoadContext::record_io(std::uint64_t bytes, std::uint64_t ranges) const {
  std::lock_guard<std::mutex> lock(impl_->state->mutex);
  if (!impl_->status.ok()) return impl_->status;
  if (cancelled()) return failure(QueryError::Cancelled, "fragment load cancelled");
  const auto& config = impl_->state->config;
  if (bytes > config.max_read_bytes_per_load - impl_->bytes ||
      ranges > config.max_read_ranges_per_load - impl_->ranges) {
    impl_->status = failure(QueryError::ResourceExhausted, "fragment load I/O budget exceeded");
    return impl_->status;
  }
  impl_->bytes += bytes;
  impl_->ranges += ranges;
  impl_->state->stats.read_bytes += bytes;
  impl_->state->stats.read_ranges += ranges;
  return QueryStatus::success();
}

void FragmentLoadContext::begin_payload_read() const {
  std::lock_guard<std::mutex> lock(impl_->state->mutex);
  auto& stats = impl_->state->stats;
  ++stats.payload_reads_active;
  stats.payload_reads_peak = std::max(stats.payload_reads_peak,
    stats.payload_reads_active);
}

void FragmentLoadContext::record_payload_read(std::uint64_t bytes,
    std::uint64_t ranges, std::uint64_t elapsed_nanoseconds) const {
  const auto range_bin = ranges <= 1 ? 0 : ranges <= 4 ? 1 :
    ranges <= 16 ? 2 : ranges <= 32 ? 3 : 4;
  const auto byte_bin = bytes <= 16384 ? 0 : bytes <= 65536 ? 1 :
    bytes <= 262144 ? 2 : bytes <= 1048576 ? 3 : 4;
  std::lock_guard<std::mutex> lock(impl_->state->mutex);
  --impl_->state->stats.payload_reads_active;
  ++impl_->state->stats.payload_read_calls;
  impl_->state->stats.payload_read_bytes += bytes;
  impl_->state->stats.payload_read_ranges += ranges;
  impl_->state->stats.payload_read_nanoseconds += elapsed_nanoseconds;
  auto& cell = impl_->state->stats.payload_read_shape[range_bin * 5 + byte_bin];
  ++cell.calls;
  cell.nanoseconds += elapsed_nanoseconds;
  cell.bytes += bytes;
  cell.ranges += ranges;
}

void FragmentLoadContext::record_decode(std::uint64_t bytes, std::uint64_t decompressions,
    std::uint64_t elapsed_nanoseconds, std::uint64_t cpu_nanoseconds,
    std::uint64_t cpu_samples) const {
  std::lock_guard<std::mutex> lock(impl_->state->mutex);
  ++impl_->state->stats.decode_count;
  impl_->state->stats.decoded_bytes += bytes;
  impl_->state->stats.decode_nanoseconds += elapsed_nanoseconds;
  impl_->state->stats.decode_cpu_nanoseconds += cpu_nanoseconds;
  impl_->state->stats.decode_cpu_samples += cpu_samples;
  impl_->state->stats.decompressions += decompressions;
}

void FragmentLoadContext::record_copy(std::uint64_t bytes) const {
  std::lock_guard<std::mutex> lock(impl_->state->mutex);
  impl_->state->stats.copy_bytes += bytes;
}

bool FragmentLoadContext::detailed_profile_enabled() const noexcept {
  return impl_->detailed_profile;
}
void FragmentLoadContext::record_decode_stages(const FragmentDecodeTimings& t) const noexcept {
  if (!impl_->detailed_profile) return;
  try {
    std::lock_guard<std::mutex> lock(impl_->state->mutex);
    auto& s = impl_->state->stats;
    s.decode_inflate_nanoseconds += t.inflate_nanoseconds;
    s.decode_shuffle_nanoseconds += t.shuffle_nanoseconds;
    s.decode_checksum_nanoseconds += t.checksum_nanoseconds;
    s.decode_filter_nanoseconds += t.inflate_nanoseconds + t.shuffle_nanoseconds + t.checksum_nanoseconds;
    s.decode_conversion_nanoseconds += t.conversion_nanoseconds;
  } catch (...) {} // Optional diagnostics cannot turn a query into a failure.
}

struct DecodedFragmentCache::Impl {
  std::shared_ptr<CacheState> state;
  std::vector<std::thread> workers;

  explicit Impl(QueryRuntimeConfig config) : state(std::make_shared<CacheState>(config)) {
    if (!config.memory_budget_bytes || !config.max_entries || !config.max_inflight ||
        !config.worker_threads || !config.wait_timeout_ms ||
        config.wait_timeout_ms > 24ULL * 60 * 60 * 1000 ||
        !config.max_read_bytes_per_load || !config.max_read_ranges_per_load)
      throw std::invalid_argument("invalid decoded fragment runtime limits");
    try {
      for (std::size_t i = 0; i < std::min(config.worker_threads, config.max_inflight); ++i)
        workers.emplace_back([shared = state] { worker(shared); });
    } catch (...) {
      shutdown();
      throw;
    }
  }

  ~Impl() { shutdown(); }

  void shutdown() {
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->stopping = true;
      for (auto& pair : state->entries) if (!pair.second->done) {
        pair.second->cancellation.cancel();
        if (auto job = pair.second->job.lock()) job->cancellation.cancel();
      }
    }
    state->cv.notify_all();
    for (auto& thread : workers) if (thread.joinable()) thread.join();
    workers.clear();
    std::unique_lock<std::mutex> lock(state->mutex);
    while (state->evict_one(true)) {}
    release_retired(state, lock);
  }

  static void worker(const std::shared_ptr<CacheState>& state) {
    for (;;) {
      std::shared_ptr<BatchJob> job;
      std::unique_lock<std::mutex> lock(state->mutex);
      state->cv.wait(lock, [&] { return state->stopping || !state->queue.empty(); });
      if (state->queue.empty()) return;
      job = std::move(state->queue.front());
      state->queue.pop_front();
      if (job->detailed_profile)
        state->stats.fragment_queue_nanoseconds += static_cast<std::uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - job->queued_at).count());
      const auto amount = job->reservation.resident_bytes + job->reservation.temporary_bytes;
      const auto count = job->entries.size();
      QueryStatus status;
      bool reserved = false;
      bool optional_reclaimed = false;
      for (;;) {
        if (state->stopping || job->cancellation.cancelled()) {
          status = failure(QueryError::Cancelled, "fragment producer cancelled");
          break;
        }
        if (state->retired_releasing || state->retired) {
          const auto start = job->detailed_profile ? Clock::now() : Clock::time_point{};
          release_retired(state, lock);
          if (job->detailed_profile) state->stats.fragment_memory_wait_nanoseconds += static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());
          continue;
        }
        // Free an unleased entry first, even when an older entry has a lease.
        // Evict a pinned entry only if every eligible entry is pinned.
        while (state->owned_entries + state->reserved_entries > state->config.max_entries - count) {
          if (!state->evict_one(false) && !state->evict_one(true)) break;
        }
        if (state->retired) {
          const auto start = job->detailed_profile ? Clock::now() : Clock::time_point{};
          release_retired(state, lock);
          if (job->detailed_profile) state->stats.fragment_memory_wait_nanoseconds += static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());
          continue;
        }
        if(amount>state->config.memory_budget_bytes-state->stats.live_bytes&&!optional_reclaimed) {
          optional_reclaimed=true;
          state->reclaim_storage_caches(lock);
          continue;
        }
        if (amount > state->config.memory_budget_bytes - state->stats.live_bytes &&
            state->evict_one(false)) {
          // detach() cannot lower live_bytes until fragment destruction, which
          // must run unlocked. Recheck the budget after this single eviction.
          const auto start = job->detailed_profile ? Clock::now() : Clock::time_point{};
          release_retired(state, lock);
          if (job->detailed_profile) state->stats.fragment_memory_wait_nanoseconds += static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());
          continue;
        }
        if (state->owned_entries + state->reserved_entries <= state->config.max_entries - count &&
            amount <= state->config.memory_budget_bytes - state->stats.live_bytes) {
          state->stats.live_bytes += amount;
          state->stats.inflight_bytes += job->reservation.resident_bytes;
          state->stats.temporary_bytes += job->reservation.temporary_bytes;
          state->stats.peak_bytes = std::max(state->stats.peak_bytes, state->stats.live_bytes);
          state->reserved_entries += count;
          reserved = true;
          break;
        }
        auto deadline = Clock::time_point::min();
        for (const auto& entry : job->entries)
          deadline = std::max(deadline, entry->deadline);
        if (Clock::now() >= deadline) {
          status = failure(QueryError::ResourceExhausted, "fragment memory backpressure timed out");
          break;
        }
        const auto start = job->detailed_profile ? Clock::now() : Clock::time_point{};
        poll_wait(*state, lock, deadline);
        optional_reclaimed=false; // A concurrent load may have added residency.
        if (job->detailed_profile) state->stats.fragment_memory_wait_nanoseconds += static_cast<std::uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());
      }
      lock.unlock();
      std::shared_ptr<FragmentLoadContext::Impl> context_impl;
      std::uint64_t total_actual = 0, load_nanoseconds = 0;
      std::uint64_t trace_reserved = 0, trace_bytes = 0;
      std::shared_ptr<const QueryMetadataTrace> metadata_fees;
      auto result = BatchFragmentResult::failure(status.ok()
        ? failure(QueryError::Internal, "fragment loader did not run") : status);
      if (reserved) {
        in_fragment_loader = true;
        try {
          ScopedQueryMetadataProducer neutral_producer;
          ScopedQueryMetadataTrace trace("", [&](std::uint64_t added) {
            std::unique_lock<std::mutex> memory_lock(state->mutex);
            if (added > (state->config.memory_budget_bytes-state->stats.live_bytes)/count) {
              memory_lock.unlock();
              reclaim_storage_read_optional();
              memory_lock.lock();
              state->reclaim_storage_caches(memory_lock);
            }
            // Account trace growth as it happens, in addition to the loader's
            // decoded/temporary reservation. Batch charging is conservative.
            while (added > (state->config.memory_budget_bytes-state->stats.live_bytes)/count) {
              if (!state->evict_one(false)) throw QueryMetadataBudgetExceeded();
              release_retired(state,memory_lock);
            }
            const auto charge=added*count;
            trace_reserved+=charge;
            state->stats.live_bytes+=charge;
            state->stats.temporary_bytes+=charge;
            state->stats.peak_bytes=std::max(state->stats.peak_bytes,state->stats.live_bytes);
          });
          context_impl = std::make_shared<FragmentLoadContext::Impl>();
          context_impl->state = state;
          context_impl->detailed_profile = job->detailed_profile;
          FragmentLoadContext context(job->cancellation, context_impl);
          ScopedQueryStorageReadControl storage_control([&] {
            auto checked=context.record_io(0,0);
            if(!checked.ok())throw QueryStorageReadInterrupted(std::move(checked));
          },[&](std::uint64_t bytes)->std::shared_ptr<void> {
            if(!bytes)return {};
            auto token=std::make_shared<StorageTemporaryReservation>(state);
            std::unique_lock<std::mutex> memory_lock(state->mutex);
            if(bytes>state->config.memory_budget_bytes-state->stats.live_bytes) {
              memory_lock.unlock();
              reclaim_storage_read_optional();
              memory_lock.lock();
              state->reclaim_storage_caches(memory_lock);
            }
            while(bytes>state->config.memory_budget_bytes-state->stats.live_bytes) {
              if(!state->evict_one(false)) {
                context_impl->status=failure(QueryError::ResourceExhausted,"storage read temporary memory exceeds runtime budget");
                throw QueryStorageReadInterrupted(context_impl->status);
              }
              release_retired(state,memory_lock);
            }
            state->stats.live_bytes+=bytes;state->stats.temporary_bytes+=bytes;
            state->stats.peak_bytes=std::max(state->stats.peak_bytes,state->stats.live_bytes);
            token->bytes=bytes;
            return token;
          },[&](std::uint64_t bytes,std::uint64_t ranges) {
            auto checked=context.record_io(bytes,ranges);
            if(!checked.ok())throw QueryStorageReadInterrupted(std::move(checked));
          },[&](const std::shared_ptr<StorageReadOptionalCache>& cache) {
            return state->register_storage_cache(cache);
          });
          {
            LoadTimer timer(load_nanoseconds);
            result = job->loader(job->indices, context);
          }
          if(trace.failed()) throw QueryMetadataBudgetExceeded();
          trace_bytes=trace.estimated_bytes();
          if(trace_bytes) metadata_fees=std::make_shared<const QueryMetadataTrace>(trace.release());
        }
        catch (const QueryStorageReadInterrupted& error) {
          result = BatchFragmentResult::failure(error.status());
        }
        catch (const QueryMetadataBudgetExceeded&) {
          result = BatchFragmentResult::failure(failure(QueryError::ResourceExhausted, "fragment metadata trace exceeds runtime budget"));
        }
        catch (const std::bad_alloc&) {
          result = BatchFragmentResult::failure(failure(QueryError::ResourceExhausted, "fragment allocation failed"));
        } catch (const std::exception&) {
          result = BatchFragmentResult::failure(failure(QueryError::Internal, "fragment loader threw an exception"));
        } catch (...) {
          result = BatchFragmentResult::failure(failure(QueryError::Internal, "fragment loader threw an exception"));
        }
        in_fragment_loader = false;
      }
      // Drop captured source handles before marking completion. The worker never
      // invokes either the loader or its capture destructors while holding mutex.
      job->loader = {};
      job->execution_lease.reset();
      lock.lock();
      // Publish timing before completion wakes any waiter taking a stats snapshot.
      state->stats.fragment_load_nanoseconds += load_nanoseconds;
      status = result.status();
      if (context_impl && !context_impl->status.ok()) status = context_impl->status;
      bool any_waiter = false;
      for (const auto& entry : job->entries) any_waiter |= entry->waiters != 0;
      if (state->stopping || job->cancellation.cancelled() || !any_waiter)
        status = failure(QueryError::Cancelled, "fragment producer has no active waiters");
      if (status.ok()) {
        if (result.value().size() != count)
          status = failure(QueryError::Internal, "fragment loader returned the wrong number of fragments");
        else for (std::size_t i = 0; i < count; ++i) {
          const auto& fragment = result.value()[i];
          if (!fragment) {
            status = failure(QueryError::Internal, "fragment loader returned null");
            break;
          }
          const auto bytes = fragment->resident_bytes();
          if (bytes > job->entries[i]->reservation.resident_bytes) {
            status = failure(QueryError::ResourceExhausted, "fragment exceeded its resident reservation");
            break;
          }
          total_actual += bytes;
        }
      }
      if (status.ok()) {
        for (std::size_t i = 0; i < count; ++i) {
          const auto& entry = job->entries[i];
          entry->fragment = std::move(result.value()[i]);
          entry->metadata_fees = metadata_fees;
          entry->bytes = entry->fragment->resident_bytes()+trace_bytes;
          entry->owned = true;
          entry->stamp = ++state->clock;
          ++state->owned_entries;
        }
        total_actual+=trace_reserved;
        state->stats.cache_owned_bytes += total_actual;
      } else {
        // Free a rejected result before releasing its reservation.
        if (result.ok()) {
          auto rejected = std::move(result.value());
          lock.unlock();
          rejected.clear();
          lock.lock();
        }
        lock.unlock();
        metadata_fees.reset();
        lock.lock();
        for (const auto& entry : job->entries) state->erase_if_current(entry);
      }
      if (reserved) {
        state->reserved_entries -= count;
        state->stats.inflight_bytes -= job->reservation.resident_bytes;
        state->stats.temporary_bytes -= job->reservation.temporary_bytes+trace_reserved;
        state->stats.live_bytes -= amount+trace_reserved;
        if (status.ok()) state->stats.live_bytes += total_actual;
      }
      for (const auto& entry : job->entries) {
        entry->done = true;
        entry->status = status;
      }
      state->inflight -= count;
      lock.unlock();
      state->cv.notify_all();
    }
  }
};

DecodedFragmentCache::DecodedFragmentCache(QueryRuntimeConfig config)
  : impl_(std::make_unique<Impl>(std::move(config))) {}
DecodedFragmentCache::~DecodedFragmentCache() = default;

QueryResult<FragmentLease> DecodedFragmentCache::get(const std::string& key,
  FragmentMemory memory, Loader loader, CancellationToken waiter) try {
  using Result = QueryResult<FragmentLease>;
  auto state = impl_->state;
  const auto deadline = deadline_after(state->config.wait_timeout_ms);
  const bool detailed_profile = state->config.enable_detailed_profile || active_query_profile;
  if (key.empty() || key.size() > 16 * 1024 || !loader)
    return Result::failure(failure(QueryError::InvalidQuery, "invalid fragment key or loader"));
  if (memory.resident_bytes > state->config.memory_budget_bytes ||
      memory.temporary_bytes > state->config.memory_budget_bytes - memory.resident_bytes)
    return Result::failure(failure(QueryError::ResourceExhausted, "fragment exceeds runtime memory budget"));
  if (in_fragment_loader)
    return Result::failure(failure(QueryError::ResourceExhausted, "nested fragment loads are not supported"));
  // Outlive the mutex guard: failed admission must also destroy source captures
  // and execution tokens outside the cache lock.
  std::shared_ptr<BatchJob> admitted_job;
  std::unique_lock<std::mutex> lock(state->mutex);
  std::shared_ptr<CacheEntry> entry;
  bool counted_wait = false;
  for (;;) {
    if (waiter.cancelled() || state->stopping)
      return Result::failure(failure(QueryError::Cancelled, "fragment wait cancelled"));
    auto found = state->entries.find(key);
    if (found != state->entries.end() && !found->second->cancellation.cancelled()) {
      entry = found->second;
      if (entry->done) ++state->stats.hits;
      else if (!counted_wait) { ++state->stats.waits; counted_wait = true; }
      break;
    }
    if (found == state->entries.end() && state->inflight < state->config.max_inflight) {
      entry = std::make_shared<CacheEntry>();
      admitted_job = std::make_shared<BatchJob>();
      const auto& job = admitted_job;
      job->execution_lease = active_query_execution_lease;
      job->detailed_profile = detailed_profile;
      entry->key = key;
      entry->reservation = memory;
      job->entries.push_back(entry);
      job->indices.push_back(0);
      job->reservation = memory;
      job->loader = [loader = std::move(loader)](const auto&, const auto& context) {
        auto result = loader(context);
        if (!result.ok()) return BatchFragmentResult::failure(result.status());
        std::vector<std::shared_ptr<const DecodedFragment>> fragments;
        fragments.push_back(std::move(result.value()));
        return BatchFragmentResult::success(std::move(fragments));
      };
      entry->job = job;
      try {
        state->entries.emplace(key, entry);
        if (job->detailed_profile) job->queued_at = Clock::now();
        state->queue.push_back(job);
      } catch (const std::bad_alloc&) {
        state->erase_if_current(entry);
        // The loader may own other fragment leases: destroy outside mutex.
        lock.unlock();
        entry.reset();
        return Result::failure(failure(QueryError::ResourceExhausted, "fragment admission allocation failed"));
      }
      ++state->inflight;
      ++state->stats.misses;
      ++state->stats.producer_count;
      if (state->evicted.count(key)) ++state->stats.redecodes;
      state->cv.notify_all();
      break;
    }
    if (Clock::now() >= deadline)
      return Result::failure(failure(QueryError::ResourceExhausted, "fragment admission timed out"));
    if (!counted_wait) { ++state->stats.waits; counted_wait = true; }
    const auto start = detailed_profile ? Clock::now() : Clock::time_point{};
    poll_wait(*state, lock, deadline);
    if (detailed_profile) state->stats.fragment_admission_nanoseconds += static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());
  }
  auto registration=std::make_unique<WaiterRegistration>(*state, entry, deadline);
  while (!entry->done) {
    if (waiter.cancelled() || state->stopping || Clock::now() >= deadline) {
      return Result::failure(waiter.cancelled() || state->stopping
        ? failure(QueryError::Cancelled, "fragment wait cancelled")
        : failure(QueryError::ResourceExhausted, "fragment wait timed out"));
    }
    poll_wait(*state, lock, deadline);
  }
  if (waiter.cancelled() || state->stopping)
    return Result::failure(failure(QueryError::Cancelled, "fragment wait cancelled"));
  if (!entry->status.ok()) return Result::failure(entry->status);
  auto owner = std::make_shared<LeaseOwner>();
  owner->state = state;
  owner->entry = entry;
  if (entry->leases++ == 0) state->stats.pinned_bytes += entry->bytes;
  entry->stamp = ++state->clock;
  auto fragment = std::shared_ptr<const DecodedFragment>(owner, entry->fragment.get());
  registration.reset();
  lock.unlock();
  // Per-waiter charging runs after pinning and outside the cache mutex. A
  // caller's sticky failure must never alter this shared entry's status.
  if(entry->metadata_fees) replay_query_metadata_trace(*entry->metadata_fees);
  if(auto budget=current_query_metadata_budget();budget && !budget->status(QueryPhase::Execution).ok())
    return Result::failure(budget->status(QueryPhase::Execution));
  return Result::success(FragmentLease(std::move(fragment)));
} catch (const QueryMetadataBudgetExceeded& error) {
  return QueryResult<FragmentLease>::failure(
    failure(QueryError::ResourceExhausted, error.what()));
} catch (const std::bad_alloc&) {
  return QueryResult<FragmentLease>::failure(
    failure(QueryError::ResourceExhausted, "fragment request allocation failed"));
}

QueryResult<std::vector<FragmentLease>> DecodedFragmentCache::get_batch(
  const std::vector<FragmentBatchItem>& items, std::uint64_t shared_temporary_bytes,
  BatchLoader loader, CancellationToken waiter) try {
  using Result = QueryResult<std::vector<FragmentLease>>;
  auto state = impl_->state;
  const auto deadline = deadline_after(state->config.wait_timeout_ms);
  const bool detailed_profile = state->config.enable_detailed_profile || active_query_profile;
  if (items.empty() || items.size() > state->config.max_entries || !loader)
    return Result::failure(failure(QueryError::InvalidQuery, "invalid fragment batch"));
  if (in_fragment_loader)
    return Result::failure(failure(QueryError::ResourceExhausted, "nested fragment loads are not supported"));
  std::unordered_set<std::string> unique;
  unique.reserve(items.size());
  for (const auto& item : items) {
    if (item.key.empty() || item.key.size() > 16 * 1024 || !unique.insert(item.key).second)
      return Result::failure(failure(QueryError::InvalidQuery, "invalid or duplicate fragment key"));
    if (item.memory.resident_bytes > state->config.memory_budget_bytes ||
        item.memory.temporary_bytes > state->config.memory_budget_bytes - item.memory.resident_bytes)
      return Result::failure(failure(QueryError::ResourceExhausted, "fragment exceeds runtime memory budget"));
  }
  std::vector<std::shared_ptr<CacheEntry>> entries(items.size());
  std::vector<std::size_t> missing;
  missing.reserve(items.size());
  std::vector<FragmentLease> leases;
  leases.reserve(items.size());
  std::shared_ptr<BatchJob> admitted_job;
  std::unique_lock<std::mutex> lock(state->mutex);
  std::vector<std::unique_ptr<WaiterRegistration>> registrations;
  registrations.reserve(items.size());
  bool counted_wait = false;
  for (;;) {
    if (waiter.cancelled() || state->stopping)
      return Result::failure(failure(QueryError::Cancelled, "fragment wait cancelled"));
    missing.clear();
    bool stale = false;
    for (std::size_t i = 0; i < items.size(); ++i) {
      auto found = state->entries.find(items[i].key);
      if (found == state->entries.end()) {
        entries[i].reset();
        missing.push_back(i);
      } else if (found->second->cancellation.cancelled()) {
        stale = true;
        break;
      } else {
        entries[i] = found->second;
      }
    }
    if (!stale && (missing.empty() ||
        (missing.size() <= state->config.max_inflight &&
         missing.size() <= state->config.max_inflight - state->inflight))) break;
    if (Clock::now() >= deadline)
      return Result::failure(failure(QueryError::ResourceExhausted, "fragment batch admission timed out"));
    if (!counted_wait) { ++state->stats.waits; counted_wait = true; }
    const auto start = detailed_profile ? Clock::now() : Clock::time_point{};
    poll_wait(*state, lock, deadline);
    if (detailed_profile) state->stats.fragment_admission_nanoseconds += static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());
  }

  if (!missing.empty()) {
    admitted_job = std::make_shared<BatchJob>();
    const auto& job = admitted_job;
    job->execution_lease = active_query_execution_lease;
    job->detailed_profile = detailed_profile;
    job->indices = missing;
    job->loader = std::move(loader);
    job->reservation.temporary_bytes = shared_temporary_bytes;
    for (auto index : missing) {
      const auto& memory = items[index].memory;
      if (memory.resident_bytes > state->config.memory_budget_bytes - job->reservation.resident_bytes ||
          memory.temporary_bytes > state->config.memory_budget_bytes -
            std::min(state->config.memory_budget_bytes, job->reservation.temporary_bytes))
        return Result::failure(failure(QueryError::ResourceExhausted, "fragment batch exceeds runtime memory budget"));
      job->reservation.resident_bytes += memory.resident_bytes;
      job->reservation.temporary_bytes += memory.temporary_bytes;
    }
    if (job->reservation.temporary_bytes > state->config.memory_budget_bytes ||
        job->reservation.resident_bytes > state->config.memory_budget_bytes -
          job->reservation.temporary_bytes)
      return Result::failure(failure(QueryError::ResourceExhausted, "fragment batch exceeds runtime memory budget"));
    try {
      for (auto index : missing) {
        auto entry = std::make_shared<CacheEntry>();
        entry->key = items[index].key;
        entry->reservation = items[index].memory;
        entry->job = job;
        job->entries.push_back(entry);
        entries[index] = entry;
      }
      for (const auto& entry : job->entries) state->entries.emplace(entry->key, entry);
      if (job->detailed_profile) job->queued_at = Clock::now();
      state->queue.push_back(job);
      for (const auto& entry : job->entries) {
        ++state->stats.misses;
        ++state->stats.producer_count;
        if (state->evicted.count(entry->key)) ++state->stats.redecodes;
      }
      state->inflight += missing.size();
      state->cv.notify_all();
    } catch (const std::bad_alloc&) {
      for (const auto& entry : job->entries) state->erase_if_current(entry);
      lock.unlock();
      admitted_job.reset();
      return Result::failure(failure(QueryError::ResourceExhausted, "fragment batch admission allocation failed"));
    }
  }

  for (std::size_t i = 0; i < entries.size(); ++i) {
    const auto& entry = entries[i];
    if (entry->done) ++state->stats.hits;
    else if (!std::binary_search(missing.begin(), missing.end(), i)) ++state->stats.waits;
    registrations.push_back(std::make_unique<WaiterRegistration>(*state, entry, deadline));
  }
  for (const auto& entry : entries) {
    while (!entry->done) {
      if (waiter.cancelled() || state->stopping || Clock::now() >= deadline)
        return Result::failure(waiter.cancelled() || state->stopping
          ? failure(QueryError::Cancelled, "fragment wait cancelled")
          : failure(QueryError::ResourceExhausted, "fragment wait timed out"));
      poll_wait(*state, lock, deadline);
    }
    if (!entry->status.ok()) return Result::failure(entry->status);
  }
  if (waiter.cancelled() || state->stopping)
    return Result::failure(failure(QueryError::Cancelled, "fragment wait cancelled"));
  for (const auto& entry : entries) {
    std::shared_ptr<LeaseOwner> owner;
    try { owner = std::make_shared<LeaseOwner>(); }
    catch (const std::bad_alloc&) {
      registrations.clear();
      lock.unlock();
      leases.clear();
      return Result::failure(failure(QueryError::ResourceExhausted, "fragment lease allocation failed"));
    }
    owner->state = state;
    owner->entry = entry;
    if (entry->leases++ == 0) state->stats.pinned_bytes += entry->bytes;
    entry->stamp = ++state->clock;
    auto fragment = std::shared_ptr<const DecodedFragment>(owner, entry->fragment.get());
    leases.push_back(FragmentLease(std::move(fragment)));
  }
  registrations.clear();
  lock.unlock();
  for(const auto& entry:entries)
    if(entry->metadata_fees) replay_query_metadata_trace(*entry->metadata_fees);
  if(auto budget=current_query_metadata_budget();budget && !budget->status(QueryPhase::Execution).ok())
    return Result::failure(budget->status(QueryPhase::Execution));
  return Result::success(std::move(leases));
} catch (const QueryMetadataBudgetExceeded& error) {
  return QueryResult<std::vector<FragmentLease>>::failure(
    failure(QueryError::ResourceExhausted, error.what()));
} catch (const std::bad_alloc&) {
  return QueryResult<std::vector<FragmentLease>>::failure(
    failure(QueryError::ResourceExhausted, "fragment batch allocation failed"));
}

QueryRuntimeStats DecodedFragmentCache::stats() const {
  std::lock_guard<std::mutex> lock(impl_->state->mutex);
  return impl_->state->stats;
}
void DecodedFragmentCache::record_copy(std::uint64_t bytes) {
  std::lock_guard<std::mutex> lock(impl_->state->mutex);
  impl_->state->stats.copy_bytes += bytes;
}
const QueryRuntimeConfig& DecodedFragmentCache::config() const { return impl_->state->config; }
void DecodedFragmentCache::evict_all() {
  std::unique_lock<std::mutex> lock(impl_->state->mutex);
  while (impl_->state->evict_one(true)) {}
  release_retired(impl_->state, lock);
  impl_->state->cv.notify_all();
}

} // namespace iotdbfs
