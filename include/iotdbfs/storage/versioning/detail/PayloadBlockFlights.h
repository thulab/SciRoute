#pragma once

#include "iotdbfs/storage/versioning/VersionDirectory.h"
#include "iotdbfs/query/core/QueryStorageReadControl.h"
#include "iotdbfs/query/core/QueryProfile.h"

#include <chrono>
#include <algorithm>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <map>
#include <list>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <utility>
#include <vector>
#include <stdexcept>

namespace iotdbfs::versioning::detail {

// Immutable reads share bounded entries. Optional cross-request residency uses
// the same hard entry/data cap and yields its ownership under runtime pressure.
// The caller must authorize each read and validate the producer's exact bytes.
class PayloadBlockFlights {
public:
  struct Stats {
    std::size_t active_flights{0}, retained_entries{0}, retained_bytes{0};
    std::uint64_t leaders{0}, joins{0}, bypasses{0}, fallbacks{0};
    std::uint64_t cache_hits{0}, cache_misses{0}, cache_insertions{0}, cache_evictions{0};
    std::uint64_t cache_pressure_reclaims{0};
    std::size_t cache_resident_entries{0}, cache_resident_bytes{0};
  };
  struct Result {
    std::shared_ptr<const Bytes> bytes;
    bool joined{false};
    bool bypassed{false};
    std::uint64_t wait_ns{0};
    // Private paths move the original loader buffer here. In particular, an
    // optional optimization must not allocate after a successful direct read.
    Bytes owned;
    const Bytes& value() const noexcept { return bytes ? *bytes : owned; }
  };

private:
  struct Entry;
  using Key = std::pair<std::string, std::size_t>;
  struct State final : StorageReadOptionalCache {
    std::mutex mutex;
    std::map<Key, std::weak_ptr<Entry>> flights;
    std::list<std::shared_ptr<Entry>> residents; // MRU first; destruction outside mutex
    std::size_t resident_limit{0};
    std::uint64_t generation{0};
    Stats counts;
    void reclaim() noexcept override;
  };
  struct Entry {
    explicit Entry(std::shared_ptr<State> owner, std::size_t size,
                   std::shared_ptr<void> accounting)
      : state(std::move(owner)), charged_bytes(size), keepalive(std::move(accounting)) {}
    ~Entry() {
      // No resource/accounting destructor runs under the registry lock.
      if (charged) {
        Bytes{}.swap(bytes);
        keepalive.reset();
        std::lock_guard<std::mutex> lock(state->mutex);
        --state->counts.retained_entries;
        state->counts.retained_bytes -= charged_bytes;
      }
    }
    std::shared_ptr<State> state;
    std::size_t charged_bytes;
    std::shared_ptr<void> keepalive;
    bool charged{false};
    std::uint64_t generation{0};
    std::mutex mutex;
    std::condition_variable changed;
    bool done{false}, success{false};
    Bytes bytes;
  };

public:
  // Explicit original scalar-request lifetime. Nested adapters use the outer
  // scope; prepared/shared sources never own one. No scope means no reuse
  // beyond the current active batch. Optional retention cannot fail a read.
  class ReuseScope {
  public:
    ReuseScope() noexcept : previous_(current_), root_(previous_?previous_->root_:this),
      reclaim_([](void* pointer) noexcept {
        auto& root=*static_cast<ReuseScope*>(pointer);
        root.entries_.clear();
        root.bytes_=0;
      },root_) {
      current_=this;
    }
    ~ReuseScope() { current_=previous_; }
    ReuseScope(const ReuseScope&)=delete;
    ReuseScope& operator=(const ReuseScope&)=delete;
  private:
    friend class PayloadBlockFlights;
    ReuseScope* previous_;
    ReuseScope* root_;
    std::vector<std::shared_ptr<Entry>> entries_;
    std::size_t bytes_{0};
    ScopedStorageReadOptionalReclaim reclaim_;
    inline static thread_local ReuseScope* current_{nullptr};
  };

  explicit PayloadBlockFlights(std::size_t max_entries = 64,
                              std::size_t max_shared_bytes = 8 * 1024 * 1024,
                              std::size_t max_resident_bytes = 0)
    : state_(std::make_shared<State>()), max_entries_(max_entries),
      max_shared_bytes_(max_shared_bytes) {
    state_->resident_limit=std::min(max_resident_bytes,max_shared_bytes);
  }
  ~PayloadBlockFlights() { clear(); }
  // Only optional cache ownership is retired. Active producers, scopes and
  // consumers keep their exact bytes and original runtime accounting pins.
  void clear() noexcept { clear_state(*state_,false); }
  PayloadBlockFlights(const PayloadBlockFlights&) = delete;
  PayloadBlockFlights& operator=(const PayloadBlockFlights&) = delete;

  // Loader executes synchronously on the caller's thread and retains its TLS.
  // A failed producer never lends its exception/cancellation to another query:
  // each surviving follower instead performs its own first physical read.
  template<class Loader, class Checkpoint>
  Result read(const std::string& identity, std::size_t exact_size, Loader loader,
              Checkpoint checkpoint, std::shared_ptr<void> keepalive = {}) {
    checkpoint();
    const auto state = state_;
    const bool resident_allowed=register_cache(state);
    std::shared_ptr<Entry> entry;
    bool leader = false;
    try {
      const Key key{identity, exact_size};
      std::unique_lock<std::mutex> lock(state->mutex);
      prune(*state);
      const auto found = state->flights.find(key);
      if (found != state->flights.end() && (entry = found->second.lock())) {
        ++state->counts.joins;
        touch(*state,entry);
      } else if (max_entries_ && exact_size && exact_size <= max_shared_bytes_ &&
                 make_room(state,lock,exact_size)) {
        if(state->resident_limit){++state->counts.cache_misses;query_profile_count("payload_cache.misses");}
        // Eviction released the mutex while destroying pins. Another reader
        // may have claimed this key in the meantime.
        const auto again=state->flights.find(key);
        if(again!=state->flights.end())entry=again->second.lock();
        if(entry) {++state->counts.joins;touch(*state,entry);}
        else {
          entry = std::make_shared<Entry>(state, exact_size, keepalive);
          entry->generation=state->generation;
          state->flights.insert_or_assign(key, entry);
          entry->charged = true;
          ++state->counts.active_flights;
          ++state->counts.retained_entries;
          state->counts.retained_bytes += exact_size;
          ++state->counts.leaders;
          leader = true;
        }
      }
    } catch (const std::bad_alloc&) {
      // Optimization admission must not add an error to a valid direct read.
      entry.reset();
    }
    if (!entry) {
      {
        std::lock_guard<std::mutex> lock(state->mutex);
        ++state->counts.bypasses;
      }
      checkpoint();
      auto bytes = loader();
      checkpoint();
      return {nullptr, false, true, 0, std::move(bytes)};
    }
    if (leader) {
      try {
        checkpoint();
        auto bytes = loader();
        checkpoint();
        // Exact-size validation normally belongs to the loader. If a generic
        // caller supplies an unexpected size, preserve its result but do not
        // retain or share bytes beyond the admitted accounting bound.
        if (bytes.size() != exact_size) {
          finish(state, entry, false);
          return {nullptr, false, true, 0, std::move(bytes)};
        }
        entry->bytes = std::move(bytes);
        remember(entry);
        finish(state, entry, true, ReuseScope::current_!=nullptr||resident_allowed,resident_allowed);
        return {std::shared_ptr<const Bytes>(entry, &entry->bytes), false, false, 0};
      } catch (...) {
        finish(state, entry, false);
        throw;
      }
    }
    const auto start = std::chrono::steady_clock::now();
    for (;;) {
      // Check this query's cancellation/deadline without holding any lock.
      checkpoint();
      std::unique_lock<std::mutex> lock(entry->mutex);
      if (entry->done) {
        const bool success = entry->success;
        lock.unlock();
        checkpoint();
        const auto waited = static_cast<std::uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - start).count());
        if (success) {
          remember(entry);
          return {std::shared_ptr<const Bytes>(entry, &entry->bytes), true, false, waited};
        }
        {
          std::lock_guard<std::mutex> count_lock(state->mutex);
          ++state->counts.fallbacks;
        }
        // Drop shared accounting before the independent loader. Its error is
        // this query's own error, with no producer exception replay or retry.
        entry.reset();
        auto bytes = loader();
        checkpoint();
        return {nullptr, true, true, waited, std::move(bytes)};
      }
      entry->changed.wait_for(lock, std::chrono::milliseconds(10));
    }
  }

  // The loader receives nullptr for a complete direct read, otherwise an
  // ordered subset of request indices. It must validate the entire physical
  // batch before returning. Every owned missing block is loaded in ONE batch
  // before waiting on another producer, including reverse-overlap requests.
  // Admission failures take the original direct path, with no new allocation
  // after that successful read. All metadata allocations precede physical I/O.
  template<class Loader, class Checkpoint>
  std::vector<Bytes> read_batch(const std::vector<Key>& keys, Loader loader,
                               Checkpoint checkpoint,
                               std::shared_ptr<void> keepalive = {}) {
    checkpoint();
    const auto state = state_;
    const bool resident_allowed=register_cache(state);
    struct Slot { std::shared_ptr<Entry> entry; bool leader{false}; };
    std::vector<Slot> slots;
    std::vector<std::size_t> owned, failed;
    std::vector<Bytes> result;
    try {
      slots.resize(keys.size());
      result.resize(keys.size());
      owned.reserve(keys.size());
      failed.reserve(keys.size());
      std::unique_lock<std::mutex> lock(state->mutex);
      prune(*state);
      for (std::size_t i = 0; i < keys.size(); ++i) {
        const auto found = state->flights.find(keys[i]);
        if (found != state->flights.end()) slots[i].entry = found->second.lock();
        if (slots[i].entry) {
          ++state->counts.joins;
          touch(*state,slots[i].entry);
          continue;
        }
        if(state->resident_limit){++state->counts.cache_misses;query_profile_count("payload_cache.misses");}
        const auto size = keys[i].second;
        if (max_entries_ && size && size <= max_shared_bytes_ &&
            make_room(state,lock,size)) {
          const auto again=state->flights.find(keys[i]);
          if(again!=state->flights.end())slots[i].entry=again->second.lock();
          if(slots[i].entry) {
            ++state->counts.joins;touch(*state,slots[i].entry);continue;
          }
          auto entry = std::make_shared<Entry>(state, size, keepalive);
          entry->generation=state->generation;
          state->flights.insert_or_assign(keys[i], entry);
          // Store a strong reference before charge: rollback never destroys a
          // charged entry (and its accounting pin) under the registry mutex.
          slots[i].entry = std::move(entry);
          slots[i].leader = true;
          slots[i].entry->charged = true;
          ++state->counts.active_flights;
          ++state->counts.retained_entries;
          state->counts.retained_bytes += size;
          ++state->counts.leaders;
        } else ++state->counts.bypasses;
        owned.push_back(i);
      }
    } catch (const std::bad_alloc&) {
      for (auto& slot : slots)
        if (slot.leader) finish(state, slot.entry, false);
      slots.clear(); // release accounting without any registry lock
      checkpoint();
      auto direct = loader(static_cast<const std::vector<std::size_t>*>(nullptr));
      checkpoint();
      return direct;
    }
    try {
      if (!owned.empty()) {
        checkpoint();
        auto values = loader(&owned);
        checkpoint();
        if (values.size() != owned.size())
          throw std::logic_error("validated block batch count mismatch");
        // Publish only after the loader validates the COMPLETE owned batch.
        for (std::size_t j = 0; j < owned.size(); ++j) {
          const auto i = owned[j];
          auto& slot = slots[i];
          if (slot.leader && values[j].size() == keys[i].second) {
            slot.entry->bytes = std::move(values[j]);
            finish(state, slot.entry, true, true,resident_allowed);
            remember(slot.entry);
          } else {
            if (slot.leader) finish(state, slot.entry, false);
            result[i] = std::move(values[j]);
          }
        }
      }
      for (std::size_t i = 0; i < slots.size(); ++i) {
        auto& slot = slots[i];
        if (!slot.entry) continue;
        if (!slot.leader) {
          for (;;) {
            checkpoint();
            std::unique_lock<std::mutex> lock(slot.entry->mutex);
            if (slot.entry->done) {
              const auto success = slot.entry->success;
              lock.unlock();
              checkpoint();
              if (!success) {
                failed.push_back(i);
                { std::lock_guard<std::mutex> count_lock(state->mutex);
                  ++state->counts.fallbacks; }
                slot.entry.reset();
              }
              break;
            }
            slot.entry->changed.wait_for(lock, std::chrono::milliseconds(10));
          }
        }
        if (slot.entry && slot.entry->success) {
          remember(slot.entry);
          result[i] = slot.entry->bytes;
        }
      }
      if (!failed.empty()) {
        checkpoint();
        auto values = loader(&failed);
        checkpoint();
        if (values.size() != failed.size())
          throw std::logic_error("validated fallback batch count mismatch");
        for (std::size_t j = 0; j < failed.size(); ++j)
          result[failed[j]] = std::move(values[j]);
      }
      return result;
    } catch (...) {
      // An owned producer never waits while unwinding, and successful entries
      // are not invalidated merely because this caller later fails/cancels.
      for (auto& slot : slots)
        if (slot.leader) finish(state, slot.entry, false);
      throw;
    }
  }

  Stats stats() const {
    std::lock_guard<std::mutex> lock(state_->mutex);
    return state_->counts;
  }

private:
  static bool register_cache(const std::shared_ptr<State>& state) {
    if(!state->resident_limit)return false;
    if(!active_storage_read_control||!active_storage_read_control->reserve)return true;
    try {return register_storage_read_optional_cache(state);}
    catch(const std::bad_alloc&) {return false;}
  }
  static void touch(State& state,const std::shared_ptr<Entry>& entry) {
    if(!state.resident_limit)return;
    for(auto it=state.residents.begin();it!=state.residents.end();++it)if(*it==entry) {
      state.residents.splice(state.residents.begin(),state.residents,it);
      ++state.counts.cache_hits;
      query_profile_count("payload_cache.hits");
      query_profile_count("payload_cache.hit_bytes",entry->charged_bytes);return;
    }
    ++state.counts.cache_misses;
    query_profile_count("payload_cache.misses");
  }
  bool make_room(const std::shared_ptr<State>& state,std::unique_lock<std::mutex>& lock,
                 std::size_t size) {
    const auto fits=[&] {return state->counts.retained_entries<max_entries_&&
      size<=max_shared_bytes_-state->counts.retained_bytes;};
    while(!fits()&&!state->residents.empty()) {
      evict_one(state,lock);
    }
    return fits();
  }
  static void evict_one(const std::shared_ptr<State>& state,std::unique_lock<std::mutex>& lock) {
    auto retired=std::move(state->residents.back());state->residents.pop_back();
    --state->counts.cache_resident_entries;
    state->counts.cache_resident_bytes-=retired->charged_bytes;
    ++state->counts.cache_evictions;
    query_profile_count("payload_cache.evictions");
    lock.unlock();retired.reset();lock.lock();
    prune(*state);
  }
  static void clear_state(State& state,bool pressure) noexcept {
    std::list<std::shared_ptr<Entry>> retired;
    {
      std::lock_guard<std::mutex> lock(state.mutex);
      ++state.generation;
      retired.swap(state.residents);
      if(pressure&&!retired.empty())++state.counts.cache_pressure_reclaims;
      if(pressure&&!retired.empty())query_profile_count("payload_cache.pressure_reclaims");
      query_profile_count("payload_cache.evictions",retired.size());
      state.counts.cache_evictions+=retired.size();
      state.counts.cache_resident_entries=state.counts.cache_resident_bytes=0;
    }
    retired.clear();
  }
  static void remember(const std::shared_ptr<Entry>& entry) noexcept {
    if (!ReuseScope::current_) return;
    auto& scope=*ReuseScope::current_->root_;
    for (const auto& held:scope.entries_) if (held==entry) return;
    constexpr std::size_t limit=8*1024*1024;
    if(scope.entries_.size()>=64 || entry->charged_bytes>limit-scope.bytes_)return;
    try {
      scope.entries_.push_back(entry);
      scope.bytes_+=entry->charged_bytes;
    } catch(const std::bad_alloc&) {} // best-effort reuse, original read succeeds
  }
  static void prune(State& state) {
    for (auto it = state.flights.begin(); it != state.flights.end();)
      if (it->second.expired()) it = state.flights.erase(it);
      else ++it;
  }
  static void finish(const std::shared_ptr<State>& state,
                     const std::shared_ptr<Entry>& entry, bool success, bool retain = false,
                     bool resident_allowed=false) {
    {
      std::lock_guard<std::mutex> lock(entry->mutex);
      if (entry->done) return;
    }
    {
      std::unique_lock<std::mutex> lock(state->mutex);
      // Never retain oversized capacity, partial validation or a producer
      // invalidated by clear/recovery/pressure. Admission is best effort.
      if(success&&resident_allowed&&entry->generation==state->generation&&
         entry->bytes.capacity()==entry->charged_bytes&&
         entry->charged_bytes<=state->resident_limit) {
        while(entry->charged_bytes>state->resident_limit-state->counts.cache_resident_bytes&&
              !state->residents.empty())evict_one(state,lock);
        try {
          if(entry->generation==state->generation) {
            state->residents.push_front(entry);
            ++state->counts.cache_insertions;++state->counts.cache_resident_entries;
            query_profile_count("payload_cache.insertions");
            state->counts.cache_resident_bytes+=entry->charged_bytes;
            retain=true;
          }
        }catch(const std::bad_alloc&) {}
      }
      // Use pointer identity rather than allocate another key while unwinding.
      for (auto it = state->flights.begin(); it != state->flights.end(); ++it) {
        if (!it->second.owner_before(entry) && !entry.owner_before(it->second)) {
          if (!success || !retain) state->flights.erase(it);
          break;
        }
      }
      --state->counts.active_flights;
    }
    {
      std::lock_guard<std::mutex> lock(entry->mutex);
      entry->success = success;
      entry->done = true;
    }
    entry->changed.notify_all();
  }
  std::shared_ptr<State> state_;
  const std::size_t max_entries_, max_shared_bytes_;
};

inline void PayloadBlockFlights::State::reclaim() noexcept {
  PayloadBlockFlights::clear_state(*this,true);
}

} // namespace iotdbfs::versioning::detail
