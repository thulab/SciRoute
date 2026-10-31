#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>

namespace iotdbfs {

// Immutable, query-independent derived data. Never retain a QuerySource,
// ReadContext, request ledger or prepared query through this interface.
class QueryDomainState {
public:
  virtual ~QueryDomainState() = default;
  virtual std::uint64_t retained_bytes() const = 0;
};

// Storage may discard an optional artifact without discarding its candidate
// domain. An acquired value pins both the immutable artifact and its charge.
// Implementations must not grant query authority or retain request state.
class QueryDomainRetention {
public:
  virtual ~QueryDomainRetention() = default;
  virtual std::shared_ptr<const void> acquire() noexcept = 0;
};

// One optional derived artifact in an already accounted frozen candidate
// domain. Storage supplies the SAME domain reservation, not another cache.
class QueryDomainCache final : public std::enable_shared_from_this<QueryDomainCache> {
  struct Allocation {
    std::function<bool(std::uint64_t)> grow;
    std::function<void(std::uint64_t)> shrink;
    std::uint64_t bytes{0};
    ~Allocation() { if(bytes) shrink(bytes); }
    bool reserve(std::uint64_t count) {
      if(!grow(count)) return false;
      bytes += count;
      return true;
    }
  };
  struct Entry {
    std::shared_ptr<Allocation> allocation;
    std::shared_ptr<const QueryDomainState> state;
  };
public:
  using Grow = std::function<bool(std::uint64_t)>;
  using Shrink = std::function<void(std::uint64_t)>;
  using Available = std::function<std::uint64_t()>;
  using Retain = std::function<std::shared_ptr<QueryDomainRetention>(std::shared_ptr<const void>)>;
  class Claim final {
  public:
    ~Claim() {
      if(owner_) {
        { std::lock_guard<std::mutex> lock(owner_->mutex_); owner_->building_ = false; }
        owner_->changed_.notify_all();
      }
    }
    std::shared_ptr<const QueryDomainState> value;
    bool building() const { return bool(owner_); }
    bool grow(std::uint64_t count) { return allocation_ && allocation_->reserve(count); }
    void shrink(std::uint64_t count) {
      if(!allocation_ || count > allocation_->bytes) return;
      allocation_->shrink(count); allocation_->bytes -= count;
    }
    std::uint64_t available() const { return owner_ ? owner_->available_() : 0; }
    std::shared_ptr<const void> residency_pin() const { return allocation_; }
    // A separately pinned state keeps its accounting alive after eviction.
    bool publish(std::shared_ptr<const QueryDomainState> state) {
      if(!owner_ || !state || published_) return false;
      auto entry = std::make_shared<Entry>();
      entry->allocation = allocation_;
      entry->state = std::move(state);
      std::shared_ptr<QueryDomainRetention> retained;
      if(owner_->retain_) {
        // Optional storage admission can allocate, evict other artifacts or
        // decline. None of that work may run under the candidate cache lock.
        try { retained = owner_->retain_(entry); }
        catch(...) { return false; }
        if(!retained) return false;
      }
      std::shared_ptr<QueryDomainRetention> retired;
      std::lock_guard<std::mutex> lock(owner_->mutex_);
      if(owner_->entry_) return false;
      if(retained) {
        retired = std::move(owner_->retained_);
        owner_->retained_ = std::move(retained);
      } else {
        owner_->entry_ = entry;
      }
      value = std::shared_ptr<const QueryDomainState>(entry, entry->state.get());
      published_ = true;
      return true;
    }
    Claim(const Claim&) = delete;
    Claim& operator=(const Claim&) = delete;
  private:
    friend class QueryDomainCache;
    Claim() = default;
    std::shared_ptr<QueryDomainCache> owner_;
    std::shared_ptr<Allocation> allocation_;
    bool published_{false};
  };
  QueryDomainCache(Grow grow, Shrink shrink, Available available, Retain retain = {})
    : grow_(std::move(grow)), shrink_(std::move(shrink)), available_(std::move(available)),
      retain_(std::move(retain)) {}
  std::shared_ptr<Claim> claim() {
    auto result = std::shared_ptr<Claim>(new Claim());
    for(;;) {
      std::shared_ptr<QueryDomainRetention> retained;
      std::shared_ptr<const void> pinned;
      std::shared_ptr<Allocation> allocation;
      std::shared_ptr<QueryDomainRetention> retired;
      std::unique_lock<std::mutex> lock(mutex_);
      changed_.wait(lock, [&] { return !building_; });
      if(entry_) {
        result->value = std::shared_ptr<const QueryDomainState>(entry_, entry_->state.get());
        return result;
      }
      retained = retained_;
      lock.unlock();
      if(retained) pinned = retained->acquire();
      // Allocate only on a miss, outside the cache lock. Another claimant may
      // begin building meanwhile; it remains the sole producer.
      if(!pinned) {
        allocation = std::make_shared<Allocation>();
        allocation->grow = grow_;
        allocation->shrink = shrink_;
      }
      lock.lock();
      if(building_ || entry_ || retained_ != retained) {
        lock.unlock();
        continue;
      }
      if(pinned) {
        const auto* entry = static_cast<const Entry*>(pinned.get());
        result->value = std::shared_ptr<const QueryDomainState>(std::move(pinned), entry->state.get());
        return result;
      }
      retired = std::move(retained_);
      result->allocation_ = std::move(allocation);
      result->owner_ = shared_from_this();
      building_ = true;
      return result;
    }
  }
private:
  Grow grow_;
  Shrink shrink_;
  Available available_;
  Retain retain_;
  std::mutex mutex_;
  std::condition_variable changed_;
  bool building_{false};
  std::shared_ptr<Entry> entry_;
  std::shared_ptr<QueryDomainRetention> retained_;
};
} // namespace iotdbfs
