#pragma once

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

namespace iotdbfs::versioning::detail {

// Reader must close its connection in its destructor. A lease is discarded
// unless the caller explicitly marks it reusable after complete validation.
template<class Reader>
class BoundedReaderPool {
public:
  struct Stats {
    std::size_t capacity{0}, active{0}, peak{0};
    std::uint64_t waits{0}, acquire_timeouts{0}, created{0}, discarded{0};
  };

private:
  struct State {
    explicit State(std::size_t capacity) { counts.capacity = capacity; idle.reserve(capacity); }
    std::mutex mutex;
    std::condition_variable changed;
    std::vector<std::unique_ptr<Reader>> idle;
    Stats counts;
  };

public:
  class Lease {
  public:
    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;
    Lease(Lease&& other) noexcept
      : state_(std::move(other.state_)), reader_(std::move(other.reader_)),
        reusable_(other.reusable_) {}
    Lease& operator=(Lease&&) = delete;
    ~Lease() {
      if (!state_) return;
      // Keep the slot occupied until a failed connection finishes closing.
      if (!reusable_) reader_.reset();
      {
        std::lock_guard<std::mutex> lock(state_->mutex);
        --state_->counts.active;
        if (reusable_) state_->idle.push_back(std::move(reader_));
        else ++state_->counts.discarded;
      }
      state_->changed.notify_one();
    }
    Reader* get() const noexcept { return reader_.get(); }
    Reader* operator->() const noexcept { return get(); }
    void reuse() noexcept { reusable_ = true; }

  private:
    friend class BoundedReaderPool;
    Lease(std::shared_ptr<State> state, std::unique_ptr<Reader> reader)
      : state_(std::move(state)), reader_(std::move(reader)) {}
    std::shared_ptr<State> state_;
    std::unique_ptr<Reader> reader_;
    bool reusable_{false};
  };

  BoundedReaderPool(std::size_t capacity, std::chrono::milliseconds acquire_timeout)
    : state_(std::make_shared<State>(validate(capacity))), timeout_(acquire_timeout) {
    if (timeout_.count() <= 0) throw std::invalid_argument("reader pool acquire timeout must be positive");
  }
  BoundedReaderPool(const BoundedReaderPool&) = delete;
  BoundedReaderPool& operator=(const BoundedReaderPool&) = delete;

  template<class Factory, class Checkpoint>
  Lease acquire(Factory factory, Checkpoint checkpoint) {
    const auto state = state_;
    const auto deadline = std::chrono::steady_clock::now() + timeout_;
    bool waited = false;
    std::unique_lock<std::mutex> lock(state->mutex);
    for (;;) {
      // No pool state has changed yet, so cancellation needs no rollback.
      lock.unlock();
      checkpoint();
      lock.lock();
      if (!state->idle.empty() || state->counts.active < state->counts.capacity) {
        std::unique_ptr<Reader> reader;
        if (!state->idle.empty()) {
          reader = std::move(state->idle.back());
          state->idle.pop_back();
        }
        ++state->counts.active;
        state->counts.peak = std::max(state->counts.peak, state->counts.active);
        lock.unlock();
        try {
          if (!reader) {
            reader = factory();
            if (!reader) throw std::runtime_error("reader pool factory returned null");
            std::lock_guard<std::mutex> count_lock(state->mutex);
            ++state->counts.created;
          }
        } catch (...) {
          std::lock_guard<std::mutex> count_lock(state->mutex);
          --state->counts.active;
          state->changed.notify_one();
          throw;
        }
        Lease result(state, std::move(reader));
        checkpoint();
        return result;
      }
      if (!waited) { ++state->counts.waits; waited = true; }
      const auto now = std::chrono::steady_clock::now();
      if (now >= deadline) {
        ++state->counts.acquire_timeouts;
        throw std::runtime_error("version store reader pool acquire timed out");
      }
      state->changed.wait_until(lock, std::min(deadline, now + std::chrono::milliseconds(10)));
    }
  }

  Stats stats() const {
    std::lock_guard<std::mutex> lock(state_->mutex);
    return state_->counts;
  }

private:
  static std::size_t validate(std::size_t capacity) {
    if (capacity == 0 || capacity > 256)
      throw std::invalid_argument("version store reader pool size must be between 1 and 256");
    return capacity;
  }
  std::shared_ptr<State> state_;
  std::chrono::milliseconds timeout_;
};

} // namespace iotdbfs::versioning::detail
