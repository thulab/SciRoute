#pragma once

#include "iotdbfs/query/core/QueryModel.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <utility>

namespace iotdbfs {

// Stores complete, immutable encoded results. An open handle pins its bytes even
// after its ID expires or is evicted; pinned bytes remain inside the resident
// budget until the last handle releases them. IDs are process-local locators,
// not authorization credentials.
class TypedResultStore {
  struct State;
  struct Payload;

public:
  using Clock = std::chrono::steady_clock;
  using ClockFn = std::function<Clock::time_point()>;

  struct Config {
    std::uint64_t max_result_bytes{64ULL * 1024 * 1024};
    std::uint64_t max_resident_bytes{256ULL * 1024 * 1024};
    std::size_t max_entries{1024};
    std::chrono::milliseconds ttl{std::chrono::minutes(5)};
  };

  struct Stats {
    std::uint64_t resident_bytes{0};
    std::size_t registered_entries{0};
    std::uint64_t open_handles{0};
  };

  // Complete bytes and their digest are prepared before the namespace gate.
  // Callers cannot replace the bytes after the digest has been computed.
  class PreparedPublication {
  public:
    PreparedPublication() = default;
    PreparedPublication(PreparedPublication&&) = default;
    PreparedPublication& operator=(PreparedPublication&&) = default;
    std::uint64_t size() const { return bytes_.size(); }
  private:
    friend class TypedResultStore;
    std::vector<std::uint8_t> bytes_;
    std::string id_;
    std::string result_id_, handle_id_;
    std::shared_ptr<Payload> identical_;
    std::shared_ptr<Payload> spare_;
  };

  class Handle;
  // Owns both the immutable payload and its accounting state. Bytes may only
  // be inspected within with_bytes; callers must not retain its pointer.
  class ReadOnlySlice {
  public:
    std::size_t size() const { return size_; }
    template <class Visitor> void with_bytes(Visitor&& visitor) const {
      std::forward<Visitor>(visitor)(data_, size_);
    }
  private:
    friend class Handle;
    std::shared_ptr<State> state_;
    std::shared_ptr<const Payload> payload_;
    const std::uint8_t* data_{nullptr};
    std::size_t size_{0};
  };

  class Handle {
  public:
    Handle() = default;
    Handle(const Handle&) = delete;
    Handle& operator=(const Handle&) = delete;
    Handle(Handle&& other) noexcept;
    Handle& operator=(Handle&& other) noexcept;
    ~Handle();

    bool valid() const { return static_cast<bool>(payload_); }
    const std::string& id() const { return id_; }
    std::uint64_t size() const;
    QueryResult<std::vector<std::uint8_t>> read(std::uint64_t offset,
      std::size_t length) const;
    QueryResult<ReadOnlySlice> slice(std::uint64_t offset, std::size_t length) const;
    void release() noexcept;

  private:
    friend class TypedResultStore;
    Handle(std::shared_ptr<State> state,
      std::shared_ptr<const Payload> payload, std::string id) noexcept;

    // State is declared first so payload destruction can still update it.
    std::shared_ptr<State> state_;
    std::shared_ptr<const Payload> payload_;
    std::string id_;
  };

  explicit TypedResultStore(Config config, ClockFn clock = Clock::now);
  TypedResultStore(const TypedResultStore&) = delete;
  TypedResultStore& operator=(const TypedResultStore&) = delete;

  // The caller must finish encoding before publish. No ID is visible if
  // admission fails. Empty buffers are invalid (even a schema-only IPC stream
  // has bytes).
  QueryResult<std::string> publish(std::vector<std::uint8_t> complete_bytes);
  // A dynamic admission failure leaves complete_bytes intact so the caller
  // can release idle cache handles and retry without encoding again. When
  // pinned is supplied, publication and the first open are atomic.
  QueryResult<std::string> publish_retryable(
    std::vector<std::uint8_t>& complete_bytes, Handle* pinned = nullptr);
  QueryResult<PreparedPublication> prepare_publication(
    std::vector<std::uint8_t> complete_bytes) const;
  // Expiry, eviction, collision comparison and allocation happen outside the
  // namespace admission gate. Concurrent store changes may require retrying.
  QueryStatus prepare_capacity(PreparedPublication& prepared);
  QueryResult<std::string> publish_prepared_retryable(
    PreparedPublication& prepared, Handle* pinned = nullptr);
  QueryResult<Handle> open(const std::string& result_id);
  // Static per-result admission ceiling. Dynamic resident pressure may still
  // reject a result after encoding if other handles remain pinned.
  std::uint64_t max_publish_bytes() const;
  Stats stats() const;

private:
  std::shared_ptr<State> state_;
};

} // namespace iotdbfs
