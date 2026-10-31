#pragma once

#include "iotdbfs/query/frontend/TypedResultStore.h"

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <atomic>
#include <functional>
#include <vector>
#include <array>

namespace iotdbfs {

// Sends already authorized immutable bytes. Admission begins before query-task
// submission and is retained through the final send or transport failure. This
// component neither executes queries nor grants permission to open a result.
class BoundedResultSender {
  struct State;
  struct Shard;
public:
  struct Config {
    std::size_t max_transfers{16};
    std::size_t sender_threads{1};
    std::size_t buffer_bytes{64 * 1024};
    std::size_t max_header_bytes{4096};
    // Like the control socket's SO_SNDTIMEO, progress restarts this timeout.
    std::chrono::milliseconds idle_timeout{std::chrono::seconds(3)};
  };
  struct Stats {
    std::size_t reserved{0};
    std::size_t active{0};
    std::size_t completed{0};
    std::size_t aborted{0};
  };
  struct TimingRecord {
    struct SendSpan {
      std::uint64_t begin_ns{0}, end_ns{0}, requested_bytes{0};
      std::int64_t result_bytes{0};
      int error_no{0};
    };
    std::uint64_t accept_ns{0}, transport_start_ns{0}, request_ready_ns{0};
    std::uint64_t submit_ns{0}, task_start_ns{0}, admit_start_ns{0}, admit_end_ns{0};
    std::uint64_t enqueue_ns{0}, dequeue_ns{0}, first_send_ns{0}, last_send_ns{0}, cleanup_ns{0};
    std::uint64_t send_cpu_ns{0}, send_calls{0}, bytes_sent{0}, eagain{0}, short_writes{0};
    std::array<SendSpan, 64> send_spans{};
    std::size_t send_span_count{0}, send_spans_dropped{0};
    std::size_t shard{0};
    bool completed{false};
    QueryError error{QueryError::None};
    std::string path;
  };
  using TimingSink = std::function<void(TimingRecord&&)>;
  class Reservation {
  public:
    ~Reservation();
    Reservation(const Reservation&) = delete;
    Reservation& operator=(const Reservation&) = delete;
    void cancel() noexcept;
  private:
    friend class BoundedResultSender;
    Reservation(std::shared_ptr<State> state, std::shared_ptr<Shard> shard);
    std::shared_ptr<State> state_;
    std::shared_ptr<Shard> shard_;
    bool counted_{false}; // Set before the reservation is published.
    std::atomic<bool> submitted_{false};
    std::atomic<bool> cancelled_{false};
  };

  BoundedResultSender();
  explicit BoundedResultSender(Config config);
  ~BoundedResultSender();
  BoundedResultSender(const BoundedResultSender&) = delete;
  BoundedResultSender& operator=(const BoundedResultSender&) = delete;
  QueryStatus start();
  void stop() noexcept;
  // Null means the sender is stopped or all admission slots are occupied.
  std::shared_ptr<Reservation> reserve();
  // Success hands off a uint32 network-order JSON frame followed by raw bytes.
  // The caller must drop its reservation reference after handoff. The supplied
  // lifetime owns fd and any request completion guard; sender never closes fd.
  // On asynchronous failure, lifetime is released and the connection closes;
  // the published route is never retired. On immediate failure nothing sends.
  QueryStatus send(std::shared_ptr<Reservation> reservation,
    std::shared_ptr<TypedResultStore::Handle> handle, int fd,
    std::shared_ptr<void> lifetime, std::string json_header,
    std::shared_ptr<TimingRecord> timing = {},
    TimingSink sink = {});
  Stats stats() const;

private:
  void run(const std::shared_ptr<Shard>& shard) noexcept;
  std::shared_ptr<State> state_;
  std::mutex lifecycle_mutex_;
  std::vector<std::thread> threads_;
};

} // namespace iotdbfs
