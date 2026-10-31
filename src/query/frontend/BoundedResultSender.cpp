#include "iotdbfs/query/frontend/BoundedResultSender.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <climits>
#include <fcntl.h>
#include <new>
#include <poll.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace iotdbfs {
namespace {
using Clock = std::chrono::steady_clock;
std::uint64_t now_ns() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
    Clock::now().time_since_epoch()).count();
}
std::uint64_t cpu_ns() noexcept {
  timespec value{};
  if (::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &value) != 0) return 0;
  return static_cast<std::uint64_t>(value.tv_sec) * 1000000000ULL + value.tv_nsec;
}
QueryStatus failure(QueryError code, const char* message) {
  return QueryStatus::failure(code, QueryPhase::Encoding, message);
}
bool nonblocking_cloexec(int fd) {
  const int flags = ::fcntl(fd, F_GETFL);
  const int fd_flags = ::fcntl(fd, F_GETFD);
  return flags >= 0 && fd_flags >= 0 &&
    ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0 &&
    ::fcntl(fd, F_SETFD, fd_flags | FD_CLOEXEC) == 0;
}
}

struct BoundedResultSender::Shard {
  struct Transfer {
    std::shared_ptr<Reservation> reservation;
    std::shared_ptr<TypedResultStore::Handle> handle;
    std::shared_ptr<void> lifetime;
    std::shared_ptr<TimingRecord> timing;
    std::function<void(TimingRecord&&)> sink;
    int fd{-1};
    bool configured{false};
    std::array<std::uint8_t, 4> prefix{};
    std::string header;
    std::size_t header_offset{0};
    std::uint64_t body_offset{0};
    Clock::time_point progressed{Clock::now()};
  };
  explicit Shard(std::size_t value) : index(value) {}
  void wake_locked() noexcept {
    if (wake_write < 0) return;
    const char byte = 1;
    ssize_t count;
    do { count = ::write(wake_write, &byte, 1); } while (count < 0 && errno == EINTR);
  }
  void wake() noexcept { std::lock_guard<std::mutex> lock(mutex); wake_locked(); }
  std::size_t index;
  std::mutex mutex;
  int wake_read{-1}, wake_write{-1};
  std::vector<std::unique_ptr<Transfer>> queued;
};
struct BoundedResultSender::State {
  explicit State(Config value) : config(value) {
    // Shard identities never change, including across stop/start. Reservations
    // may safely outlive a stop, but cannot send until the sender starts again.
    for (std::size_t i = 0; i < std::min<std::size_t>(config.sender_threads, 2); ++i)
      shards.push_back(std::make_shared<Shard>(i));
  }
  Config config;
  std::atomic<bool> running{false};
  std::atomic<std::size_t> reserved{0}, active{0}, completed{0}, aborted{0}, next_shard{0};
  std::vector<std::shared_ptr<Shard>> shards;
  void fail_closed() noexcept {
    running = false;
    for (const auto& shard : shards) shard->wake();
  }
};

BoundedResultSender::Reservation::Reservation(std::shared_ptr<State> state,
    std::shared_ptr<Shard> shard) : state_(std::move(state)), shard_(std::move(shard)) {}
BoundedResultSender::Reservation::~Reservation() {
  if (counted_) --state_->reserved;
}
void BoundedResultSender::Reservation::cancel() noexcept {
  cancelled_ = true;
  if (shard_) shard_->wake();
}
BoundedResultSender::BoundedResultSender() : BoundedResultSender(Config{}) {}
BoundedResultSender::BoundedResultSender(Config config)
  : state_(std::make_shared<State>(config)) {}
BoundedResultSender::~BoundedResultSender() { stop(); }

QueryStatus BoundedResultSender::start() {
  std::lock_guard<std::mutex> lifecycle(lifecycle_mutex_);
  if (state_->running) return QueryStatus::success();
  if (!threads_.empty())
    return failure(QueryError::Cancelled, "stop result sender before restarting");
  const auto& config = state_->config;
  if (config.max_transfers == 0 || config.max_transfers > 16 ||
      config.sender_threads == 0 || config.sender_threads > 2 ||
      config.buffer_bytes == 0 || config.buffer_bytes > 64 * 1024 ||
      config.max_header_bytes == 0 || config.max_header_bytes > 4096 ||
      config.idle_timeout <= std::chrono::milliseconds::zero())
    return failure(QueryError::InvalidQuery, "invalid result sender limits");
  auto close_pipes = [&] {
    for (const auto& shard : state_->shards) {
      std::lock_guard<std::mutex> lock(shard->mutex);
      if (shard->wake_read >= 0) ::close(shard->wake_read);
      if (shard->wake_write >= 0) ::close(shard->wake_write);
      shard->wake_read = shard->wake_write = -1;
    }
  };
  for (const auto& shard : state_->shards) {
    int fds[2];
    if (::pipe(fds) != 0) {
      close_pipes();
      return failure(QueryError::ReadFailure, "cannot create result sender wakeup pipe");
    }
    if (!nonblocking_cloexec(fds[0]) || !nonblocking_cloexec(fds[1])) {
      ::close(fds[0]); ::close(fds[1]); close_pipes();
      return failure(QueryError::ReadFailure, "cannot configure result sender wakeup pipe");
    }
    // cancel() can race with restart through an old reservation.
    std::lock_guard<std::mutex> lock(shard->mutex);
    shard->wake_read = fds[0]; shard->wake_write = fds[1];
  }
  state_->running = true;
  try {
    threads_.reserve(config.sender_threads);
    for (const auto& shard : state_->shards)
      threads_.emplace_back([this, shard] { run(shard); });
  } catch (...) {
    state_->fail_closed();
    for (auto& thread : threads_) if (thread.joinable()) thread.join();
    // A concurrent caller may have handed off to a shard whose thread could
    // not be created. In the stopped state run() only drains and releases its
    // queue; it never polls or sends. Drain every shard before closing pipes.
    for (const auto& shard : state_->shards) run(shard);
    threads_.clear(); close_pipes();
    return failure(QueryError::ResourceExhausted, "cannot start result sender");
  }
  return QueryStatus::success();
}
void BoundedResultSender::stop() noexcept {
  std::lock_guard<std::mutex> lifecycle(lifecycle_mutex_);
  state_->fail_closed();
  for (auto& thread : threads_) if (thread.joinable()) thread.join();
  threads_.clear();
  for (const auto& shard : state_->shards) {
    std::lock_guard<std::mutex> lock(shard->mutex);
    if (shard->wake_read >= 0) ::close(shard->wake_read);
    if (shard->wake_write >= 0) ::close(shard->wake_write);
    shard->wake_read = shard->wake_write = -1;
  }
}
std::shared_ptr<BoundedResultSender::Reservation> BoundedResultSender::reserve() {
  if (!state_->running) return {};
  const auto index = state_->next_shard.fetch_add(1) % state_->shards.size();
  auto reservation = std::shared_ptr<Reservation>(new Reservation(state_, state_->shards[index]));
  auto reserved = state_->reserved.load();
  do {
    if (reserved >= state_->config.max_transfers || !state_->running) return {};
  } while (!state_->reserved.compare_exchange_weak(reserved, reserved + 1));
  reservation->counted_ = true;
  if (!state_->running) return {};
  return reservation;
}
QueryStatus BoundedResultSender::send(std::shared_ptr<Reservation> reservation,
    std::shared_ptr<TypedResultStore::Handle> handle, int fd,
    std::shared_ptr<void> lifetime, std::string json_header,
    std::shared_ptr<TimingRecord> timing, std::function<void(TimingRecord&&)> sink) {
  if (!reservation || reservation->state_ != state_ || !handle ||
      !handle->valid() || fd < 0 || !lifetime || json_header.empty() ||
      json_header.size() > state_->config.max_header_bytes)
    return failure(QueryError::InvalidQuery, "invalid immutable result transfer");
  try {
    auto transfer = std::make_unique<Shard::Transfer>();
    transfer->reservation = std::move(reservation);
    transfer->handle = std::move(handle);
    transfer->lifetime = std::move(lifetime);
    transfer->fd = fd;
    transfer->header = std::move(json_header);
    transfer->timing = std::move(timing);
    transfer->sink = std::move(sink);
    const auto length = static_cast<std::uint32_t>(transfer->header.size());
    for (std::size_t i = 0; i < 4; ++i)
      transfer->prefix[i] = static_cast<std::uint8_t>(length >> (24 - i * 8));
#ifdef SO_NOSIGPIPE
    const int no_sigpipe = 1;
    if (::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &no_sigpipe, sizeof(no_sigpipe)) != 0)
      return failure(QueryError::ReadFailure, "cannot suppress result socket SIGPIPE");
#endif
    const auto& shard = transfer->reservation->shard_;
    {
      std::lock_guard<std::mutex> lock(shard->mutex);
      if (!state_->running || transfer->reservation->cancelled_)
        return failure(QueryError::Cancelled, "result sender stopped or transfer cancelled");
      if (transfer->reservation->submitted_.exchange(true))
        return failure(QueryError::InvalidQuery, "result admission already submitted");
      try {
        if (transfer->timing) {
          transfer->timing->enqueue_ns = now_ns();
          transfer->timing->shard = shard->index;
        }
        shard->queued.push_back(std::move(transfer));
      } catch (...) {
        transfer->reservation->submitted_ = false;
        throw;
      }
      ++state_->active;
      shard->wake_locked();
    }
    return QueryStatus::success();
  } catch (const std::bad_alloc&) {
    return failure(QueryError::ResourceExhausted, "cannot allocate result transfer");
  }
}
BoundedResultSender::Stats BoundedResultSender::stats() const {
  return Stats{state_->reserved.load(), state_->active.load(),
    state_->completed.load(), state_->aborted.load()};
}

void BoundedResultSender::run(const std::shared_ptr<Shard>& shard) noexcept {
  std::vector<std::unique_ptr<Shard::Transfer>> active;
  auto retire = [&](std::unique_ptr<Shard::Transfer> retired, bool completed,
      QueryError error) {
    --state_->active;
    if (completed) ++state_->completed; else ++state_->aborted;
    auto timing = std::move(retired->timing);
    auto sink = std::move(retired->sink);
    // Close/completion, payload pin and reservation refund all run without
    // sender locks. A trace observes sender-owned resources already released.
    retired->lifetime.reset();
    retired->handle.reset();
    retired->reservation.reset();
    retired.reset();
    if (timing) {
      timing->completed = completed;
      timing->error = error;
      timing->cleanup_ns = now_ns();
      if (sink) try { sink(std::move(*timing)); } catch (...) {}
    }
  };
  auto finish = [&](std::size_t index, bool completed, QueryError error) {
    auto retired = std::move(active[index]);
    active.erase(active.begin() + index);
    retire(std::move(retired), completed, error);
  };
  try {
    active.reserve(state_->config.max_transfers);
    while (true) {
      {
        std::lock_guard<std::mutex> lock(shard->mutex);
        for (auto& transfer : shard->queued) {
          if (transfer->timing) transfer->timing->dequeue_ns = now_ns();
          active.push_back(std::move(transfer));
        }
        shard->queued.clear();
      }
      if (!state_->running) break;
      const auto now = Clock::now();
      int timeout = 1000;
      std::vector<pollfd> poll_fds;
      poll_fds.reserve(active.size() + 1);
      poll_fds.push_back({shard->wake_read, POLLIN, 0});
      for (std::size_t i = 0; i < active.size();) {
        // MSG_DONTWAIT alone does not reliably keep sendmsg nonblocking on
        // every Unix-domain implementation. Configure only after handoff, so
        // immediate rejections retain the control socket's write semantics.
        if (!active[i]->configured) {
          const int flags = ::fcntl(active[i]->fd, F_GETFL);
          if (flags < 0 || ::fcntl(active[i]->fd, F_SETFL, flags | O_NONBLOCK) != 0) {
            finish(i, false, QueryError::ReadFailure);
            continue;
          }
          active[i]->configured = true;
        }
        const auto remaining = state_->config.idle_timeout -
          std::chrono::duration_cast<std::chrono::milliseconds>(now - active[i]->progressed);
        if (active[i]->reservation->cancelled_ || remaining <= std::chrono::milliseconds::zero()) {
          finish(i, false, active[i]->reservation->cancelled_ ?
            QueryError::Cancelled : QueryError::DeadlineExceeded);
          continue;
        }
        timeout = std::min(timeout, static_cast<int>(std::min<std::int64_t>(remaining.count(), INT_MAX)));
        poll_fds.push_back({active[i]->fd, POLLOUT, 0}); ++i;
      }
      const int ready = ::poll(poll_fds.data(), poll_fds.size(), timeout);
      if (ready < 0) { if (errno == EINTR) continue; state_->fail_closed(); break; }
      if (poll_fds[0].revents & POLLIN) {
        std::array<char, 128> drain;
        while (::read(shard->wake_read, drain.data(), drain.size()) > 0) {}
      }
      for (std::size_t i = active.size(); i > 0; --i) {
        auto& transfer = *active[i - 1];
        const auto events = poll_fds[i].revents;
        if (events & (POLLERR | POLLHUP | POLLNVAL)) {
          finish(i - 1, false, QueryError::ReadFailure); continue;
        }
        if (!(events & POLLOUT)) continue;
        if (transfer.reservation->cancelled_ || !state_->running) {
          finish(i - 1, false, QueryError::Cancelled); continue;
        }
        const auto framed_header = transfer.header.size() + 4;
        std::array<iovec, 3> vectors{};
        std::size_t vector_count = 0, offered = 0;
        auto add = [&](const void* data, std::size_t count) {
          count = std::min(count, state_->config.buffer_bytes - offered);
          if (count) { vectors[vector_count++] = {const_cast<void*>(data), count}; offered += count; }
        };
        if (transfer.header_offset < 4)
          add(transfer.prefix.data() + transfer.header_offset, 4 - transfer.header_offset);
        if (transfer.header_offset < framed_header) {
          const auto offset = transfer.header_offset > 4 ? transfer.header_offset - 4 : 0;
          add(transfer.header.data() + offset, transfer.header.size() - offset);
        }
        // The slice pins both bytes and their resident-budget owner. No body
        // staging allocation or user-space copy is needed, even on partial I/O.
        auto slice = transfer.handle->slice(transfer.body_offset,
          state_->config.buffer_bytes - offered);
        if (!slice.ok()) { finish(i - 1, false, slice.status().code()); continue; }
        auto* timing = transfer.timing.get();
        ssize_t sent = -1;
        int send_errno = 0;
        std::uint64_t send_end_ns = 0;
        slice.value().with_bytes([&](const std::uint8_t* data, std::size_t size) {
          add(data, size);
          msghdr message{};
          message.msg_iov = vectors.data(); message.msg_iovlen = vector_count;
          int flags = MSG_DONTWAIT;
#ifdef MSG_NOSIGNAL
          flags |= MSG_NOSIGNAL;
#endif
          const auto before = timing ? cpu_ns() : 0;
          const auto send_begin = timing ? now_ns() : 0;
          sent = ::sendmsg(transfer.fd, &message, flags);
          send_errno = errno;
          const auto send_end = timing ? now_ns() : 0;
          send_end_ns = send_end;
          if (timing) {
            const auto after = cpu_ns();
            if (after >= before) timing->send_cpu_ns += after - before;
            ++timing->send_calls;
            if (timing->send_span_count < timing->send_spans.size())
              timing->send_spans[timing->send_span_count++] = {send_begin, send_end,
                offered, sent, sent < 0 ? send_errno : 0};
            else ++timing->send_spans_dropped;
          }
        });
        // Do not retain this additional payload pin across finish/cleanup.
        slice = QueryResult<TypedResultStore::ReadOnlySlice>::success({});
        if (sent < 0 && (send_errno == EAGAIN || send_errno == EWOULDBLOCK || send_errno == EINTR)) {
          if (timing && send_errno != EINTR) ++timing->eagain;
          continue;
        }
        if (sent <= 0) { finish(i - 1, false, QueryError::ReadFailure); continue; }
        transfer.progressed = Clock::now();
        if (timing) {
          if (!timing->first_send_ns) timing->first_send_ns = send_end_ns;
          timing->bytes_sent += static_cast<std::uint64_t>(sent);
          if (static_cast<std::size_t>(sent) < offered) ++timing->short_writes;
        }
        const auto header_sent = std::min<std::size_t>(sent, framed_header - transfer.header_offset);
        transfer.header_offset += header_sent;
        transfer.body_offset += static_cast<std::size_t>(sent) - header_sent;
        if (transfer.header_offset == framed_header && transfer.body_offset == transfer.handle->size()) {
          if (timing) timing->last_send_ns = send_end_ns;
          finish(i - 1, true, QueryError::None);
        }
      }
    }
  } catch (...) {
    state_->fail_closed();
  }
  std::vector<std::unique_ptr<Shard::Transfer>> queued;
  {
    std::lock_guard<std::mutex> lock(shard->mutex);
    queued.swap(shard->queued);
  }
  for (auto& transfer : active) retire(std::move(transfer), false, QueryError::Cancelled);
  for (auto& transfer : queued) retire(std::move(transfer), false, QueryError::Cancelled);
}

} // namespace iotdbfs
