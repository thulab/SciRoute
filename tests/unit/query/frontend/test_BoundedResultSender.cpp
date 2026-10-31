#include "iotdbfs/query/frontend/BoundedResultSender.h"

#include <gtest/gtest.h>

#include <atomic>
#include <algorithm>
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <poll.h>
#include <stdexcept>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {
using namespace iotdbfs;
using namespace std::chrono_literals;

struct SocketOwner {
  explicit SocketOwner(int value) : fd(value) {}
  ~SocketOwner() { ::close(fd); }
  int fd;
};
struct Pair {
  Pair() {
    int sockets[2];
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) != 0)
      throw std::runtime_error("socketpair failed");
    sender = std::make_shared<SocketOwner>(sockets[0]);
    receiver = sockets[1];
    const int small = 1024;
    ::setsockopt(sender->fd, SOL_SOCKET, SO_SNDBUF, &small, sizeof(small));
  }
  ~Pair() { if (receiver >= 0) ::close(receiver); }
  std::shared_ptr<SocketOwner> sender;
  int receiver;
};
bool until(const std::function<bool()>& predicate, std::chrono::milliseconds limit = 2s) {
  const auto deadline = std::chrono::steady_clock::now() + limit;
  while (!predicate()) {
    if (std::chrono::steady_clock::now() >= deadline) return false;
    std::this_thread::sleep_for(1ms);
  }
  return true;
}
std::shared_ptr<TypedResultStore::Handle> payload(TypedResultStore& store,
    const std::vector<std::uint8_t>& bytes) {
  auto published = store.publish(bytes);
  if (!published.ok()) throw std::runtime_error(published.status().message());
  auto opened = store.open(published.value());
  if (!opened.ok()) throw std::runtime_error(opened.status().message());
  return std::make_shared<TypedResultStore::Handle>(std::move(opened.value()));
}
std::vector<std::uint8_t> receive(int fd, std::size_t wanted) {
  std::vector<std::uint8_t> bytes;
  bytes.reserve(wanted);
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (bytes.size() < wanted && std::chrono::steady_clock::now() < deadline) {
    pollfd poll_fd{fd, POLLIN, 0};
    if (::poll(&poll_fd, 1, 10) <= 0) continue;
    std::uint8_t buffer[8192];
    auto count = ::read(fd, buffer, std::min(sizeof(buffer), wanted - bytes.size()));
    if (count <= 0) break;
    bytes.insert(bytes.end(), buffer, buffer + count);
  }
  return bytes;
}
std::vector<std::uint8_t> framed(const std::string& header,
    const std::vector<std::uint8_t>& bytes) {
  const auto size = static_cast<std::uint32_t>(header.size());
  std::vector<std::uint8_t> result{
    static_cast<std::uint8_t>(size >> 24), static_cast<std::uint8_t>(size >> 16),
    static_cast<std::uint8_t>(size >> 8), static_cast<std::uint8_t>(size)};
  result.insert(result.end(), header.begin(), header.end());
  result.insert(result.end(), bytes.begin(), bytes.end());
  return result;
}
BoundedResultSender::Config small_limits() {
  BoundedResultSender::Config config;
  config.max_transfers = 2;
  config.idle_timeout = 500ms;
  return config;
}
void handoff(BoundedResultSender& sender, Pair& pair,
    std::shared_ptr<TypedResultStore::Handle> handle, std::string header = "{}") {
  auto reservation = sender.reserve();
  ASSERT_NE(reservation, nullptr);
  ASSERT_TRUE(sender.send(std::move(reservation), std::move(handle), pair.sender->fd,
    pair.sender, std::move(header)).ok());
  pair.sender.reset();
}

TEST(BoundedResultSenderTest, ReservesBeforeAuthorizationAndCountsUntilRelease) {
  BoundedResultSender sender(small_limits());
  EXPECT_EQ(sender.reserve(), nullptr);
  ASSERT_TRUE(sender.start().ok());
  auto first = sender.reserve();
  auto second = sender.reserve();
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(sender.reserve(), nullptr);
  EXPECT_EQ(sender.stats().reserved, 2u);
  first.reset();
  EXPECT_NE(sender.reserve(), nullptr);
  EXPECT_EQ(sender.stats().reserved, 1u);
  sender.stop();
  EXPECT_EQ(sender.reserve(), nullptr);
  second.reset();
  EXPECT_EQ(sender.stats().reserved, 0u);
}

TEST(BoundedResultSenderTest, RejectsUnboundedAndNonprogressingConfiguration) {
  auto config = small_limits();
  config.sender_threads = 0;
  EXPECT_EQ(BoundedResultSender(config).start().code(), QueryError::InvalidQuery);
  config.sender_threads = 3;
  EXPECT_EQ(BoundedResultSender(config).start().code(), QueryError::InvalidQuery);
  config.sender_threads = 1;
  config.buffer_bytes = 65537;
  EXPECT_EQ(BoundedResultSender(config).start().code(), QueryError::InvalidQuery);
  config.buffer_bytes = 0;
  EXPECT_EQ(BoundedResultSender(config).start().code(), QueryError::InvalidQuery);
  config.buffer_bytes = 65536;
  config.max_transfers = 17;
  EXPECT_EQ(BoundedResultSender(config).start().code(), QueryError::InvalidQuery);
  config.max_transfers = 0;
  EXPECT_EQ(BoundedResultSender(config).start().code(), QueryError::InvalidQuery);
  config.max_transfers = 1;
  config.max_header_bytes = 4097;
  EXPECT_EQ(BoundedResultSender(config).start().code(), QueryError::InvalidQuery);
  config.max_header_bytes = 4096;
  config.idle_timeout = 0ms;
  EXPECT_EQ(BoundedResultSender(config).start().code(), QueryError::InvalidQuery);
}

TEST(BoundedResultSenderTest, StreamsExactFrameAndImmutableBodyAcrossPartialWrites) {
  BoundedResultSender sender;
  ASSERT_TRUE(sender.start().ok());
  TypedResultStore store({});
  std::vector<std::uint8_t> body(256 * 1024);
  for (std::size_t i = 0; i < body.size(); ++i) body[i] = i % 251;
  Pair pair;
  const std::string header = "{\"ok\":true,\"size\":262144}";
  handoff(sender, pair, payload(store, body), header);
  const auto expected = framed(header, body);
  EXPECT_EQ(receive(pair.receiver, expected.size()), expected);
  ASSERT_TRUE(until([&] { return sender.stats().reserved == 0; }));
  EXPECT_EQ(sender.stats().completed, 1u);
  EXPECT_EQ(store.stats().open_handles, 0u);
}

TEST(BoundedResultSenderTest, SlowReaderDoesNotBlockAnotherTransfer) {
  BoundedResultSender sender(small_limits());
  ASSERT_TRUE(sender.start().ok());
  TypedResultStore store({});
  Pair slow, fast;
  handoff(sender, slow, payload(store, std::vector<std::uint8_t>(1024 * 1024, 42)));
  const std::vector<std::uint8_t> body{7, 6, 5, 4};
  handoff(sender, fast, payload(store, body));
  EXPECT_EQ(receive(fast.receiver, 10), framed("{}", body));
  // Completion counters precede lock-free transfer destruction, which refunds
  // its reservation. Wait for both rather than treating completion as refund.
  ASSERT_TRUE(until([&] {
    const auto stats = sender.stats();
    return stats.completed == 1 && stats.reserved == 1 && stats.active == 1;
  }));
  EXPECT_EQ(sender.stats().active, 1u);
  EXPECT_EQ(sender.stats().reserved, 1u);
}

TEST(BoundedResultSenderTest, RejectsCancelledReservationBeforeHandoffWithoutSending) {
  BoundedResultSender sender;
  ASSERT_TRUE(sender.start().ok());
  TypedResultStore store({});
  Pair pair;
  auto reservation = sender.reserve();
  reservation->cancel();
  auto status = sender.send(reservation, payload(store, {1}), pair.sender->fd,
    pair.sender, "{}");
  EXPECT_EQ(status.code(), QueryError::Cancelled);
  pollfd descriptor{pair.receiver, POLLIN, 0};
  EXPECT_EQ(::poll(&descriptor, 1, 20), 0);
  EXPECT_EQ(sender.stats().active, 0u);
  reservation.reset();
  EXPECT_EQ(sender.stats().reserved, 0u);
}

TEST(BoundedResultSenderTest, CancellationAbortsBlockedTransferAndReleasesItsPin) {
  BoundedResultSender sender;
  ASSERT_TRUE(sender.start().ok());
  TypedResultStore store({});
  Pair pair;
  auto reservation = sender.reserve();
  ASSERT_TRUE(sender.send(reservation, payload(store, std::vector<std::uint8_t>(1024 * 1024)),
    pair.sender->fd, pair.sender, "{}").ok());
  pair.sender.reset();
  reservation->cancel();
  reservation.reset();
  ASSERT_TRUE(until([&] { return sender.stats().reserved == 0; }));
  EXPECT_EQ(sender.stats().aborted, 1u);
  EXPECT_EQ(store.stats().open_handles, 0u);
}

TEST(BoundedResultSenderTest, IdleTimeoutAbortsSlowReaderAndRestoresAdmission) {
  auto config = small_limits();
  config.max_transfers = 1;
  config.idle_timeout = 30ms;
  BoundedResultSender sender(config);
  ASSERT_TRUE(sender.start().ok());
  TypedResultStore store({});
  Pair pair;
  handoff(sender, pair, payload(store, std::vector<std::uint8_t>(1024 * 1024)));
  ASSERT_TRUE(until([&] { return sender.stats().aborted == 1; }));
  ASSERT_TRUE(until([&] { return sender.stats().reserved == 0; }));
  EXPECT_NE(sender.reserve(), nullptr);
  EXPECT_EQ(store.stats().open_handles, 0u);
}

TEST(BoundedResultSenderTest, StopAbortsWithoutWaitingForSlowReaderAndCanRestart) {
  BoundedResultSender sender;
  ASSERT_TRUE(sender.start().ok());
  TypedResultStore store({});
  Pair pair;
  handoff(sender, pair, payload(store, std::vector<std::uint8_t>(1024 * 1024)));
  const auto began = std::chrono::steady_clock::now();
  sender.stop();
  EXPECT_LT(std::chrono::steady_clock::now() - began, 500ms);
  EXPECT_EQ(sender.stats().active, 0u);
  EXPECT_EQ(sender.stats().reserved, 0u);
  EXPECT_EQ(sender.stats().aborted, 1u);
  ASSERT_TRUE(sender.start().ok());
  EXPECT_NE(sender.reserve(), nullptr);
}

TEST(BoundedResultSenderTest, DisconnectAbortsOnlyTheAffectedTransfer) {
  BoundedResultSender sender;
  ASSERT_TRUE(sender.start().ok());
  TypedResultStore store({});
  Pair broken, healthy;
  handoff(sender, broken, payload(store, std::vector<std::uint8_t>(1024 * 1024)));
  ::close(broken.receiver);
  broken.receiver = -1;
  handoff(sender, healthy, payload(store, {3, 4}));
  EXPECT_EQ(receive(healthy.receiver, 8), framed("{}", {3, 4}));
  ASSERT_TRUE(until([&] { return sender.stats().reserved == 0; }));
  EXPECT_EQ(sender.stats().completed, 1u);
  EXPECT_EQ(sender.stats().aborted, 1u);
}

TEST(BoundedResultSenderTest, SustainedProgressRestartsTimeoutRatherThanCappingTotalTime) {
  auto config = small_limits();
  config.idle_timeout = 100ms;
  BoundedResultSender sender(config);
  ASSERT_TRUE(sender.start().ok());
  TypedResultStore store({});
  Pair pair;
  const std::vector<std::uint8_t> body(64 * 1024, 9);
  handoff(sender, pair, payload(store, body));
  const auto expected = framed("{}", body);
  std::vector<std::uint8_t> received;
  const auto began = std::chrono::steady_clock::now();
  while (received.size() < expected.size() &&
      std::chrono::steady_clock::now() - began < 2s) {
    auto chunk = receive(pair.receiver, std::min<std::size_t>(1024, expected.size() - received.size()));
    if (chunk.empty()) break;
    received.insert(received.end(), chunk.begin(), chunk.end());
    std::this_thread::sleep_for(5ms);
  }
  EXPECT_GT(std::chrono::steady_clock::now() - began, 100ms);
  EXPECT_EQ(received, expected);
  EXPECT_EQ(sender.stats().aborted, 0u);
}

TEST(BoundedResultSenderTest, ConcurrentAdmissionCannotExceedConfiguredLimit) {
  auto config = small_limits();
  config.max_transfers = 4;
  BoundedResultSender sender(config);
  ASSERT_TRUE(sender.start().ok());
  std::atomic<std::size_t> arrived{0};
  std::atomic<bool> release{false};
  std::vector<std::thread> workers;
  std::atomic<std::size_t> accepted{0};
  for (int i = 0; i < 16; ++i) workers.emplace_back([&] {
    auto reservation = sender.reserve();
    if (reservation) ++accepted;
    ++arrived;
    while (!release.load()) std::this_thread::yield();
  });
  const bool all_arrived = until([&] { return arrived == 16; });
  EXPECT_TRUE(all_arrived);
  EXPECT_EQ(accepted.load(), 4u);
  EXPECT_EQ(sender.stats().reserved, 4u);
  release = true;
  for (auto& worker : workers) worker.join();
  EXPECT_EQ(sender.stats().reserved, 0u);
}

TEST(BoundedResultSenderTest, RetainsResidentBudgetAcrossRegistryEvictionUntilStop) {
  BoundedResultSender sender;
  ASSERT_TRUE(sender.start().ok());
  TypedResultStore::Config config;
  config.max_result_bytes = 1024 * 1024;
  config.max_resident_bytes = 1024 * 1024 + 1;
  config.max_entries = 1;
  TypedResultStore store(config);
  Pair pair;
  handoff(sender, pair, payload(store, std::vector<std::uint8_t>(1024 * 1024, 2)));
  ASSERT_TRUE(store.publish({1}).ok());
  EXPECT_EQ(store.stats().resident_bytes, 1024u * 1024u + 1u);
  EXPECT_EQ(store.publish({1, 2}).status().code(), QueryError::ResourceExhausted);
  sender.stop();
  EXPECT_EQ(store.stats().open_handles, 0u);
  EXPECT_LE(store.stats().resident_bytes, 1u);
  EXPECT_TRUE(store.publish({1, 2}).ok());
}

TEST(BoundedResultSenderTest, CompletionLifetimeRunsOutsideSenderMutex) {
  BoundedResultSender sender;
  ASSERT_TRUE(sender.start().ok());
  TypedResultStore store({});
  Pair pair;
  std::atomic<bool> released{false};
  auto lifetime = std::shared_ptr<void>(new int(1),
    [&, socket = pair.sender](void* ptr) {
      delete static_cast<int*>(ptr);
      (void)sender.stats(); // Would deadlock if cleanup held State::mutex.
      released = true;
    });
  auto reservation = sender.reserve();
  ASSERT_TRUE(sender.send(std::move(reservation),
    payload(store, std::vector<std::uint8_t>(1024 * 1024)), pair.sender->fd,
    std::move(lifetime), "{}").ok());
  pair.sender.reset();
  sender.stop();
  EXPECT_TRUE(released.load());
}

TEST(BoundedResultSenderTest, RejectsForeignReusedAndOversizedRequests) {
  BoundedResultSender sender, other;
  ASSERT_TRUE(sender.start().ok());
  ASSERT_TRUE(other.start().ok());
  TypedResultStore store({});
  Pair pair;
  auto handle = payload(store, std::vector<std::uint8_t>(1024 * 1024));
  auto foreign = other.reserve();
  EXPECT_EQ(sender.send(foreign, handle, pair.sender->fd, pair.sender, "{}").code(),
    QueryError::InvalidQuery);
  auto reservation = sender.reserve();
  EXPECT_EQ(sender.send(reservation, handle, pair.sender->fd, pair.sender,
    std::string(4097, 'x')).code(), QueryError::InvalidQuery);
  ASSERT_TRUE(sender.send(reservation, handle, pair.sender->fd, pair.sender, "{}").ok());
  EXPECT_EQ(sender.send(reservation, handle, pair.sender->fd, pair.sender, "{}").code(),
    QueryError::InvalidQuery);
  reservation->cancel();
}

TEST(BoundedResultSenderTest, CoalescesSmallPrefixHeaderAndBodyInOneSendmsg) {
  BoundedResultSender sender;
  ASSERT_TRUE(sender.start().ok());
  TypedResultStore store({});
  Pair pair;
  const std::vector<std::uint8_t> body(100, 17);
  auto timing = std::make_shared<BoundedResultSender::TimingRecord>();
  timing->path = "/already-authorized";
  std::promise<BoundedResultSender::TimingRecord> done;
  auto future = done.get_future();
  ASSERT_TRUE(sender.send(sender.reserve(), payload(store, body), pair.sender->fd,
    pair.sender, "{\"ok\":true}", std::move(timing),
    [&](BoundedResultSender::TimingRecord&& record) {
      EXPECT_EQ(sender.stats().reserved, 0u);
      EXPECT_EQ(store.stats().open_handles, 0u);
      done.set_value(std::move(record));
      throw std::runtime_error("diagnostic failure must be isolated");
    }).ok());
  pair.sender.reset();
  const auto expected = framed("{\"ok\":true}", body);
  EXPECT_EQ(receive(pair.receiver, expected.size()), expected);
  ASSERT_EQ(future.wait_for(2s), std::future_status::ready);
  const auto record = future.get();
  EXPECT_TRUE(record.completed);
  EXPECT_EQ(record.error, QueryError::None);
  EXPECT_EQ(record.path, "/already-authorized");
  EXPECT_EQ(record.send_calls, 1u);
  EXPECT_EQ(record.bytes_sent, expected.size());
  EXPECT_EQ(record.short_writes, 0u);
  EXPECT_GT(record.enqueue_ns, 0u);
  EXPECT_LE(record.enqueue_ns, record.dequeue_ns);
  EXPECT_LE(record.dequeue_ns, record.first_send_ns);
  EXPECT_LE(record.first_send_ns, record.last_send_ns);
  EXPECT_LE(record.last_send_ns, record.cleanup_ns);
  sender.stop();
}

TEST(BoundedResultSenderTest, IovecProgressPreservesFrameAtEveryBoundaryAndRoundLimit) {
  for (const auto limit : {1u, 4u, 5u, 16u, 64u}) {
    SCOPED_TRACE(limit);
    auto config = small_limits();
    config.sender_threads = 2;
    config.buffer_bytes = limit;
    BoundedResultSender sender(config);
    ASSERT_TRUE(sender.start().ok());
    TypedResultStore store({});
    Pair pair;
    const std::vector<std::uint8_t> body(93, 54);
    const std::string header = "{\"length\":93}";
    std::promise<BoundedResultSender::TimingRecord> done;
    auto future = done.get_future();
    ASSERT_TRUE(sender.send(sender.reserve(), payload(store, body), pair.sender->fd,
      pair.sender, header, std::make_shared<BoundedResultSender::TimingRecord>(),
      [&](BoundedResultSender::TimingRecord&& record) { done.set_value(std::move(record)); }).ok());
    pair.sender.reset();
    const auto expected = framed(header, body);
    EXPECT_EQ(receive(pair.receiver, expected.size()), expected);
    ASSERT_EQ(future.wait_for(2s), std::future_status::ready);
    const auto record = future.get();
    EXPECT_TRUE(record.completed);
    EXPECT_EQ(record.bytes_sent, expected.size());
    EXPECT_GE(record.send_calls, (expected.size() + limit - 1) / limit);
    EXPECT_EQ(record.send_span_count + record.send_spans_dropped, record.send_calls);
    EXPECT_LE(record.send_span_count, 64u);
    if (limit == 1) EXPECT_GT(record.send_spans_dropped, 0u);
    for (std::size_t i = 0; i < record.send_span_count; ++i) {
      EXPECT_LE(record.send_spans[i].begin_ns, record.send_spans[i].end_ns);
      EXPECT_LE(record.send_spans[i].end_ns, record.cleanup_ns);
      EXPECT_LE(record.send_spans[i].requested_bytes, limit);
      EXPECT_GT(record.send_spans[i].requested_bytes, 0u);
      if (record.send_spans[i].result_bytes >= 0)
        EXPECT_EQ(record.send_spans[i].error_no, 0);
    }
  }
}

TEST(BoundedResultSenderTest, TwoShardsShareAdmissionAndProgressIndependently) {
  auto config = small_limits();
  config.sender_threads = 2;
  BoundedResultSender sender(config);
  ASSERT_TRUE(sender.start().ok());
  TypedResultStore store({});
  Pair slow, fast;
  auto slow_reservation = sender.reserve();
  auto fast_reservation = sender.reserve();
  ASSERT_NE(slow_reservation, nullptr);
  ASSERT_NE(fast_reservation, nullptr);
  EXPECT_EQ(sender.reserve(), nullptr);
  std::promise<BoundedResultSender::TimingRecord> slow_done, fast_done;
  auto slow_future = slow_done.get_future(), fast_future = fast_done.get_future();
  ASSERT_TRUE(sender.send(slow_reservation, payload(store, std::vector<std::uint8_t>(1024 * 1024)),
    slow.sender->fd, slow.sender, "{}", std::make_shared<BoundedResultSender::TimingRecord>(),
    [&](BoundedResultSender::TimingRecord&& record) { slow_done.set_value(std::move(record)); }).ok());
  slow.sender.reset();
  ASSERT_TRUE(sender.send(std::move(fast_reservation), payload(store, {3, 2, 1}),
    fast.sender->fd, fast.sender, "{}", std::make_shared<BoundedResultSender::TimingRecord>(),
    [&](BoundedResultSender::TimingRecord&& record) { fast_done.set_value(std::move(record)); }).ok());
  fast.sender.reset();
  EXPECT_EQ(receive(fast.receiver, 9), framed("{}", {3, 2, 1}));
  ASSERT_EQ(fast_future.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(sender.stats().active, 1u);
  EXPECT_EQ(sender.stats().reserved, 1u);
  slow_reservation->cancel();
  slow_reservation.reset();
  ASSERT_EQ(slow_future.wait_for(2s), std::future_status::ready);
  const auto fast_record = fast_future.get(), slow_record = slow_future.get();
  EXPECT_NE(fast_record.shard, slow_record.shard);
  EXPECT_TRUE(fast_record.completed);
  EXPECT_FALSE(slow_record.completed);
  EXPECT_EQ(slow_record.error, QueryError::Cancelled);
  EXPECT_EQ(slow_record.last_send_ns, 0u);
  EXPECT_EQ(sender.stats().reserved, 0u);
  EXPECT_EQ(store.stats().open_handles, 0u);
}

TEST(BoundedResultSenderTest, PartialSendmsgRecordsExactBytesAndShortWrites) {
  auto config = small_limits();
  config.sender_threads = 2;
  BoundedResultSender sender(config);
  ASSERT_TRUE(sender.start().ok());
  TypedResultStore store({});
  Pair pair;
  const std::vector<std::uint8_t> body(256 * 1024, 71);
  std::promise<BoundedResultSender::TimingRecord> done;
  auto future = done.get_future();
  ASSERT_TRUE(sender.send(sender.reserve(), payload(store, body), pair.sender->fd,
    pair.sender, "{}", std::make_shared<BoundedResultSender::TimingRecord>(),
    [&](BoundedResultSender::TimingRecord&& record) { done.set_value(std::move(record)); }).ok());
  pair.sender.reset();
  const auto expected = framed("{}", body);
  EXPECT_EQ(receive(pair.receiver, expected.size()), expected);
  ASSERT_EQ(future.wait_for(2s), std::future_status::ready);
  const auto record = future.get();
  EXPECT_TRUE(record.completed);
  EXPECT_EQ(record.bytes_sent, expected.size());
  EXPECT_GT(record.short_writes, 0u);
  EXPECT_GT(record.send_calls, 4u);
}

TEST(BoundedResultSenderTest, TwoShardsStopAndCancelReleaseEveryQueuedTransfer) {
  for (int iteration = 0; iteration < 10; ++iteration) {
    auto config = small_limits();
    config.sender_threads = 2;
    config.max_transfers = 16;
    BoundedResultSender sender(config);
    ASSERT_TRUE(sender.start().ok());
    TypedResultStore store({});
    std::vector<std::unique_ptr<Pair>> pairs;
    std::vector<std::shared_ptr<BoundedResultSender::Reservation>> reservations;
    std::atomic<std::size_t> traced{0};
    for (int i = 0; i < 16; ++i) {
      pairs.push_back(std::make_unique<Pair>());
      auto& pair = *pairs.back();
      auto reservation = sender.reserve();
      ASSERT_NE(reservation, nullptr);
      ASSERT_TRUE(sender.send(reservation, payload(store, std::vector<std::uint8_t>(128 * 1024)),
        pair.sender->fd, pair.sender, "{}", std::make_shared<BoundedResultSender::TimingRecord>(),
        [&](BoundedResultSender::TimingRecord&& record) {
          EXPECT_FALSE(record.completed);
          EXPECT_EQ(record.error, QueryError::Cancelled);
          ++traced;
        }).ok());
      pair.sender.reset();
      reservations.push_back(std::move(reservation));
    }
    EXPECT_EQ(sender.reserve(), nullptr);
    std::thread canceller([&] { for (auto& reservation : reservations) reservation->cancel(); });
    sender.stop();
    canceller.join();
    reservations.clear();
    EXPECT_EQ(traced, 16u);
    EXPECT_EQ(sender.stats().active, 0u);
    EXPECT_EQ(sender.stats().reserved, 0u);
    EXPECT_EQ(store.stats().open_handles, 0u);
    ASSERT_TRUE(sender.start().ok());
    EXPECT_NE(sender.reserve(), nullptr);
  }
}

TEST(BoundedResultSenderTest, ImmediateRejectionDoesNotConsumeDiagnosticSink) {
  BoundedResultSender sender;
  ASSERT_TRUE(sender.start().ok());
  TypedResultStore store({});
  Pair pair;
  auto reservation = sender.reserve();
  reservation->cancel();
  std::atomic<std::size_t> calls{0};
  const auto status = sender.send(reservation, payload(store, {1, 2}), pair.sender->fd,
    pair.sender, "{}", std::make_shared<BoundedResultSender::TimingRecord>(),
    [&](BoundedResultSender::TimingRecord&&) { ++calls; });
  EXPECT_EQ(status.code(), QueryError::Cancelled);
  EXPECT_EQ(calls, 0u);
  EXPECT_EQ(sender.stats().active, 0u);
  EXPECT_EQ(sender.stats().aborted, 0u);
}

} // namespace
