#include "iotdbfs/storage/versioning/detail/OwnerReadTiming.h"
#include "iotdbfs/storage/versioning/detail/BoundedReaderPool.h"

#include <gtest/gtest.h>
#include <algorithm>
#include <fstream>
#include <iterator>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using iotdbfs::versioning::detail::OwnerReadTiming;
using iotdbfs::versioning::detail::OwnerReadTraceSink;
using iotdbfs::versioning::detail::BoundedReaderPool;

namespace {
class OwnerReadTimingTest : public ::testing::Test {
protected:
  void SetUp() override {
    char pattern[] = "/tmp/iotdbfs-owner-timing-XXXXXX";
    const auto created = ::mkdtemp(pattern);
    ASSERT_NE(created, nullptr);
    directory = created;
    path = directory + "/trace.jsonl";
  }
  void TearDown() override {
    ::unlink((directory + "/link").c_str());
    ::unlink((directory + "/fifo").c_str());
    ::unlink((directory + "/ready").c_str());
    ::unlink(path.c_str());
    ::rmdir(directory.c_str());
  }
  std::string read() const {
    std::ifstream file(path);
    return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
  }
  static std::uint64_t number(const std::string& line, const std::string& key) {
    const auto position = line.find('"' + key + "\":");
    if (position == std::string::npos) throw std::runtime_error("missing trace field");
    return std::stoull(line.substr(position + key.size() + 3));
  }
  std::string directory, path;
};
struct Reader { explicit Reader(int& closed) : closed(closed) {} ~Reader() { ++closed; } int& closed; };
}

TEST_F(OwnerReadTimingTest, DisabledTraceLeavesClockFieldsZero) {
  const auto gate = directory + "/ready";
  OwnerReadTraceSink sink(nullptr, gate.c_str());
  OwnerReadTiming timing(false, 1, sink);
  timing.mark(timing.record.acquire_start_ns);
  { OwnerReadTiming::Duration duration(timing, timing.record.digest_ns); }
  EXPECT_EQ(timing.record.begin_ns, 0u);
  EXPECT_EQ(timing.record.acquire_start_ns, 0u);
  EXPECT_EQ(timing.record.digest_ns, 0u);
  EXPECT_FALSE(sink.enabled());
}

TEST_F(OwnerReadTimingTest, SuccessRecordsRealLeaseReleaseAndNestedDurations) {
  OwnerReadTraceSink sink(path.c_str());
  ASSERT_TRUE(sink.enabled());
  int closed = 0;
  BoundedReaderPool<Reader> pool(1, std::chrono::milliseconds(100));
  {
    OwnerReadTiming timing(false, 1, sink);
    timing.record.payload_blocks = true;
    using Lease = BoundedReaderPool<Reader>::Lease;
    std::optional<Lease> lease;
    {
      OwnerReadTiming::Release<std::optional<Lease>> release(timing, lease);
      timing.mark(timing.record.acquire_start_ns);
      lease.emplace(pool.acquire([&] {
        timing.mark(timing.record.connect_start_ns);
        auto result = std::make_unique<Reader>(closed);
        timing.mark(timing.record.connect_end_ns);
        return result;
      }, [] {}));
      timing.mark(timing.record.acquire_end_ns);
      {
        OwnerReadTiming::Duration visitor(timing, timing.record.visitor_ns);
        OwnerReadTiming::Duration digest(timing, timing.record.digest_ns);
      }
      lease->reuse();
      timing.record.success = true;
      timing.stage("complete");
    }
    EXPECT_FALSE(lease.has_value());
    EXPECT_EQ(pool.stats().active, 0u);
    EXPECT_EQ(pool.stats().discarded, 0u);
    EXPECT_EQ(closed, 0);
    EXPECT_LE(timing.record.release_start_ns, timing.record.release_end_ns);
  }
  const auto output = read();
  EXPECT_NE(output.find("\"success\":true"), std::string::npos);
  EXPECT_NE(output.find("\"payload_blocks\":true"), std::string::npos);
  EXPECT_NE(output.find("\"stage\":\"complete\""), std::string::npos);
  EXPECT_LE(number(output, "acquire_start_ns"), number(output, "connect_start_ns"));
  EXPECT_LE(number(output, "connect_end_ns"), number(output, "acquire_end_ns"));
  EXPECT_LE(number(output, "digest_ns"), number(output, "visitor_ns"));
  EXPECT_LE(number(output, "release_end_ns"), number(output, "end_ns"));
  EXPECT_EQ(std::count(output.begin(), output.end(), '\n'), 1);
}

TEST_F(OwnerReadTimingTest, ExceptionPreservedAndFailedLeaseDiscardedBeforeRecord) {
  OwnerReadTraceSink sink(path.c_str());
  int closed = 0;
  BoundedReaderPool<Reader> pool(1, std::chrono::milliseconds(100));
  try {
    OwnerReadTiming timing(true, 4, sink);
    using Lease = BoundedReaderPool<Reader>::Lease;
    std::optional<Lease> lease;
    OwnerReadTiming::Release<std::optional<Lease>> release(timing, lease);
    lease.emplace(pool.acquire([&] { return std::make_unique<Reader>(closed); }, [] {}));
    timing.stage("visitor");
    throw std::runtime_error("original cancellation");
  } catch (const std::runtime_error& error) {
    EXPECT_STREQ(error.what(), "original cancellation");
  }
  EXPECT_EQ(closed, 1);
  EXPECT_EQ(pool.stats().active, 0u);
  EXPECT_EQ(pool.stats().discarded, 1u);
  const auto output = read();
  EXPECT_NE(output.find("\"success\":false"), std::string::npos);
  EXPECT_NE(output.find("\"payload_blocks\":false"), std::string::npos);
  EXPECT_NE(output.find("\"stage\":\"visitor\""), std::string::npos);
  EXPECT_GT(number(output, "release_start_ns"), 0u);
  EXPECT_LE(number(output, "release_end_ns"), number(output, "end_ns"));
  EXPECT_EQ(output.find("original cancellation"), std::string::npos);
}

TEST_F(OwnerReadTimingTest, UnsafeOrUnavailableSinkIsInert) {
  OwnerReadTraceSink missing((directory + "/missing/file").c_str());
  EXPECT_FALSE(missing.enabled());
  { std::ofstream file(path); }
  ASSERT_EQ(::symlink(path.c_str(), (directory + "/link").c_str()), 0);
  OwnerReadTraceSink symlink((directory + "/link").c_str());
  EXPECT_FALSE(symlink.enabled());
  ASSERT_EQ(::mkfifo((directory + "/fifo").c_str(), 0600), 0);
  OwnerReadTraceSink fifo((directory + "/fifo").c_str());
  EXPECT_FALSE(fifo.enabled());
  OwnerReadTraceSink folder(directory.c_str());
  EXPECT_FALSE(folder.enabled());
  EXPECT_NO_THROW({ OwnerReadTiming timing(false, 1, missing); });
  EXPECT_TRUE(read().empty());
}

TEST_F(OwnerReadTimingTest, ConcurrentRecordsAreBoundedAndNotInterleaved) {
  const auto gate = directory + "/ready";
  OwnerReadTraceSink sink(path.c_str(), gate.c_str());
  ASSERT_TRUE(sink.enabled());
  // More than the complete cap passes while the gate is absent. None of these
  // calls may consume admission slots, produce a record, or sample the clock.
  std::vector<std::thread> blocked;
  for (int worker = 0; worker < 8; ++worker) blocked.emplace_back([&] {
    for (int i = 0; i < 600; ++i) {
      OwnerReadTiming timing(false, 1, sink);
      timing.mark(timing.record.acquire_start_ns);
      EXPECT_EQ(timing.record.begin_ns, 0u);
      EXPECT_EQ(timing.record.acquire_start_ns, 0u);
    }
  });
  for (auto& worker : blocked) worker.join();
  EXPECT_TRUE(read().empty());
  ASSERT_EQ(::symlink(path.c_str(), gate.c_str()), 0);
  {
    OwnerReadTiming timing(false, 1, sink);
    EXPECT_EQ(timing.record.begin_ns, 0u);
  }
  EXPECT_TRUE(read().empty());
  ASSERT_EQ(::unlink(gate.c_str()), 0);
  { std::ofstream ready(gate); ASSERT_TRUE(ready.good()); }
  {
    OwnerReadTiming timing(false, 1, sink);
    EXPECT_GT(timing.record.begin_ns, 0u);
    timing.record.success = true;
    timing.stage("complete");
  }
  // Readiness is latched: removal does not turn tracing off or add a stat to
  // each subsequent call. The same 4096-record bound still applies.
  ASSERT_EQ(::unlink(gate.c_str()), 0);
  std::vector<std::thread> workers;
  for (int worker = 0; worker < 8; ++worker) workers.emplace_back([&] {
    for (int i = 0; i < 600; ++i) {
      OwnerReadTiming timing(false, 1, sink);
      timing.record.success = true;
      timing.stage("complete");
    }
  });
  for (auto& worker : workers) worker.join();
  std::istringstream input(read());
  std::set<std::uint64_t> sequences;
  std::string line;
  while (std::getline(input, line)) {
    ASSERT_FALSE(line.empty());
    EXPECT_EQ(line.front(), '{');
    EXPECT_EQ(line.back(), '}');
    EXPECT_EQ(std::count(line.begin(), line.end(), '{'), 1);
    EXPECT_EQ(std::count(line.begin(), line.end(), '}'), 1);
    EXPECT_TRUE(sequences.insert(number(line, "sequence")).second);
  }
  EXPECT_EQ(sequences.size(), OwnerReadTraceSink::kMaximumRecords);
}
