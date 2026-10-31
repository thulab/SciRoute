#include "iotdbfs/Platform.h"
#include "iotdbfs/StagedFile.h"
#include "TestSupport.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <optional>
#include <string>
#include <thread>
#include <vector>

// Linux ELF wrappers are linked into the unit test executable only. Every
// unarmed call, including calls from other threads, reaches the real syscall.
namespace platform_faults {

enum class Operation { Read, Write, Resize, Create, Unlink, Sync, Readlink, Count };

struct Step {
  int error = 0;
  std::optional<size_t> limit;
  static Step fail(int error) { return {error, std::nullopt}; }
  static Step transfer(size_t limit) { return {0, limit}; }
};

struct Call {
  int fd;
  uint64_t offset;
  size_t size;
};

struct Script {
  bool armed = false;
  std::deque<Step> pending;
  std::vector<Call> calls;
};

class ScopedFaults;
thread_local ScopedFaults* active = nullptr;

class ScopedFaults {
public:
  ScopedFaults() : previous_(active) { active = this; }
  ~ScopedFaults() { active = previous_; }
  ScopedFaults(const ScopedFaults&) = delete;
  ScopedFaults& operator=(const ScopedFaults&) = delete;

  void arm(Operation operation, std::initializer_list<Step> steps) {
    auto& script = scripts_[static_cast<size_t>(operation)];
    script.armed = true;
    script.pending = steps;
  }
  Script& script(Operation operation) {
    return scripts_[static_cast<size_t>(operation)];
  }
  Step next(Operation operation, Call call = {-1, 0, 0}) {
    auto& script = this->script(operation);
    if (!script.armed)
      return {};
    script.calls.push_back(call);
    if (script.pending.empty())
      return {};
    const auto step = script.pending.front();
    script.pending.pop_front();
    return step;
  }

  // Capture a created descriptor only when a cleanup failure is being tested.
  int created_fd = -1;
  std::string created_path;

private:
  ScopedFaults* previous_;
  std::array<Script, static_cast<size_t>(Operation::Count)> scripts_;
};

Step next(Operation operation, Call call = {-1, 0, 0}) {
  return active ? active->next(operation, call) : Step{};
}

} // namespace platform_faults

extern "C" {
ssize_t __real_pread64(int, void*, size_t, off64_t);
ssize_t __real___pread64_chk(int, void*, size_t, off64_t, size_t);
ssize_t __real_pwrite64(int, const void*, size_t, off64_t);
int __real_ftruncate64(int, off64_t);
int __real_mkstemp64(char*);
int __real_unlink(const char*);
int __real_fsync(int);
ssize_t __real_readlink(const char*, char*, size_t);
ssize_t __real___readlink_chk(const char*, char*, size_t, size_t);

ssize_t __wrap_pread64(int fd, void* data, size_t size, off64_t offset) {
  const auto step = platform_faults::next(platform_faults::Operation::Read,
    {fd, static_cast<uint64_t>(offset), size});
  if (step.error) { errno = step.error; return -1; }
  if (step.limit && *step.limit == 0) return 0;
  return __real_pread64(fd, data, step.limit ? std::min(size, *step.limit) : size, offset);
}

// GCC's optimized FORTIFY call sites include the destination buffer size.
// Preserve their bounds checks while injecting the same observable I/O faults.
ssize_t __wrap___pread64_chk(int fd, void* data, size_t size, off64_t offset,
  size_t buffer_size) {
  const auto step = platform_faults::next(platform_faults::Operation::Read,
    {fd, static_cast<uint64_t>(offset), size});
  if (step.error) { errno = step.error; return -1; }
  if (step.limit && *step.limit == 0) return 0;
  return __real___pread64_chk(fd, data,
    step.limit ? std::min(size, *step.limit) : size, offset, buffer_size);
}

ssize_t __wrap_pwrite64(int fd, const void* data, size_t size, off64_t offset) {
  const auto step = platform_faults::next(platform_faults::Operation::Write,
    {fd, static_cast<uint64_t>(offset), size});
  if (step.error) { errno = step.error; return -1; }
  if (step.limit && *step.limit == 0) return 0;
  return __real_pwrite64(fd, data, step.limit ? std::min(size, *step.limit) : size, offset);
}

int __wrap_ftruncate64(int fd, off64_t size) {
  const auto step = platform_faults::next(platform_faults::Operation::Resize,
    {fd, static_cast<uint64_t>(size), 0});
  if (step.error) { errno = step.error; return -1; }
  return __real_ftruncate64(fd, size);
}

int __wrap_mkstemp64(char* pattern) {
  const auto step = platform_faults::next(platform_faults::Operation::Create);
  if (step.error) { errno = step.error; return -1; }
  const int fd = __real_mkstemp64(pattern);
  auto* faults = platform_faults::active;
  if (fd >= 0 && faults && faults->script(platform_faults::Operation::Unlink).armed) {
    faults->created_fd = fd;
    faults->created_path = pattern;
  }
  return fd;
}

int __wrap_unlink(const char* path) {
  const auto step = platform_faults::next(platform_faults::Operation::Unlink);
  if (step.error) { errno = step.error; return -1; }
  return __real_unlink(path);
}

int __wrap_fsync(int fd) {
  const auto step = platform_faults::next(platform_faults::Operation::Sync, {fd, 0, 0});
  if (step.error) { errno = step.error; return -1; }
  return __real_fsync(fd);
}

ssize_t __wrap_readlink(const char* path, char* data, size_t size) {
  // Do not inject into filesystem canonicalization or unrelated symlinks.
  const auto step = std::strcmp(path, "/proc/self/exe") == 0
    ? platform_faults::next(platform_faults::Operation::Readlink) : platform_faults::Step{};
  if (step.error) { errno = step.error; return -1; }
  if (step.limit && *step.limit == 0) return 0;
  return __real_readlink(path, data, size);
}

ssize_t __wrap___readlink_chk(const char* path, char* data, size_t size,
  size_t buffer_size) {
  const auto step = std::strcmp(path, "/proc/self/exe") == 0
    ? platform_faults::next(platform_faults::Operation::Readlink) : platform_faults::Step{};
  if (step.error) { errno = step.error; return -1; }
  if (step.limit && *step.limit == 0) return 0;
  return __real___readlink_chk(path, data, size, buffer_size);
}
} // extern "C"

namespace {
using platform_faults::Operation;
using platform_faults::ScopedFaults;
using platform_faults::Step;

class StagedFileFaultTest : public ::testing::Test {
protected:
  void SetUp() override {
    stage_directory_.set(directory_.file(""));
    ASSERT_EQ(file_.create(), 0);
  }
  void expect_bytes(const std::string& expected) {
    std::vector<char> actual(expected.size() + 1, '!');
    ASSERT_EQ(file_.read(actual.data(), actual.size(), 0), expected.size());
    EXPECT_EQ(std::string(actual.data(), expected.size()), expected);
    EXPECT_EQ(actual.back(), '!');
    EXPECT_EQ(file_.size(), expected.size());
  }
  test_support::TempDirectory directory_;
  test_support::ScopedEnvironment stage_directory_{"IOTDB_STAGE_DIR"};
  iotdbfs::StagedFile file_;
};

TEST_F(StagedFileFaultTest, InterruptedWriteRetriesWithoutAdvancingOffsetOrDroppingBytes) {
  ScopedFaults faults;
  faults.arm(Operation::Write, {Step::fail(EINTR), Step::fail(EINTR)});
  ASSERT_EQ(file_.write("hello", 5, 2), 0);
  const auto& calls = faults.script(Operation::Write).calls;
  ASSERT_EQ(calls.size(), 3u);
  for (const auto& call : calls) {
    EXPECT_EQ(call.offset, 2u);
    EXPECT_EQ(call.size, 5u);
  }
  expect_bytes(std::string("\0\0hello", 7));
}

TEST_F(StagedFileFaultTest, PartialWriteAndInterruptionResumeAtFirstUnwrittenByte) {
  ScopedFaults faults;
  faults.arm(Operation::Write, {Step::transfer(2), Step::fail(EINTR), Step::transfer(1)});
  ASSERT_EQ(file_.write("abcdef", 6, 3), 0);
  const auto& calls = faults.script(Operation::Write).calls;
  ASSERT_EQ(calls.size(), 4u);
  const std::array<uint64_t, 4> offsets{3, 5, 5, 6};
  const std::array<size_t, 4> lengths{6, 4, 4, 3};
  for (size_t i = 0; i < calls.size(); ++i) {
    EXPECT_EQ(calls[i].offset, offsets[i]);
    EXPECT_EQ(calls[i].size, lengths[i]);
  }
  expect_bytes(std::string("\0\0\0abcdef", 9));
}

TEST_F(StagedFileFaultTest, ZeroWriteReportsIoErrorWithoutExtendingFile) {
  ASSERT_EQ(file_.write("keep", 4, 0), 0);
  ScopedFaults faults;
  faults.arm(Operation::Write, {Step::transfer(0)});
  EXPECT_EQ(file_.write("replace", 7, 20), -EIO);
  EXPECT_EQ(faults.script(Operation::Write).calls.size(), 1u);
  expect_bytes("keep");
}

TEST_F(StagedFileFaultTest, WriteErrorPreservesExistingBytesAndAllowsRetry) {
  ASSERT_EQ(file_.write("keep", 4, 0), 0);
  ScopedFaults faults;
  faults.arm(Operation::Write, {Step::fail(ENOSPC)});
  EXPECT_EQ(file_.write("data", 4, 0), -ENOSPC);
  expect_bytes("keep");
  ASSERT_EQ(file_.write("data", 4, 0), 0);
  expect_bytes("data");
}

TEST_F(StagedFileFaultTest, ErrorAfterPartialWritePreservesExactlyTheTransferredPrefix) {
  ScopedFaults faults;
  faults.arm(Operation::Write, {Step::transfer(2), Step::fail(ENOSPC)});
  EXPECT_EQ(file_.write("abcdef", 6, 0), -ENOSPC);
  expect_bytes("ab");
  ASSERT_EQ(file_.write("cdef", 4, 2), 0);
  expect_bytes("abcdef");
}

TEST_F(StagedFileFaultTest, InterruptedReadRetriesWithoutChangingRequestedRange) {
  ASSERT_EQ(file_.write("01234567", 8, 0), 0);
  ScopedFaults faults;
  faults.arm(Operation::Read, {Step::fail(EINTR), Step::fail(EINTR)});
  std::array<char, 4> data{};
  ASSERT_EQ(file_.read(data.data(), data.size(), 2), 4);
  EXPECT_EQ(std::string(data.data(), data.size()), "2345");
  const auto& calls = faults.script(Operation::Read).calls;
  ASSERT_EQ(calls.size(), 3u);
  for (const auto& call : calls) {
    EXPECT_EQ(call.offset, 2u);
    EXPECT_EQ(call.size, 4u);
  }
}

TEST_F(StagedFileFaultTest, PartialReadAndInterruptionFillTheRemainingBuffer) {
  ASSERT_EQ(file_.write("0123456789", 10, 0), 0);
  ScopedFaults faults;
  faults.arm(Operation::Read, {Step::transfer(2), Step::fail(EINTR), Step::transfer(1)});
  std::array<char, 6> data{};
  ASSERT_EQ(file_.read(data.data(), data.size(), 3), 6);
  EXPECT_EQ(std::string(data.data(), data.size()), "345678");
  const auto& calls = faults.script(Operation::Read).calls;
  ASSERT_EQ(calls.size(), 4u);
  const std::array<uint64_t, 4> offsets{3, 5, 5, 6};
  const std::array<size_t, 4> lengths{6, 4, 4, 3};
  for (size_t i = 0; i < calls.size(); ++i) {
    EXPECT_EQ(calls[i].offset, offsets[i]);
    EXPECT_EQ(calls[i].size, lengths[i]);
  }
}

TEST_F(StagedFileFaultTest, PrematureEofReportsIoErrorWithoutInventingBytes) {
  ASSERT_EQ(file_.write("abcdef", 6, 0), 0);
  ScopedFaults faults;
  faults.arm(Operation::Read, {Step::transfer(2), Step::transfer(0)});
  std::array<char, 6> data;
  data.fill('!');
  EXPECT_EQ(file_.read(data.data(), data.size(), 0), -EIO);
  EXPECT_EQ(std::string(data.data(), data.size()), "ab!!!!");
  EXPECT_EQ(file_.size(), 6u);
  expect_bytes("abcdef");
}

TEST_F(StagedFileFaultTest, ReadErrorPropagatesWithoutChangingStoredData) {
  ASSERT_EQ(file_.write("abcdef", 6, 0), 0);
  ScopedFaults faults;
  faults.arm(Operation::Read, {Step::fail(EIO)});
  std::array<char, 6> data;
  data.fill('!');
  EXPECT_EQ(file_.read(data.data(), data.size(), 0), -EIO);
  EXPECT_EQ(std::string(data.data(), data.size()), "!!!!!!");
  expect_bytes("abcdef");
}

TEST_F(StagedFileFaultTest, FailedTruncatePreservesLengthAndDataUntilRetry) {
  ASSERT_EQ(file_.write("abcdef", 6, 0), 0);
  ScopedFaults faults;
  faults.arm(Operation::Resize, {Step::fail(ENOSPC)});
  EXPECT_EQ(file_.resize(2), -ENOSPC);
  expect_bytes("abcdef");
  ASSERT_EQ(file_.resize(2), 0);
  expect_bytes("ab");
}

TEST_F(StagedFileFaultTest, FailedExtendPreservesLengthAndAllowsZeroFilledRetry) {
  ASSERT_EQ(file_.write("ab", 2, 0), 0);
  ScopedFaults faults;
  faults.arm(Operation::Resize, {Step::fail(EFBIG)});
  EXPECT_EQ(file_.resize(6), -EFBIG);
  expect_bytes("ab");
  ASSERT_EQ(file_.resize(6), 0);
  expect_bytes(std::string("ab\0\0\0\0", 6));
}

TEST_F(StagedFileFaultTest, SyncFailureIsReportedAndRetryLeavesBytesIntact) {
  ASSERT_EQ(file_.write("durable", 7, 0), 0);
  ScopedFaults faults;
  faults.arm(Operation::Sync, {Step::fail(EIO)});
  EXPECT_EQ(file_.sync(), -EIO);
  EXPECT_EQ(file_.sync(), 0);
  expect_bytes("durable");
}

TEST_F(StagedFileFaultTest, LoadStopsAtFailedWriteWithoutRequestingTheNextChunk) {
  ScopedFaults faults;
  faults.arm(Operation::Write, {Step::fail(ENOSPC)});
  size_t reads = 0;
  EXPECT_EQ(file_.load(2 * 1024 * 1024, [&](uint64_t offset, uint64_t size) {
    ++reads;
    EXPECT_EQ(offset, 0u);
    return std::optional<std::vector<uint8_t>>(std::vector<uint8_t>(size, 'x'));
  }), -ENOSPC);
  EXPECT_EQ(reads, 1u);
  EXPECT_EQ(file_.size(), 0u);
}

TEST_F(StagedFileFaultTest, LoadReportsFinalResizeFailureAndCanBeRetried) {
  ASSERT_EQ(file_.write("old tail", 8, 0), 0);
  const auto reader = [](uint64_t, uint64_t) {
    return std::optional<std::vector<uint8_t>>(std::vector<uint8_t>{'n', 'e', 'w'});
  };
  ScopedFaults faults;
  faults.arm(Operation::Resize, {Step::fail(EIO)});
  EXPECT_EQ(file_.load(3, reader), -EIO);
  EXPECT_EQ(file_.size(), 8u);
  ASSERT_EQ(file_.load(3, reader), 0);
  expect_bytes("new");
}

TEST(PlatformFaults, StagingCreationFailureIsRetriedWithoutCallingReader) {
  test_support::TempDirectory directory;
  test_support::ScopedEnvironment stage_directory("IOTDB_STAGE_DIR");
  stage_directory.set(directory.file(""));
  iotdbfs::StagedFile file;
  ScopedFaults faults;
  faults.arm(Operation::Create, {Step::fail(EMFILE), Step::fail(ENOSPC)});
  EXPECT_EQ(file.create(), -EMFILE);
  bool reader_called = false;
  EXPECT_EQ(file.load(3, [&](uint64_t, uint64_t) {
    reader_called = true;
    return std::optional<std::vector<uint8_t>>(std::vector<uint8_t>{'a', 'b', 'c'});
  }), -ENOSPC);
  EXPECT_FALSE(reader_called);
  EXPECT_EQ(file.size(), 0u);
  ASSERT_EQ(file.create(), 0);
  ASSERT_EQ(file.write("abc", 3, 0), 0);
  EXPECT_EQ(file.size(), 3u);
  EXPECT_TRUE(std::filesystem::is_empty(directory.file("")));
  EXPECT_EQ(faults.script(Operation::Create).calls.size(), 3u);
}

TEST(PlatformFaults, FailedUnlinkClosesDescriptorAndPreservesOriginalError) {
  test_support::TempDirectory directory;
  ScopedFaults faults;
  faults.arm(Operation::Unlink, {Step::fail(EACCES)});
  EXPECT_EQ(iotdbfs::platform::create_staged_file(directory.file("").c_str()), -EACCES);
  ASSERT_GE(faults.created_fd, 0);
  errno = 0;
  EXPECT_EQ(::fcntl(faults.created_fd, F_GETFD), -1);
  EXPECT_EQ(errno, EBADF);
  ASSERT_FALSE(faults.created_path.empty());
  EXPECT_TRUE(std::filesystem::is_regular_file(faults.created_path));
  EXPECT_EQ(__real_unlink(faults.created_path.c_str()), 0);
  EXPECT_TRUE(std::filesystem::is_empty(directory.file("")));
}

TEST(PlatformFaults, ClosingAlreadyClosedDescriptorReportsBadFileDescriptor) {
  const int fd = iotdbfs::platform::create_staged_file(nullptr);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(iotdbfs::platform::close_fd(fd), 0);
  EXPECT_EQ(iotdbfs::platform::close_fd(fd), -EBADF);
}

TEST(PlatformFaults, MissingProcExecutableLinkUsesCanonicalAbsoluteArgvPath) {
  test_support::TempDirectory directory;
  const auto target = directory.write("program", "executable fixture");
  const auto input = directory.file("./program");
  ScopedFaults faults;
  faults.arm(Operation::Readlink, {Step::fail(ENOENT)});
  EXPECT_EQ(iotdbfs::platform::executable_path(input.c_str()), std::filesystem::path(target));
  EXPECT_EQ(faults.script(Operation::Readlink).calls.size(), 1u);
}

TEST(PlatformFaults, EmptyProcExecutableLinkUsesCanonicalRelativeArgvPath) {
  const auto input = std::filesystem::path(".") / "iotdbfs-ut-executable";
  const auto expected = std::filesystem::weakly_canonical(std::filesystem::current_path() / input);
  ScopedFaults faults;
  faults.arm(Operation::Readlink, {Step::transfer(0)});
  EXPECT_EQ(iotdbfs::platform::executable_path(input.c_str()), expected);
  EXPECT_EQ(faults.script(Operation::Readlink).calls.size(), 1u);
}

TEST(PlatformFaults, MissingProcLinkAndMissingArgvProduceEmptyPath) {
  ScopedFaults faults;
  faults.arm(Operation::Readlink, {Step::fail(ENOENT), Step::fail(EACCES)});
  EXPECT_TRUE(iotdbfs::platform::executable_path(nullptr).empty());
  EXPECT_TRUE(iotdbfs::platform::executable_path("").empty());
  EXPECT_EQ(faults.script(Operation::Readlink).calls.size(), 2u);
}

TEST_F(StagedFileFaultTest, FaultScriptsAreThreadLocalAndRestoreNormalCallsAfterScope) {
  ASSERT_EQ(file_.write("abc", 3, 0), 0);
  char other_thread_data = '!';
  int other_thread_result = -1;
  {
    ScopedFaults faults;
    faults.arm(Operation::Read, {Step::fail(EIO)});
    std::thread reader([&] {
      other_thread_result = file_.read(&other_thread_data, 1, 0);
    });
    reader.join();
    EXPECT_EQ(other_thread_result, 1);
    EXPECT_EQ(other_thread_data, 'a');
    char same_thread_data = '!';
    EXPECT_EQ(file_.read(&same_thread_data, 1, 0), -EIO);
    EXPECT_EQ(same_thread_data, '!');
    EXPECT_EQ(faults.script(Operation::Read).calls.size(), 1u);
  }
  expect_bytes("abc");
}

} // namespace
