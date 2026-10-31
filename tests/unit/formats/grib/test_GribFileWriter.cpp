#include <gtest/gtest.h>

#include "iotdbfs/Filesystem.h"
#include "iotdbfs/formats/grib/GribFileWriter.h"

#include <eccodes.h>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <memory>
#include <vector>
#include <cerrno>
#include <csignal>
#include <stdexcept>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>
#include "iotdbfs/Logging.h"

namespace {

using tools::GribWriterStatus;

class GribFileWriter : public ::testing::Test {
protected:
  fs::path directory;
  fs::path source;
  std::vector<char> message;

  void SetUp() override {
    auto pattern = (fs::temp_directory_path() / "iotdbfs-grib-writer-ut-XXXXXX").string();
    std::vector<char> mutable_pattern(pattern.begin(), pattern.end());
    mutable_pattern.push_back('\0');
    const auto* created = mkdtemp(mutable_pattern.data());
    ASSERT_NE(created, nullptr);
    directory = created;
    source = directory / "source.grib2";
    std::unique_ptr<codes_handle, decltype(&codes_handle_delete)> handle(
      codes_grib_handle_new_from_samples(nullptr, "GRIB2"), codes_handle_delete);
    ASSERT_NE(handle, nullptr);
    const void* bytes = nullptr;
    size_t size = 0;
    ASSERT_EQ(codes_get_message(handle.get(), &bytes, &size), CODES_SUCCESS);
    const auto* first = static_cast<const char*>(bytes);
    message.assign(first, first + size);
    write_bytes(source, message);
  }

  void TearDown() override {
    if (!directory.empty()) {
      std::error_code error;
      fs::remove_all(directory, error);
      EXPECT_FALSE(error) << error.message();
    }
  }

  void write_bytes(const fs::path& path, const std::vector<char>& bytes) {
    std::ofstream output(path, std::ios::binary);
    ASSERT_TRUE(output.is_open());
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    output.close();
    ASSERT_TRUE(output);
  }

  std::vector<char> read_bytes(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    EXPECT_TRUE(input.is_open());
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
  }

  void expect_status(const fs::path& src, const fs::path& target, GribWriterStatus expected) {
    EXPECT_EQ(tools::write_grib_file(src.string(), target.string()), static_cast<int>(expected));
  }

  void expect_no_temporary_files() {
    for (const auto& entry : fs::recursive_directory_iterator(directory))
      EXPECT_EQ(entry.path().filename().string().find(".tmp."), std::string::npos)
        << entry.path().string();
  }
};

TEST_F(GribFileWriter, CopiesBytesAndCreatesNestedTargetDirectories) {
  const auto target = directory / "nested" / "weather" / "output.grib2";
  expect_status(source, target, GribWriterStatus::Success);
  EXPECT_EQ(read_bytes(target), message);
  EXPECT_EQ(read_bytes(source), message);
  expect_no_temporary_files();
}

TEST_F(GribFileWriter, ExistingDirectoryUsesSourceFilename) {
  const auto target_directory = directory / "output";
  ASSERT_TRUE(fs::create_directory(target_directory));
  expect_status(source, target_directory, GribWriterStatus::Success);
  EXPECT_EQ(read_bytes(target_directory / source.filename()), message);
  expect_no_temporary_files();
}

TEST_F(GribFileWriter, TrailingSlashCreatesDirectoryAndUsesSourceFilename) {
  const auto target = directory / "new-directory";
  expect_status(source, target.string() + "/", GribWriterStatus::Success);
  EXPECT_EQ(read_bytes(target / source.filename()), message);
  expect_no_temporary_files();
}

TEST_F(GribFileWriter, ReplacesExistingTargetWithoutStaleSuffix) {
  const auto target = directory / "output.grib2";
  write_bytes(target, std::vector<char>(message.size() * 2, 'x'));
  expect_status(source, target, GribWriterStatus::Success);
  EXPECT_EQ(read_bytes(target), message);
  expect_no_temporary_files();
}

TEST_F(GribFileWriter, CopiesMultipleMessagesAcrossBufferBoundary) {
  std::vector<char> large;
  while (large.size() <= (1u << 20))
    large.insert(large.end(), message.begin(), message.end());
  write_bytes(source, large);
  const auto target = directory / "large.grib2";
  expect_status(source, target, GribWriterStatus::Success);
  EXPECT_EQ(read_bytes(target), large);
  expect_no_temporary_files();
}

TEST_F(GribFileWriter, ValidMessageDoesNotRequireRecognizedExtension) {
  const auto renamed = directory / "weather.dat";
  write_bytes(renamed, message);
  const auto target = directory / "output.grib2";
  expect_status(renamed, target, GribWriterStatus::Success);
  EXPECT_EQ(read_bytes(target), message);
}

TEST_F(GribFileWriter, EmptyArgumentsAreRejected) {
  expect_status({}, directory / "output.grib2", GribWriterStatus::InvalidArgument);
  expect_status(source, {}, GribWriterStatus::InvalidArgument);
}

TEST_F(GribFileWriter, MissingOrNonRegularSourceIsRejectedWithoutTarget) {
  const auto target = directory / "output.grib2";
  expect_status(directory / "missing.grib2", target, GribWriterStatus::SourceNotFound);
  expect_status(directory, target, GribWriterStatus::SourceNotRegular);
  EXPECT_FALSE(fs::exists(target));
  expect_no_temporary_files();
}

TEST_F(GribFileWriter, InvalidSourcePreservesExistingTarget) {
  const auto target = directory / "output.grib2";
  const std::vector<char> original{'k', 'e', 'e', 'p'};
  write_bytes(target, original);
  for (const auto& invalid : {std::vector<char>{}, std::vector<char>{'n', 'o', 't', '-', 'g', 'r', 'i', 'b'}}) {
    write_bytes(source, invalid);
    expect_status(source, target, GribWriterStatus::NotAGribFile);
    EXPECT_EQ(read_bytes(target), original);
    expect_no_temporary_files();
  }
}

TEST_F(GribFileWriter, FileAsTargetParentFailsWithoutModifyingIt) {
  const auto parent_file = directory / "parent-file";
  const std::vector<char> original{'k', 'e', 'e', 'p'};
  write_bytes(parent_file, original);
  expect_status(source, parent_file / "output.grib2", GribWriterStatus::TargetOpenFailed);
  EXPECT_EQ(read_bytes(parent_file), original);
  expect_no_temporary_files();
}

TEST_F(GribFileWriter, FinalizeDirectoryCollisionRemovesTemporaryFile) {
  const auto target_directory = directory / "output";
  const auto colliding_directory = target_directory / source.filename();
  ASSERT_TRUE(fs::create_directories(colliding_directory));
  const auto sentinel = colliding_directory / "keep";
  write_bytes(sentinel, {'k'});
  expect_status(source, target_directory, GribWriterStatus::TargetFinalizeFailed);
  EXPECT_EQ(read_bytes(sentinel), std::vector<char>{'k'});
  expect_no_temporary_files();
}

TEST(GribWriterStatus, EveryPublicStatusHasAnExactDiagnostic) {
  const std::vector<std::pair<GribWriterStatus, std::string>> statuses{
    {GribWriterStatus::Success, "success"},
    {GribWriterStatus::InvalidArgument, "invalid argument"},
    {GribWriterStatus::SourceNotFound, "source not found"},
    {GribWriterStatus::SourceNotRegular, "source is not a regular file"},
    {GribWriterStatus::SourceOpenFailed, "failed to open source file"},
    {GribWriterStatus::SourceReadError, "failed to read source file"},
    {GribWriterStatus::NotAGribFile, "source is not a valid GRIB file"},
    {GribWriterStatus::EnsureTargetDirFailed, "failed to ensure target directory"},
    {GribWriterStatus::TargetOpenFailed, "failed to open target file"},
    {GribWriterStatus::TargetWriteFailed, "failed to write to target file"},
    {GribWriterStatus::TargetFinalizeFailed, "failed to finalise target file"}};
  for (const auto& [status, diagnostic] : statuses) {
    SCOPED_TRACE(static_cast<int>(status));
    EXPECT_EQ(tools::grib_writer_status_message(status), diagnostic);
  }
  EXPECT_STREQ(tools::grib_writer_status_message(static_cast<GribWriterStatus>(999)), "unknown status");
}

TEST_F(GribFileWriter, CannotCreateDirectoriesBelowARegularFile) {
  const auto blocker = directory / "blocker";
  const std::vector<char> original{'p', 'r', 'e', 's', 'e', 'r', 'v', 'e'};
  write_bytes(blocker, original);
  expect_status(source, blocker / "child" / "data.grib2", GribWriterStatus::EnsureTargetDirFailed);
  EXPECT_EQ(read_bytes(blocker), original);
  EXPECT_EQ(read_bytes(source), message);
  expect_no_temporary_files();
}

TEST_F(GribFileWriter, SymlinkLoopAtTargetDoesNotModifySource) {
  const auto target = directory / "loop";
  ASSERT_EQ(::symlink("loop", target.c_str()), 0);
  expect_status(source, target / "nested" / "data.grib2", GribWriterStatus::EnsureTargetDirFailed);
  EXPECT_EQ(read_bytes(source), message);
  EXPECT_TRUE(fs::is_symlink(fs::symlink_status(target)));
}

// Force deterministic write failures without filling the host disk. Restore
// the signal handler, limit and logging level before making any assertions.
class ScopedFileSizeLimit {
public:
  explicit ScopedFileSizeLimit(rlim_t bytes) : level_(get_logging_level()) {
    if (::getrlimit(RLIMIT_FSIZE, &original_) != 0)
      throw std::runtime_error("getrlimit failed");
    struct sigaction ignored{};
    ignored.sa_handler = SIG_IGN;
    ::sigemptyset(&ignored.sa_mask);
    if (::sigaction(SIGXFSZ, &ignored, &original_signal_) != 0)
      throw std::runtime_error("sigaction failed");
    auto limit = original_;
    limit.rlim_cur = std::min(bytes, original_.rlim_max);
    if (::setrlimit(RLIMIT_FSIZE, &limit) != 0) {
      ::sigaction(SIGXFSZ, &original_signal_, nullptr);
      throw std::runtime_error("setrlimit failed");
    }
    set_logging_level(LogLevel::OFF);
  }
  ~ScopedFileSizeLimit() {
    ::setrlimit(RLIMIT_FSIZE, &original_);
    ::sigaction(SIGXFSZ, &original_signal_, nullptr);
    set_logging_level(level_);
  }
private:
  struct rlimit original_{};
  struct sigaction original_signal_{};
  LogLevel level_;
};

TEST_F(GribFileWriter, BufferedFlushFailurePreservesTargetAndRemovesTemporaryFile) {
  const auto target = directory / "target.grib2";
  const std::vector<char> original{'k', 'e', 'e', 'p'};
  write_bytes(target, original);
  ASSERT_GT(message.size(), 64u);
  int result;
  {
    ScopedFileSizeLimit limit(64);
    result = tools::write_grib_file(source.string(), target.string());
  }
  EXPECT_EQ(result, static_cast<int>(GribWriterStatus::TargetWriteFailed));
  EXPECT_EQ(read_bytes(source), message);
  EXPECT_EQ(read_bytes(target), original);
  expect_no_temporary_files();
}

TEST_F(GribFileWriter, StreamingWriteFailurePreservesTargetAndRemovesTemporaryFile) {
  std::vector<char> large = message;
  while (large.size() <= (1u << 20)) large.insert(large.end(), message.begin(), message.end());
  write_bytes(source, large);
  const auto target = directory / "target.grib2";
  const std::vector<char> original{'k', 'e', 'e', 'p'};
  write_bytes(target, original);
  int result;
  {
    ScopedFileSizeLimit limit(4096);
    result = tools::write_grib_file(source.string(), target.string());
  }
  EXPECT_EQ(result, static_cast<int>(GribWriterStatus::TargetWriteFailed));
  EXPECT_EQ(read_bytes(source), large);
  EXPECT_EQ(read_bytes(target), original);
  expect_no_temporary_files();
}

} // namespace
