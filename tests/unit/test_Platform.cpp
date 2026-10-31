#include "iotdbfs/Platform.h"

#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <vector>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

TEST(Platform, XattrWriteCapabilityIsExplicit) {
  EXPECT_FALSE(iotdbfs::platform::xattr_write_supported());
}

#if !defined(_WIN32)
namespace {

class ScopedFd {
public:
  explicit ScopedFd(int fd) : fd_(fd) {}
  ~ScopedFd() { iotdbfs::platform::close_fd(fd_); }
  int get() const { return fd_; }
private:
  int fd_;
};

class PlatformDirectoryTest : public ::testing::Test {
protected:
  void SetUp() override {
    std::string pattern = (std::filesystem::temp_directory_path() /
      "iotdbfs-platform-ut-XXXXXX").string();
    std::vector<char> bytes(pattern.begin(), pattern.end());
    bytes.push_back('\0');
    const char* path = ::mkdtemp(bytes.data());
    ASSERT_NE(path, nullptr);
    directory_ = path;
  }
  void TearDown() override {
    std::error_code error;
    if (!directory_.empty())
      std::filesystem::remove_all(directory_, error);
  }
  std::filesystem::path directory_;
};

class SavedEnvironment {
public:
  explicit SavedEnvironment(std::string name) : name_(std::move(name)) {
    if (const char* previous = std::getenv(name_.c_str()))
      value_ = previous;
  }
  ~SavedEnvironment() {
    if (value_)
      ::setenv(name_.c_str(), value_->c_str(), 1);
    else
      ::unsetenv(name_.c_str());
  }
private:
  std::string name_;
  std::optional<std::string> value_;
};

} // namespace

TEST(Platform, StagedFileSupportsRandomIoAndResize) {
  const int fd = iotdbfs::platform::create_staged_file(nullptr);
  ASSERT_GE(fd, 0);

  constexpr char kPrefix[] = "prefix";
  constexpr char kSuffix[] = "suffix";
  ASSERT_EQ(iotdbfs::platform::write_at(fd, kPrefix, sizeof(kPrefix) - 1, 0),
    static_cast<ssize_t>(sizeof(kPrefix) - 1));
  ASSERT_EQ(iotdbfs::platform::write_at(fd, kSuffix, sizeof(kSuffix) - 1, 16),
    static_cast<ssize_t>(sizeof(kSuffix) - 1));

  std::array<char, sizeof(kPrefix) - 1> prefix{};
  ASSERT_EQ(iotdbfs::platform::read_at(fd, prefix.data(), prefix.size(), 0),
    static_cast<ssize_t>(prefix.size()));
  EXPECT_EQ(std::string(prefix.data(), prefix.size()), "prefix");

  ASSERT_EQ(iotdbfs::platform::resize_fd(fd, 3), 0);
  ASSERT_EQ(iotdbfs::platform::sync_fd(fd), 0);
  ASSERT_EQ(iotdbfs::platform::close_fd(fd), 0);
}

TEST(Platform, StagedFileRejectsOffsetsAndSizesOutsideOffT) {
  const int fd = iotdbfs::platform::create_staged_file(nullptr);
  ASSERT_GE(fd, 0);
  const auto too_large = static_cast<uint64_t>(std::numeric_limits<off_t>::max()) + 1;
  char byte = 'x';

  errno = 0;
  EXPECT_EQ(iotdbfs::platform::read_at(fd, &byte, 1, too_large), -1);
  EXPECT_EQ(errno, EOVERFLOW);
  errno = 0;
  EXPECT_EQ(iotdbfs::platform::write_at(fd, &byte, 1, too_large), -1);
  EXPECT_EQ(errno, EOVERFLOW);
  errno = 0;
  EXPECT_EQ(iotdbfs::platform::resize_fd(fd, too_large), -1);
  EXPECT_EQ(errno, EFBIG);
  EXPECT_EQ(iotdbfs::platform::close_fd(fd), 0);
}

TEST_F(PlatformDirectoryTest, StagedFileIsPrivateUnlinkedAndCloseOnExec) {
  ScopedFd fd(iotdbfs::platform::create_staged_file(directory_.c_str()));
  ASSERT_GE(fd.get(), 0);
  EXPECT_TRUE(std::filesystem::is_empty(directory_));
  struct stat info{};
  ASSERT_EQ(::fstat(fd.get(), &info), 0);
  EXPECT_EQ(info.st_nlink, 0u);
  // A stricter caller umask can remove owner permissions from mkstemp's 0600.
  EXPECT_EQ(info.st_mode & 0177, 0u);
  EXPECT_TRUE(S_ISREG(info.st_mode));
  const int descriptor_flags = ::fcntl(fd.get(), F_GETFD);
  ASSERT_GE(descriptor_flags, 0);
  EXPECT_NE(descriptor_flags & FD_CLOEXEC, 0);
  ASSERT_EQ(iotdbfs::platform::write_at(fd.get(), "data", 4, 0), 4);
  char bytes[4]{};
  ASSERT_EQ(iotdbfs::platform::read_at(fd.get(), bytes, sizeof(bytes), 0), 4);
  EXPECT_EQ(std::string(bytes, sizeof(bytes)), "data");
}

TEST_F(PlatformDirectoryTest, StagedFileCreationReportsMissingOrNonDirectoryPath) {
  const auto missing = directory_ / "missing";
  EXPECT_EQ(iotdbfs::platform::create_staged_file(missing.c_str()), -ENOENT);
  const auto regular_file = directory_ / "ordinary-file";
  { std::ofstream output(regular_file); output << "content"; }
  ASSERT_TRUE(std::filesystem::is_regular_file(regular_file));
  EXPECT_EQ(iotdbfs::platform::create_staged_file(regular_file.c_str()), -ENOTDIR);
}

TEST(Platform, PositionalIoDoesNotChangeDescriptorPosition) {
  ScopedFd fd(iotdbfs::platform::create_staged_file(nullptr));
  ASSERT_GE(fd.get(), 0);
  ASSERT_EQ(::lseek(fd.get(), 7, SEEK_SET), 7);
  ASSERT_EQ(iotdbfs::platform::write_at(fd.get(), "abc", 3, 0), 3);
  EXPECT_EQ(::lseek(fd.get(), 0, SEEK_CUR), 7);
  char bytes[3]{};
  ASSERT_EQ(iotdbfs::platform::read_at(fd.get(), bytes, sizeof(bytes), 0), 3);
  EXPECT_EQ(::lseek(fd.get(), 0, SEEK_CUR), 7);
  EXPECT_EQ(std::string(bytes, sizeof(bytes)), "abc");
}

TEST(Platform, SparseWritesAndResizeProduceZeroFilledHoles) {
  ScopedFd fd(iotdbfs::platform::create_staged_file(nullptr));
  ASSERT_GE(fd.get(), 0);
  ASSERT_EQ(iotdbfs::platform::write_at(fd.get(), "z", 1, 4), 1);
  ASSERT_EQ(iotdbfs::platform::resize_fd(fd.get(), 8), 0);
  std::array<char, 10> bytes;
  bytes.fill('!');
  ASSERT_EQ(iotdbfs::platform::read_at(fd.get(), bytes.data(), bytes.size(), 0), 8);
  EXPECT_EQ(std::string(bytes.data(), bytes.size()), std::string("\0\0\0\0z\0\0\0!!", 10));
  ASSERT_EQ(iotdbfs::platform::resize_fd(fd.get(), 2), 0);
  EXPECT_EQ(iotdbfs::platform::read_at(fd.get(), bytes.data(), bytes.size(), 2), 0);
}

TEST(Platform, InvalidDescriptorsReportBadFileDescriptor) {
  char byte = 'x';
  errno = 0;
  EXPECT_EQ(iotdbfs::platform::read_at(-1, &byte, 1, 0), -1);
  EXPECT_EQ(errno, EBADF);
  errno = 0;
  EXPECT_EQ(iotdbfs::platform::write_at(-1, &byte, 1, 0), -1);
  EXPECT_EQ(errno, EBADF);
  errno = 0;
  EXPECT_EQ(iotdbfs::platform::resize_fd(-1, 0), -1);
  EXPECT_EQ(errno, EBADF);
  errno = 0;
  EXPECT_EQ(iotdbfs::platform::sync_fd(-1), -1);
  EXPECT_EQ(errno, EBADF);
  EXPECT_EQ(iotdbfs::platform::close_fd(-1), 0);
}

TEST(Platform, EnvironmentRespectsOverwriteAndAllowsEmptyValues) {
  const char* name = "IOTDBFS_UT_PLATFORM_ENVIRONMENT";
  SavedEnvironment restore(name);
  ASSERT_EQ(::unsetenv(name), 0);
  ASSERT_EQ(iotdbfs::platform::set_environment(name, "first", false), 0);
  ASSERT_NE(std::getenv(name), nullptr);
  EXPECT_STREQ(std::getenv(name), "first");
  ASSERT_EQ(iotdbfs::platform::set_environment(name, "second", false), 0);
  EXPECT_STREQ(std::getenv(name), "first");
  ASSERT_EQ(iotdbfs::platform::set_environment(name, "second"), 0);
  EXPECT_STREQ(std::getenv(name), "second");
  ASSERT_EQ(iotdbfs::platform::set_environment(name, ""), 0);
  EXPECT_STREQ(std::getenv(name), "");
}

TEST(Platform, EnvironmentRejectsInvalidVariableNames) {
  EXPECT_EQ(iotdbfs::platform::set_environment("", "value"), -EINVAL);
  EXPECT_EQ(iotdbfs::platform::set_environment("invalid=name", "value"), -EINVAL);
}

#if defined(__linux__)
TEST(Platform, ExecutablePathResolvesRunningBinaryWithoutArgv) {
  for (const char* argv0 : {static_cast<const char*>(nullptr), "", "/not/the/program"}) {
    const auto executable = iotdbfs::platform::executable_path(argv0);
    EXPECT_TRUE(executable.is_absolute());
    EXPECT_TRUE(std::filesystem::is_regular_file(executable));
    EXPECT_TRUE(std::filesystem::equivalent(executable, "/proc/self/exe"));
  }
}
#endif
#endif
