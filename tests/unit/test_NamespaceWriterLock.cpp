#include "iotdbfs/NamespaceWriterLock.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <string>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

class LockDirectory {
public:
  LockDirectory() {
    char pattern[] = "/tmp/iotdbfs-writer-lock-XXXXXX";
    char* path = ::mkdtemp(pattern);
    if (path) path_ = path;
  }
  ~LockDirectory() {
    if (path_.empty()) return;
    ::unlink((path_ + "/namespace.lock").c_str());
    ::unlink((path_ + "/alias.lock").c_str());
    ::rmdir(path_.c_str());
  }
  std::string path() const { return path_ + "/namespace.lock"; }
  std::string alias() const { return path_ + "/alias.lock"; }
  bool valid() const { return !path_.empty(); }

private:
  std::string path_;
};

TEST(NamespaceWriterLock, RejectsRelativePathAndSymlink) {
  LockDirectory dir;
  ASSERT_TRUE(dir.valid());
  std::string error;
  EXPECT_FALSE(iotdbfs::NamespaceWriterLock::acquire(
    "namespace.lock", &error));
  EXPECT_NE(error.find("absolute"), std::string::npos);

  auto first = iotdbfs::NamespaceWriterLock::acquire(dir.path(), &error);
  ASSERT_TRUE(first) << error;
  ASSERT_EQ(::symlink(dir.path().c_str(), dir.alias().c_str()), 0);
  EXPECT_FALSE(iotdbfs::NamespaceWriterLock::acquire(dir.alias(), &error));
}

TEST(NamespaceWriterLock, ExcludesIndependentOpenAndReleasesOnClose) {
  LockDirectory dir;
  ASSERT_TRUE(dir.valid());
  std::string error;
  auto first = iotdbfs::NamespaceWriterLock::acquire(dir.path(), &error);
  ASSERT_TRUE(first) << error;
  EXPECT_FALSE(iotdbfs::NamespaceWriterLock::acquire(dir.path(), &error));
  EXPECT_NE(error.find("held"), std::string::npos);
  first.reset();
  auto second = iotdbfs::NamespaceWriterLock::acquire(dir.path(), &error);
  ASSERT_TRUE(second) << error;
  EXPECT_TRUE(error.empty());
}

TEST(NamespaceWriterLock, RejectsWritableExistingLockFile) {
  LockDirectory dir;
  ASSERT_TRUE(dir.valid());
  std::string error;
  auto first = iotdbfs::NamespaceWriterLock::acquire(dir.path(), &error);
  ASSERT_TRUE(first) << error;
  first.reset();
  ASSERT_EQ(::chmod(dir.path().c_str(), 0660), 0);
  EXPECT_FALSE(iotdbfs::NamespaceWriterLock::acquire(dir.path(), &error));
  EXPECT_NE(error.find("private"), std::string::npos);
}

TEST(NamespaceWriterLock, ExcludesAnotherProcess) {
  LockDirectory dir;
  ASSERT_TRUE(dir.valid());
  std::string error;
  auto parent_lock = iotdbfs::NamespaceWriterLock::acquire(dir.path(), &error);
  ASSERT_TRUE(parent_lock) << error;

  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    auto contender = iotdbfs::NamespaceWriterLock::acquire(dir.path());
    _exit(contender ? 1 : 0);
  }
  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST(NamespaceWriterLock, CrashReleasesKernelLockWithoutDeletingFile) {
  LockDirectory dir;
  ASSERT_TRUE(dir.valid());
  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    auto lease = iotdbfs::NamespaceWriterLock::acquire(dir.path());
    _exit(lease ? 0 : 1);
  }
  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(WEXITSTATUS(status), 0);
  std::string error;
  auto recovered = iotdbfs::NamespaceWriterLock::acquire(dir.path(), &error);
  ASSERT_TRUE(recovered) << error;
  struct stat st {};
  EXPECT_EQ(::stat(dir.path().c_str(), &st), 0);
}

} // namespace
