#include "Hdf5CfTestFixture.h"
#include <gtest/gtest.h>
using namespace iotdbfs;
using hdf5_cf_test::fixture;

#include "iotdbfs/formats/hdf5/Hdf5ScaleWorker.h"
#include <chrono>
#include <sys/stat.h>
namespace {
struct EnvironmentValue {
  std::string key;
  std::optional<std::string> old;
  EnvironmentValue(const char *k, const char *value) : key(k) {
    if (auto p = std::getenv(k))
      old = p;
    setenv(k, value, 1);
  }
  ~EnvironmentValue() {
    if (old)
      setenv(key.c_str(), old->c_str(), 1);
    else
      unsetenv(key.c_str());
  }
};
FormatSource format_source(const BinaryValue &bytes) {
  return {bytes.size(),
          [&](std::uint64_t o, std::uint64_t n) -> std::optional<BinaryValue> {
            if (o > bytes.size() || n > bytes.size() - o)
              return {};
            return BinaryValue(bytes.begin() + o, bytes.begin() + o + n);
          }};
}
} // namespace
TEST(Hdf5ScaleWorker, ShortReadsAndCallbackExceptionsAreReadFailures) {
#ifdef __APPLE__
  GTEST_SKIP()
      << "macOS cannot enforce the configured RLIMIT_AS; tested on Linux";
#endif
  auto bytes = fixture();
  auto source = format_source(bytes);
  source.read = [](auto, auto) -> std::optional<BinaryValue> {
    return BinaryValue{};
  };
  auto r = read_hdf5_dimension_associations_isolated(source);
  ASSERT_FALSE(r.ok());
  EXPECT_EQ(r.status().code(), QueryError::ReadFailure) << r.status().message();
  source.read = [](auto, auto) -> std::optional<BinaryValue> {
    throw std::runtime_error("fixture");
  };
  r = read_hdf5_dimension_associations_isolated(source);
  ASSERT_FALSE(r.ok());
  EXPECT_EQ(r.status().code(), QueryError::ReadFailure) << r.status().message();
}
TEST(Hdf5ScaleWorker, ReadAndAssociationBudgetsFailWithoutPartialOutput) {
#ifdef __APPLE__
  GTEST_SKIP()
      << "macOS cannot enforce the configured RLIMIT_AS; tested on Linux";
#endif
  auto bytes = fixture();
  auto source = format_source(bytes);
  Hdf5ScaleWorkerLimits l;
  l.max_read_bytes = 1;
  auto r = read_hdf5_dimension_associations_isolated(source, l);
  ASSERT_FALSE(r.ok());
  l = {};
  l.max_associations = 1;
  r = read_hdf5_dimension_associations_isolated(source, l);
  ASSERT_FALSE(r.ok());
  EXPECT_EQ(r.status().code(), QueryError::ResourceExhausted)
      << r.status().message();
}
TEST(Hdf5ScaleWorker, RejectsDriverEnvironment) {
#ifdef __APPLE__
  GTEST_SKIP()
      << "macOS cannot enforce the configured RLIMIT_AS; tested on Linux";
#endif
  auto bytes = fixture();
  auto source = format_source(bytes);
  EnvironmentValue env("HDF5_DRIVER", "core");
  auto r = read_hdf5_dimension_associations_isolated(source);
  ASSERT_FALSE(r.ok());
  EXPECT_EQ(r.status().code(), QueryError::UnsupportedCapability)
      << r.status().message();
}
TEST(Hdf5ScaleWorker, WallDeadlineReapsUnresponsiveWorker) {
  char path[] = "/tmp/iotdbfs-cf-worker-XXXXXX";
  int fd = mkstemp(path);
  ASSERT_GE(fd, 0);
  const char content[] = "#!/bin/sh\nexec sleep 10\n";
  ASSERT_EQ(write(fd, content, sizeof(content) - 1), sizeof(content) - 1);
  close(fd);
  ASSERT_EQ(chmod(path, 0700), 0);
  struct Remove {
    char *p;
    ~Remove() { unlink(p); }
  } remove{path};
  EnvironmentValue env("IOTDBFS_HDF5_SCALE_WORKER", path);
  auto bytes = fixture();
  Hdf5ScaleWorkerLimits l;
  l.wall_ms = 30;
  const auto start = std::chrono::steady_clock::now();
  auto r = read_hdf5_dimension_associations_isolated(format_source(bytes), l);
  EXPECT_FALSE(r.ok());
  EXPECT_EQ(r.status().code(), QueryError::ResourceExhausted);
  EXPECT_LT(
      std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
          .count(),
      1);
}
