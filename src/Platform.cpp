#include "iotdbfs/Platform.h"
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <string>
#include <vector>
#if defined(_WIN32)
namespace iotdbfs::platform {
int create_staged_file(const char*) { return -ENOTSUP; }
int close_fd(int) { return 0; }
ssize_t read_at(int, void*, size_t, uint64_t) { return -ENOTSUP; }
ssize_t write_at(int, const void*, size_t, uint64_t) { return -ENOTSUP; }
int resize_fd(int, uint64_t) { return -ENOTSUP; }
int sync_fd(int) { return -ENOTSUP; }
std::filesystem::path executable_path(const char* argv0) { return argv0 && *argv0 ? std::filesystem::path(argv0) : std::filesystem::path{}; }
int set_environment(const char*, const char*, bool) { return -ENOTSUP; }
bool xattr_write_supported() { return false; }
}
#else
#include <fcntl.h>
#include <limits.h>
#include <unistd.h>
#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif
namespace {
std::filesystem::path fallback_executable_path(const char* argv0) {
  if (!argv0 || !*argv0) return {};
  std::filesystem::path p(argv0); std::error_code ec;
  if (p.is_absolute()) return std::filesystem::weakly_canonical(p, ec);
  return std::filesystem::weakly_canonical(std::filesystem::current_path(ec) / p, ec);
}
}
namespace iotdbfs::platform {
int create_staged_file(const char* dir) {
  std::string pattern = (dir && *dir) ? dir : "/tmp"; pattern += "/iotdbfs-stage-XXXXXX";
  std::vector<char> name(pattern.begin(), pattern.end()); name.push_back('\0');
  int fd = ::mkstemp(name.data()); if (fd < 0) return -errno;
  if (::unlink(name.data()) != 0) { int e = errno; ::close(fd); return -e; }
  if (::fcntl(fd, F_SETFD, FD_CLOEXEC) != 0) { int e = errno; ::close(fd); return -e; }
  return fd;
}
int close_fd(int fd) { return fd < 0 || ::close(fd) == 0 ? 0 : -errno; }
ssize_t read_at(int fd, void* b, size_t s, uint64_t o) { if (o > (uint64_t)std::numeric_limits<off_t>::max()) { errno = EOVERFLOW; return -1; } return ::pread(fd,b,s,(off_t)o); }
ssize_t write_at(int fd, const void* b, size_t s, uint64_t o) { if (o > (uint64_t)std::numeric_limits<off_t>::max()) { errno = EOVERFLOW; return -1; } return ::pwrite(fd,b,s,(off_t)o); }
int resize_fd(int fd, uint64_t s) { if (s > (uint64_t)std::numeric_limits<off_t>::max()) { errno = EFBIG; return -1; } return ::ftruncate(fd,(off_t)s); }
int sync_fd(int fd) { return ::fsync(fd); }
std::filesystem::path executable_path(const char* argv0) {
#if defined(__APPLE__)
  uint32_t size = 0; if (_NSGetExecutablePath(nullptr, &size) == -1 && size) { std::vector<char> b(size+1); if (_NSGetExecutablePath(b.data(), &size) == 0) return b.data(); }
#else
  char b[PATH_MAX+1]; ssize_t n = ::readlink("/proc/self/exe", b, PATH_MAX); if (n > 0) { b[n] = '\0'; return b; }
#endif
  return fallback_executable_path(argv0);
}
int set_environment(const char* n, const char* v, bool o) { return ::setenv(n,v,o ? 1 : 0) == 0 ? 0 : -errno; }
bool xattr_write_supported() { return false; }
}
#endif
