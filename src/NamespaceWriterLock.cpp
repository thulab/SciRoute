#include "iotdbfs/NamespaceWriterLock.h"
#include <cstdlib>
#include <cstdint>
#include <iomanip>
#include <sstream>

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

namespace iotdbfs {
namespace {

void set_error(std::string* error, const std::string& message) {
  if (error) *error = message;
}

} // namespace

std::optional<NamespaceWriterLock> NamespaceWriterLock::acquire(
  const std::string& absolute_path, std::string* error) {
  if (absolute_path.empty() || absolute_path.front() != '/') {
    set_error(error, "namespace writer lock path must be absolute");
    return std::nullopt;
  }

  const int fd = ::open(absolute_path.c_str(),
    O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (fd < 0) {
    set_error(error, "cannot open namespace writer lock " + absolute_path +
      ": " + std::strerror(errno));
    return std::nullopt;
  }

  struct stat st {};
  if (::fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_nlink != 1 ||
      st.st_uid != ::geteuid() || (st.st_mode & (S_IWGRP | S_IWOTH))) {
    set_error(error, "namespace writer lock must be a private regular file "
      "owned by the daemon user with one link");
    ::close(fd);
    return std::nullopt;
  }
  if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
    const int saved_errno = errno;
    set_error(error, saved_errno == EWOULDBLOCK || saved_errno == EAGAIN
      ? "namespace writer lock is held by another process"
      : "cannot acquire namespace writer lock " + absolute_path + ": " +
          std::strerror(saved_errno));
    ::close(fd);
    return std::nullopt;
  }
  if (error) error->clear();
  return NamespaceWriterLock(fd, absolute_path);
}

NamespaceWriterLock::NamespaceWriterLock(NamespaceWriterLock&& other) noexcept
  : fd_(std::exchange(other.fd_, -1)), path_(std::move(other.path_)) {}

NamespaceWriterLock& NamespaceWriterLock::operator=(
  NamespaceWriterLock&& other) noexcept {
  if (this != &other) {
    release();
    fd_ = std::exchange(other.fd_, -1);
    path_ = std::move(other.path_);
  }
  return *this;
}

std::string namespace_writer_lock_path(const std::string& host, int port, const std::string& database) {
  if (const char* chosen = std::getenv("IOTDBFS_NAMESPACE_LOCK"))
    return chosen;
  const std::string scope = host + ':' + std::to_string(port) + '/' + database;
  std::uint64_t hash = 14695981039346656037ULL;
  for (unsigned char c : scope) {
    hash ^= c;
    hash *= 1099511628211ULL;
  }
  std::ostringstream out;
  out << "/tmp/iotdbfs-writer-" << std::hex << std::setw(16)
      << std::setfill('0') << hash << ".lock";
  return out.str();
}

NamespaceWriterLock::~NamespaceWriterLock() { release(); }

void NamespaceWriterLock::release() noexcept {
  if (fd_ < 0) return;
  ::close(fd_);
  fd_ = -1;
}

} // namespace iotdbfs
