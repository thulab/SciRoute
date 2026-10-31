#pragma once

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <optional>
#include <string>
#include <vector>
#include "iotdbfs/Platform.h"

namespace iotdbfs {

// An unlinked, private file provides random access with a fixed-size transfer
// buffer. No whole-file memory allocation is needed, including sparse writes.
class StagedFile {
public:
  using Reader = std::function<std::optional<std::vector<uint8_t>>(
    uint64_t, uint64_t)>;
  StagedFile() = default;
  ~StagedFile() { if (fd_ >= 0) platform::close_fd(fd_); }
  StagedFile(const StagedFile&) = delete;
  StagedFile& operator=(const StagedFile&) = delete;

  int create() {
    if (fd_ >= 0)
      return 0;
    const char* configured = std::getenv("IOTDB_STAGE_DIR");
    fd_ = platform::create_staged_file(configured);
    if (fd_ < 0)
      return fd_;
    return 0;
  }

  int load(uint64_t size, const Reader& reader) {
    int rc = create();
    if (rc != 0)
      return rc;
    constexpr uint64_t chunk = 1024 * 1024;
    for (uint64_t off = 0; off < size; off += chunk) {
      const uint64_t count = std::min(chunk, size - off);
      auto bytes = reader(off, count);
      if (!bytes || bytes->size() != count)
        return -EIO;
      rc = write(bytes->data(), bytes->size(), off);
      if (rc != 0)
        return rc;
    }
    return resize(size);
  }

  int write(const void* data, size_t size, uint64_t off) {
    if (fd_ < 0)
      return -EBADF;
    if (off > static_cast<uint64_t>(std::numeric_limits<off_t>::max()) ||
      size > static_cast<uint64_t>(std::numeric_limits<off_t>::max()) - off)
      return -EFBIG;
    size_t done = 0;
    while (done < size) {
      ssize_t n = platform::write_at(fd_, static_cast<const char*>(data) + done,
        size - done, off + done);
      if (n < 0 && errno == EINTR)
        continue;
      if (n <= 0)
        return n < 0 ? -errno : -EIO;
      done += static_cast<size_t>(n);
      size_ = std::max(size_, off + done);
    }
    return 0;
  }

  int read(void* data, size_t size, uint64_t off) const {
    if (fd_ < 0)
      return -EBADF;
    if (off >= size_)
      return 0;
    const size_t wanted = static_cast<size_t>(std::min<uint64_t>(
      std::min<size_t>(size, static_cast<size_t>(std::numeric_limits<int>::max())),
      size_ - off));
    size_t done = 0;
    while (done < wanted) {
      ssize_t n = platform::read_at(fd_, static_cast<char*>(data) + done,
        wanted - done, off + done);
      if (n < 0 && errno == EINTR)
        continue;
      if (n <= 0)
        return n < 0 ? -errno : -EIO;
      done += static_cast<size_t>(n);
    }
    return static_cast<int>(done);
  }

  int resize(uint64_t size) {
    if (fd_ < 0)
      return -EBADF;
    if (size > static_cast<uint64_t>(std::numeric_limits<off_t>::max()))
      return -EFBIG;
    if (platform::resize_fd(fd_, size) != 0)
      return -errno;
    size_ = size;
    return 0;
  }
  int sync() const { return platform::sync_fd(fd_) == 0 ? 0 : -errno; }
  uint64_t size() const { return size_; }

private:
  int fd_{ -1 };
  uint64_t size_{ 0 };
};

} // namespace iotdbfs
