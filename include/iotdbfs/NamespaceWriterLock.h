#pragma once

#include <optional>
#include <string>
#include <utility>


namespace iotdbfs {
// Mount and offline commands must use the same explicit path or endpoint/database.
std::string namespace_writer_lock_path(const std::string& host, int port, const std::string& database);

// One local host may have only one writer for an IoTDB namespace. The mount
// daemon and an offline migration command must use the same absolute lock
// path and keep the returned object alive for their entire writing lifetime.
// The lock file is deliberately never unlinked: replacing it while another
// process holds its inode would permit two independent writers.
class NamespaceWriterLock final {
public:
  static std::optional<NamespaceWriterLock> acquire(
    const std::string& absolute_path, std::string* error = nullptr);

  NamespaceWriterLock(const NamespaceWriterLock&) = delete;
  NamespaceWriterLock& operator=(const NamespaceWriterLock&) = delete;
  NamespaceWriterLock(NamespaceWriterLock&& other) noexcept;
  NamespaceWriterLock& operator=(NamespaceWriterLock&& other) noexcept;
  ~NamespaceWriterLock();

  const std::string& path() const { return path_; }

private:
  NamespaceWriterLock(int fd, std::string path)
    : fd_(fd), path_(std::move(path)) {}
  void release() noexcept;

  int fd_{-1};
  std::string path_;
};

} // namespace iotdbfs
