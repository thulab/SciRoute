#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <mutex>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace iotdbfs::versioning::detail {

// Optional diagnostic output, independent of thread-local query profiles.
// One process-wide sink, at most 4096 attempted records. Failures disable output
// and never affect an object read. No SQL, object identity or payload is logged.
class OwnerReadTraceSink {
public:
  static constexpr std::uint64_t kMaximumRecords = 4096;
  // An optional ready file delays admission until startup work has finished.
  // The pointed-to path must outlive this sink (process environment is fixed).
  explicit OwnerReadTraceSink(const char* path, const char* ready_file = nullptr) noexcept
    : ready_file_(ready_file), ready_(!ready_file) {
    if (!path || !*path) return;
    fd_ = ::open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW, 0600);
    struct stat status{};
    if (fd_ >= 0 && (::fstat(fd_, &status) != 0 || !S_ISREG(status.st_mode))) {
      ::close(fd_); fd_ = -1;
    }
  }
  ~OwnerReadTraceSink() { if (fd_ >= 0) ::close(fd_); }
  OwnerReadTraceSink(const OwnerReadTraceSink&) = delete;
  OwnerReadTraceSink& operator=(const OwnerReadTraceSink&) = delete;
  bool enabled() const noexcept { return fd_ >= 0; }
  bool reserve() noexcept {
    if (!enabled()) return false;
    if (!ready_.load(std::memory_order_relaxed)) {
      struct stat status{};
      // lstat rejects a symlink at the gate itself. A missing/non-regular gate
      // neither consumes the record cap nor starts any read clocks.
      if (::lstat(ready_file_, &status) != 0 || !S_ISREG(status.st_mode)) return false;
      ready_.store(true, std::memory_order_relaxed);
    }
    return attempts_.fetch_add(1, std::memory_order_relaxed) < kMaximumRecords;
  }
  void append(const std::string& line) noexcept {
    try {
      std::lock_guard<std::mutex> lock(mutex_);
      if (failed_) return;
      // One append syscall, no retry, including EINTR/partial writes.
      if (::write(fd_, line.data(), line.size()) != static_cast<ssize_t>(line.size())) failed_ = true;
    } catch (...) {}
  }
  static OwnerReadTraceSink& process() noexcept {
    static OwnerReadTraceSink sink(std::getenv("IOTDBFS_OWNER_READ_TRACE_JSONL"),
      std::getenv("IOTDBFS_OWNER_READ_TRACE_READY_FILE"));
    return sink;
  }
private:
  int fd_{-1};
  const char* ready_file_;
  std::atomic<bool> ready_;
  std::atomic<std::uint64_t> attempts_{0};
  std::mutex mutex_;
  bool failed_{false};
};

class OwnerReadTiming {
public:
  struct Record {
    std::uint64_t begin_ns{0}, end_ns{0};
    std::uint64_t acquire_start_ns{0}, acquire_end_ns{0};
    std::uint64_t connect_start_ns{0}, connect_end_ns{0};
    std::uint64_t execute_start_ns{0}, execute_end_ns{0};
    std::uint64_t first_row_ns{0}, consume_end_ns{0};
    std::uint64_t operation_close_start_ns{0}, operation_close_end_ns{0};
    std::uint64_t release_start_ns{0}, release_end_ns{0};
    std::uint64_t visitor_ns{0}, copy_ns{0}, digest_ns{0}, validation_ns{0};
    std::uint64_t rows{0}, returned_bytes{0}, close_attempts{0}, reader_id{0};
    bool success{false}, payload_blocks{false};
    const char* stage{"acquire"};
  } record;

  explicit OwnerReadTiming(bool batch, std::size_t objects,
      OwnerReadTraceSink& sink = OwnerReadTraceSink::process()) noexcept
    : sink_(sink.reserve() ? &sink : nullptr), batch_(batch), objects_(objects) {
    if (sink_) {
      static std::atomic<std::uint64_t> next{0};
      sequence_ = next.fetch_add(1, std::memory_order_relaxed) + 1;
      mark(record.begin_ns);
    }
  }
  ~OwnerReadTiming() noexcept {
    if (!sink_) return;
    mark(record.end_ns);
    try {
      std::ostringstream out;
      out << "{\"schema\":\"iotdbfs.owner_read/1\",\"sequence\":" << sequence_
          << ",\"pid\":" << ::getpid() << ",\"batch\":" << (batch_ ? "true" : "false")
          << ",\"objects\":" << objects_ << ",\"success\":" << (record.success ? "true" : "false")
          << ",\"payload_blocks\":" << (record.payload_blocks ? "true" : "false")
          << ",\"stage\":\"" << record.stage << "\"";
#define IOTDBFS_READ_TIME_FIELD(name) out << ",\"" #name "\":" << record.name
      IOTDBFS_READ_TIME_FIELD(begin_ns); IOTDBFS_READ_TIME_FIELD(end_ns);
      IOTDBFS_READ_TIME_FIELD(acquire_start_ns); IOTDBFS_READ_TIME_FIELD(acquire_end_ns);
      IOTDBFS_READ_TIME_FIELD(connect_start_ns); IOTDBFS_READ_TIME_FIELD(connect_end_ns);
      IOTDBFS_READ_TIME_FIELD(execute_start_ns); IOTDBFS_READ_TIME_FIELD(execute_end_ns);
      IOTDBFS_READ_TIME_FIELD(first_row_ns); IOTDBFS_READ_TIME_FIELD(consume_end_ns);
      IOTDBFS_READ_TIME_FIELD(operation_close_start_ns); IOTDBFS_READ_TIME_FIELD(operation_close_end_ns);
      IOTDBFS_READ_TIME_FIELD(release_start_ns); IOTDBFS_READ_TIME_FIELD(release_end_ns);
      IOTDBFS_READ_TIME_FIELD(visitor_ns); IOTDBFS_READ_TIME_FIELD(copy_ns);
      IOTDBFS_READ_TIME_FIELD(digest_ns); IOTDBFS_READ_TIME_FIELD(validation_ns);
      IOTDBFS_READ_TIME_FIELD(rows); IOTDBFS_READ_TIME_FIELD(returned_bytes);
      IOTDBFS_READ_TIME_FIELD(close_attempts); IOTDBFS_READ_TIME_FIELD(reader_id);
#undef IOTDBFS_READ_TIME_FIELD
      out << "}\n";
      sink_->append(out.str());
    } catch (...) {}
  }
  OwnerReadTiming(const OwnerReadTiming&) = delete;
  OwnerReadTiming& operator=(const OwnerReadTiming&) = delete;
  void mark(std::uint64_t& field) const noexcept { if (sink_) field = now(); }
  void stage(const char* value) noexcept { record.stage = value; }
  class Duration {
  public:
    Duration(const OwnerReadTiming& timing, std::uint64_t& total) noexcept
      : total_(total), start_(timing.sink_ ? now() : 0) {}
    ~Duration() noexcept { if (start_) total_ += now() - start_; }
  private:
    std::uint64_t& total_;
    std::uint64_t start_;
  };
  // Lease is optional so reset measures the real pool release, including close
  // of a discarded reader, without changing the pool's reuse/error semantics.
  template<class OptionalLease>
  class Release {
  public:
    Release(OwnerReadTiming& timing, OptionalLease& lease) noexcept : timing_(timing), lease_(lease) {}
    ~Release() noexcept {
      if (!lease_) return;
      timing_.mark(timing_.record.release_start_ns);
      lease_.reset();
      timing_.mark(timing_.record.release_end_ns);
    }
  private:
    OwnerReadTiming& timing_;
    OptionalLease& lease_;
  };
private:
  static std::uint64_t now() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
  }
  OwnerReadTraceSink* sink_;
  bool batch_;
  std::size_t objects_;
  std::uint64_t sequence_{0};
};

} // namespace iotdbfs::versioning::detail
