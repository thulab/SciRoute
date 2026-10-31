#include "iotdbfs/formats/hdf5/Hdf5ScaleWorker.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <stdexcept>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

extern char **environ;

namespace iotdbfs {
namespace {
using Bytes = std::vector<std::uint8_t>;
using Associations = std::vector<Hdf5DimensionAssociation>;
using Clock = std::chrono::steady_clock;
constexpr std::size_t kMaxFrame = 1024 * 1024;
constexpr std::size_t kMaxRead = 1024 * 1024;
constexpr std::uint64_t kMaxReferences = 65536;
constexpr std::uint64_t kMaxBudget = 1024ULL * 1024 * 1024;

QueryResult<Associations> failure(QueryError code, const std::string &why) {
  return QueryResult<Associations>::failure(
      QueryStatus::failure(code, QueryPhase::Planning, why));
}
void number(Bytes &b, std::uint64_t x) {
  for (unsigned i = 0; i != 8; ++i)
    b.push_back(static_cast<std::uint8_t>(x >> (56 - i * 8)));
}
std::uint64_t read_number(const Bytes &b, std::size_t &at) {
  if (at > b.size() || b.size() - at < 8)
    throw std::runtime_error("truncated helper number");
  std::uint64_t x = 0;
  for (unsigned i = 0; i != 8; ++i)
    x = (x << 8) | b[at++];
  return x;
}
void string(Bytes &b, const std::string &s) {
  if (s.size() > 4096 || b.size() + s.size() + 8 > kMaxFrame)
    throw std::runtime_error("helper output exceeds limit");
  number(b, s.size());
  b.insert(b.end(), s.begin(), s.end());
}
std::string read_string(const Bytes &b, std::size_t &at) {
  const auto n = read_number(b, at);
  if (n > 4096 || n > b.size() - at)
    throw std::runtime_error("invalid helper string");
  std::string result(b.begin() + at, b.begin() + at + n);
  at += n;
  return result;
}
bool ready(int fd, short events, Clock::time_point deadline) {
  for (;;) {
    const auto now = Clock::now();
    if (now >= deadline)
      return false;
    const auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now)
            .count();
    pollfd p{fd, events, 0};
    const int rc = ::poll(
        &p, 1, static_cast<int>(std::min<std::int64_t>(ms + 1, INT32_MAX)));
    if (rc < 0 && errno == EINTR)
      continue;
    return rc > 0 && (p.revents & events);
  }
}
bool transfer(int fd, std::uint8_t *bytes, std::size_t size, bool write,
              Clock::time_point deadline) {
  while (size) {
    if (!ready(fd, write ? POLLOUT : POLLIN, deadline))
      return false;
#ifdef MSG_NOSIGNAL
    const int flags = MSG_NOSIGNAL;
#else
    const int flags = 0;
#endif
    const auto n =
        write ? ::send(fd, bytes, size, flags) : ::recv(fd, bytes, size, 0);
    if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK))
      continue;
    if (n <= 0)
      return false;
    bytes += n;
    size -= static_cast<std::size_t>(n);
  }
  return true;
}
bool send_frame(int fd, const Bytes &bytes, Clock::time_point deadline) {
  if (bytes.empty() || bytes.size() > kMaxFrame + 1)
    return false;
  std::uint8_t header[4];
  const auto n = static_cast<std::uint32_t>(bytes.size());
  for (unsigned i = 0; i != 4; ++i)
    header[i] = static_cast<std::uint8_t>(n >> (24 - 8 * i));
  return transfer(fd, header, sizeof(header), true, deadline) &&
         transfer(fd, const_cast<std::uint8_t *>(bytes.data()), bytes.size(),
                  true, deadline);
}
bool receive_frame(int fd, Bytes &bytes, Clock::time_point deadline) {
  std::uint8_t header[4];
  if (!transfer(fd, header, sizeof(header), false, deadline))
    return false;
  std::uint32_t n = 0;
  for (auto b : header)
    n = (n << 8) | b;
  if (!n || n > kMaxFrame + 1)
    return false;
  bytes.resize(n);
  return transfer(fd, bytes.data(), bytes.size(), false, deadline);
}
bool valid_limits(const Hdf5ScaleWorkerLimits &l) {
  return l.max_read_bytes && l.max_read_bytes <= kMaxBudget &&
         l.max_read_calls && l.max_read_calls <= 1048576 &&
         l.max_associations <= kMaxReferences &&
         l.address_space_bytes >= 64ULL * 1024 * 1024 &&
         l.address_space_bytes <= 4ULL * kMaxBudget && l.cpu_seconds &&
         l.cpu_seconds <= 60 && l.wall_ms && l.wall_ms <= 120000;
}
std::string executable() {
  if (const auto configured = std::getenv("IOTDBFS_HDF5_SCALE_WORKER")) {
    std::filesystem::path p(configured);
    if (!p.is_absolute())
      return {};
    return p.string();
  }
  std::string path;
#ifdef __APPLE__
  std::uint32_t n = 0;
  _NSGetExecutablePath(nullptr, &n);
  if (!n || n > 65536)
    return {};
  std::vector<char> b(n);
  if (_NSGetExecutablePath(b.data(), &n) != 0)
    return {};
  path = b.data();
#else
  char b[65536];
  const auto n = ::readlink("/proc/self/exe", b, sizeof(b));
  if (n <= 0 || static_cast<std::size_t>(n) == sizeof(b))
    return {};
  path.assign(b, n);
#endif
  return (std::filesystem::path(path).parent_path() /
          "iotdbfs_hdf5_scale_worker")
      .string();
}
struct Child {
  pid_t pid{-1};
  int fd{-1};
  ~Child() {
    if (fd >= 0)
      ::close(fd);
    if (pid > 0) {
      ::kill(pid, SIGKILL); // Unreaped child pid cannot be reused.
      int status;
      while (::waitpid(pid, &status, 0) < 0 && errno == EINTR) {
      }
    }
  }
};
bool limit(int resource, std::uint64_t value) {
  const rlimit r{static_cast<rlim_t>(value), static_cast<rlim_t>(value)};
  return ::setrlimit(resource, &r) == 0;
}
} // namespace

QueryResult<Associations>
read_hdf5_dimension_associations_isolated(const FormatSource &source,
                                          const Hdf5ScaleWorkerLimits &limits) {
  if (!source.read || !valid_limits(limits))
    return failure(QueryError::InvalidQuery,
                   "invalid HDF5 helper source or limits");
  const auto path = executable();
  if (path.empty() || ::access(path.c_str(), X_OK) != 0)
    return failure(QueryError::UnsupportedCapability,
                   "HDF5 dimension-scale helper is not installed");
  int fds[2];
  if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0)
    return failure(QueryError::ResourceExhausted,
                   "cannot create HDF5 helper channel");
  for (unsigned i = 0; i < 2; ++i) {
    if (fds[i] <= STDERR_FILENO) {
      const int moved = ::fcntl(fds[i], F_DUPFD_CLOEXEC, 3);
      if (moved < 0) {
        ::close(fds[0]);
        ::close(fds[1]);
        return failure(QueryError::ResourceExhausted,
                       "cannot isolate helper descriptors");
      }
      ::close(fds[i]);
      fds[i] = moved;
    }
    if (::fcntl(fds[i], F_SETFD, FD_CLOEXEC) < 0 ||
        ::fcntl(fds[i], F_SETFL, O_NONBLOCK) < 0) {
      ::close(fds[0]);
      ::close(fds[1]);
      return failure(QueryError::ResourceExhausted,
                     "cannot bound helper descriptors");
    }
  }
#ifdef SO_NOSIGPIPE
  const int yes = 1;
  ::setsockopt(fds[0], SOL_SOCKET, SO_NOSIGPIPE, &yes, sizeof(yes));
  ::setsockopt(fds[1], SOL_SOCKET, SO_NOSIGPIPE, &yes, sizeof(yes));
#endif
  posix_spawn_file_actions_t actions;
  if (::posix_spawn_file_actions_init(&actions) != 0) {
    ::close(fds[0]);
    ::close(fds[1]);
    return failure(QueryError::ResourceExhausted,
                   "cannot configure HDF5 helper");
  }
  int rc = ::posix_spawn_file_actions_addopen(&actions, STDERR_FILENO,
                                              "/dev/null", O_WRONLY, 0);
  rc |= ::posix_spawn_file_actions_adddup2(&actions, fds[1], STDIN_FILENO);
  rc |= ::posix_spawn_file_actions_adddup2(&actions, fds[1], STDOUT_FILENO);
  rc |= ::posix_spawn_file_actions_addclose(&actions, fds[0]);
  if (fds[1] > STDOUT_FILENO)
    rc |= ::posix_spawn_file_actions_addclose(&actions, fds[1]);
  Child child;
  char *argv[]{const_cast<char *>(path.c_str()), nullptr};
  if (!rc)
    rc = ::posix_spawn(&child.pid, path.c_str(), &actions, nullptr, argv,
                       environ);
  ::posix_spawn_file_actions_destroy(&actions);
  ::close(fds[1]);
  child.fd = fds[0];
  if (rc)
    return failure(QueryError::UnsupportedCapability,
                   "cannot start HDF5 dimension-scale helper");
  const auto deadline =
      Clock::now() + std::chrono::milliseconds(limits.wall_ms);
  try {
    Bytes init{'I'};
    for (auto v : {source.size, limits.max_read_bytes, limits.max_read_calls,
                   limits.max_associations, limits.address_space_bytes,
                   static_cast<std::uint64_t>(limits.cpu_seconds),
                   static_cast<std::uint64_t>(limits.wall_ms)})
      number(init, v);
    if (!send_frame(child.fd, init, deadline))
      return failure(QueryError::ResourceExhausted,
                     "HDF5 helper initialization timed out");
    std::uint64_t bytes = 0, calls = 0;
    for (;;) {
      Bytes message;
      if (!receive_frame(child.fd, message, deadline))
        return failure(QueryError::ResourceExhausted,
                       "HDF5 helper stopped or exceeded execution limits");
      std::size_t at = 1;
      if (message[0] == 'R') {
        const auto offset = read_number(message, at),
                   length = read_number(message, at);
        if (at != message.size() || offset > source.size ||
            length > source.size - offset || length > kMaxRead ||
            calls >= limits.max_read_calls ||
            length > limits.max_read_bytes - bytes)
          return failure(QueryError::ResourceExhausted,
                         "HDF5 helper source-read budget exceeded");
        ++calls;
        bytes += length;
        std::optional<Bytes> data;
        try {
          data = source.read(offset, length);
        } catch (...) {
          return failure(QueryError::ReadFailure,
                         "HDF5 helper source read threw");
        }
        if (!data || data->size() != length)
          return failure(QueryError::ReadFailure,
                         "HDF5 helper source read failed or was short");
        Bytes response{'D'};
        response.insert(response.end(), data->begin(), data->end());
        if (!send_frame(child.fd, response, deadline))
          return failure(QueryError::ResourceExhausted,
                         "HDF5 helper read response exceeded deadline");
      } else if (message[0] == 'A') {
        const auto n = read_number(message, at);
        if (n > limits.max_associations)
          throw std::runtime_error("too many dimension associations");
        Associations result;
        result.reserve(n);
        for (std::uint64_t i = 0; i != n; ++i) {
          Hdf5DimensionAssociation a;
          a.dataset_id = read_string(message, at);
          const auto axis = read_number(message, at);
          if (axis >= 32)
            throw std::runtime_error("invalid dimension ordinal");
          a.axis = static_cast<std::uint32_t>(axis);
          a.scale_id = read_string(message, at);
          if (a.dataset_id.empty() || a.scale_id.empty())
            throw std::runtime_error("missing object identity");
          result.push_back(std::move(a));
        }
        if (at != message.size())
          throw std::runtime_error("trailing dimension-association bytes");
        return QueryResult<Associations>::success(std::move(result));
      } else if (message[0] == 'E') {
        const auto code = read_number(message, at);
        const auto why = read_string(message, at);
        if (at != message.size() || code == 0 ||
            code > static_cast<unsigned>(QueryError::Internal))
          throw std::runtime_error("invalid helper error");
        return failure(static_cast<QueryError>(code),
                       "HDF5 dimension-scale helper: " + why);
      } else
        throw std::runtime_error("invalid helper message");
    }
  } catch (const std::exception &e) {
    return failure(QueryError::CorruptLayout,
                   std::string("invalid HDF5 helper reply: ") + e.what());
  }
}

int hdf5_scale_worker_main() {
  ::signal(SIGPIPE, SIG_IGN);
  // Bound protocol bootstrap before accepting caller-supplied lower limits.
  const auto bootstrap = Clock::now() + std::chrono::seconds(10);
  Bytes init;
  if (!receive_frame(STDIN_FILENO, init, bootstrap) || init[0] != 'I')
    return 2;
  try {
    std::size_t at = 1;
    const auto size = read_number(init, at);
    Hdf5ScaleWorkerLimits l;
    l.max_read_bytes = read_number(init, at);
    l.max_read_calls = read_number(init, at);
    l.max_associations = read_number(init, at);
    l.address_space_bytes = read_number(init, at);
    const auto cpu = read_number(init, at), wall = read_number(init, at);
    if (cpu > UINT32_MAX || wall > UINT32_MAX)
      return 2;
    l.cpu_seconds = static_cast<std::uint32_t>(cpu);
    l.wall_ms = static_cast<std::uint32_t>(wall);
    if (at != init.size() || !valid_limits(l))
      return 2;
    if (!limit(RLIMIT_AS, l.address_space_bytes) ||
        !limit(RLIMIT_CPU, l.cpu_seconds) || !limit(RLIMIT_CORE, 0) ||
        !limit(RLIMIT_FSIZE, 0)) {
      Bytes error{'E'};
      number(error, static_cast<unsigned>(QueryError::UnsupportedCapability));
      string(error, "cannot enforce required HDF5 helper process resource "
                    "limits on this platform");
      send_frame(STDOUT_FILENO, error, bootstrap);
      return 3;
    }
    // Drop inherited descriptors; the source is accessible only via fd 0/1 RPC.
    rlimit descriptors{};
    if (::getrlimit(RLIMIT_NOFILE, &descriptors) != 0)
      return 3;
    const auto n = std::min<rlim_t>(descriptors.rlim_cur, 1048576);
    for (int fd = 3; static_cast<rlim_t>(fd) < n; ++fd)
      ::close(fd);
    if (!limit(RLIMIT_NOFILE, 64))
      return 3;
    const auto deadline = Clock::now() + std::chrono::milliseconds(l.wall_ms);
    std::uint64_t bytes = 0, calls = 0;
    FormatSource source{
        size,
        [&](std::uint64_t offset,
            std::uint64_t length) -> std::optional<Bytes> {
          if (offset > size || length > size - offset ||
              calls >= l.max_read_calls || length > l.max_read_bytes - bytes ||
              length > kMaxRead)
            return std::nullopt;
          ++calls;
          bytes += length;
          Bytes request{'R'};
          number(request, offset);
          number(request, length);
          Bytes response;
          if (!send_frame(STDOUT_FILENO, request, deadline) ||
              !receive_frame(STDIN_FILENO, response, deadline) ||
              response[0] != 'D' || response.size() - 1 != length)
            return std::nullopt;
          return Bytes(response.begin() + 1, response.end());
        }};
    const auto result =
        inspect_hdf5_dimension_associations_native(source, l.max_associations);
    Bytes response;
    if (result.ok()) {
      if (result.value().size() > l.max_associations)
        throw std::runtime_error("too many scale references");
      response.push_back('A');
      number(response, result.value().size());
      for (const auto &a : result.value()) {
        string(response, a.dataset_id);
        number(response, a.axis);
        string(response, a.scale_id);
      }
    } else {
      response.push_back('E');
      number(response, static_cast<unsigned>(result.status().code()));
      string(response, result.status().message().substr(0, 4096));
    }
    return send_frame(STDOUT_FILENO, response, deadline) ? 0 : 4;
  } catch (...) {
    return 4;
  }
}
} // namespace iotdbfs
