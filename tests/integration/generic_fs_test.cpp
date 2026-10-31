// Linux-only, opt-in regression against an explicitly selected IoTDB test database.
// The controller never performs FUSE I/O: bounded child phases let it clean up
// its own mount and servers even when a filesystem operation stops responding.
#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/xattr.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

struct Config {
  fs::path fs_bin, web_bin, work_dir = "/tmp";
  std::string db, host = "127.0.0.1", user = "root", pass = "root";
  int port = 6667, web_port = 18080, pool_size = 4, timeout_seconds = 300;
  bool grib_enabled = true, skip_web = false;
};

struct Environment {
  Config cfg;
  fs::path run_dir, mount_dir, config, test_root, fusermount;
  pid_t fs_pid = -1, web_pid = -1, phase_pid = -1;
  unsigned mount_count = 0;
  Clock::time_point deadline;
};

static Environment env;
static volatile sig_atomic_t interrupted = 0;
static void on_signal(int sig) { interrupted = sig; }

static void require(bool condition, const std::string& message) {
  if (!condition)
    throw std::runtime_error(message);
}

static std::string error(const std::string& message) {
  return message + ": " + std::strerror(errno);
}

class Fd {
 public:
  explicit Fd(int fd) : fd_(fd) { require(fd >= 0, error("open/socket")); }
  ~Fd() { if (fd_ >= 0) ::close(fd_); }
  Fd(const Fd&) = delete;
  Fd& operator=(const Fd&) = delete;
  int get() const { return fd_; }
  int release_fd() {
    int fd = fd_;
    fd_ = -1;
    return fd;
  }
  void close() {
    int fd = fd_;
    fd_ = -1; // A failed Linux close must not be retried on a reused descriptor.
    require(::close(fd) == 0, error("close"));
  }
 private:
  int fd_;
};

static void write_all(int fd, const std::string& bytes, off_t offset = -1) {
  size_t done = 0;
  while (done < bytes.size()) {
    ssize_t n = offset < 0
      ? ::write(fd, bytes.data() + done, bytes.size() - done)
      : ::pwrite(fd, bytes.data() + done, bytes.size() - done, offset + done);
    if (n < 0 && errno == EINTR)
      continue;
    require(n > 0, error("write made no progress"));
    done += static_cast<size_t>(n);
  }
}

static void write_file(const fs::path& path, const std::string& bytes,
  int flags = O_TRUNC, mode_t mode = 0666) {
  Fd fd(::open(path.c_str(), O_WRONLY | O_CREAT | flags, mode));
  write_all(fd.get(), bytes);
  require(::fsync(fd.get()) == 0, error("fsync " + path.string()));
  fd.close();
}

static std::string read_file(const fs::path& path) {
  Fd fd(::open(path.c_str(), O_RDONLY));
  struct stat st {};
  require(::fstat(fd.get(), &st) == 0 && st.st_size >= 0, error("fstat"));
  require(st.st_size <= 4 * 1024 * 1024, "unexpected oversized test file");
  std::string bytes(static_cast<size_t>(st.st_size), '\0');
  size_t done = 0;
  while (done < bytes.size()) {
    ssize_t n = ::read(fd.get(), bytes.data() + done, bytes.size() - done);
    if (n < 0 && errno == EINTR)
      continue;
    require(n > 0, error("short read " + path.string()));
    done += static_cast<size_t>(n);
  }
  char extra;
  ssize_t n;
  do { n = ::read(fd.get(), &extra, 1); } while (n < 0 && errno == EINTR);
  require(n == 0, "expected EOF " + path.string());
  fd.close();
  return bytes;
}

static void expect_content(const fs::path& path, const std::string& expected) {
  require(read_file(path) == expected, "content mismatch: " + path.string());
}

static struct stat stat_file(const fs::path& path) {
  struct stat st {};
  require(::stat(path.c_str(), &st) == 0, error("stat " + path.string()));
  return st;
}

static void expect_failure(int rc, int expected_errno, const std::string& what) {
  const int saved = errno;
  require(rc == -1, what + " unexpectedly succeeded");
  require(saved == expected_errno, what + ": unexpected errno " + std::to_string(saved));
}

static void expect_open_failure(const fs::path& path, int flags, int expected) {
  int fd = ::open(path.c_str(), flags, 0600);
  int saved = errno;
  if (fd >= 0)
    ::close(fd);
  errno = saved;
  expect_failure(fd, expected, "open " + path.string());
}

static void checkpoint() {
  require(!interrupted, "interrupted by signal " + std::to_string(interrupted));
  require(Clock::now() < env.deadline, "test exceeded --timeout-seconds");
}

static bool reap(pid_t& pid, int* exit_code = nullptr) {
  if (pid <= 0)
    return true;
  int status = 0;
  pid_t rc = ::waitpid(pid, &status, WNOHANG);
  if (rc == pid || (rc < 0 && errno == ECHILD)) {
    if (exit_code)
      *exit_code = rc < 0 ? 255 : WIFEXITED(status)
        ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
    pid = -1;
    return true;
  }
  require(rc >= 0 || errno == EINTR, error("waitpid"));
  return false;
}

static void stop_process(pid_t& pid) {
  if (reap(pid))
    return;
  ::kill(pid, SIGTERM);
  for (int i = 0; i < 30 && !reap(pid); ++i)
    std::this_thread::sleep_for(100ms);
  if (pid > 0) {
    ::kill(pid, SIGKILL);
    for (int i = 0; i < 20 && !reap(pid); ++i)
      std::this_thread::sleep_for(100ms);
  }
  require(pid <= 0, "owned child could not be stopped");
}

static void reset_child_signals() {
  ::signal(SIGTERM, SIG_DFL);
  ::signal(SIGINT, SIG_DFL);
  ::signal(SIGPIPE, SIG_IGN);
}

static pid_t spawn(const fs::path& exe, const std::vector<std::string>& args,
  const std::string& log_name) {
  std::cout.flush();
  pid_t pid = ::fork();
  require(pid >= 0, error("fork"));
  if (pid == 0) {
    reset_child_signals();
    if (::setenv("IOTDB_CONFIG_FILE", env.config.c_str(), 1) != 0 ||
      ::setenv("IOTDB_GRIB_EXTENSION_ENABLED", env.cfg.grib_enabled ? "true" : "false", 1) != 0)
      _exit(126);
    int out = ::open((env.run_dir / (log_name + ".log")).c_str(),
      O_CREAT | O_EXCL | O_WRONLY, 0600);
    if (out < 0 || ::dup2(out, STDOUT_FILENO) < 0 || ::dup2(out, STDERR_FILENO) < 0)
      _exit(126);
    ::close(out);
    std::vector<char*> argv{ const_cast<char*>(exe.c_str()) };
    for (const auto& arg : args)
      argv.push_back(const_cast<char*>(arg.c_str()));
    argv.push_back(nullptr);
    ::execv(exe.c_str(), argv.data());
    _exit(127);
  }
  return pid;
}

static int wait_bounded(pid_t& pid, std::chrono::seconds limit,
  bool enforce_deadline = true) {
  const auto end = Clock::now() + limit;
  int result = 255;
  try {
    while (!reap(pid, &result)) {
      if (enforce_deadline)
        checkpoint();
      require(Clock::now() < end, "child phase timed out");
      std::this_thread::sleep_for(50ms);
    }
  } catch (...) {
    stop_process(pid);
    throw;
  }
  return result;
}

static void phase(const std::string& name, const std::function<void()>& action,
  bool cleanup_phase = false) {
  std::cout << "[test] " << name << std::endl;
  env.phase_pid = ::fork();
  require(env.phase_pid >= 0, error("fork phase"));
  if (env.phase_pid == 0) {
    reset_child_signals();
    try {
      action();
      _exit(0);
    } catch (const std::exception& e) {
      std::cerr << "[FAIL] " << name << ": " << e.what() << std::endl;
      _exit(1);
    }
  }
  require(wait_bounded(env.phase_pid, cleanup_phase ? 15s : 180s, !cleanup_phase) == 0,
    name + " failed");
}

// Reading mountinfo does not enter a potentially stuck FUSE callback.
static bool mounted() {
  if (env.mount_dir.empty())
    return false;
  std::ifstream in("/proc/self/mountinfo");
  require(in.good(), "cannot read /proc/self/mountinfo");
  std::string line;
  while (std::getline(in, line)) {
    std::istringstream fields(line);
    std::string value;
    for (int i = 0; i < 5; ++i)
      fields >> value;
    std::string decoded;
    for (size_t i = 0; i < value.size(); ++i) {
      if (value[i] == '\\' && i + 3 < value.size() &&
        value[i + 1] >= '0' && value[i + 1] <= '7' &&
        value[i + 2] >= '0' && value[i + 2] <= '7' &&
        value[i + 3] >= '0' && value[i + 3] <= '7') {
        decoded += static_cast<char>((value[i + 1] - '0') * 64 +
          (value[i + 2] - '0') * 8 + value[i + 3] - '0');
        i += 3;
      } else {
        decoded += value[i];
      }
    }
    if (decoded == env.mount_dir.string())
      return true;
  }
  return false;
}

static void unmount(bool lazy = false) {
  if (!mounted())
    return;
  pid_t pid = spawn(env.fusermount,
    { lazy ? "-uz" : "-u", env.mount_dir.string() },
    "unmount-" + std::to_string(env.mount_count) + (lazy ? "-lazy" : ""));
  require(wait_bounded(pid, 5s, false) == 0, "unmount failed");
  require(!mounted(), "test mount is still attached");
}

static void start_filesystem() {
  ++env.mount_count;
  env.fs_pid = spawn(env.cfg.fs_bin,
    { "-f", "-o", "default_permissions", env.mount_dir.string() },
    "filesystem-" + std::to_string(env.mount_count));
  const auto end = Clock::now() + 30s;
  while (Clock::now() < end) {
    checkpoint();
    require(!reap(env.fs_pid), "filesystem exited before mount became ready");
    if (mounted())
      return;
    std::this_thread::sleep_for(100ms);
  }
  throw std::runtime_error("filesystem mount timed out");
}

static std::string http_get(const std::string& path) {
  Fd fd(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0));
  sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(env.cfg.web_port));
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  int rc = ::connect(fd.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  require(rc == 0 || errno == EINPROGRESS, error("HTTP connect"));
  pollfd pfd{ fd.get(), POLLOUT, 0 };
  require(::poll(&pfd, 1, 2000) > 0, "HTTP connect timeout");
  int socket_error = 0;
  socklen_t len = sizeof(socket_error);
  require(::getsockopt(fd.get(), SOL_SOCKET, SO_ERROR, &socket_error, &len) == 0 &&
    socket_error == 0, "HTTP connection failed");
  require(::fcntl(fd.get(), F_SETFL, 0) == 0, error("fcntl socket"));
  timeval timeout{ 2, 0 };
  require(::setsockopt(fd.get(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0 &&
    ::setsockopt(fd.get(), SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == 0,
    error("socket timeout"));
  write_all(fd.get(), "GET " + path + " HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
  std::string response;
  char buf[4096];
  for (;;) {
    ssize_t n = ::read(fd.get(), buf, sizeof(buf));
    if (n < 0 && errno == EINTR)
      continue;
    require(n >= 0, error("HTTP read"));
    if (n == 0)
      break;
    response.append(buf, static_cast<size_t>(n));
    require(response.size() < 4 * 1024 * 1024, "HTTP response too large");
  }
  fd.close();
  const auto header_end = response.find("\r\n\r\n");
  require(header_end != std::string::npos, "incomplete HTTP response");
  // This server emits Content-Length, so EOF alone must not hide a short body.
  const std::string marker = "\r\nContent-Length: ";
  const auto length_start = response.find(marker);
  require(length_start < header_end, "HTTP response omitted Content-Length");
  size_t used = 0;
  const auto body_size = std::stoull(response.substr(length_start + marker.size()), &used);
  require(response.substr(length_start + marker.size() + used, 2) == "\r\n" &&
    body_size == response.size() - header_end - 4, "short or malformed HTTP body");
  return response;
}

static void test_files() {
  require(fs::create_directory(env.test_root), "private database test path already exists");
  require(fs::create_directories(env.test_root / "dir/nested"), "mkdir nested");
  write_file(env.test_root / "dir/nested/hello.txt", "hello IoTDB-FS\n");
  expect_content(env.test_root / "dir/nested/hello.txt", "hello IoTDB-FS\n");
  std::mt19937 rng(12345);
  std::string blob(1536 * 1024, '\0');
  for (auto& ch : blob)
    ch = static_cast<char>(rng() & 255);
  write_file(env.run_dir / "source.bin", blob);
  // Explicit checked reads/writes also validate short I/O and close errors.
  write_file(env.test_root / "blob.bin", read_file(env.run_dir / "source.bin"));
  expect_content(env.test_root / "blob.bin", blob);
  Fd slice(::open((env.test_root / "blob.bin").c_str(), O_RDONLY));
  std::string part(8192, '\0');
  size_t done = 0;
  while (done < part.size()) {
    ssize_t n = ::pread(slice.get(), part.data() + done, part.size() - done, 4096 + done);
    if (n < 0 && errno == EINTR)
      continue;
    require(n > 0, error("short pread"));
    done += static_cast<size_t>(n);
  }
  slice.close();
  require(part == blob.substr(4096, 8192), "random read mismatch");

  write_file(env.test_root / "empty.txt", "");
  expect_content(env.test_root / "empty.txt", "");
  write_file(env.test_root / "append.txt", "alpha");
  write_file(env.test_root / "append.txt", "-beta", O_APPEND);
  expect_content(env.test_root / "append.txt", "alpha-beta");
  write_file(env.test_root / "truncate.txt", "refill");
  Fd trunc(::open((env.test_root / "truncate.txt").c_str(), O_WRONLY));
  require(::ftruncate(trunc.get(), 3) == 0, error("truncate to nonzero length"));
  require(::fsync(trunc.get()) == 0, error("fsync truncate"));
  trunc.close();
  expect_content(env.test_root / "truncate.txt", "ref");
  require(::truncate((env.test_root / "truncate.txt").c_str(), 8) == 0, error("extend truncate"));
  expect_content(env.test_root / "truncate.txt", std::string("ref") + std::string(5, '\0'));
  require(::truncate((env.test_root / "truncate.txt").c_str(), 0) == 0, error("zero truncate"));
  expect_content(env.test_root / "truncate.txt", "");
  write_file(env.test_root / "truncate.txt", "refill");

  expect_open_failure(env.test_root / "missing.txt", O_RDONLY, ENOENT);
  expect_open_failure(env.test_root / "missing-dir/file", O_CREAT | O_WRONLY, ENOENT);
  expect_open_failure(env.test_root / "append.txt/child", O_CREAT | O_WRONLY, ENOTDIR);

  write_file(env.test_root / "random.bin", "abcdefghij");
  Fd random(::open((env.test_root / "random.bin").c_str(), O_WRONLY));
  write_all(random.get(), "XYZ", 4);
  require(::fsync(random.get()) == 0, error("fsync random"));
  // A checkpoint must not discard the active write session.
  write_all(random.get(), "12", 0);
  require(::fsync(random.get()) == 0, error("fsync after continued write"));
  random.close();
  expect_content(env.test_root / "random.bin", "12cdXYZhij");

  Fd sparse(::open((env.test_root / "sparse.bin").c_str(), O_CREAT | O_WRONLY, 0600));
  write_all(sparse.get(), "tail", 1024 * 1024 + 17);
  require(::fsync(sparse.get()) == 0, error("fsync sparse"));
  sparse.close();
  expect_content(env.test_root / "sparse.bin", std::string(1024 * 1024 + 17, '\0') + "tail");

  write_file(env.test_root / "shared.txt", "abcdefghij");
  Fd first(::open((env.test_root / "shared.txt").c_str(), O_WRONLY));
  Fd second(::open((env.test_root / "shared.txt").c_str(), O_WRONLY));
  write_all(first.get(), "XYZ", 0);
  write_all(second.get(), "123", 7);
  require(::fsync(first.get()) == 0, error("fsync shared first"));
  first.close();
  write_all(second.get(), "!", 6);
  require(::fsync(second.get()) == 0, error("fsync surviving handle"));
  second.close();
  expect_content(env.test_root / "shared.txt", "XYZdef!123");
}

static fs::path find_history() {
  for (const auto& entry : fs::directory_iterator(env.test_root)) {
    const auto name = entry.path().filename().string();
    if (name.rfind(".version_", 0) == 0 && entry.path().extension() == ".txt" &&
      read_file(entry.path()) == "version-one")
      return entry.path();
  }
  throw std::runtime_error("version-one history was not listed");
}

static void test_metadata_and_history() {
  const auto version = env.test_root / "version.txt";
  write_file(version, "version-one");
  write_file(version, "version-two");
  expect_content(version, "version-two");
  const auto history = find_history();
  expect_open_failure(history, O_WRONLY | O_TRUNC, EPERM);
  expect_failure(::chmod(history.c_str(), 0600), EPERM, "chmod history");
  timespec stamp[2]{{1700000000, 123000000}, {1700000001, 456000000}};
  expect_failure(::utimensat(AT_FDCWD, history.c_str(), stamp, 0), EPERM, "utimens history");
  expect_content(history, "version-one");

  const auto file = env.test_root / "private.txt";
  const auto dir = env.test_root / "private-dir";
  mode_t previous_umask = ::umask(0077);
  try {
    write_file(file, "secret");
    require(::mkdir(dir.c_str(), 0777) == 0, error("mkdir private"));
  } catch (...) {
    ::umask(previous_umask);
    throw;
  }
  ::umask(previous_umask);
  require((stat_file(file).st_mode & 0777) == 0600, "file umask");
  require((stat_file(dir).st_mode & 0777) == 0700, "directory umask");
  require(::chmod(file.c_str(), 0644) == 0 && ::chmod(dir.c_str(), 0755) == 0,
    error("chmod"));
  require(::chown(file.c_str(), ::getuid(), ::getgid()) == 0, error("chown self"));
  // Also protect metadata set while a dirty write handle remains open.
  Fd active(::open(file.c_str(), O_WRONLY));
  write_all(active.get(), "SECRET", 0);
  require(::utimensat(AT_FDCWD, file.c_str(), stamp, 0) == 0, error("utimens active"));
  require(::fsync(active.get()) == 0, error("fsync explicit timestamps"));
  active.close();
  auto st = stat_file(file);
  require(st.st_mtim.tv_sec == stamp[1].tv_sec && st.st_mtim.tv_nsec == stamp[1].tv_nsec,
    "write publication lost explicit mtime");
  expect_failure(::chmod((env.test_root / "missing.txt").c_str(), 0600), ENOENT, "chmod missing");
  expect_failure(::mkdir(dir.c_str(), 0755), EEXIST, "duplicate mkdir");
}

static void test_namespace() {
  fs::rename(env.test_root / "dir", env.test_root / "renamed-dir");
  expect_content(env.test_root / "renamed-dir/nested/hello.txt", "hello IoTDB-FS\n");
  fs::create_directory(env.test_root / "nonempty");
  write_file(env.test_root / "nonempty/file", "x");
  expect_failure(::rmdir((env.test_root / "nonempty").c_str()), ENOTEMPTY, "nonempty rmdir");
  require(::unlink((env.test_root / "nonempty/file").c_str()) == 0, error("unlink"));
  require(::rmdir((env.test_root / "nonempty").c_str()) == 0, error("rmdir"));
  write_file(env.test_root / "rename-source.txt", "persisted rename payload");
  fs::rename(env.test_root / "rename-source.txt", env.test_root / "rename-target.txt");
  expect_content(env.test_root / "rename-target.txt", "persisted rename payload");
  write_file(env.test_root / "replace-source.txt", "replacement payload");
  write_file(env.test_root / "replace-target.txt", "old payload");
  fs::rename(env.test_root / "replace-source.txt", env.test_root / "replace-target.txt");
  expect_content(env.test_root / "replace-target.txt", "replacement payload");
  expect_open_failure(env.test_root / "rename-source.txt", O_RDONLY, ENOENT);
  expect_open_failure(env.test_root / "replace-source.txt", O_RDONLY, ENOENT);
  write_file(env.test_root / "sample.grib2", "unrecognized bytes");
  expect_content(env.test_root / "sample.grib2", "unrecognized bytes");
  if (!env.cfg.grib_enabled) {
    expect_open_failure(env.test_root / "sample.grib2?function=read", O_RDONLY, ENOENT);
    char value[256];
    require(::getxattr((env.test_root / "sample.grib2").c_str(), "user.grib.shortName",
      value, sizeof(value)) == -1, "disabled GRIB xattr unexpectedly exists");
  }
}

static void test_web() {
  const auto end = Clock::now() + 20s;
  bool ready = false;
  while (Clock::now() < end) {
    try {
      ready = http_get("/api/list?path=%2F").rfind("HTTP/1.1 200 ", 0) == 0;
    } catch (const std::exception&) {}
    if (ready)
      break;
    std::this_thread::sleep_for(100ms);
  }
  require(ready, "Web readiness timeout");
  const auto list = http_get("/api/list?path=%2F");
  require(list.find("\"name\":\"sample.grib2\"") != std::string::npos,
    "Web list omitted test file");
  const auto preview = http_get("/api/preview?path=%2Fappend.txt");
  require(preview.rfind("HTTP/1.1 200 ", 0) == 0 &&
    preview.find("alpha-beta") != std::string::npos, "Web preview content");
  const auto index = http_get("/");
  require(index.find(std::string("gribExtensionEnabled: ") +
    (env.cfg.grib_enabled ? "true" : "false")) != std::string::npos,
    "Web GRIB feature flag disagrees with test configuration");
  if (!env.cfg.grib_enabled) {
    const auto partial = http_get("/api/preview?path=%2Fsample.grib2&read_query=function%3Dread");
    require(partial.rfind("HTTP/1.1 400 ", 0) == 0 &&
      partial.find("GRIB extension is disabled") != std::string::npos,
      "disabled GRIB Web query must be rejected");
  }
}

static void test_persistence() {
  const auto st = stat_file(env.test_root / "private.txt");
  require(st.st_atim.tv_sec == 1700000000 && st.st_atim.tv_nsec == 123000000,
    "atime changed across remount");
  require(st.st_mtim.tv_sec == 1700000001 && st.st_mtim.tv_nsec == 456000000,
    "mtime changed across remount");
  require((st.st_mode & 0777) == 0644 && st.st_uid == ::getuid() && st.st_gid == ::getgid(),
    "file permissions/ownership changed across remount");
  require((stat_file(env.test_root / "private-dir").st_mode & 0777) == 0755,
    "directory permissions changed across remount");
  expect_content(env.test_root / "private.txt", "SECRET");
  expect_content(env.test_root / "append.txt", "alpha-beta");
  expect_content(env.test_root / "random.bin", "12cdXYZhij");
  expect_content(env.test_root / "sparse.bin", std::string(1024 * 1024 + 17, '\0') + "tail");
  expect_content(env.test_root / "shared.txt", "XYZdef!123");
  expect_content(env.test_root / "truncate.txt", "refill");
  expect_content(env.test_root / "rename-target.txt", "persisted rename payload");
  expect_content(env.test_root / "replace-target.txt", "replacement payload");
  expect_content(env.test_root / "renamed-dir/nested/hello.txt", "hello IoTDB-FS\n");
  expect_content(env.test_root / "blob.bin", read_file(env.run_dir / "source.bin"));
  expect_content(env.test_root / "version.txt", "version-two");
  expect_content(find_history(), "version-one");
}

// Validate that an interrupted daemon cannot publish a partially written
// replacement over the last committed inode version.  The test deliberately
// leaves a tiny write unflushed, terminates the FUSE process, lazily detaches
// the mount, and verifies the committed bytes after a fresh mount.
static void test_crash_recovery() {
  const auto path = env.test_root / "crash-recovery.txt";
  write_file(path, "committed-before-crash");
  Fd dirty(::open(path.c_str(), O_WRONLY));
  write_all(dirty.get(), "uncommitted", 0);
  require(env.fs_pid > 0, "filesystem process is not running for crash test");
  require(::kill(env.fs_pid, SIGKILL) == 0, error("kill filesystem for crash test"));
  int status = 0;
  require(::waitpid(env.fs_pid, &status, 0) == env.fs_pid,
    error("wait filesystem after crash"));
  env.fs_pid = -1;
  // The open handle belongs to the dead FUSE session; close it locally and
  // tolerate the resulting I/O error from the kernel side of the mount.
  (void)::close(dirty.release_fd());
  if (mounted())
    unmount(true);
  start_filesystem();
  expect_content(path, "committed-before-crash");
}

static void remove_test_directory(const fs::path& directory) {
  for (;;) {
    fs::path child;
    {
      // Unlinking a current file also removes its virtual history entries.
      // Close this directory snapshot before mutation and enumerate afresh,
      // rather than interpreting stale history entries as deletion failures.
      fs::directory_iterator entry(directory);
      if (entry == fs::directory_iterator())
        break;
      child = entry->path();
    }
    struct stat st {};
    require(::lstat(child.c_str(), &st) == 0, error("cleanup lstat " + child.string()));
    if (S_ISDIR(st.st_mode))
      remove_test_directory(child);
    else
      require(::unlink(child.c_str()) == 0, error("cleanup unlink " + child.string()));
  }
  require(::rmdir(directory.c_str()) == 0, error("cleanup rmdir " + directory.string()));
}

static void remove_private_test_tree(bool allow_missing = false) {
  struct stat st {};
  const int rc = ::lstat(env.test_root.c_str(), &st);
  if (allow_missing && rc == -1 && errno == ENOENT)
    return;
  require(rc == 0 && S_ISDIR(st.st_mode), error("cleanup private test root"));
  remove_test_directory(env.test_root);
  expect_failure(::lstat(env.test_root.c_str(), &st), ENOENT,
    "private test root after cleanup");
}

static bool cleanup() {
  bool clean = true;
  auto attempt = [&](const std::function<void()>& action) {
    try { action(); } catch (const std::exception& e) {
      std::cerr << "[cleanup] " << e.what() << std::endl;
      clean = false;
    }
  };
  attempt([] { stop_process(env.web_pid); });
  attempt([] {
    if (mounted() && env.phase_pid <= 0 && !env.test_root.empty()) {
      phase("cleanup this run's private filesystem subtree", [] {
        remove_private_test_tree(true);
      }, true);
    }
  });
  attempt([] { unmount(); });
  attempt([] { stop_process(env.fs_pid); });
  attempt([] { if (mounted()) unmount(true); });
  attempt([] { stop_process(env.phase_pid); });
  if (!env.config.empty()) {
    std::error_code ec;
    fs::remove(env.config, ec);
    if (ec) {
      std::cerr << "[cleanup] cannot remove private credential file: " << ec.message() << std::endl;
      clean = false;
    }
  }
  return clean;
}

static int positive_integer(const std::string& value, const std::string& option, int maximum) {
  size_t used = 0;
  int number = std::stoi(value, &used);
  require(used == value.size() && number > 0 && number <= maximum,
    option + " is outside its valid range");
  return number;
}

static void parse_args(int argc, char** argv) {
  auto from_env = [](const char* name, auto& field) {
    if (const char* value = ::getenv(name))
      field = value;
  };
  from_env("IOTDB_FS_BIN", env.cfg.fs_bin);
  from_env("IOTDB_FS_WEB_BIN", env.cfg.web_bin);
  from_env("IOTDB_FS_TEST_WORK", env.cfg.work_dir);
  from_env("IOTDB_FS_TEST_DB", env.cfg.db);
  from_env("IOTDB_HOST", env.cfg.host);
  from_env("IOTDB_USER", env.cfg.user);
  from_env("IOTDB_PASS", env.cfg.pass);
  if (const char* value = ::getenv("IOTDB_PORT"))
    env.cfg.port = positive_integer(value, "IOTDB_PORT", 65535);
  if (const char* value = ::getenv("IOTDB_FS_TEST_WEB_PORT"))
    env.cfg.web_port = positive_integer(value, "IOTDB_FS_TEST_WEB_PORT", 65535);
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      std::cout << "Usage: " << argv[0]
        << " --fs-bin ABS_PATH --web-bin ABS_PATH --db TEST_DATABASE\n"
        << "  [--work-dir PARENT] [--host HOST] [--port PORT] [--user USER]\n"
        << "  [--grib-enabled true|false] [--pool-size N] [--web-port PORT]\n"
        << "  [--timeout-seconds N] [--skip-web]\n"
        << "Use IOTDB_PASS for the password, IOTDB_FS_TEST_DB for the test database.\n"
        << "A private run directory is created under PARENT; logs are retained.\n"
        << "Only this run's unique filesystem subtree is deleted; the database is never dropped.\n";
      std::exit(0);
    }
    if (arg == "--skip-web") {
      env.cfg.skip_web = true;
      continue;
    }
    require(i + 1 < argc, arg + " requires a value");
    std::string value = argv[++i];
    if (arg == "--fs-bin") env.cfg.fs_bin = value;
    else if (arg == "--web-bin") env.cfg.web_bin = value;
    else if (arg == "--work-dir") env.cfg.work_dir = value;
    else if (arg == "--db") env.cfg.db = value;
    else if (arg == "--host") env.cfg.host = value;
    else if (arg == "--user") env.cfg.user = value;
    else if (arg == "--port") env.cfg.port = positive_integer(value, arg, 65535);
    else if (arg == "--web-port") env.cfg.web_port = positive_integer(value, arg, 65535);
    else if (arg == "--pool-size") env.cfg.pool_size = positive_integer(value, arg, 64);
    else if (arg == "--timeout-seconds") env.cfg.timeout_seconds = positive_integer(value, arg, 3600);
    else if (arg == "--grib-enabled") {
      require(value == "true" || value == "false", "--grib-enabled expects true or false");
      env.cfg.grib_enabled = value == "true";
    } else throw std::runtime_error("unknown argument: " + arg);
  }
  require(!env.cfg.db.empty(), "select a test database with --db or IOTDB_FS_TEST_DB (no default)");
  require(std::all_of(env.cfg.db.begin(), env.cfg.db.end(), [](unsigned char ch) {
    return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
      (ch >= '0' && ch <= '9') || ch == '_';
  }), "test database must be an identifier containing only letters, digits, underscores");
  for (const auto& value : { env.cfg.host, env.cfg.user, env.cfg.pass, env.cfg.work_dir.string() })
    require(!value.empty() && value.find_first_of("\r\n") == std::string::npos,
      "configuration values must be nonempty single lines");
  auto binary = [](const fs::path& path) {
    require(path.is_absolute() && fs::is_regular_file(path) && ::access(path.c_str(), X_OK) == 0,
      "an executable absolute binary path is required: " + path.string());
  };
  binary(env.cfg.fs_bin);
  if (!env.cfg.skip_web)
    binary(env.cfg.web_bin);
}

static void prepare() {
  require(::access("/dev/fuse", R_OK | W_OK) == 0, error("/dev/fuse is required"));
  const char* path = ::getenv("PATH");
  std::istringstream dirs(path ? path : "");
  std::string dir;
  while (std::getline(dirs, dir, ':')) {
    fs::path candidate = fs::absolute(fs::path(dir) / "fusermount");
    if (::access(candidate.c_str(), X_OK) == 0) {
      env.fusermount = candidate;
      break;
    }
  }
  require(!env.fusermount.empty(), "fusermount is required for this FUSE 2 project");
  fs::create_directories(env.cfg.work_dir);
  env.cfg.work_dir = fs::canonical(env.cfg.work_dir);
  std::string pattern = (env.cfg.work_dir / "iotdb-fs-generic.XXXXXX").string();
  std::vector<char> buffer(pattern.begin(), pattern.end());
  buffer.push_back('\0');
  require(::mkdtemp(buffer.data()) != nullptr, error("mkdtemp"));
  env.run_dir = buffer.data();
  env.mount_dir = env.run_dir / "mnt";
  env.test_root = env.mount_dir / (".generic_" + env.run_dir.filename().string());
  env.config = env.run_dir / "config.properties";
  require(fs::create_directory(env.mount_dir), "create private mount directory");
  std::ostringstream config;
  config << "iotdb.host=" << env.cfg.host << "\niotdb.port=" << env.cfg.port
    << "\niotdb.user=" << env.cfg.user << "\niotdb.pass=" << env.cfg.pass
    << "\niotdb.db=" << env.cfg.db << "\niotdb.pool_size=" << env.cfg.pool_size
    << "\ngrib.extension.enabled=" << (env.cfg.grib_enabled ? "true" : "false")
    << "\nlog.dir=" << (env.run_dir / "logs").string() << "\nlog.level=INFO"
    << "\nmount.point=" << env.mount_dir.string()
    << "\ncache.read.chunk_size=256KB\ncache.read.full_prefetch_max_bytes=4MB"
    << "\ncache.raw_file.max_bytes=16MB\nwriter.db_request_max_bytes=512KB"
    << "\ninode.cache.capacity=1024\ninode.cache.flush_interval_ms=200\n";
  write_file(env.config, config.str(), O_EXCL, 0600);
  env.deadline = Clock::now() + std::chrono::seconds(env.cfg.timeout_seconds);
  std::cout << "[test] artifacts: " << env.run_dir << "\n[test] database=" << env.cfg.db
    << " grib=" << (env.cfg.grib_enabled ? "true" : "false")
    << " pool=" << env.cfg.pool_size << std::endl;
}

int main(int argc, char** argv) {
  ::signal(SIGTERM, on_signal);
  ::signal(SIGINT, on_signal);
  ::signal(SIGPIPE, SIG_IGN);
  bool passed = false;
  try {
    parse_args(argc, argv);
    prepare();
    start_filesystem();
    phase("ordinary files, append, truncate, random/sparse/shared writes", test_files);
    phase("immutable history, permissions and explicit timestamps", test_metadata_and_history);
    phase("directory and overwrite renames, GRIB feature switch", test_namespace);
    std::cout << "[test] crash recovery keeps last committed bytes" << std::endl;
    test_crash_recovery();
    pid_t grep = spawn(env.cfg.fs_bin, { "grep", "--help" }, "grep-feature-switch");
    require(wait_bounded(grep, 15s) == (env.cfg.grib_enabled ? 0 : 2),
      "grep command does not follow the GRIB feature switch");
    if (!env.cfg.skip_web) {
      env.web_pid = spawn(env.cfg.web_bin,
        { "--root", env.test_root.string(), "--host", "127.0.0.1",
          "--port", std::to_string(env.cfg.web_port), "--workers", "2" }, "web");
      phase("HTTP listing, preview and GRIB feature switch", test_web);
      require(!reap(env.web_pid), "owned Web server exited (possible port collision)");
    } else {
      std::cout << "[test] Web explicitly skipped with --skip-web" << std::endl;
    }
    stop_process(env.web_pid);
    unmount();
    require(wait_bounded(env.fs_pid, 10s) == 0, "filesystem did not shut down cleanly");
    start_filesystem();
    phase("content, history, permissions and times after remount", test_persistence);
    phase("delete only this run's private filesystem subtree", [] {
      remove_private_test_tree();
    }, true);
    passed = true;
  } catch (const std::exception& e) {
    std::cerr << "[FAIL] " << e.what() << std::endl;
  }
  const bool cleaned = cleanup();
  if (!env.run_dir.empty())
    std::cout << "[test] retained logs: " << env.run_dir << std::endl;
  if (!passed)
    std::cerr << "[test] failed-run data may remain only under " << env.test_root << std::endl;
  if (passed && cleaned)
    std::cout << "[PASS] generic filesystem regression" << std::endl;
  return passed && cleaned ? 0 : 1;
}
