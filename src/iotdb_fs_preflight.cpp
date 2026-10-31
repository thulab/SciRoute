#include "iotdbfs/EnvironmentPreflight.h"
#include "iotdbfs/IoTDBClient.h"
#include "iotdbfs/Logging.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace fs = std::filesystem;
using iotdbfs::preflight::CheckResult;
using iotdbfs::preflight::CheckStatus;

namespace {

struct Options {
  std::string host = "127.0.0.1";
  int port = 6667;
  std::string user = "root";
  std::string pass = "root";
  std::string db;
  fs::path fs_bin;
  fs::path web_bin;
  int timeout_ms = 1500;
};

void usage(const char* name) {
  std::cout << "Usage: " << name << " --db DATABASE --fs-bin PATH --web-bin PATH\n"
    << "  [--host HOST] [--port PORT] [--user USER] [--pass PASS]\n"
    << "  [--timeout-ms N]\n"
    << "Environment: IOTDB_HOST, IOTDB_PORT, IOTDB_USER, IOTDB_PASS,\n"
    << "  IOTDB_FS_TEST_DB, IOTDB_FS_BIN, IOTDB_FS_WEB_BIN\n"
    << "Each check emits one JSON object; exit 0 means all checks passed.\n";
}

[[noreturn]] void argument_error(const std::string& message) {
  throw std::invalid_argument(message);
}

int parse_positive(const std::string& value, const std::string& option,
  int maximum) {
  if (value.empty())
    argument_error(option + " must be a positive integer");
  size_t used = 0;
  long parsed = 0;
  try {
    parsed = std::stol(value, &used);
  }
  catch (...) {
    argument_error(option + " must be a positive integer");
  }
  if (used != value.size() || parsed <= 0 || parsed > maximum)
    argument_error(option + " must be in [1, " + std::to_string(maximum) + "]");
  return static_cast<int>(parsed);
}

void env_value(const char* name, std::string& target) {
  if (const char* value = std::getenv(name))
    target = value;
}

Options parse_options(int argc, char** argv) {
  Options options;
  env_value("IOTDB_HOST", options.host);
  env_value("IOTDB_USER", options.user);
  env_value("IOTDB_PASS", options.pass);
  env_value("IOTDB_FS_TEST_DB", options.db);
  std::string port;
  env_value("IOTDB_PORT", port);
  if (!port.empty()) options.port = parse_positive(port, "IOTDB_PORT", 65535);
  std::string fs_bin;
  env_value("IOTDB_FS_BIN", fs_bin);
  options.fs_bin = fs_bin;
  std::string web_bin;
  env_value("IOTDB_FS_WEB_BIN", web_bin);
  options.web_bin = web_bin;

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      usage(argv[0]);
      std::exit(0);
    }
    if (i + 1 >= argc)
      argument_error(arg + " requires a value");
    const std::string value = argv[++i];
    if (arg == "--host") options.host = value;
    else if (arg == "--port") options.port = parse_positive(value, arg, 65535);
    else if (arg == "--user") options.user = value;
    else if (arg == "--pass") options.pass = value;
    else if (arg == "--db") options.db = value;
    else if (arg == "--fs-bin") options.fs_bin = value;
    else if (arg == "--web-bin") options.web_bin = value;
    else if (arg == "--timeout-ms") options.timeout_ms = parse_positive(value, arg, 60000);
    else argument_error("unknown option: " + arg);
  }
  if (options.host.empty() || options.user.empty() || options.pass.empty())
    argument_error("host, user and pass must be nonempty");
  if (!iotdbfs::preflight::is_valid_identifier(options.db))
    argument_error("--db must start with a letter and contain only ASCII letters, digits and underscores");
  if (options.fs_bin.empty() || options.web_bin.empty())
    argument_error("--fs-bin and --web-bin are required (or set IOTDB_FS_BIN/IOTDB_FS_WEB_BIN)");
  return options;
}

CheckResult pass(const std::string& check, const std::string& code,
  const std::string& message) {
  return {CheckStatus::Pass, check, code, message};
}

CheckResult fail(const std::string& check, const std::string& code,
  const std::string& message) {
  return {CheckStatus::Fail, check, code, message};
}

CheckResult skip(const std::string& check, const std::string& code,
  const std::string& message) {
  return {CheckStatus::Skip, check, code, message};
}

void emit(const CheckResult& result) {
  std::cout << iotdbfs::preflight::to_json(result) << '\n';
  std::cout.flush();
}

CheckResult check_port(const Options& options) {
  addrinfo hints{};
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_family = AF_UNSPEC;
  addrinfo* addresses = nullptr;
  const std::string service = std::to_string(options.port);
  const int lookup = ::getaddrinfo(options.host.c_str(), service.c_str(),
    &hints, &addresses);
  if (lookup != 0)
    return fail("iotdb_port", "IOTDB_HOST_RESOLUTION_FAILED",
      std::string("cannot resolve ") + options.host + ":" + service +
      " (" + gai_strerror(lookup) + ")");

  std::string detail;
  bool connected = false;
  for (addrinfo* address = addresses; address; address = address->ai_next) {
    const int fd = ::socket(address->ai_family, address->ai_socktype,
      address->ai_protocol);
    if (fd < 0) continue;
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
      detail = std::strerror(errno);
      ::close(fd);
      continue;
    }
    int rc = ::connect(fd, address->ai_addr, address->ai_addrlen);
    if (rc < 0 && errno == EINPROGRESS) {
      pollfd pfd{fd, POLLOUT, 0};
      rc = ::poll(&pfd, 1, options.timeout_ms);
      if (rc > 0) {
        int error = 0;
        socklen_t size = sizeof(error);
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &size) == 0) {
          if (error == 0) rc = 0;
          else { rc = -1; detail = std::strerror(error); }
        } else { rc = -1; detail = std::strerror(errno); }
      } else if (rc == 0) {
        detail = "timed out after " + std::to_string(options.timeout_ms) + " ms";
        rc = -1;
      } else {
        detail = std::strerror(errno);
      }
    } else if (rc < 0) {
      detail = std::strerror(errno);
    }
    ::close(fd);
    if (rc == 0) { connected = true; break; }
  }
  ::freeaddrinfo(addresses);
  if (connected)
    return pass("iotdb_port", "IOTDB_PORT_REACHABLE",
      options.host + ":" + service + " accepted a TCP connection");
  return fail("iotdb_port", "IOTDB_PORT_UNREACHABLE",
    options.host + ":" + service + " is unreachable (" + detail + ")");
}

CheckResult check_fuse_device() {
#ifdef __linux__
  struct stat st{};
  if (::stat("/dev/fuse", &st) != 0)
    return fail("fuse_device", "FUSE_DEVICE_MISSING",
      std::string("/dev/fuse is unavailable: ") + std::strerror(errno));
  if (!S_ISCHR(st.st_mode))
    return fail("fuse_device", "FUSE_DEVICE_NOT_CHARACTER_DEVICE",
      "/dev/fuse exists but is not a character device");
  if (::access("/dev/fuse", R_OK | W_OK) != 0)
    return fail("fuse_device", "FUSE_DEVICE_INACCESSIBLE",
      std::string("/dev/fuse is not readable and writable: ") + std::strerror(errno));
  return pass("fuse_device", "FUSE_DEVICE_READY", "/dev/fuse is readable and writable");
#else
  return skip("fuse_device", "FUSE_DEVICE_PLATFORM_UNSUPPORTED",
    "the Linux /dev/fuse check is unavailable on this platform");
#endif
}

std::optional<fs::path> find_command(const std::string& name) {
  const char* path = std::getenv("PATH");
  std::istringstream dirs(path ? path : "");
  std::string dir;
  while (std::getline(dirs, dir, ':')) {
    if (dir.empty()) dir = ".";
    fs::path candidate = fs::path(dir) / name;
    if (::access(candidate.c_str(), X_OK) == 0)
      return fs::absolute(candidate);
  }
  return std::nullopt;
}

CheckResult check_fusermount() {
  const auto command = find_command("fusermount");
  if (!command)
    return fail("fusermount", "FUSERMOUNT_MISSING",
      "fusermount is not executable in PATH; this project requires FUSE 2 tooling");
  return pass("fusermount", "FUSERMOUNT_READY", command->string());
}

CheckResult check_binary(const std::string& name, const fs::path& path) {
  if (!path.is_absolute())
    return fail(name, "BINARY_PATH_NOT_ABSOLUTE", path.string());
  if (!fs::is_regular_file(path))
    return fail(name, "BINARY_MISSING", path.string());
  if (::access(path.c_str(), X_OK) != 0)
    return fail(name, "BINARY_NOT_EXECUTABLE", path.string());
  return pass(name, "BINARY_READY", path.string());
}

std::string shell_quote(const std::string& value) {
  std::string quoted = "'";
  for (char c : value) {
    if (c == '\'') quoted += "'\\''";
    else quoted += c;
  }
  return quoted + "'";
}

CheckResult check_dynamic_dependencies(const std::string& name, const fs::path& path) {
  if (!path.is_absolute() || !fs::is_regular_file(path) ||
      ::access(path.c_str(), X_OK) != 0)
    return skip(name, "DEPENDENCY_NOT_CHECKED_BINARY_INVALID",
      "runtime dependencies were not inspected because the executable check failed");
  if (!find_command("ldd"))
    return fail(name, "DEPENDENCY_TOOL_MISSING", "ldd is required to inspect runtime dependencies");
  const std::string command = "ldd " + shell_quote(path.string()) + " 2>&1";
  FILE* pipe = ::popen(command.c_str(), "r");
  if (!pipe)
    return fail(name, "DEPENDENCY_INSPECTION_FAILED", std::strerror(errno));
  std::string output;
  char buffer[512];
  while (std::fgets(buffer, sizeof(buffer), pipe)) output += buffer;
  const int status = ::pclose(pipe);
  if (output.find("not found") != std::string::npos)
    return fail(name, "DEPENDENCY_MISSING", "ldd reported an unresolved library");
  if (status != 0 && output.find("not a dynamic executable") == std::string::npos)
    return fail(name, "DEPENDENCY_INSPECTION_FAILED", "ldd could not inspect the executable");
  return pass(name, "DEPENDENCIES_READY", "all linked runtime libraries are resolved");
}

CheckResult check_iotdb(const Options& options) {
  IoTDBClientConfig config;
  config.host = options.host;
  config.port = options.port;
  config.user = options.user;
  config.pass = options.pass;
  config.db = options.db;
  config.auto_create_db = true;
  config.auto_use_db = true;
  config.acquire_timeout = std::chrono::milliseconds(options.timeout_ms);
  class SilenceSdkStderr {
  public:
    SilenceSdkStderr() : saved_(::dup(STDERR_FILENO)), saved_stdout_(::dup(STDOUT_FILENO)),
      cout_state_(std::cout.rdstate()) {
      const int null_fd = ::open("/dev/null", O_WRONLY);
      if (saved_ >= 0 && saved_stdout_ >= 0 && null_fd >= 0) {
        ::dup2(null_fd, STDERR_FILENO);
        ::dup2(null_fd, STDOUT_FILENO);
        ::close(null_fd);
        std::cout.setstate(std::ios::failbit);
        active_ = true;
      } else if (null_fd >= 0) {
        ::close(null_fd);
        if (saved_ >= 0) ::close(saved_);
        if (saved_stdout_ >= 0) ::close(saved_stdout_);
        saved_ = -1;
        saved_stdout_ = -1;
      }
    }
    ~SilenceSdkStderr() {
      if (active_) {
        // The SDK emits diagnostics through std::cout in addition to stderr;
        // flush while descriptors still point at /dev/null so JSON remains
        // the only stdout protocol produced by this executable.
        std::cout.flush();
        std::fflush(stdout);
        ::dup2(saved_, STDERR_FILENO);
        ::dup2(saved_stdout_, STDOUT_FILENO);
        ::close(saved_);
        ::close(saved_stdout_);
        std::cout.clear(cout_state_);
      }
    }
    SilenceSdkStderr(const SilenceSdkStderr&) = delete;
    SilenceSdkStderr& operator=(const SilenceSdkStderr&) = delete;
  private:
    int saved_ = -1;
    int saved_stdout_ = -1;
    std::ios::iostate cout_state_{};
    bool active_ = false;
  } silence_sdk_stderr;
  try {
    IoTDBClient client(config, 1);
    const auto unique_id = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
    const std::string table = "preflight_" + std::to_string(::getpid()) + "_" +
      std::to_string(unique_id);
    bool table_created = false;
    const auto cleanup = [&] {
      if (table_created)
        (void)client.executeNonQuery("DROP TABLE " + table);
    };
    if (!client.executeNonQuery("CREATE TABLE " + table +
        " (value INT64 FIELD)"))
      return fail("iotdb_schema", "IOTDB_SCHEMA_REGION_UNAVAILABLE",
        "CREATE TABLE probe failed; schema region may be unready");
    table_created = true;
    const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
    if (!client.executeNonQuery("INSERT INTO " + table +
        " (time, value) VALUES (" + std::to_string(now) + ", 1)")) {
      cleanup();
      return fail("iotdb_schema", "IOTDB_SCHEMA_REGION_UNAVAILABLE",
        "INSERT probe failed; schema region may be unready");
    }
    auto result = client.executeQuery("SELECT value FROM " + table);
    if (!result) {
      cleanup();
      return fail("iotdb_schema", "IOTDB_SCHEMA_REGION_UNAVAILABLE",
        "SELECT probe failed; schema region may be unready");
    }
    cleanup();
    return pass("iotdb_schema", "IOTDB_SCHEMA_REGION_READY",
      "credentials accepted and CREATE/INSERT/SELECT probe succeeded");
  }
  catch (const std::exception& error) {
    const std::string code = iotdbfs::preflight::classify_iotdb_error(error.what());
    return fail("iotdb_schema", code,
      "IoTDB readiness check failed: " + std::string(error.what()));
  }
  catch (...) {
    return fail("iotdb_schema", "IOTDB_CHECK_FAILED",
      "IoTDB readiness check failed with an unknown exception");
  }
}

} // namespace

int main(int argc, char** argv) {
  set_logging_level(LogLevel::OFF);
  try {
    const Options options = parse_options(argc, argv);
    const std::vector<CheckResult> results{
      check_port(options),
      check_fuse_device(),
      check_fusermount(),
      check_binary("fs_binary", options.fs_bin),
      check_binary("web_binary", options.web_bin),
      check_dynamic_dependencies("fs_dependencies", options.fs_bin),
      check_dynamic_dependencies("web_dependencies", options.web_bin),
      check_iotdb(options),
    };
    bool failed = false;
    for (const auto& result : results) {
      emit(result);
      failed = failed || result.status == CheckStatus::Fail;
    }
    return failed ? 1 : 0;
  }
  catch (const std::exception& error) {
    emit(fail("config", "INVALID_ARGUMENT", error.what()));
    return 2;
  }
}
