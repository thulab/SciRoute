// Experiment-only, read-only transport for an immutable published Arrow blob.
// The parent supplies an inherited Unix socketpair endpoint; this is not a
// listening service and performs no authentication or production route checks.
// Manifest validation occurs ONCE at startup. Timed reads exclude that setup,
// query planning, authorization, and IPC encoding; report this boundary clearly.
#include "iotdbfs/IoTDBConnector.h"
#include "iotdbfs/IoTDBStructuredBlobBackend.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>

namespace {
using Clock = std::chrono::steady_clock;
constexpr std::uint32_t kMaxHeader = 64 * 1024;
// The current backend's one-call ceiling is 8 MiB, below the experiment's
// 128 MiB safety ceiling. Never silently split a request into multiple calls.
constexpr std::uint64_t kMaxBytes = iotdbfs::kMaxPayloadBytesPerBatch;
static_assert(kMaxBytes <= 128ULL * 1024 * 1024);

void require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}
std::string trim(const std::string& text) {
  const auto first = text.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) return {};
  return text.substr(first, text.find_last_not_of(" \t\r\n") - first + 1);
}
std::map<std::string, std::string> properties(const std::string& path) {
  std::ifstream file(path);
  require(bool(file), "cannot open config file");
  std::map<std::string, std::string> values;
  for (std::string line; std::getline(file, line);) {
    line = trim(line);
    if (line.empty() || line[0] == '#') continue;
    const auto equal = line.find('=');
    if (equal != std::string::npos)
      values[trim(line.substr(0, equal))] = trim(line.substr(equal + 1));
  }
  return values;
}
std::string setting(const std::map<std::string, std::string>& props,
                    const char* key, const char* env,
                    const char* fallback = nullptr) {
  const auto found = props.find(key);
  if (found != props.end() && !found->second.empty()) return found->second;
  if (env) if (const auto value = std::getenv(env); value && *value) return value;
  if (fallback) return fallback;
  throw std::runtime_error(std::string("missing config setting: ") + key);
}
std::uint64_t number(const std::string& text) {
  require(!text.empty(), "empty unsigned integer");
  std::uint64_t result = 0;
  for (const unsigned char c : text) {
    require(c >= '0' && c <= '9', "invalid unsigned integer");
    const auto digit = static_cast<std::uint64_t>(c - '0');
    require(result <= (std::numeric_limits<std::uint64_t>::max() - digit) / 10,
            "unsigned integer overflow");
    result = result * 10 + digit;
  }
  return result;
}
std::string quote(const std::string& value) {
  const char* hex = "0123456789abcdef";
  std::string output = "\"";
  for (const unsigned char c : value) {
    if (c == '"' || c == '\\') { output += '\\'; output += c; }
    else if (c < 32) { output += "\\u00"; output += hex[c >> 4]; output += hex[c & 15]; }
    else output += c;
  }
  return output + '"';
}
// False means clean EOF before any byte; a partial frame is an error.
bool read_exact(int fd, void* target, std::size_t size) {
  auto* bytes = static_cast<char*>(target);
  std::size_t done = 0;
  while (done < size) {
    const auto count = ::read(fd, bytes + done, size - done);
    if (count < 0 && errno == EINTR) continue;
    require(count >= 0, "socket read failed");
    if (count == 0) {
      require(done == 0, "truncated socket frame");
      return false;
    }
    done += static_cast<std::size_t>(count);
  }
  return true;
}
void write_exact(int fd, const void* source, std::size_t size) {
  const auto* bytes = static_cast<const char*>(source);
  std::size_t done = 0;
  while (done < size) {
    const auto count = ::write(fd, bytes + done, size - done);
    if (count < 0 && errno == EINTR) continue;
    require(count > 0, "socket write failed");
    done += static_cast<std::size_t>(count);
  }
}
void send_json(int fd, const std::string& json) {
  require(json.size() <= kMaxHeader, "response header too large");
  const auto size = htonl(static_cast<std::uint32_t>(json.size()));
  write_exact(fd, &size, sizeof(size));
  write_exact(fd, json.data(), json.size());
}
std::string error_json(const char* error) {
  return "{\"ok\":false,\"error\":" + quote(error) + "}";
}
iotdbfs::StructuredBlobPayloadRanges parse_ranges(
    const std::string& json, std::uint64_t source_size) {
  boost::property_tree::ptree root;
  std::istringstream input(json);
  boost::property_tree::read_json(input, root);
  require(root.size() == 1 && root.front().first == "ranges" && root.data().empty(),
          "request must contain only ranges");
  const auto& array = root.front().second;
  require(array.data().empty() && !array.empty() &&
          array.size() <= iotdbfs::kMaxPayloadRangesPerBatch,
          "range count must be 1 through 64");
  iotdbfs::StructuredBlobPayloadRanges ranges;
  std::uint64_t total = 0;
  for (const auto& item : array) {
    require(item.first.empty() && item.second.size() == 2 && item.second.data().empty(),
            "each range must contain offset and length");
    bool have_offset = false, have_length = false;
    iotdbfs::StructuredBlobPayloadRange range;
    for (const auto& field : item.second) {
      require(field.second.empty(), "range fields must be unsigned integers");
      if (field.first == "offset" && !have_offset) {
        range.offset = number(field.second.data()); have_offset = true;
      } else if (field.first == "length" && !have_length) {
        range.length = number(field.second.data()); have_length = true;
      } else throw std::runtime_error("unknown or duplicate range field");
    }
    require(have_offset && have_length && range.length > 0 &&
            range.offset <= source_size && range.length <= source_size - range.offset,
            "range exceeds immutable source bounds or is empty");
    require(range.length <= kMaxBytes - total, "range batch exceeds backend byte limit");
    total += range.length;
    ranges.push_back(range);
  }
  return ranges;
}
} // namespace

int main(int argc, char** argv) {
  int fd = -1;
  std::signal(SIGPIPE, SIG_IGN);
  try {
    std::map<std::string, std::string> args;
    for (int i = 1; i < argc; i += 2) {
      require(i + 1 < argc, "every option requires a value");
      require(args.emplace(argv[i], argv[i + 1]).second, "duplicate option");
    }
    require(args.size() == 5 && args.count("--config") && args.count("--logical") &&
            args.count("--version") && args.count("--size") && args.count("--fd"),
            "usage: arrow_derived_range_bridge --config FILE --logical PATH --version N --size N --fd N");
    const auto descriptor = number(args.at("--fd"));
    require(descriptor <= std::numeric_limits<int>::max(), "invalid socket fd");
    fd = static_cast<int>(descriptor);
    sockaddr_storage address{}; socklen_t address_size = sizeof(address);
    require(::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &address_size) == 0 &&
            address.ss_family == AF_UNIX, "fd must be a Unix socketpair endpoint");
    const auto version = number(args.at("--version"));
    const auto source_size = number(args.at("--size"));
    require(version <= std::numeric_limits<std::int64_t>::max() && source_size > 0,
            "invalid version or source size");
    const auto setup_start = Clock::now();
    const auto props = properties(args.at("--config"));
    IoTDBConfig config;
    config.host = setting(props, "iotdb.host", "IOTDB_HOST", "127.0.0.1");
    config.port = std::stoi(setting(props, "iotdb.port", "IOTDB_PORT", "6667"));
    config.user = setting(props, "iotdb.user", "IOTDB_USER", "root");
    config.pass = props.count("iotdb.pass") ? props.at("iotdb.pass") :
      setting(props, "iotdb.password", "IOTDB_PASS");
    config.db = setting(props, "iotdb.db", nullptr);
    IoTDBConnector::instance().initialize(config, 1);
    const iotdbfs::StructuredBlobVersionKey key{
      args.at("--logical"), static_cast<std::int64_t>(version)};
    iotdbfs::IoTDBStructuredBlobBackend backend;
    const auto loaded = backend.load_manifest(key);
    require(loaded.ok(), "cannot load exact manifest");
    const auto& manifest = loaded.value();
    require(manifest.key == key && manifest.published &&
            manifest.phase == iotdbfs::StructuredBlobPhase::Published &&
            manifest.format_id == std::optional<std::string>{"arrow"} &&
            manifest.payload_size == source_size,
            "exact published Arrow version or size mismatch");
    const auto setup_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      Clock::now() - setup_start).count();
    send_json(fd, "{\"ok\":true,\"kind\":\"ready\",\"revision\":" +
      std::to_string(manifest.revision) + ",\"layout_size\":" +
      std::to_string(manifest.layout_size) + ",\"payload_size\":" +
      std::to_string(source_size) + ",\"setup_ns\":" + std::to_string(setup_ns) +
      ",\"max_ranges\":64,\"max_bytes\":" + std::to_string(kMaxBytes) +
      ",\"validation\":\"once_at_startup\"}");
    while (true) {
      std::uint32_t frame_size = 0;
      if (!read_exact(fd, &frame_size, sizeof(frame_size))) break;
      frame_size = ntohl(frame_size);
      require(frame_size > 0 && frame_size <= kMaxHeader, "invalid request header size");
      std::string json(frame_size, '\0');
      require(read_exact(fd, json.data(), json.size()), "missing request header");
      iotdbfs::StructuredBlobPayloadRanges ranges;
      try { ranges = parse_ranges(json, source_size); }
      catch (const std::exception&) {
        send_json(fd, error_json("invalid ranges request or exceeded bounds"));
        continue;
      }
      const auto start = Clock::now();
      const auto result = backend.read_payload_ranges(key, ranges);
      const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
        Clock::now() - start).count();
      if (!result.ok() || result.value().size() != ranges.size()) {
        send_json(fd, error_json("payload range read failed")); continue;
      }
      bool sizes_match = true;
      for (std::size_t i = 0; i < ranges.size(); ++i)
        sizes_match &= result.value()[i].size() == ranges[i].length;
      if (!sizes_match) {
        send_json(fd, error_json("payload range read was short")); continue;
      }
      std::uint64_t total = 0;
      std::ostringstream response;
      response << "{\"ok\":true,\"read_ns\":" << elapsed
               << ",\"ranges\":" << ranges.size() << ",\"lengths\":[";
      for (std::size_t i = 0; i < ranges.size(); ++i) {
        if (i) response << ',';
        response << ranges[i].length; total += ranges[i].length;
      }
      response << "],\"bytes\":" << total << '}';
      send_json(fd, response.str());
      for (const auto& bytes : result.value())
        write_exact(fd, bytes.data(), bytes.size());
    }
    ::close(fd);
    return 0;
  } catch (const std::exception& error) {
    if (fd >= 0) {
      try { send_json(fd, error_json(error.what())); } catch (...) {}
      ::close(fd);
    }
    std::cerr << "derived range bridge: " << error.what() << '\n';
    return 1;
  }
}
