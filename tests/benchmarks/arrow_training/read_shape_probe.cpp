// Isolate bytes and READ_OBJECT column count on one immutable published blob.
// Usage: arrow_read_shape_probe CONFIG PROTOCOL_PATH VERSION SOURCE_SIZE CYCLES
// Output is one CSV observation per successful read; no credentials are printed.
#include "iotdbfs/IoTDBConnector.h"
#include "iotdbfs/IoTDBStructuredBlobBackend.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <map>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
std::string trim(std::string text) {
  const auto first = text.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) return {};
  const auto last = text.find_last_not_of(" \t\r\n");
  return text.substr(first, last - first + 1);
}
std::map<std::string, std::string> properties(const char* path) {
  std::ifstream file(path);
  if (!file) throw std::runtime_error("cannot open config file");
  std::map<std::string, std::string> values;
  std::string line;
  while (std::getline(file, line)) {
    line = trim(line);
    if (line.empty() || line[0] == '#') continue;
    const auto equal = line.find('=');
    if (equal != std::string::npos)
      values[trim(line.substr(0, equal))] = trim(line.substr(equal + 1));
  }
  return values;
}
std::string required(const std::map<std::string, std::string>& values,
    const char* key) {
  auto it = values.find(key);
  if (it == values.end() || it->second.empty())
    throw std::runtime_error(std::string("missing config property ") + key);
  return it->second;
}
std::string setting(const std::map<std::string, std::string>& values,
    const char* key, const char* environment, const char* fallback = nullptr) {
  auto it = values.find(key);
  if (it != values.end() && !it->second.empty()) return it->second;
  if (const char* value = std::getenv(environment); value && *value)
    return value;
  if (fallback) return fallback;
  throw std::runtime_error(std::string("missing setting ") + key);
}
struct Shape {
  const char* family;
  std::uint64_t bytes;
  std::size_t ranges;
  bool adjacent{false};
};
std::vector<Shape> shapes() {
  return {{"fixed_bytes", 1048576, 1}, {"fixed_bytes", 1048576, 4},
          {"fixed_bytes", 1048576, 16}, {"fixed_bytes", 1048576, 64},
          {"fixed_ranges", 65536, 16}, {"fixed_ranges", 262144, 16},
          {"fixed_ranges", 4194304, 16},
          {"small_bytes", 65536, 1}, {"small_bytes", 65536, 64},
          {"adjacent_fixed_bytes", 1048576, 1, true},
          {"adjacent_fixed_bytes", 1048576, 4, true},
          {"adjacent_fixed_bytes", 1048576, 16, true},
          {"adjacent_fixed_bytes", 1048576, 64, true}};
}
} // namespace

int main(int argc, char** argv) {
  try {
    if (argc != 6)
      throw std::runtime_error("usage: arrow_read_shape_probe CONFIG PROTOCOL_PATH VERSION SOURCE_SIZE CYCLES");
    const auto props = properties(argv[1]);
    IoTDBConfig config;
    config.host = setting(props, "iotdb.host", "IOTDB_HOST", "127.0.0.1");
    config.port = std::stoi(setting(props, "iotdb.port", "IOTDB_PORT", "6667"));
    config.user = setting(props, "iotdb.user", "IOTDB_USER", "root");
    auto pass = props.find("iotdb.pass");
    config.pass = pass != props.end() ? pass->second :
      setting(props, "iotdb.password", "IOTDB_PASS");
    config.db = required(props, "iotdb.db");
    IoTDBConnector::instance().initialize(config, 1);
    const iotdbfs::StructuredBlobVersionKey key{argv[2], std::stoll(argv[3])};
    const auto source_size = std::stoull(argv[4]);
    const int cycles = std::stoi(argv[5]);
    if (cycles < 1 || cycles > 1000 || source_size < 16ULL * 1024 * 1024)
      throw std::runtime_error("invalid cycles or source size");
    iotdbfs::IoTDBStructuredBlobBackend backend;
    auto manifest = backend.load_manifest(key);
    if (!manifest.ok() || manifest.value().payload_size != source_size ||
        !manifest.value().published)
      throw std::runtime_error("exact published source version or size mismatch");
    auto cases = shapes();
    std::mt19937 random(20260929);
    std::cout << "cycle,family,bytes,ranges,nanoseconds\n";
    for (int cycle = -1; cycle < cycles; ++cycle) {
      std::shuffle(cases.begin(), cases.end(), random);
      for (const auto& shape : cases) {
        iotdbfs::StructuredBlobPayloadRanges ranges;
        const auto length = shape.bytes / shape.ranges;
        for (std::size_t i = 0; i < shape.ranges; ++i)
          ranges.push_back({1048576 + i * (length +
            (shape.adjacent ? 0 : 65536)), length});
        if (ranges.back().offset + ranges.back().length > source_size)
          throw std::runtime_error("probe range exceeds exact source version");
        const auto start = std::chrono::steady_clock::now();
        auto result = backend.read_payload_ranges(key, ranges);
        const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - start).count();
        if (!result.ok() || result.value().size() != shape.ranges)
          throw std::runtime_error("payload read failed or returned short range batch");
        if (cycle >= 0)
          std::cout << cycle << ',' << shape.family << ',' << shape.bytes << ','
                    << shape.ranges << ',' << elapsed << '\n';
      }
    }
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
