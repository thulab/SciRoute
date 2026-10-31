#pragma once

#include "iotdbfs/InodeTypes.h"
#include "iotdbfs/MetadataConfig.h"

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace iotdbfs {

enum class GribVirtualType { None, Binary, Data };

struct GribQuerySpec {
  bool ok{ false };
  bool is_function_chain{ false };
  GribVirtualType type{ GribVirtualType::None };
  std::map<std::string, std::string> kv_all;
  std::map<std::string, std::string> kv_idx;
};

struct GribVirtualAttr {
  bool ok{ false };
  uint64_t size{ 0 };
};

struct GribBinaryView {
  uint64_t offset{ 0 };
  uint64_t length{ 0 };
};

struct GribDataView {
  std::vector<uint8_t> bytes;
};

struct GribVirtualOpenResult {
  bool ok{ false };
  GribVirtualType type{ GribVirtualType::None };
  GribBinaryView binary;
  GribDataView data;
};

using GribXattrList = std::vector<std::pair<std::string, std::string>>;

using ReadObjectPart =
  std::function<std::optional<std::vector<uint8_t>>(long long row_time,
    const std::string& path, uint64_t off, uint64_t len)>;
using ReadLayoutText =
  std::function<std::optional<std::string>(long long row_time,
    const std::string& path)>;
using IsPathInWriting = std::function<bool(const std::string& path)>;

} // namespace iotdbfs
