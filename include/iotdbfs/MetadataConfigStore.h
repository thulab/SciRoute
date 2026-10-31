#pragma once

#include "iotdbfs/MetadataConfig.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

namespace iotdbfs {

// Owns an immutable configuration snapshot. Readers get a shared snapshot and
// never observe a partially reloaded schema.
class MetadataConfigStore {
public:
  MetadataConfigStore() = default;

  MetadataResult<std::uint64_t> reload(const std::string& path);
  std::shared_ptr<const MetadataConfig> snapshot() const;
  std::uint64_t generation() const;

private:
  mutable std::mutex mutex_;
  std::shared_ptr<const MetadataConfig> config_;
  std::uint64_t generation_{0};
};

} // namespace iotdbfs
