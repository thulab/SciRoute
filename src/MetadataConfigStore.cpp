#include "iotdbfs/MetadataConfigStore.h"

namespace iotdbfs {

MetadataResult<std::uint64_t> MetadataConfigStore::reload(const std::string& path) {
  auto loaded = MetadataConfig::load_file(path);
  if (!loaded.ok()) return MetadataResult<std::uint64_t>::failure(loaded.error());
  std::lock_guard<std::mutex> lock(mutex_);
  config_ = std::make_shared<const MetadataConfig>(std::move(loaded.value()));
  ++generation_;
  return MetadataResult<std::uint64_t>::success(generation_);
}

std::shared_ptr<const MetadataConfig> MetadataConfigStore::snapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return config_;
}

std::uint64_t MetadataConfigStore::generation() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return generation_;
}

} // namespace iotdbfs
