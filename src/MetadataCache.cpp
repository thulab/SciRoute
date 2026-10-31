#include "iotdbfs/MetadataCache.h"

#include "iotdbfs/MetadataSerialization.h"

#include <algorithm>
#include <functional>
#include <iterator>
#include <limits>
#include <utility>

namespace iotdbfs {

MetadataCache::MetadataCache(std::size_t capacity, std::size_t memory_budget)
  : capacity_(capacity), memory_budget_(memory_budget) {}

std::size_t MetadataCache::KeyHash::operator()(const Key& key) const noexcept {
  const auto h1 = std::hash<std::int64_t>{}(key.row_time);
  const auto h2 = std::hash<std::string>{}(key.path);
  const auto h3 = std::hash<std::string>{}(key.metadata);
  return h1 ^ (h2 + static_cast<std::size_t>(0x9e3779b9) + (h1 << 6) +
    (h1 >> 2)) ^ (h3 + static_cast<std::size_t>(0x9e3779b9) + (h2 << 6) +
    (h2 >> 2));
}

std::optional<MetadataResult<MetadataMap>> MetadataCache::get(
    std::int64_t row_time, const std::string& path,
    const std::string& metadata_json) {
  std::lock_guard<std::mutex> lock(mutex_);
  Key key{row_time, path, metadata_json};
  const auto found = index_.find(key);
  if (found == index_.end()) return std::nullopt;
  lru_.splice(lru_.begin(), lru_, found->second);
  return found->second->result;
}

MetadataResult<MetadataMap> MetadataCache::get_or_deserialize(
    std::int64_t row_time, const std::string& path,
    const std::string& metadata_json, std::size_t max_bytes) {
  const auto limit = std::min<std::size_t>(65536, max_bytes);
  if (metadata_json.size() > limit)
    return MetadataResult<MetadataMap>::failure("metadata exceeds size cap");
  if (auto cached = get(row_time, path, metadata_json)) {
    return *cached;
  }
  auto result = deserialize_metadata(metadata_json, max_bytes);
  // Invalid documents are deliberately not retained: callers may repair a
  // row in place and a cached parse failure would otherwise mask the update.
  if (result.ok())
    put(row_time, path, metadata_json, result);
  return result;
}

void MetadataCache::put(std::int64_t row_time, const std::string& path,
    const std::string& metadata_json,
    const MetadataResult<MetadataMap>& result) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (capacity_ == 0 || !result.ok()) return;
  Key key{row_time, path, metadata_json};
  const auto memory_bytes = estimate_memory(key, result);
  if (memory_bytes > memory_budget_) return;
  const auto found = index_.find(key);
  if (found != index_.end()) {
    memory_usage_ -= found->second->memory_bytes;
    found->second->result = result;
    found->second->memory_bytes = memory_bytes;
    memory_usage_ += memory_bytes;
    lru_.splice(lru_.begin(), lru_, found->second);
    return;
  }
  lru_.push_front(Entry{std::move(key), result, memory_bytes});
  memory_usage_ += memory_bytes;
  index_[lru_.front().key] = lru_.begin();
  evict_under_lock();
}

void MetadataCache::invalidate(std::int64_t row_time, const std::string& path) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto it = lru_.begin(); it != lru_.end();) {
    if (it->key.row_time == row_time && it->key.path == path) {
      index_.erase(it->key);
      memory_usage_ -= it->memory_bytes;
      it = lru_.erase(it);
    } else {
      ++it;
    }
  }
}

void MetadataCache::clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  index_.clear();
  lru_.clear();
  memory_usage_ = 0;
}

void MetadataCache::set_capacity(std::size_t capacity) {
  std::lock_guard<std::mutex> lock(mutex_);
  capacity_ = capacity;
  evict_under_lock();
}

void MetadataCache::set_memory_budget(std::size_t memory_budget) {
  std::lock_guard<std::mutex> lock(mutex_);
  memory_budget_ = memory_budget;
  evict_under_lock();
}

std::size_t MetadataCache::size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return lru_.size();
}

std::size_t MetadataCache::capacity() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return capacity_;
}

std::size_t MetadataCache::memory_usage() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return memory_usage_;
}

std::size_t MetadataCache::memory_budget() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return memory_budget_;
}

std::size_t MetadataCache::estimate_memory(const Key& key,
    const MetadataResult<MetadataMap>& result) {
  // The key is held by both the LRU list and unordered-map index. Account for
  // both copies so the configured budget remains a conservative bound.
  const auto add = [](std::size_t& total, std::size_t amount) {
    if (amount > std::numeric_limits<std::size_t>::max() - total)
      total = std::numeric_limits<std::size_t>::max();
    else
      total += amount;
  };
  std::size_t bytes = sizeof(Entry);
  add(bytes, key.path.size());
  add(bytes, key.metadata.size());
  add(bytes, key.path.size());
  add(bytes, key.metadata.size());
  if (!result.ok()) {
    add(bytes, result.error().size());
    return bytes;
  }
  for (const auto& [id, value] : result.value()) {
    add(bytes, id.size());
    add(bytes, sizeof(value));
    const auto& raw = value.value();
    if (std::holds_alternative<std::string>(raw))
      add(bytes, std::get<std::string>(raw).size());
  }
  return bytes;
}

void MetadataCache::evict_under_lock() {
  while (lru_.size() > capacity_ || memory_usage_ > memory_budget_) {
    auto last = std::prev(lru_.end());
    index_.erase(last->key);
    memory_usage_ -= last->memory_bytes;
    lru_.pop_back();
  }
}

} // namespace iotdbfs
