#pragma once

#include "iotdbfs/MetadataConfig.h"

#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace iotdbfs {

// Thread-safe bounded LRU cache for decoded inode metadata.  The persisted
// JSON is part of the key so updates to a row cannot return stale values.
class MetadataCache {
public:
  // Default memory budget is eight MiB, in addition to the entry limit.
  explicit MetadataCache(std::size_t capacity = 1024,
    std::size_t memory_budget = 8 * 1024 * 1024);

  std::optional<MetadataResult<MetadataMap>> get(std::int64_t row_time,
    const std::string& path, const std::string& metadata_json);

  MetadataResult<MetadataMap> get_or_deserialize(std::int64_t row_time,
    const std::string& path, const std::string& metadata_json,
    std::size_t max_bytes = 65536);

  void put(std::int64_t row_time, const std::string& path,
    const std::string& metadata_json,
    const MetadataResult<MetadataMap>& result);

  // Invalidate all metadata revisions belonging to one inode row.
  void invalidate(std::int64_t row_time, const std::string& path);
  void clear();
  void set_capacity(std::size_t capacity);
  void set_memory_budget(std::size_t memory_budget);
  std::size_t size() const;
  std::size_t capacity() const;
  std::size_t memory_usage() const;
  std::size_t memory_budget() const;

private:
  struct Key {
    std::int64_t row_time;
    std::string path;
    std::string metadata;
    bool operator==(const Key& other) const {
      return row_time == other.row_time && path == other.path &&
        metadata == other.metadata;
    }
  };
  struct KeyHash {
    std::size_t operator()(const Key& key) const noexcept;
  };
  struct Entry {
    Key key;
    MetadataResult<MetadataMap> result;
    std::size_t memory_bytes{0};
  };
  using List = std::list<Entry>;
  using Index = std::unordered_map<Key, List::iterator, KeyHash>;

  void evict_under_lock();
  static std::size_t estimate_memory(const Key& key,
    const MetadataResult<MetadataMap>& result);

  mutable std::mutex mutex_;
  std::size_t capacity_;
  std::size_t memory_budget_;
  std::size_t memory_usage_{0};
  List lru_;
  Index index_;
};

} // namespace iotdbfs
