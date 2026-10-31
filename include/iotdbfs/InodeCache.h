#pragma once

#include "iotdbfs/InodeTypes.h"

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

// Thread-safe in-memory cache for inode metadata persisted in IoTDB.
class InodeCache {
public:
  // Hooks used by the cache to talk to the persistent store.
  struct DbHooks {
    // Load a single inode by absolute path.
    std::function<std::optional<Inode>(const std::string&)> load_by_path;
    // Load direct children of the supplied directory path.
    std::function<std::vector<Inode>(const std::string&)> load_children;
    // Insert or update the inode in the backing store.
    std::function<bool(const Inode&)> upsert;
    // Delete a single inode (no recursion).
    std::function<bool(const std::string&, std::optional<long long>)> erase_one;
    // Delete a directory and its recursive descendants.
   std::function<bool(const std::string&)> erase_tree;
  };

  static InodeCache& instance();

  // Configure underlying hooks and cache sizing.
  void configure(const DbHooks& hooks, size_t capacity,
    std::chrono::milliseconds dirty_flush_interval);

  // Retrieve an inode if resident or load it from the database.
  std::optional<Inode> get(const std::string& path);
  // Insert/replace an inode, optionally marking it dirty for write-back.
  bool put(const Inode& inode, bool mark_dirty);
  // Remove a single inode both in cache and persistent store.
  bool erase(const std::string& path,
    std::optional<long long> row_time = std::nullopt);
  // Remove a directory subtree recursively.
  bool erase_subtree(const std::string& path);
  // List all cached or stored children of a directory.
  std::vector<Inode> list_children(const std::string& parent);

  // Convenience wrapper to mutate an inode within the cache.
  bool update(const std::string& path,
    const std::function<void(Inode&)>& mutator, bool touch_dirty);

  // Flush one inode if dirty.
  bool flush(const std::string& path);
  // Flush every dirty inode.
  bool flush_all();
  // Remove a cache entry without touching the backing store.
  void discard(const std::string& path);
  // Clear all cache entries without touching the backing store (for testing).
  void clear();

private:
  struct Entry {
    Inode inode;
    bool dirty{ false };
    uint64_t generation{ 0 };
    std::chrono::steady_clock::time_point last_access;
    std::chrono::steady_clock::time_point dirty_since;
  };

  InodeCache() = default;
  InodeCache(const InodeCache&) = delete;
  InodeCache& operator=(const InodeCache&) = delete;

  void maybe_evict_under_lock();
  void touch_children_cache(const std::string& parent);
  void invalidate_children_cache(const std::string& parent);

  DbHooks hooks_;
  size_t capacity_{ 1024 };
  std::chrono::milliseconds flush_interval_{ 500 };
  std::unordered_map<std::string, Entry> cache_;
  std::unordered_map<std::string, std::vector<std::string>> children_cache_;
  mutable std::mutex mu_;
};
