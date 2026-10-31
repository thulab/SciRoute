#include "iotdbfs/InodeCache.h"

#include <algorithm>
#include <limits>
#include <tuple>

InodeCache& InodeCache::instance() {
  static InodeCache inst;
  return inst;
}

void InodeCache::configure(const DbHooks& hooks, size_t capacity,
  std::chrono::milliseconds dirty_flush_interval) {
  std::lock_guard<std::mutex> lk(mu_);
  hooks_ = hooks;
  if (capacity > 0)
    capacity_ = capacity;
  flush_interval_ = dirty_flush_interval.count() > 0
    ? dirty_flush_interval
    : std::chrono::milliseconds(500);
}

std::optional<Inode> InodeCache::get(const std::string& path) {
  DbHooks hooks;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = cache_.find(path);
    if (it != cache_.end()) {
      it->second.last_access = std::chrono::steady_clock::now();
      return it->second.inode;
    }
    hooks.load_by_path = hooks_.load_by_path;
  }
  if (!hooks.load_by_path)
    return std::nullopt;
  auto ino = hooks.load_by_path(path);
  if (!ino)
    return std::nullopt;
  std::lock_guard<std::mutex> lk(mu_);
  auto it = cache_.find(path);
  if (it != cache_.end())
    return it->second.inode;
  Entry entry;
  entry.inode = *ino;
  entry.last_access = std::chrono::steady_clock::now();
  cache_[path] = entry;
  touch_children_cache(entry.inode.parent);
  maybe_evict_under_lock();
  return ino;
}

bool InodeCache::put(const Inode& inode, bool mark_dirty) {
  std::lock_guard<std::mutex> lk(mu_);
  auto& entry = cache_[inode.path];
  entry.inode = inode;
  ++entry.generation;
  entry.dirty = mark_dirty ? true : entry.dirty;
  if (mark_dirty)
    entry.dirty_since = std::chrono::steady_clock::now();
  entry.last_access = std::chrono::steady_clock::now();
  invalidate_children_cache(inode.parent);
  maybe_evict_under_lock();
  return true;
}

bool InodeCache::erase(const std::string& path,
  std::optional<long long> row_time) {
  std::function<bool(const std::string&, std::optional<long long>)> erase_one;
  {
    std::lock_guard<std::mutex> lk(mu_);
    erase_one = hooks_.erase_one;
  }
  if (erase_one && !erase_one(path, row_time))
    return false;
  std::lock_guard<std::mutex> lk(mu_);
  auto it = cache_.find(path);
  if (it != cache_.end()) {
    auto parent = it->second.inode.parent;
    cache_.erase(it);
    invalidate_children_cache(parent);
  }
  return true;
}

bool InodeCache::erase_subtree(const std::string& path) {
  std::function<bool(const std::string&)> erase_tree;
  {
    std::lock_guard<std::mutex> lk(mu_);
    erase_tree = hooks_.erase_tree;
  }
  if (erase_tree && !erase_tree(path))
    return false;
  std::lock_guard<std::mutex> lk(mu_);
  for (auto it = cache_.begin(); it != cache_.end();) {
    if (it->first == path ||
      (it->first.size() > path.size() &&
        it->first.compare(0, path.size(), path) == 0 &&
        it->first[path.size()] == '/')) {
      auto parent = it->second.inode.parent;
      it = cache_.erase(it);
      invalidate_children_cache(parent);
    }
    else {
      ++it;
    }
  }
  return true;
}

std::vector<Inode> InodeCache::list_children(const std::string& parent) {
  std::vector<Inode> result;
  std::function<std::vector<Inode>(const std::string&)> load_children;
  std::function<std::optional<Inode>(const std::string&)> load_by_path;
  std::vector<std::string> cached_paths;
  std::vector<std::string> missing_paths;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto cached = children_cache_.find(parent);
    if (cached != children_cache_.end()) {
      for (const auto& path : cached->second) {
        auto it = cache_.find(path);
        if (it != cache_.end())
          result.push_back(it->second.inode);
        else
          missing_paths.push_back(path);
      }
      load_by_path = hooks_.load_by_path;
      cached_paths = cached->second;
      if (missing_paths.empty() || !load_by_path)
        return result;
    }
    else {
      load_children = hooks_.load_children;
    }
  }
  if (!missing_paths.empty()) {
    std::vector<Inode> loaded;
    for (const auto& path : missing_paths) {
      auto inode = load_by_path(path);
      if (inode)
        loaded.push_back(*inode);
    }
    std::lock_guard<std::mutex> lk(mu_);
    for (const auto& inode : loaded) {
      Entry entry;
      entry.inode = inode;
      entry.last_access = std::chrono::steady_clock::now();
      cache_[inode.path] = entry;
      result.push_back(inode);
    }
    maybe_evict_under_lock();
    return result;
  }
  if (!load_children)
    return result;

  std::map<std::string, Inode> merged;
  for (const auto& ino : load_children(parent)) {
    if (ino.path != parent)
      merged[ino.path] = ino;
  }
  std::lock_guard<std::mutex> lk(mu_);
  for (const auto& kv : cache_) {
    if (kv.first != parent && kv.second.inode.parent == parent)
      merged[kv.first] = kv.second.inode;
  }

  std::vector<std::string> names;
  names.reserve(merged.size());
  auto now = std::chrono::steady_clock::now();
  result.reserve(merged.size());
  for (const auto& kv : merged) {
    const auto& path = kv.first;
    const auto& ino = kv.second;
    auto c_it = cache_.find(path);
    if (c_it == cache_.end()) {
      Entry entry;
      entry.inode = ino;
      entry.dirty = false;
      entry.last_access = now;
      cache_[path] = entry;
    }
    else {
      c_it->second.last_access = now;
      if (!c_it->second.dirty)
        c_it->second.inode = ino;
    }
    result.push_back(cache_[path].inode);
    names.push_back(path);
  }
  children_cache_[parent] = names;
  maybe_evict_under_lock();
  return result;
}

bool InodeCache::update(const std::string& path,
  const std::function<void(Inode&)>& mutator, bool touch_dirty) {
  auto current = get(path);
  std::lock_guard<std::mutex> lk(mu_);
  auto it = cache_.find(path);
  if (it == cache_.end()) {
    if (!current)
      return false;
    Entry entry;
    entry.inode = *current;
    entry.last_access = std::chrono::steady_clock::now();
    cache_[path] = entry;
    it = cache_.find(path);
  }
  mutator(it->second.inode);
  ++it->second.generation;
  if (touch_dirty) {
    it->second.dirty = true;
    it->second.dirty_since = std::chrono::steady_clock::now();
  }
  it->second.last_access = std::chrono::steady_clock::now();
  invalidate_children_cache(it->second.inode.parent);
  maybe_evict_under_lock();
  return true;
}

bool InodeCache::flush(const std::string& path) {
  Inode inode;
  uint64_t generation = 0;
  std::function<bool(const Inode&)> upsert;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = cache_.find(path);
    if (it == cache_.end() || !hooks_.upsert)
      return false;
    if (!it->second.dirty)
      return true;
    inode = it->second.inode;
    generation = it->second.generation;
    upsert = hooks_.upsert;
  }
  if (!upsert(inode))
    return false;
  std::lock_guard<std::mutex> lk(mu_);
  auto it = cache_.find(path);
  if (it != cache_.end() && it->second.dirty && it->second.generation == generation) {
    it->second.dirty = false;
    it->second.dirty_since = std::chrono::steady_clock::time_point{};
  }
  return true;
}

bool InodeCache::flush_all() {
  std::vector<std::tuple<std::string, Inode, uint64_t>> pending;
  std::function<bool(const Inode&)> upsert;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (!hooks_.upsert)
      return false;
    for (const auto& kv : cache_)
      if (kv.second.dirty)
        pending.emplace_back(kv.first, kv.second.inode, kv.second.generation);
    upsert = hooks_.upsert;
  }
  bool ok = true;
  for (const auto& item : pending) {
    if (!upsert(std::get<1>(item))) {
      ok = false;
      continue;
    }
    std::lock_guard<std::mutex> relock(mu_);
    auto it = cache_.find(std::get<0>(item));
    if (it != cache_.end() && it->second.dirty &&
      it->second.generation == std::get<2>(item)) {
      it->second.dirty = false;
      it->second.dirty_since = std::chrono::steady_clock::time_point{};
    }
  }
  return ok;
}

void InodeCache::discard(const std::string& path) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = cache_.find(path);
  if (it == cache_.end())
    return;
  const std::string parent = it->second.inode.parent;
  cache_.erase(it);
  invalidate_children_cache(parent);
}

void InodeCache::clear() {
  std::lock_guard<std::mutex> lk(mu_);
  cache_.clear();
  children_cache_.clear();
}

void InodeCache::maybe_evict_under_lock() {
  auto now = std::chrono::steady_clock::now();
  if (cache_.size() <= capacity_)
    return;
  std::string victim_path;
  auto oldest = std::chrono::steady_clock::time_point::max();
  for (auto& kv : cache_) {
    if (kv.second.dirty)
      continue;
    if (kv.second.last_access < oldest) {
      oldest = kv.second.last_access;
      victim_path = kv.first;
    }
  }
  if (!victim_path.empty())
    cache_.erase(victim_path);
}

void InodeCache::touch_children_cache(const std::string& parent) {
  if (parent.empty())
    return;
  auto it = children_cache_.find(parent);
  if (it == children_cache_.end())
    return;
}

void InodeCache::invalidate_children_cache(const std::string& parent) {
  if (!parent.empty())
    children_cache_.erase(parent);
}
