#pragma once

#include "iotdbfs/IoTDBPublishedCatalogSql.h"

#include <list>
#include <iterator>
#include <memory>
#include <tuple>

namespace iotdbfs {

struct DatasetCatalogCacheIdentity {
  std::uint64_t storage_namespace{0}, manifest_revision{0};
  std::optional<std::string> format_id;
  std::uint64_t payload_size{0}, layout_size{0};
  DatasetCatalogMarker marker;
  // Strict's parsed artifact and the authoritative sealed index are separate
  // admissions; neither can bypass the other's integrity validation.
  std::string substructure_seal_digest;
  friend bool operator<(const DatasetCatalogCacheIdentity& a,
                        const DatasetCatalogCacheIdentity& b) {
    const auto fields = [](const DatasetCatalogCacheIdentity& x) {
      return std::tie(x.storage_namespace, x.manifest_revision, x.format_id,
        x.payload_size, x.layout_size, x.marker.key.path, x.marker.key.version,
        x.marker.size, x.marker.protocol, x.marker.layout_digest, x.marker.layout_protocol,
        x.marker.directory_complete, x.marker.record_count, x.marker.attribute_count,
        x.substructure_seal_digest);
    };
    return fields(a) < fields(b);
  }
};

inline DatasetCatalogCacheIdentity make_dataset_catalog_cache_identity(
    std::uint64_t storage_namespace, const StructuredBlobManifest& manifest,
    const DatasetCatalogMarker& marker, const std::string& seal_digest = {}) {
  return {storage_namespace, manifest.revision, manifest.format_id,
    manifest.payload_size, manifest.layout_size, marker, seal_digest};
}

// Conservative resident estimate: vector/string capacity, both copies of the
// identity in the list/index, and node/allocation overhead. It never charges
// zero bytes and saturates on overflow so oversized entries are declined.
inline std::size_t estimate_dataset_catalog_cache_bytes(
    const DatasetCatalogCacheIdentity& identity, const DatasetCatalogArtifact& artifact) {
  std::size_t bytes = 2 * sizeof(DatasetCatalogCacheIdentity) + sizeof(DatasetCatalogArtifact) + 256;
  const auto add = [&](std::size_t n) {
    bytes = n > std::numeric_limits<std::size_t>::max() - bytes
      ? std::numeric_limits<std::size_t>::max() : bytes + n;
  };
  const auto text = [&](const std::string& s) { add(s.capacity()); add(32); };
  for (unsigned copy = 0; copy < 2; ++copy) {
    text(identity.marker.key.path); text(identity.marker.protocol);
    text(identity.marker.layout_digest); text(identity.marker.layout_protocol);
    text(identity.substructure_seal_digest);
    if (identity.format_id) text(*identity.format_id);
  }
  text(artifact.protocol); text(artifact.format_id); text(artifact.layout_protocol); text(artifact.layout_digest);
  add(artifact.records.capacity() * sizeof(DatasetCatalogRecord)); add(32);
  for (const auto& record : artifact.records) {
    text(record.object_id); text(record.canonical_path);
    add(record.attributes.capacity() * sizeof(DatasetCatalogAttribute)); add(32);
    for (const auto& attribute : record.attributes) {
      text(attribute.name); text(attribute.reason);
      add(attribute.shape.capacity() * sizeof(std::uint64_t)); add(32);
      if (attribute.value)
        if (const auto* value = std::get_if<std::string>(&*attribute.value)) text(*value);
    }
  }
  return bytes;
}

// Owned by one backend instance and synchronized by its mutation_mu_. The
// cache stores validated immutable content; permissions, marker/manifest
// freshness, request budgets and schemas stay live. Strict mode also retains
// secondary COUNT checks; sealed mode partitions entries by verified digest.
class DatasetCatalogParsedCache {
public:
  explicit DatasetCatalogParsedCache(std::size_t capacity = 65536,
      std::size_t memory_budget = 64ULL * 1024 * 1024)
    : capacity_(capacity), memory_budget_(memory_budget) {}
  DatasetCatalogParsedCache(const DatasetCatalogParsedCache&) = delete;
  DatasetCatalogParsedCache& operator=(const DatasetCatalogParsedCache&) = delete;

  std::shared_ptr<const DatasetCatalogArtifact> get(const DatasetCatalogCacheIdentity& identity) {
    const auto found = index_.find(identity);
    if (found == index_.end()) return {};
    lru_.splice(lru_.begin(), lru_, found->second);
    return found->second->artifact;
  }
  bool put_validated(const DatasetCatalogCacheIdentity& identity,
      std::shared_ptr<const DatasetCatalogArtifact> artifact) {
    if (!artifact || !capacity_) return false;
    const auto charge = estimate_dataset_catalog_cache_bytes(identity, *artifact);
    if (charge > memory_budget_) return false;
    const auto existing = index_.find(identity);
    if (existing != index_.end()) erase(existing->second);
    while (!lru_.empty() && (lru_.size() >= capacity_ || charge > memory_budget_ - memory_bytes_)) {
      erase(std::prev(lru_.end())); ++evictions_;
    }
    try { lru_.push_front({identity, std::move(artifact), charge}); }
    catch (...) { return false; }
    try { index_.emplace(lru_.front().identity, lru_.begin()); }
    catch (...) { lru_.pop_front(); return false; }
    memory_bytes_ += charge;
    return true;
  }
  std::size_t invalidate_path(const std::string& path,
      std::optional<std::int64_t> version = std::nullopt) {
    const auto normalized = path_utils::norm(path);
    return invalidate([&](const auto& key) {
      return key.marker.key.path == normalized && (!version || key.marker.key.version == *version);
    });
  }
  std::size_t invalidate_tree(const std::string& path) {
    const auto normalized = path_utils::norm(path);
    return invalidate([&](const auto& key) {
      const auto& candidate = key.marker.key.path;
      return normalized == "/" || candidate == normalized ||
        candidate.compare(0, normalized.size() + 1, normalized + "/") == 0;
    });
  }
  std::size_t size() const { return lru_.size(); }
  std::size_t memory_bytes() const { return memory_bytes_; }
  std::uint64_t evictions() const { return evictions_; }

private:
  struct Entry {
    DatasetCatalogCacheIdentity identity;
    std::shared_ptr<const DatasetCatalogArtifact> artifact;
    std::size_t charge;
  };
  using Entries = std::list<Entry>;
  void erase(Entries::iterator entry) {
    memory_bytes_ -= entry->charge;
    index_.erase(entry->identity); lru_.erase(entry);
  }
  template<class Predicate> std::size_t invalidate(Predicate matches) {
    std::size_t count = 0;
    for (auto current = lru_.begin(); current != lru_.end();) {
      auto entry = current++;
      if (matches(entry->identity)) { erase(entry); ++count; }
    }
    return count;
  }
  std::size_t capacity_, memory_budget_, memory_bytes_{0};
  std::uint64_t evictions_{0};
  Entries lru_;
  std::map<DatasetCatalogCacheIdentity, Entries::iterator> index_;
};

} // namespace iotdbfs
