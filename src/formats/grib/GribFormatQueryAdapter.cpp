#include "iotdbfs/formats/grib/GribFormatQueryAdapter.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "iotdbfs/query/core/QueryReadContext.h"
#include "iotdbfs/query/core/QueryStorageReadControl.h"
#include "iotdbfs/query/core/ImmutableEqualityIndex.h"

#include <algorithm>
#include <array>
#include <openssl/evp.h>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <condition_variable>
#include <deque>
#include <exception>
#include <functional>
#include <future>
#include <list>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>

namespace iotdbfs {

struct GribIndexedMessage {
  grib_utils::MsgInfo info;
  std::optional<std::string> short_name;
  std::optional<std::string> type_of_level;
  std::optional<std::int64_t> level;
  std::optional<std::int64_t> step;
  std::optional<std::string> step_range;
  std::int32_t edition{0};
  std::optional<std::string> unit;
  std::optional<std::int64_t> param_id;
  std::optional<std::int64_t> validity_date;
  std::optional<std::int64_t> validity_time;
};

struct GribParsedIndex {
  std::vector<GribIndexedMessage> messages;
  std::size_t memory_bytes{0};
};

class GribParsedIndexCache {
  using Key = std::tuple<bool, std::string, std::uint64_t,
    StructuredBlobVersionKey, std::uint64_t, std::uint64_t, std::uint64_t,
    std::string>;
public:
  using Index = std::shared_ptr<const GribParsedIndex>;
  using Result = QueryResult<Index>;
  using Loader = std::function<Result()>;

  explicit GribParsedIndexCache(GribParsedIndexCacheConfig config)
    : config_(config) {}

  Result get_or_load(const QuerySource& source,
      const Loader& loader,
      std::optional<std::string>* verified_identity = nullptr) {
    // Re-admit this exact source before consulting shared parsed state.
    auto identity = source.verified_layout_identity();
    if (!identity.ok()) return Result::failure(attributed(identity.status(), source));
    if (verified_identity) *verified_identity = identity.value();
    if (config_.max_versions == 0 || config_.memory_budget_bytes == 0) {
      query_profile_count("grib.index.cache.disabled");
      return loader();
    }

    const auto& manifest = source.manifest();
    const bool shared = identity.value().has_value();
    const Key key{shared, identity.value().value_or(""),
      shared ? 0 : source.storage_namespace(),
      shared ? StructuredBlobVersionKey{} : manifest.key,
      shared ? 0 : manifest.revision, manifest.payload_size, manifest.layout_size,
      manifest.format_id.value_or("") + ":grib-index/2"};

    std::shared_future<Result> future;
    std::shared_ptr<std::promise<Result>> promise;
    bool load = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto found = index_.find(key);
      if (found != index_.end()) {
        query_profile_count(found->second->ready ? "grib.index.cache.hits" :
          "grib.index.cache.joins");
        lru_.splice(lru_.begin(), lru_, found->second);
        future = found->second->future;
      } else {
        query_profile_count("grib.index.cache.misses");
        promise = std::make_shared<std::promise<Result>>();
        future = promise->get_future().share();
        lru_.push_front(Entry{key, future, false, 0});
        index_.emplace(lru_.front().key, lru_.begin());
        load = true;
      }
    }

    auto load_for_source = [&]() -> Result {
      try {
        auto result = loader();
        return result.ok() ? result : Result::failure(attributed(result.status(), source));
      } catch (const std::exception& error) {
        return Result::failure(attributed(QueryStatus::failure(QueryError::Internal,
          QueryPhase::Planning, error.what()), source));
      } catch (...) {
        return Result::failure(attributed(QueryStatus::failure(QueryError::Internal,
          QueryPhase::Planning, "GRIB parsed index loader failed"), source));
      }
    };
    if (!load) {
      auto result = future.get();
      // Failures concern the producing source, never another version. A failed
      // singleflight waiter obtains its own storage result and attribution.
      return result.ok() ? result : load_for_source();
    }

    Result result = load_for_source();
    promise->set_value(result);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto found = index_.find(key);
      if (found != index_.end()) {
        if (!result.ok()) {
          lru_.erase(found->second);
          index_.erase(found);
        } else {
          found->second->ready = true;
          found->second->memory_bytes = estimate_memory(key,
            *result.value());
          memory_usage_ += found->second->memory_bytes;
          query_profile_count("grib.index.cache.loaded_bytes", found->second->memory_bytes);
          evict_under_lock();
        }
      }
    }
    return result.ok() ? result : Result::failure(attributed(result.status(), source));
  }

private:
  static QueryStatus attributed(const QueryStatus& status, const QuerySource& source) {
    auto detail = status.detail();
    const auto& manifest = source.manifest();
    detail.source = QuerySourceReference{manifest.key.path, manifest.key.version,
      manifest.format_id.value_or("")};
    return QueryStatus::failure(status.code(), status.phase(), status.message(),
      std::move(detail));
  }

  struct Entry {
    Key key;
    std::shared_future<Result> future;
    bool ready{false};
    std::size_t memory_bytes{0};
  };
  using List = std::list<Entry>;
  using IndexMap = std::map<Key, List::iterator>;

  static std::size_t estimate_memory(const Key& key,
      const GribParsedIndex& index) {
    return sizeof(Entry) + (std::get<1>(key).size() +
      std::get<3>(key).path.size() + std::get<7>(key).size()) * 2 + index.memory_bytes;
  }

  void evict_under_lock() {
    while (lru_.size() > config_.max_versions ||
        memory_usage_ > config_.memory_budget_bytes) {
      auto victim = lru_.end();
      for (auto current = lru_.end(); current != lru_.begin();) {
        --current;
        if (current->ready) {
          victim = current;
          break;
        }
      }
      if (victim == lru_.end()) return;
      query_profile_count("grib.index.cache.evictions");
      if (lru_.size() > config_.max_versions)
        query_profile_count("grib.index.cache.version_limit_evictions");
      if (memory_usage_ > config_.memory_budget_bytes)
        query_profile_count("grib.index.cache.byte_limit_evictions");
      memory_usage_ -= victim->memory_bytes;
      index_.erase(victim->key);
      lru_.erase(victim);
    }
  }

  GribParsedIndexCacheConfig config_;
  std::mutex mutex_;
  std::size_t memory_usage_{0};
  List lru_;
  IndexMap index_;
};

// Workers collect events independently of the caller's synchronous profile map.
struct GribNativeCacheCounters {
  std::atomic_uint64_t hits{0}, misses{0}, waits{0}, loads{0}, load_failures{0};
  std::atomic_uint64_t evictions{0}, loaded_bytes{0}, loaded_field_bytes{0};
  std::atomic_uint64_t miss_message_bytes{0}, alias_hits{0}, alias_misses{0};
  std::atomic_uint64_t alias_evictions{0}, hashes{0}, content_hits{0};
};

class GribDecodedFieldCache {
public:
  using Field = std::shared_ptr<const GribNativeField>;
  using Result = QueryResult<Field>;
  using Loader = std::function<Result()>;
  struct Encoded {
    std::string digest;
    Loader decode; // Temporarily pins an encoded lease; never retained by this cache.
  };
  using EncodedResult = QueryResult<Encoded>;
  using EncodedLoader = std::function<EncodedResult()>;

  explicit GribDecodedFieldCache(GribDecodedFieldCacheConfig config) : config_(config) {}

  Result get_or_load(const QuerySource& source, const grib_utils::MsgInfo& message,
      const EncodedLoader& loader, const std::shared_ptr<GribNativeCacheCounters>& counters) {
    try {
      // Logical request admission precedes this method, including every alias hit.
      // An alias binds only one immutable exact source to verified encoded bytes.
      const auto& m = source.manifest();
      const AliasKey alias_key{source.storage_namespace(), m.key, m.revision,
        m.payload_size, m.layout_size, message.off, message.len};
      const bool enabled = config_.max_fields && config_.memory_budget_bytes;
      std::optional<Encoded> encoded;
      auto load_alias = [&]() -> AliasResult {
        try {
          NeutralProducer producer;
          ScopedQueryMetadataTrace trace("");
          auto loaded = loader();
          if (!loaded.ok()) return AliasResult::failure(attributed(loaded.status(), source));
          encoded = std::move(loaded.value());
          auto fees = trace.release();
          if (std::none_of(fees.begin(), fees.end(), [](const auto& fee) {
                return fee.first.compare(0, 18, "v04-payload-bytes/") == 0;
              })) {
            require_query_payload_demand(message.len, 1);
            auto demand = trace.release();
            fees.insert(demand.begin(), demand.end());
          }
          auto alias = std::make_shared<Alias>();
          alias->content_key = "grib-native-f64/2:" + std::to_string(source.storage_namespace()) +
            ":" + std::to_string(message.len) + ":" + encoded->digest;
          alias->fees = std::move(fees);
          alias->fee_bytes = trace.estimated_bytes();
          return AliasResult::success(std::move(alias));
        } catch (const QueryMetadataBudgetExceeded& e) {
          return AliasResult::failure(failure(QueryError::ResourceExhausted, e.what(), source));
        } catch (const std::bad_alloc&) {
          return AliasResult::failure(failure(QueryError::ResourceExhausted,
            "GRIB source alias exhausted memory", source));
        } catch (const std::exception& e) {
          return AliasResult::failure(failure(QueryError::Internal, e.what(), source));
        } catch (...) {
          return AliasResult::failure(failure(QueryError::Internal,
            "GRIB source alias loader failed", source));
        }
      };
      auto alias = enabled ? get_alias(alias_key, load_alias, counters) : load_alias();
      if (!alias.ok()) return Result::failure(attributed(alias.status(), source));
      // A distinct version always replays its own fees, even when its content
      // shares a native field. A caller's exhausted ledger never poisons a flight.
      replay_query_metadata_trace(alias.value()->fees);
      auto decode = [&]() -> Result {
        try {
          // Recreating an evicted native field must not charge a second set of
          // demand sequence IDs in addition to this exact alias's stable fees.
          NeutralProducer producer;
          ScopedQueryMetadataTrace ignored_trace("");
          if (!encoded) {
            auto loaded = loader();
            if (!loaded.ok()) return Result::failure(attributed(loaded.status(), source));
            encoded = std::move(loaded.value());
          }
          const auto key = "grib-native-f64/2:" + std::to_string(source.storage_namespace()) +
            ":" + std::to_string(message.len) + ":" + encoded->digest;
          if (key != alias.value()->content_key)
            return Result::failure(failure(QueryError::CorruptPayload,
              "GRIB immutable source encoded identity changed", source));
          counters->loads.fetch_add(1, std::memory_order_relaxed);
          auto result = encoded->decode();
          if (!result.ok()) counters->load_failures.fetch_add(1, std::memory_order_relaxed);
          return result.ok() ? result : Result::failure(attributed(result.status(), source));
        } catch (const QueryMetadataBudgetExceeded& e) {
          return Result::failure(failure(QueryError::ResourceExhausted, e.what(), source));
        } catch (const std::bad_alloc&) {
          return Result::failure(failure(QueryError::ResourceExhausted,
            "GRIB native decode exhausted memory", source));
        } catch (const std::exception& e) {
          return Result::failure(failure(QueryError::Internal, e.what(), source));
        } catch (...) {
          return Result::failure(failure(QueryError::Internal, "GRIB native decode failed", source));
        }
      };
      if (!enabled) return decode();
      return get_field(alias.value()->content_key, message.len, decode, counters);
    } catch (const QueryMetadataBudgetExceeded& e) {
      return Result::failure(failure(QueryError::ResourceExhausted, e.what(), source));
    } catch (const std::bad_alloc&) {
      return Result::failure(failure(QueryError::ResourceExhausted,
        "GRIB native cache exhausted memory", source));
    } catch (const std::exception& e) {
      return Result::failure(failure(QueryError::Internal, e.what(), source));
    }
  }

  void report(const GribNativeCacheCounters& counters) noexcept {
    if (!active_query_profile) return;
#define GRIB_NATIVE_COUNT(name) query_profile_count("grib.native_field_cache." #name, counters.name.load(std::memory_order_relaxed))
    GRIB_NATIVE_COUNT(hits); GRIB_NATIVE_COUNT(misses); GRIB_NATIVE_COUNT(waits);
    GRIB_NATIVE_COUNT(loads); GRIB_NATIVE_COUNT(load_failures); GRIB_NATIVE_COUNT(evictions);
    GRIB_NATIVE_COUNT(loaded_bytes); GRIB_NATIVE_COUNT(loaded_field_bytes);
    GRIB_NATIVE_COUNT(miss_message_bytes); GRIB_NATIVE_COUNT(alias_hits);
    GRIB_NATIVE_COUNT(alias_misses); GRIB_NATIVE_COUNT(alias_evictions);
    GRIB_NATIVE_COUNT(hashes); GRIB_NATIVE_COUNT(content_hits);
#undef GRIB_NATIVE_COUNT
    try {
      std::lock_guard<std::mutex> lock(mutex_);
      auto& output = active_query_profile->counters;
      output["grib.native_field_cache.resident_bytes"] = memory_usage_;
      output["grib.native_field_cache.resident_entries"] = probation_.size() + protected_.size();
      output["grib.native_field_cache.aliases"] = aliases_.size();
      output["grib.native_field_cache.ghosts"] = ghosts_.size();
      output["grib.native_field_cache.byte_budget"] = config_.memory_budget_bytes;
      output["grib.native_field_cache.entry_limit"] = config_.max_fields;
    } catch (...) {}
  }

private:
  // Producers must not leak physical replay IDs into an enclosing consumer
  // trace. Each source alias replays its stable logical trace explicitly.
  class NeutralProducer {
  public:
    NeutralProducer() : previous_trace_(active_query_metadata_trace) {
      active_query_metadata_trace = nullptr;
    }
    ~NeutralProducer() { active_query_metadata_trace = previous_trace_; }
  private:
    ScopedQueryMetadataProducer budget_;
    ScopedQueryMetadataTrace* previous_trace_;
  };
  static constexpr std::size_t alias_limit = 256, ghost_limit = 256;
  using AliasKey = std::tuple<std::uint64_t, StructuredBlobVersionKey,
    std::uint64_t, std::uint64_t, std::uint64_t, std::uint64_t, std::uint64_t>;
  struct Alias { std::string content_key; QueryMetadataTrace fees; std::size_t fee_bytes{0}; };
  using AliasResult = QueryResult<std::shared_ptr<const Alias>>;
  struct AliasEntry { AliasKey key; std::shared_future<AliasResult> future; bool ready{false}; std::size_t bytes{0}; };
  using Aliases = std::list<AliasEntry>;
  struct Entry {
    std::string key;
    std::shared_future<Result> future;
    bool ready{false}, hot{false};
    std::size_t bytes{0};
  };
  using Fields = std::list<Entry>;

  static QueryStatus failure(QueryError code, const std::string& message, const QuerySource& source) {
    return attributed(QueryStatus::failure(code, QueryPhase::Execution, message), source);
  }
  static QueryStatus attributed(const QueryStatus& status, const QuerySource& source) {
    auto detail = status.detail();
    const auto& m = source.manifest();
    detail.source = QuerySourceReference{m.key.path, m.key.version, m.format_id.value_or("")};
    return QueryStatus::failure(status.code(), status.phase(), status.message(), std::move(detail));
  }
  static std::size_t alias_base_bytes(const AliasKey& key) {
    return sizeof(AliasEntry) + sizeof(Alias) + sizeof(AliasKey) +
      2 * std::get<1>(key).path.capacity() + 256 + 6 * sizeof(void*);
  }
  static std::size_t alias_bytes(const AliasKey& key, const Alias& alias) {
    return alias_base_bytes(key) + alias.content_key.capacity() + alias.fee_bytes;
  }
  static std::size_t entry_base_bytes(const std::string& key) {
    return sizeof(Entry) + 2 * key.capacity() + 256 + 6 * sizeof(void*);
  }
  static std::size_t entry_bytes(const Entry& entry, const GribNativeField& field) {
    return entry_base_bytes(entry.key) + field.memory_bytes();
  }
  static std::size_t ghost_bytes(const std::string& key) {
    return 2 * key.capacity() + 2 * sizeof(std::string) + 8 * sizeof(void*);
  }

  AliasResult get_alias(const AliasKey& key, const std::function<AliasResult()>& loader,
      const std::shared_ptr<GribNativeCacheCounters>& counters) {
    std::shared_future<AliasResult> future;
    std::shared_ptr<std::promise<AliasResult>> promise;
    bool bypass = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto found = alias_index_.find(key);
      if (found != alias_index_.end()) {
        counters->alias_hits.fetch_add(1, std::memory_order_relaxed);
        aliases_.splice(aliases_.begin(), aliases_, found->second);
        future = found->second->future;
      } else {
        counters->alias_misses.fetch_add(1, std::memory_order_relaxed);
        while (aliases_.size() >= alias_limit && evict_alias(counters)) {}
        const auto base = alias_base_bytes(key);
        if (aliases_.size() >= alias_limit || !make_room(base, counters)) bypass = true;
        else {
          promise = std::make_shared<std::promise<AliasResult>>();
          future = promise->get_future().share();
          aliases_.push_front({key, future, false, base});
          try { alias_index_.emplace(key, aliases_.begin()); }
          catch (...) { aliases_.pop_front(); throw; }
          alias_usage_ += base; memory_usage_ += base;
        }
      }
    }
    if (bypass) return loader();
    if (!promise) {
      auto result = future.get();
      // Failure state belongs to its producing source; never retain a failed
      // operation or turn a waiter's ledger failure into a cached status.
      return result.ok() ? result : loader();
    }
    auto result = loader();
    try { promise->set_value(result); }
    catch (...) {
      // Copying a failed status into the shared state can allocate. Remove
      // our pending reservation even if delivery itself runs out of memory.
      std::lock_guard<std::mutex> lock(mutex_);
      auto found = alias_index_.find(key);
      if (found != alias_index_.end() && !found->second->ready)
        erase_alias(found->second, counters);
      throw;
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto found = alias_index_.find(key);
      if (found != alias_index_.end()) {
        auto entry = found->second;
        const auto bytes = result.ok() ? alias_bytes(key, *result.value()) : 0;
        const auto added = bytes > entry->bytes ? bytes - entry->bytes : 0;
        if (!result.ok() || !make_room(added, counters)) erase_alias(entry, counters);
        else {
          alias_usage_ += added; memory_usage_ += added;
          entry->bytes = bytes;
          entry->ready = true;
          trim(counters);
        }
      }
    }
    return result;
  }

  Result get_field(const std::string& key, std::uint64_t message_bytes, const Loader& loader,
      const std::shared_ptr<GribNativeCacheCounters>& counters) {
    std::shared_future<Result> future;
    std::shared_ptr<std::promise<Result>> promise;
    bool bypass = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto found = index_.find(key);
      if (found != index_.end()) {
        counters->hits.fetch_add(1, std::memory_order_relaxed);
        if (!found->second->ready) counters->waits.fetch_add(1, std::memory_order_relaxed);
        if (found->second->ready) {
          counters->content_hits.fetch_add(1, std::memory_order_relaxed);
          if (!found->second->hot) {
            probation_usage_ -= found->second->bytes;
            found->second->hot = true;
            protected_.splice(protected_.begin(), probation_, found->second);
          } else protected_.splice(protected_.begin(), protected_, found->second);
        }
        future = found->second->future;
      } else {
        counters->misses.fetch_add(1, std::memory_order_relaxed);
        counters->miss_message_bytes.fetch_add(message_bytes, std::memory_order_relaxed);
        const auto ghost = ghost_index_.find(key);
        const bool hot = ghost != ghost_index_.end();
        if (hot) erase_ghost(ghost->second);
        while (index_.size() >= config_.max_fields && evict_field(counters)) {}
        const auto base = entry_base_bytes(key);
        if (index_.size() >= config_.max_fields || !make_room(base, counters)) bypass = true;
        else {
          promise = std::make_shared<std::promise<Result>>();
          future = promise->get_future().share();
          auto& list = hot ? protected_ : probation_;
          list.push_front({key, future, false, hot, base});
          try { index_.emplace(key, list.begin()); }
          catch (...) { list.pop_front(); throw; }
          memory_usage_ += base;
          if (!hot) probation_usage_ += base;
        }
      }
    }
    if (bypass) return loader();
    if (!promise) {
      auto result = future.get();
      return result.ok() ? result : loader(); // Retry failures with this source.
    }
    auto result = loader();
    try { promise->set_value(result); }
    catch (...) {
      std::lock_guard<std::mutex> lock(mutex_);
      auto found = index_.find(key);
      if (found != index_.end() && !found->second->ready) {
        auto entry = found->second;
        memory_usage_ -= entry->bytes;
        if (!entry->hot) probation_usage_ -= entry->bytes;
        auto& list = entry->hot ? protected_ : probation_;
        list.erase(entry); index_.erase(found);
      }
      throw;
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto found = index_.find(key);
      if (found != index_.end()) {
        auto entry = found->second;
        auto& list = entry->hot ? protected_ : probation_;
        const auto bytes = result.ok() ? entry_bytes(*entry, *result.value()) : 0;
        const auto added = bytes > entry->bytes ? bytes - entry->bytes : 0;
        if (!result.ok() || !make_room(added, counters)) {
          memory_usage_ -= entry->bytes;
          if (!entry->hot) probation_usage_ -= entry->bytes;
          list.erase(entry); index_.erase(found);
        } else {
          entry->ready = true;
          memory_usage_ += added;
          if (!entry->hot) probation_usage_ += added;
          entry->bytes = bytes;
          counters->loaded_bytes.fetch_add(entry->bytes, std::memory_order_relaxed);
          counters->loaded_field_bytes.fetch_add(result.value()->memory_bytes(), std::memory_order_relaxed);
          trim(counters);
        }
      }
    }
    return result;
  }

  void erase_alias(Aliases::iterator it, const std::shared_ptr<GribNativeCacheCounters>& counters) {
    memory_usage_ -= it->bytes; alias_usage_ -= it->bytes;
    alias_index_.erase(it->key); aliases_.erase(it);
    counters->alias_evictions.fetch_add(1, std::memory_order_relaxed);
  }
  bool evict_alias(const std::shared_ptr<GribNativeCacheCounters>& counters) {
    for (auto it = aliases_.end(); it != aliases_.begin();) {
      --it; if (it->ready) { erase_alias(it, counters); return true; }
    }
    return false;
  }
  void erase_ghost(std::list<std::string>::iterator it) {
    const auto bytes = ghost_bytes(*it);
    memory_usage_ -= bytes; ghost_usage_ -= bytes;
    ghost_index_.erase(*it); ghosts_.erase(it);
  }
  void add_ghost(const std::string& key) {
    try {
      if (ghost_index_.find(key) != ghost_index_.end()) return;
      ghosts_.push_front(key);
      try { ghost_index_.emplace(key, ghosts_.begin()); }
      catch (...) { ghosts_.pop_front(); return; }
      auto bytes = ghost_bytes(ghosts_.front());
      memory_usage_ += bytes; ghost_usage_ += bytes;
      while (ghosts_.size() > ghost_limit) erase_ghost(std::prev(ghosts_.end()));
    } catch (const std::bad_alloc&) {} // Optional history cannot make a query fail.
  }
  bool evict_field(const std::shared_ptr<GribNativeCacheCounters>& counters) {
    auto find_ready = [](Fields& list) {
      for (auto it = list.end(); it != list.begin();) { --it; if (it->ready) return it; }
      return list.end();
    };
    // 2Q: one-use entries enter a FIFO probation queue. Only a second resident
    // reference or a bounded ghost hit enters the protected LRU queue.
    bool cold = probation_usage_ > config_.memory_budget_bytes / 4 ||
      probation_.size() > config_.max_fields / 4;
    auto& first = cold ? probation_ : protected_;
    auto& second = cold ? protected_ : probation_;
    auto victim = find_ready(first);
    Fields* list = &first;
    if (victim == first.end()) { victim = find_ready(second); list = &second; }
    if (victim == list->end()) return false;
    memory_usage_ -= victim->bytes;
    if (!victim->hot) probation_usage_ -= victim->bytes;
    index_.erase(victim->key);
    auto key = std::move(victim->key); list->erase(victim);
    counters->evictions.fetch_add(1, std::memory_order_relaxed);
    add_ghost(key);
    return true;
  }
  bool make_room(std::size_t added, const std::shared_ptr<GribNativeCacheCounters>& counters) {
    if (added > config_.memory_budget_bytes) return false;
    while (added > config_.memory_budget_bytes - memory_usage_) {
      if (!ghosts_.empty()) { erase_ghost(std::prev(ghosts_.end())); continue; }
      if (alias_usage_ > config_.memory_budget_bytes / 4 && evict_alias(counters)) continue;
      if (evict_field(counters)) continue;
      if (!evict_alias(counters)) return false;
    }
    return true;
  }
  void trim(const std::shared_ptr<GribNativeCacheCounters>& counters) {
    while (aliases_.size() > alias_limit && evict_alias(counters)) {}
    // Management data is included in the same retention budget. Cap it first
    // when necessary so unusually large fee traces cannot starve native data.
    while (alias_usage_ + ghost_usage_ > config_.memory_budget_bytes / 4) {
      if (!ghosts_.empty()) erase_ghost(std::prev(ghosts_.end()));
      else if (!evict_alias(counters)) break;
    }
    while (index_.size() > config_.max_fields || memory_usage_ > config_.memory_budget_bytes) {
      if (evict_field(counters)) continue;
      if (!ghosts_.empty()) { erase_ghost(std::prev(ghosts_.end())); continue; }
      if (!evict_alias(counters)) break;
    }
  }

  GribDecodedFieldCacheConfig config_;
  std::mutex mutex_;
  std::size_t memory_usage_{0}, probation_usage_{0}, alias_usage_{0}, ghost_usage_{0};
  Fields probation_, protected_;
  std::map<std::string, Fields::iterator> index_;
  Aliases aliases_;
  std::map<AliasKey, Aliases::iterator> alias_index_;
  std::list<std::string> ghosts_;
  std::map<std::string, std::list<std::string>::iterator> ghost_index_;
};

class GribDecodeExecutor {
public:
  using Result = QueryResult<GribGridResult>;

  struct Submission {
    std::future<Result> future;
    std::shared_ptr<void> permit;
  };

  explicit GribDecodeExecutor(std::size_t worker_count)
    : worker_count_(std::max<std::size_t>(1, worker_count)) {
    workers_.reserve(worker_count_);
    for (std::size_t index = 0; index < worker_count_; ++index) {
      workers_.emplace_back([this] { run(); });
    }
  }

  ~GribDecodeExecutor() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopping_ = true;
    }
    ready_.notify_all();
    for (auto& worker : workers_)
      if (worker.joinable()) worker.join();
  }

  std::size_t concurrency() const { return worker_count_; }

  std::optional<Submission> try_submit(std::function<Result()> function) {
    auto outstanding = outstanding_->load(std::memory_order_relaxed);
    while (outstanding < outstanding_limit_) {
      if (outstanding_->compare_exchange_weak(outstanding, outstanding + 1,
          std::memory_order_acq_rel))
        break;
    }
    if (outstanding >= outstanding_limit_) return std::nullopt;
    auto permit = std::shared_ptr<void>(nullptr,
      [outstanding = outstanding_](void*) {
        outstanding->fetch_sub(1, std::memory_order_acq_rel);
      });
    auto task = std::make_shared<std::packaged_task<Result()>>(
      std::move(function));
    auto future = task->get_future();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (stopping_)
        throw std::runtime_error("GRIB decode executor is stopping");
      tasks_.push_back([task] { (*task)(); });
    }
    ready_.notify_one();
    return Submission{std::move(future), std::move(permit)};
  }

private:
  void run() {
    for (;;) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        ready_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });
        if (stopping_ && tasks_.empty()) return;
        task = std::move(tasks_.front());
        tasks_.pop_front();
      }
      task();
    }
  }

  std::size_t worker_count_;
  const std::size_t outstanding_limit_{worker_count_ * 2};
  std::shared_ptr<std::atomic_size_t> outstanding_{
    std::make_shared<std::atomic_size_t>(0)};
  std::mutex mutex_;
  std::condition_variable ready_;
  std::deque<std::function<void()>> tasks_;
  std::vector<std::thread> workers_;
  bool stopping_{false};
};

namespace {

std::size_t default_decode_worker_count() {
  const auto detected = static_cast<std::size_t>(
    std::thread::hardware_concurrency());
  return std::max<std::size_t>(2,
    std::min<std::size_t>(4, detected == 0 ? 2 : detected));
}


enum class GribPlanKind { BinaryView, MessageRows, DenseGrid };
enum class PredicateTruth { False, True, Unknown };

class GribEncodedMessage final : public DecodedFragment {
public:
  explicit GribEncodedMessage(std::size_t size)
    : bytes(new std::uint8_t[size]), size(size) {}
  std::uint64_t resident_bytes() const override {
    return sizeof(GribEncodedMessage) + size;
  }
  std::unique_ptr<std::uint8_t[]> bytes;
  std::size_t size;
  std::array<char, 64> digest{};
};

std::string encoded_message_key(const QuerySource& source,
    const grib_utils::MsgInfo& message) {
  const auto& m = source.manifest();
  return "grib/encoded-message/1:" + std::to_string(source.storage_namespace()) +
    ":" + std::to_string(m.key.path.size()) + ":" + m.key.path + ":" +
    std::to_string(m.key.version) + ":" + std::to_string(m.revision) + ":" +
    std::to_string(m.payload_size) + ":" + std::to_string(m.layout_size) +
    ":" + std::to_string(message.off) + ":" + std::to_string(message.len);
}

class GribAdapterExecutionPlan final : public AdapterExecutionPlan {
public:
  explicit GribAdapterExecutionPlan(LogicalSchema output_schema)
    : schema(std::move(output_schema)) {}

  StructuredBlobVersionKey source;
  std::uint64_t storage_namespace{0}, revision{0}, payload_size{0}, layout_size{0};
  GribPlanKind kind{GribPlanKind::MessageRows};
  bool offset_only{false};
  GribTypedOutputPlan output;
  std::vector<DomainSlice> slices;
  LogicalSchema schema;
  std::shared_ptr<const GribParsedIndex> index;
  std::vector<std::size_t> message_indices;
  std::shared_ptr<const std::vector<std::size_t>> hinted_messages;
  const std::vector<std::size_t>& messages() const {
    return hinted_messages ? *hinted_messages : message_indices;
  }

  QueryStatus visit_unit_locators(
      const std::function<QueryStatus(const FormatUnitLocator&)>& visitor) const override {
    if (kind != GribPlanKind::MessageRows || !offset_only)
      return QueryStatus::failure(QueryError::UnsupportedCapability,
        QueryPhase::Planning, "GRIB unit discovery requires offset-only metadata");
    if (!visitor)
      return QueryStatus::failure(QueryError::InvalidQuery,
        QueryPhase::Planning, "GRIB unit discovery requires a visitor");
    for (const auto ordinal : messages()) {
      const auto& message = index->messages[ordinal].info;
      auto result = visitor({"message:" + std::to_string(message.off) + ":" +
        std::to_string(message.len), message.off, message.len});
      if (!result.ok()) return result;
    }
    return QueryStatus::success();
  }
};

QueryStatus adapter_failure(QueryError error, QueryPhase phase,
    std::string message, const QuerySource* source = nullptr,
    std::optional<std::string> field_id = std::nullopt) {
  QueryErrorDetail detail;
  detail.operation = "grib-format-query-adapter";
  detail.field_id = std::move(field_id);
  if (source) {
    const auto& manifest = source->manifest();
    detail.source = QuerySourceReference{manifest.key.path,
      manifest.key.version, manifest.format_id.value_or("")};
  }
  return QueryStatus::failure(error, phase, std::move(message),
    std::move(detail));
}

template<typename T>
QueryResult<T> adapter_failure_result(QueryError error, QueryPhase phase,
    std::string message, const QuerySource* source = nullptr,
    std::optional<std::string> field_id = std::nullopt) {
  return QueryResult<T>::failure(adapter_failure(error, phase,
    std::move(message), source, std::move(field_id)));
}

bool is_grid_field(const std::string& field_id) {
  using namespace grib_query_fields;
  return field_id == kLatitude || field_id == kLongitude ||
    field_id == kValue;
}

PredicateTruth logical_not(PredicateTruth value) {
  if (value == PredicateTruth::True) return PredicateTruth::False;
  if (value == PredicateTruth::False) return PredicateTruth::True;
  return PredicateTruth::Unknown;
}

PredicateTruth logical_and(PredicateTruth left, PredicateTruth right) {
  if (left == PredicateTruth::False || right == PredicateTruth::False)
    return PredicateTruth::False;
  if (left == PredicateTruth::Unknown || right == PredicateTruth::Unknown)
    return PredicateTruth::Unknown;
  return PredicateTruth::True;
}

PredicateTruth logical_or(PredicateTruth left, PredicateTruth right) {
  if (left == PredicateTruth::True || right == PredicateTruth::True)
    return PredicateTruth::True;
  if (left == PredicateTruth::Unknown || right == PredicateTruth::Unknown)
    return PredicateTruth::Unknown;
  return PredicateTruth::False;
}

QueryResult<FragmentLease> load_encoded_message(
    const std::shared_ptr<QueryRuntime>& runtime,
    const std::shared_ptr<const QuerySource>& producer,
    const grib_utils::MsgInfo& range, CancellationToken cancellation = {},
    std::shared_ptr<GribNativeCacheCounters> counters = {}) {
  if (range.len > std::numeric_limits<std::size_t>::max() ||
      range.len > std::numeric_limits<std::uint64_t>::max() - sizeof(GribEncodedMessage))
    return QueryResult<FragmentLease>::failure(adapter_failure(QueryError::ResourceExhausted,
      QueryPhase::Execution, "GRIB encoded message exceeds addressable memory", producer.get()));
  return runtime->cache().get(encoded_message_key(*producer, range),
      {range.len + sizeof(GribEncodedMessage), range.len},
      [owned = producer, range, counters = std::move(counters)](const FragmentLoadContext& load)
          -> QueryResult<std::shared_ptr<const DecodedFragment>> {
        using Result = QueryResult<std::shared_ptr<const DecodedFragment>>;
        if (load.cancelled())
          return Result::failure(adapter_failure(QueryError::Cancelled,
            QueryPhase::Execution, "GRIB binary message load cancelled", owned.get()));
        auto admitted = load.record_io(range.len);
        if (!admitted.ok()) return Result::failure(admitted);
        load.begin_payload_read();
        struct ReadAccounting {
          const FragmentLoadContext& load;
          std::uint64_t bytes;
          std::chrono::steady_clock::time_point begin{std::chrono::steady_clock::now()};
          ~ReadAccounting() {
            load.record_payload_read(bytes, 1, std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now() - begin).count());
          }
        };
        auto bytes = [&] {
          ReadAccounting accounting{load, range.len};
          return GribQueryKernel::read_source_message(*owned, range);
        }();
        if (!bytes.ok()) return Result::failure(bytes.status());
        if (load.cancelled())
          return Result::failure(adapter_failure(QueryError::Cancelled,
            QueryPhase::Execution, "GRIB binary message load cancelled", owned.get()));
        // Use an exact allocation: the transport vector's spare capacity is
        // not part of the fragment reservation. Its bytes are temporary.
        auto fragment = std::make_shared<GribEncodedMessage>(bytes.value().size());
        std::copy(bytes.value().begin(), bytes.value().end(), fragment->bytes.get());
        // The digest covers the actual verified encoded message, not its
        // layout or the surrounding file. The encoded cache computes it once.
        std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
        unsigned digest_length = 0;
        if (EVP_Digest(fragment->bytes.get(), fragment->size, digest.data(),
            &digest_length, EVP_sha256(), nullptr) != 1 || digest_length != 32)
          return Result::failure(adapter_failure(QueryError::Internal,
            QueryPhase::Execution, "GRIB message SHA-256 failed", owned.get()));
        static constexpr char hex[] = "0123456789abcdef";
        for (std::size_t i = 0; i < digest_length; ++i) {
          fragment->digest[2 * i] = hex[digest[i] >> 4];
          fragment->digest[2 * i + 1] = hex[digest[i] & 15];
        }
        if (counters) counters->hashes.fetch_add(1, std::memory_order_relaxed);
        load.record_copy(range.len);
        return Result::success(std::move(fragment));
      }, cancellation);
}

// The decoder can access only the admitted encoded-message lease. Native-grid
// allocations and the decoder's message copy retain their existing GRIB model;
// storage block amplification occurs solely in load_encoded_message's worker.
class GribLeasedMessageSource final : public QuerySource {
public:
  GribLeasedMessageSource(const QuerySource& source, grib_utils::MsgInfo message,
      FragmentLease lease) : source_(source), message_(message), lease_(std::move(lease)),
      encoded_(lease_.as<GribEncodedMessage>()) {}
  const StructuredBlobManifest& manifest() const override { return source_.manifest(); }
  std::uint64_t storage_namespace() const override { return source_.storage_namespace(); }
  QueryResult<StructuredBlobBytes> read_payload(std::uint64_t offset, std::uint64_t length) const override {
    if (!encoded_ || encoded_->size != message_.len || offset < message_.off ||
        offset - message_.off > message_.len || length > message_.len - (offset - message_.off))
      return QueryResult<StructuredBlobBytes>::failure(adapter_failure(QueryError::ReadFailure,
        QueryPhase::Storage, "GRIB leased message read is outside admitted range", &source_));
    const auto* begin = encoded_->bytes.get() + (offset - message_.off);
    return QueryResult<StructuredBlobBytes>::success({begin, begin + length});
  }
  QueryResult<StructuredBlobBytes> read_layout(std::uint64_t, std::uint64_t) const override {
    return QueryResult<StructuredBlobBytes>::failure(adapter_failure(QueryError::ReadFailure,
      QueryPhase::Storage, "GRIB native decoder has no layout read capability", &source_));
  }
private:
  const QuerySource& source_;
  grib_utils::MsgInfo message_;
  FragmentLease lease_;
  std::shared_ptr<const GribEncodedMessage> encoded_;
};

template<typename Integer>
std::optional<Integer> parse_integer(const std::string& text) {
  Integer value{};
  const auto parsed = std::from_chars(text.data(),
    text.data() + text.size(), value);
  if (text.empty() || parsed.ec != std::errc{} ||
      parsed.ptr != text.data() + text.size())
    return std::nullopt;
  return value;
}

QueryResult<PredicateTruth> equal_label(const GribIndexedMessage& message,
    const FieldRef& field, const LogicalValue& expected,
    const QuerySource& source) {
  if (expected.is_null())
    return QueryResult<PredicateTruth>::success(PredicateTruth::Unknown);

  using namespace grib_query_fields;
  if (field.field_id == kOffset) {
    if (expected.type() != LogicalTypeId::UInt64)
      return adapter_failure_result<PredicateTruth>(QueryError::TypeMismatch,
        QueryPhase::Planning, "GRIB predicate literal has the wrong type",
        &source, field.field_id);
    return QueryResult<PredicateTruth>::success(message.info.off ==
        std::get<std::uint64_t>(expected.value())
      ? PredicateTruth::True : PredicateTruth::False);
  }
  if (field.field_id == kEdition) {
    if (expected.type() != LogicalTypeId::Int32)
      return adapter_failure_result<PredicateTruth>(QueryError::TypeMismatch,
        QueryPhase::Planning, "GRIB predicate literal has the wrong type",
        &source, field.field_id);
    return QueryResult<PredicateTruth>::success(message.edition ==
        std::get<std::int32_t>(expected.value())
      ? PredicateTruth::True : PredicateTruth::False);
  }

  const std::optional<std::string>* text = nullptr;
  if (field.field_id == kShortName) text = &message.short_name;
  else if (field.field_id == kTypeOfLevel) text = &message.type_of_level;
  else if (field.field_id == kStepRange) text = &message.step_range;
  else if (field.field_id == kUnit) text = &message.unit;
  if (text) {
    if (expected.type() != LogicalTypeId::Utf8)
      return adapter_failure_result<PredicateTruth>(QueryError::TypeMismatch,
        QueryPhase::Planning, "GRIB predicate literal has the wrong type",
        &source, field.field_id);
    if (!*text)
      return QueryResult<PredicateTruth>::success(PredicateTruth::Unknown);
    return QueryResult<PredicateTruth>::success(**text ==
        std::get<std::string>(expected.value())
      ? PredicateTruth::True : PredicateTruth::False);
  }

  const std::optional<std::int64_t>* integer = nullptr;
  if (field.field_id == kLevel) integer = &message.level;
  else if (field.field_id == kStep) integer = &message.step;
  else if (field.field_id == kParamId) integer = &message.param_id;
  else if (field.field_id == kValidityDate) integer = &message.validity_date;
  else if (field.field_id == kValidityTime) integer = &message.validity_time;
  if (integer) {
    if (expected.type() != LogicalTypeId::Int64)
      return adapter_failure_result<PredicateTruth>(QueryError::TypeMismatch,
        QueryPhase::Planning, "GRIB predicate literal has the wrong type",
        &source, field.field_id);
    if (!*integer) {
      if (field.field_id == kParamId || field.field_id == kValidityDate ||
          field.field_id == kValidityTime)
        return adapter_failure_result<PredicateTruth>(QueryError::CorruptLayout,
          QueryPhase::Planning,
          "GRIB layout lacks the indexed field required by this predicate",
          &source, field.field_id);
      return QueryResult<PredicateTruth>::success(PredicateTruth::Unknown);
    }
    return QueryResult<PredicateTruth>::success(**integer ==
        std::get<std::int64_t>(expected.value())
      ? PredicateTruth::True : PredicateTruth::False);
  }

  return adapter_failure_result<PredicateTruth>(
      QueryError::UnsupportedCapability, QueryPhase::Planning,
      "GRIB message predicate is not layout-addressable", &source,
      field.field_id);
}

QueryResult<PredicateTruth> evaluate_predicate(
    const QueryExpressionPtr& expression, const GribIndexedMessage& message,
    const QuerySource& source) {
  if (!expression)
    return QueryResult<PredicateTruth>::success(PredicateTruth::True);
  return std::visit([&](const auto& node) -> QueryResult<PredicateTruth> {
    using Node = std::decay_t<decltype(node)>;
    if constexpr (std::is_same_v<Node, ComparisonExpression>) {
      if (node.operation != ComparisonOperator::Equal)
        return adapter_failure_result<PredicateTruth>(
          QueryError::UnsupportedCapability, QueryPhase::Planning,
          "GRIB supports only equality message predicates", &source,
          node.field.field_id);
      return equal_label(message, node.field, node.value, source);
    } else if constexpr (std::is_same_v<Node, InExpression>) {
      PredicateTruth result = PredicateTruth::False;
      for (const auto& candidate : node.values) {
        auto equal = equal_label(message, node.field, candidate, source);
        if (!equal.ok()) return equal;
        result = logical_or(result, equal.value());
      }
      if (node.negated) result = logical_not(result);
      return QueryResult<PredicateTruth>::success(result);
    } else if constexpr (std::is_same_v<Node, NullExpression>) {
      return adapter_failure_result<PredicateTruth>(
        QueryError::UnsupportedCapability, QueryPhase::Planning,
        "GRIB does not publish null-predicate capability", &source,
        node.field.field_id);
    } else {
      if (node.operation == LogicalOperator::Not) {
        auto operand = evaluate_predicate(node.operands.front(), message,
          source);
        if (!operand.ok()) return operand;
        return QueryResult<PredicateTruth>::success(
          logical_not(operand.value()));
      }
      PredicateTruth result = node.operation == LogicalOperator::And
        ? PredicateTruth::True : PredicateTruth::False;
      for (const auto& operand_expression : node.operands) {
        auto operand = evaluate_predicate(operand_expression, message,
          source);
        if (!operand.ok()) return operand;
        result = node.operation == LogicalOperator::And
          ? logical_and(result, operand.value())
          : logical_or(result, operand.value());
      }
      return QueryResult<PredicateTruth>::success(result);
    }
  }, expression->node);
}

std::size_t nonempty_layout_records(const std::string& records) {
  std::size_t count = 0;
  std::size_t start = 0;
  while (start < records.size()) {
    const auto newline = records.find('\n', start);
    const auto end = newline == std::string::npos ? records.size() : newline;
    if (end != start) ++count;
    start = newline == std::string::npos ? records.size() : newline + 1;
  }
  return count;
}

std::optional<std::string> take_label(
    std::map<std::string, std::string>& labels, const char* name) {
  const auto found = labels.find(name);
  if (found == labels.end()) return std::nullopt;
  return std::move(found->second);
}

template<typename Integer>
QueryResult<std::optional<Integer>> take_integer_label(
    std::map<std::string, std::string>& labels, const char* name,
    bool required, const QuerySource& source) {
  const auto found = labels.find(name);
  if (found == labels.end()) {
    if (!required)
      return QueryResult<std::optional<Integer>>::success(std::nullopt);
    return adapter_failure_result<std::optional<Integer>>(
      QueryError::CorruptLayout, QueryPhase::Planning,
      std::string("GRIB layout omits required label ") + name, &source);
  }
  const auto value = parse_integer<Integer>(found->second);
  if (!value)
    return adapter_failure_result<std::optional<Integer>>(
      QueryError::CorruptLayout, QueryPhase::Planning,
      std::string("GRIB layout has invalid integer label ") + name, &source);
  return QueryResult<std::optional<Integer>>::success(*value);
}

QueryResult<std::shared_ptr<const GribParsedIndex>> parse_index(
    const QuerySource& source, const std::string& records) {
  auto matches = grib_utils::find_all_msg_matches(records, {});
  if (matches.size() != nonempty_layout_records(records))
    return adapter_failure_result<std::shared_ptr<const GribParsedIndex>>(
      QueryError::CorruptLayout, QueryPhase::Planning,
      "GRIB layout contains a malformed message record", &source);

  auto index = std::make_shared<GribParsedIndex>();
  index->messages.reserve(matches.size());
  for (auto& match : matches) {
    if (!match.info.ok || match.info.len == 0 ||
        match.info.off > source.manifest().payload_size ||
        match.info.len > source.manifest().payload_size - match.info.off)
      return adapter_failure_result<std::shared_ptr<const GribParsedIndex>>(
        QueryError::CorruptLayout, QueryPhase::Planning,
        "GRIB layout contains an invalid message range", &source);

    auto level = take_integer_label<std::int64_t>(match.labels, "level",
      false, source);
    if (!level.ok())
      return QueryResult<std::shared_ptr<const GribParsedIndex>>::failure(
        level.status());
    auto step = take_integer_label<std::int64_t>(match.labels, "step",
      false, source);
    if (!step.ok())
      return QueryResult<std::shared_ptr<const GribParsedIndex>>::failure(
        step.status());
    auto edition = take_integer_label<std::int32_t>(match.labels, "edition",
      true, source);
    if (!edition.ok())
      return QueryResult<std::shared_ptr<const GribParsedIndex>>::failure(
        edition.status());
    auto param_id = take_integer_label<std::int64_t>(match.labels, "paramId",
      false, source);
    if (!param_id.ok())
      return QueryResult<std::shared_ptr<const GribParsedIndex>>::failure(
        param_id.status());
    auto validity_date = take_integer_label<std::int64_t>(match.labels,
      "validityDate", false, source);
    if (!validity_date.ok())
      return QueryResult<std::shared_ptr<const GribParsedIndex>>::failure(
        validity_date.status());
    auto validity_time = take_integer_label<std::int64_t>(match.labels,
      "validityTime", false, source);
    if (!validity_time.ok())
      return QueryResult<std::shared_ptr<const GribParsedIndex>>::failure(
        validity_time.status());

    GribIndexedMessage message;
    message.info = match.info;
    message.short_name = take_label(match.labels, "shortName");
    message.type_of_level = take_label(match.labels, "typeOfLevel");
    message.level = level.value();
    message.step = step.value();
    message.step_range = take_label(match.labels, "stepRange");
    message.edition = *edition.value();
    message.unit = take_label(match.labels, "unit");
    message.param_id = param_id.value();
    message.validity_date = validity_date.value();
    message.validity_time = validity_time.value();
    index->messages.push_back(std::move(message));
  }

  index->memory_bytes = sizeof(GribParsedIndex) +
    index->messages.capacity() * sizeof(GribIndexedMessage);
  for (const auto& message : index->messages) {
    if (message.short_name)
      index->memory_bytes += message.short_name->capacity();
    if (message.type_of_level)
      index->memory_bytes += message.type_of_level->capacity();
    if (message.step_range)
      index->memory_bytes += message.step_range->capacity();
    if (message.unit) index->memory_bytes += message.unit->capacity();
  }
  return QueryResult<std::shared_ptr<const GribParsedIndex>>::success(
    std::move(index));
}

QueryResult<std::vector<std::size_t>> prepare_messages(
    const QuerySource& source, const GribParsedIndex& index,
    const QueryExpressionPtr& predicate) {
  std::vector<std::size_t> selected;
  selected.reserve(index.messages.size());
  for (std::size_t position = 0; position < index.messages.size(); ++position) {
    query_profile_count("grib.message.predicate_evaluations");
    auto included = evaluate_predicate(predicate, index.messages[position],
      source);
    if (!included.ok())
      return QueryResult<std::vector<std::size_t>>::failure(
        included.status());
    if (included.value() == PredicateTruth::True)
      selected.push_back(position);
  }
  return QueryResult<std::vector<std::size_t>>::success(
    std::move(selected));
}

using GribDomainKey = EqualityCompositeKey<2, 4>;
constexpr std::array<std::string_view, 6> grib_domain_fields{{
  grib_query_fields::kShortName, grib_query_fields::kTypeOfLevel,
  grib_query_fields::kLevel, grib_query_fields::kStep,
  grib_query_fields::kValidityDate, grib_query_fields::kValidityTime}};

// Metadata discovery is deliberately the first certified domain. Its complete
// schema and output validation are independent of message payloads. Grid and
// binary execution retain their normal exact-source planning path.
std::optional<GribDomainKey> grib_domain_key(const QueryFragment& fragment) {
  if (fragment.physical_array || !fragment.slices.empty() ||
      fragment.output.mode != QueryFragment::ResultMode::LogicalValues ||
      fragment.output.interpolation || !fragment.output.type_overrides.empty() ||
      fragment.projection.empty()) return std::nullopt;
  using namespace grib_query_fields;
  constexpr std::array<std::string_view, 11> metadata_fields{{kShortName,
    kTypeOfLevel, kLevel, kStep, kStepRange, kEdition, kUnit, kParamId,
    kValidityDate, kValidityTime, kOffset}};
  for (std::size_t i = 0; i < fragment.projection.size(); ++i) {
    const auto& field = fragment.projection[i].field_id;
    if (std::find(metadata_fields.begin(), metadata_fields.end(),
        std::string_view(field)) == metadata_fields.end()) return std::nullopt;
    for (std::size_t previous = 0; previous < i; ++previous)
      if (fragment.projection[previous].field_id == field) return std::nullopt;
  }
  return compile_complete_equality_key<2, 4>(fragment.predicate, grib_domain_fields);
}

bool same_grib_limits(const QueryLimits& a, const QueryLimits& b) {
  return a.max_sources == b.max_sources && a.max_ranges == b.max_ranges &&
    a.max_layout_bytes == b.max_layout_bytes &&
    a.max_payload_bytes == b.max_payload_bytes &&
    a.max_result_rows == b.max_result_rows &&
    a.target_batch_rows == b.target_batch_rows &&
    a.target_batch_bytes == b.target_batch_bytes;
}

// Immutable exact-source binding only. A domain never retains a source,
// request context, request ledger or execution capability.
struct GribDomainSource {
  std::shared_ptr<const GribParsedIndex> index;
  StructuredBlobVersionKey key;
  std::uint64_t storage_namespace{0}, revision{0}, payload_size{0}, layout_size{0};
  std::optional<std::string> layout_identity;

  bool matches(const QuerySource& source) const {
    const auto& manifest = source.manifest();
    return storage_namespace == source.storage_namespace() && key == manifest.key &&
      revision == manifest.revision && payload_size == manifest.payload_size &&
      layout_size == manifest.layout_size && manifest.format_id &&
      *manifest.format_id == "grib";
  }
};

class GribMessageSelection final : public AdapterPlanningState {
public:
  std::shared_ptr<const GribDomainSource> owner;
  std::array<std::string, 2> text;
  std::array<std::int64_t, 4> number{};
  QueryLimits limits;
  std::shared_ptr<const std::vector<std::size_t>> messages;

  bool matches(const GribDomainKey& key) const {
    return number == key.number && text[0] == key.text[0] && text[1] == key.text[1];
  }
};

class GribMessageDomainIndex final : public AdapterDomainIndex {
public:
  using Locator = std::pair<std::size_t, std::size_t>;
  using Index = OrderedEqualityIndex<GribDomainKey, Locator>;

  std::vector<std::shared_ptr<const GribDomainSource>> owners;
  Index index;
  std::uint64_t bytes{0}, full_layout_bytes{0};
  QueryLimits certified_limits;

  std::uint64_t retained_bytes() const override { return bytes; }
  QueryResult<std::optional<AdapterDomainSelection>> select(
      const QueryFragment& fragment, const QueryLimits& limits) const override {
    using Result = QueryResult<std::optional<AdapterDomainSelection>>;
    const auto key = grib_domain_key(fragment);
    if (!key || owners.size() > limits.max_sources ||
        full_layout_bytes > limits.max_layout_bytes ||
        !same_grib_limits(limits, certified_limits))
      return Result::success(std::nullopt);
    const auto range = index.equal_range(*key);
    AdapterDomainSelection matches;
    // Postings retain both source and message ordinals. The fresh hint owns
    // its predicate strings; it never borrows the query AST or an index key.
    for (auto it = range.first; it != range.second;) {
      const auto source = it->locator.first;
      auto end = it;
      while (end != range.second && end->locator.first == source) ++end;
      auto hint = std::make_shared<GribMessageSelection>();
      hint->owner = owners[source];
      hint->text = {std::string(key->text[0]), std::string(key->text[1])};
      hint->number = key->number;
      hint->limits = limits;
      auto messages = std::make_shared<std::vector<std::size_t>>();
      messages->reserve(static_cast<std::size_t>(end - it));
      for (; it != end; ++it) messages->push_back(it->locator.second);
      hint->messages = std::move(messages);
      matches.push_back({source, std::move(hint)});
    }
    return Result::success(std::move(matches));
  }
};

bool bind_grib_domain_key(const GribIndexedMessage& message, GribDomainKey& key) {
  // A partial tuple may mean Unknown or a predicate-dependent CorruptLayout.
  // Decline the whole domain rather than hiding a nonmatching record's error.
  if (!message.short_name || !message.type_of_level || !message.level ||
      !message.step || !message.validity_date || !message.validity_time) return false;
  key.text = {*message.short_name, *message.type_of_level};
  key.number = {*message.level, *message.step, *message.validity_date,
    *message.validity_time};
  return true;
}

QueryResult<LogicalSchema> output_schema(const QueryFragment& fragment,
    const GribTypedOutputPlan& output, const QuerySource& source) {
  auto base = make_grib_logical_schema();
  if (!base.ok()) return base;
  std::vector<LogicalField> fields;
  fields.reserve(fragment.projection.size());
  for (const auto& reference : fragment.projection) {
    const auto* field = base.value().find(reference.field_id);
    if (!field)
      return adapter_failure_result<LogicalSchema>(QueryError::UnknownField,
        QueryPhase::Planning, "GRIB projection references an unknown field",
        &source, reference.field_id);
    fields.push_back(*field);
    if (reference.field_id == grib_query_fields::kValue)
      fields.back().type.id = output.value_type == grib_utils::DataDType::F64
        ? LogicalTypeId::Float64 : LogicalTypeId::Float32;
  }
  return LogicalSchema::create(std::move(fields));
}

QueryResult<LogicalArray> repeated_attribute(const LogicalField& field,
    const GribIndexedMessage& message, std::size_t count,
    const QuerySource& source) {
  using namespace grib_query_fields;
  std::vector<std::uint8_t> validity;
  if (field.type.id == LogicalTypeId::Utf8) {
    const std::optional<std::string>* value = nullptr;
    if (field.field_id == kShortName) value = &message.short_name;
    else if (field.field_id == kTypeOfLevel) value = &message.type_of_level;
    else if (field.field_id == kStepRange) value = &message.step_range;
    else if (field.field_id == kUnit) value = &message.unit;
    if (!value)
      return adapter_failure_result<LogicalArray>(QueryError::Internal,
        QueryPhase::Execution, "GRIB output field has no typed index value",
        &source, field.field_id);
    if (!*value) validity.assign((count + 7) / 8, 0);
    return QueryResult<LogicalArray>::success({field.type,
      std::vector<std::string>(count, *value ? **value : ""),
      std::move(validity)});
  }
  if (field.type.id == LogicalTypeId::Int32) {
    if (field.field_id != kEdition)
      return adapter_failure_result<LogicalArray>(QueryError::Internal,
        QueryPhase::Execution, "GRIB output field has no typed index value",
        &source, field.field_id);
    return QueryResult<LogicalArray>::success({field.type,
      std::vector<std::int32_t>(count, message.edition), {}});
  }
  if (field.type.id == LogicalTypeId::Int64) {
    const std::optional<std::int64_t>* value = nullptr;
    if (field.field_id == kLevel) value = &message.level;
    else if (field.field_id == kStep) value = &message.step;
    else if (field.field_id == kParamId) value = &message.param_id;
    else if (field.field_id == kValidityDate) value = &message.validity_date;
    else if (field.field_id == kValidityTime) value = &message.validity_time;
    if (!value)
      return adapter_failure_result<LogicalArray>(QueryError::Internal,
        QueryPhase::Execution, "GRIB output field has no typed index value",
        &source, field.field_id);
    if (!*value) validity.assign((count + 7) / 8, 0);
    return QueryResult<LogicalArray>::success({field.type,
      std::vector<std::int64_t>(count, *value ? **value : 0),
      std::move(validity)});
  }
  if (field.type.id == LogicalTypeId::UInt64) {
    if (field.field_id != kOffset)
      return adapter_failure_result<LogicalArray>(QueryError::Internal,
        QueryPhase::Execution, "GRIB output field has no typed index value",
        &source, field.field_id);
    return QueryResult<LogicalArray>::success({field.type,
      std::vector<std::uint64_t>(count, message.info.off), {}});
  }
  return adapter_failure_result<LogicalArray>(QueryError::Internal,
    QueryPhase::Execution, "GRIB layout output type is unsupported", &source,
    field.field_id);
}

template<typename T>
std::vector<T> array_slice(const std::vector<T>& values, std::size_t offset,
    std::size_t count) {
  return {values.begin() + static_cast<std::ptrdiff_t>(offset),
    values.begin() + static_cast<std::ptrdiff_t>(offset + count)};
}

std::vector<double> grid_axis_slice(const std::vector<double>& axis,
    std::size_t row_width, std::size_t offset, std::size_t count,
    bool latitude) {
  std::vector<double> output;
  output.reserve(count);
  for (std::size_t index = offset; index < offset + count; ++index)
    output.push_back(axis[latitude ? index / row_width : index % row_width]);
  return output;
}

// Per-cursor logical reads are reserved even when a native field is already
// cached. The shared producer uses the independent unbudgeted source below.
class GribGridReadBudget {
public:
  explicit GribGridReadBudget(QueryExecutionOptions options) : options_(options) {}
  QueryStatus reserve(const QuerySource& source, std::uint64_t bytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (ranges_ >= options_.max_ranges || bytes > options_.max_payload_bytes - bytes_)
      return adapter_failure(QueryError::ResourceExhausted, QueryPhase::Execution,
        "GRIB grid logical request budget exhausted", &source);
    auto status = source.reserve_logical_payload(bytes);
    if (!status.ok()) return status;
    ++ranges_; bytes_ += bytes;
    return QueryStatus::success();
  }
private:
  QueryExecutionOptions options_;
  std::mutex mutex_;
  std::uint64_t bytes_{0};
  std::size_t ranges_{0};
};

// Match the context of the former decode-executor job. In particular a cursor
// created without a request ledger must not borrow a different next() caller's
// ledger, and producer fee replay must not enter that caller's collectors.
class GribSingleMessageDecodeScope {
public:
  explicit GribSingleMessageDecodeScope(
      const std::shared_ptr<QueryMetadataBudget>& budget)
    : previous_budget_(std::move(active_query_metadata_budget)),
      previous_trace_(active_query_metadata_trace),
      previous_profile_(active_query_profile),
      previous_storage_control_(active_storage_read_control) {
    active_query_metadata_budget = budget;
    active_query_metadata_trace = nullptr;
    active_query_profile = nullptr;
    active_storage_read_control = nullptr;
  }
  ~GribSingleMessageDecodeScope() {
    active_storage_read_control = previous_storage_control_;
    active_query_profile = previous_profile_;
    active_query_metadata_trace = previous_trace_;
    active_query_metadata_budget = std::move(previous_budget_);
  }
  GribSingleMessageDecodeScope(const GribSingleMessageDecodeScope&) = delete;
  GribSingleMessageDecodeScope& operator=(const GribSingleMessageDecodeScope&) = delete;
private:
  std::shared_ptr<QueryMetadataBudget> previous_budget_;
  ScopedQueryMetadataTrace* previous_trace_;
  QueryProfileRecord* previous_profile_;
  const QueryStorageReadControl* previous_storage_control_;
};

QueryResult<GribGridResult> decode_cached_grid(const QuerySource& source,
    const std::shared_ptr<const QuerySource>& producer_source, const GribGridPlan& plan,
    const std::shared_ptr<GribDecodedFieldCache>& cache, GribGridReadBudget& budget,
    const std::shared_ptr<QueryRuntime>& runtime,
    const std::shared_ptr<GribNativeCacheCounters>& counters) {
  auto reserved = budget.reserve(source, plan.message.len);
  if (!reserved.ok()) return QueryResult<GribGridResult>::failure(reserved);
  auto field = cache->get_or_load(source, plan.message, [&] {
    using EncodedResult = GribDecodedFieldCache::EncodedResult;
    auto lease = load_encoded_message(runtime, producer_source, plan.message, {}, counters);
    if (!lease.ok()) return EncodedResult::failure(lease.status());
    const auto encoded = lease.value().as<GribEncodedMessage>();
    if (!encoded)
      return EncodedResult::failure(adapter_failure(QueryError::Internal,
        QueryPhase::Execution, "GRIB encoded cache entry has the wrong type", &source));
    GribDecodedFieldCache::Encoded result;
    result.digest.assign(encoded->digest.data(), encoded->digest.size());
    result.decode = [producer_source, plan, lease = lease.value()] {
      GribLeasedMessageSource admitted(*producer_source, plan.message, lease);
      auto decoded = GribQueryKernel::decode_native_field(admitted, plan);
      if (!decoded.ok()) return GribDecodedFieldCache::Result::failure(decoded.status());
      GribDecodedFieldCache::Field shared =
        std::make_shared<const GribNativeField>(std::move(decoded.value()));
      return GribDecodedFieldCache::Result::success(std::move(shared));
    };
    return EncodedResult::success(std::move(result));
  }, counters);
  if (!field.ok()) return QueryResult<GribGridResult>::failure(field.status());
  return GribQueryKernel::render_grid(source, plan, *field.value());
}

class GribQueryCursor final : public QueryBatchCursor {
public:
  GribQueryCursor(std::shared_ptr<const QuerySource> source,
      std::shared_ptr<const QuerySource> producer_source,
      std::shared_ptr<const GribAdapterExecutionPlan> plan,
      QueryExecutionOptions options,
      std::shared_ptr<GribDecodeExecutor> decode_executor,
      std::shared_ptr<GribDecodedFieldCache> decoded_field_cache,
      std::shared_ptr<QueryRuntime> runtime)
    : source_owner_(std::move(source)), source_(*source_owner_),
      plan_(std::move(plan)), options_(options),
      decode_executor_(std::move(decode_executor)),
      decoded_field_cache_(std::move(decoded_field_cache)),
      producer_source_(std::move(producer_source)), runtime_(std::move(runtime)),
      cancelled_(std::make_shared<std::atomic_bool>(false)),
      metadata_budget_(current_query_metadata_budget()),
      execution_lease_(current_query_execution_lease()),
      grid_budget_(std::make_shared<GribGridReadBudget>(options_)) {
    if (plan_->kind == GribPlanKind::DenseGrid) {
      native_counters_ = std::make_shared<GribNativeCacheCounters>();
      schedule_grid_decodes();
    }
  }

  ~GribQueryCursor() override {
    binary_cancellation_.cancel();
    cancelled_->store(true, std::memory_order_release);
    for (auto& decode : scheduled_decodes_) decode.future.wait();
    report_native_counters();
  }

  const LogicalSchema& schema() const override { return plan_->schema; }

  QueryResult<std::optional<QueryBatch>> next() override {
    if (cancelled_->load(std::memory_order_acquire))
      return adapter_failure_result<std::optional<QueryBatch>>(
        QueryError::Cancelled, QueryPhase::Execution,
        "GRIB query cursor was cancelled", &source_);
    if (plan_->kind == GribPlanKind::DenseGrid) {
      auto result = next_grid();
      if (result.ok() && !result.value()) report_native_counters();
      return result;
    }
    if (message_index_ >= plan_->messages().size())
      return QueryResult<std::optional<QueryBatch>>::success(std::nullopt);
    const auto& message = plan_->index->messages[
      plan_->messages()[message_index_++]];
    return plan_->kind == GribPlanKind::BinaryView
      ? binary_batch(message) : message_batch(message);
  }

  void cancel() override {
    binary_cancellation_.cancel();
    cancelled_->store(true, std::memory_order_release);
  }

private:
  void report_native_counters() noexcept {
    if (!native_counters_) return;
    if (!native_counters_reported_) {
      decoded_field_cache_->report(*native_counters_);
      native_counters_reported_ = true;
    }
  }
  struct ScheduledDecode {
    const GribIndexedMessage* message{nullptr};
    std::future<QueryResult<GribGridResult>> future;
    std::shared_ptr<void> permit;
  };

  GribGridPlan make_grid_plan(const GribIndexedMessage& message) const {
    GribGridPlan grid_plan;
    grid_plan.message = message.info;
    grid_plan.value_type = plan_->output.value_type;
    grid_plan.slices = plan_->slices;
    grid_plan.interpolation = plan_->output.interpolation;
    const auto& manifest = source_.manifest();
    grid_plan.cache_token = std::to_string(source_.storage_namespace()) + ":" +
      manifest.key.path + "@" + std::to_string(manifest.key.version) + ":" +
      std::to_string(manifest.revision) + ":" +
      std::to_string(grid_plan.message.off) + ":typed";
    return grid_plan;
  }

  void schedule_grid_decodes() {
    // A single selected message has no prefetch to overlap. Decode/render on
    // the caller (the public query worker) through next_grid's existing
    // synchronous path rather than handing even native-cache hits to a second
    // executor. Selection, not the total source message count, determines this.
    if (plan_->messages().size() == 1) return;
    while (!cancelled_->load(std::memory_order_acquire) &&
        scheduled_decodes_.size() < decode_executor_->concurrency() &&
        message_index_ < plan_->messages().size()) {
      const auto* message = &plan_->index->messages[
        plan_->messages()[message_index_++]];
      auto grid_plan = make_grid_plan(*message);
      auto cancellation = cancelled_;
      const auto* source = &source_;
      auto cache = decoded_field_cache_;
      auto producer = producer_source_;
      auto metadata_budget = metadata_budget_;
      auto grid_budget = grid_budget_;
      auto execution_lease = execution_lease_;
      auto runtime = runtime_;
      auto counters = native_counters_;
      auto submission = decode_executor_->try_submit(
        [source, grid_plan = std::move(grid_plan), cancellation,
            cache = std::move(cache), producer = std::move(producer),
            metadata_budget = std::move(metadata_budget), grid_budget = std::move(grid_budget),
            execution_lease = std::move(execution_lease), runtime = std::move(runtime),
            counters = std::move(counters)]() mutable {
          ScopedQueryExecutionLease execution_scope(std::move(execution_lease));
          auto metadata_scope = metadata_budget ?
            std::make_unique<ScopedQueryMetadataBudget>(metadata_budget) : nullptr;
          if (cancellation->load(std::memory_order_acquire))
            return QueryResult<GribGridResult>::failure(adapter_failure(
              QueryError::Cancelled, QueryPhase::Execution,
              "GRIB query cursor was cancelled", source));
          return decode_cached_grid(*source, producer, grid_plan, cache, *grid_budget, runtime, counters);
        });
      if (!submission) {
        --message_index_;
        break;
      }
      scheduled_decodes_.push_back({message,
        std::move(submission->future), std::move(submission->permit)});
    }
  }

  template<typename Decode>
  QueryResult<GribGridResult> translate_grid_decode_exceptions(Decode&& decode) {
    try {
      return decode();
    } catch (const std::exception& error) {
      return QueryResult<GribGridResult>::failure(adapter_failure(
        QueryError::Internal, QueryPhase::Execution,
        std::string("GRIB parallel decode failed: ") + error.what(),
        &source_));
    } catch (...) {
      return QueryResult<GribGridResult>::failure(adapter_failure(
        QueryError::Internal, QueryPhase::Execution,
        "GRIB parallel decode failed with an unknown exception",
        &source_));
    }
  }

  QueryResult<std::optional<QueryBatch>> finish_batch(
      std::uint64_t rows, std::vector<LogicalArray> columns,
      std::optional<DomainDescriptor> domain = std::nullopt) const {
    const auto& manifest = source_.manifest();
    auto batch = QueryBatch::create(plan_->schema, rows, std::move(columns),
      std::move(domain), {{manifest.key.path, manifest.key.version,
        manifest.format_id.value_or("")}});
    if (!batch.ok())
      return QueryResult<std::optional<QueryBatch>>::failure(batch.status());
    return QueryResult<std::optional<QueryBatch>>::success(
      std::move(batch.value()));
  }

  QueryResult<std::optional<QueryBatch>> binary_batch(
      const GribIndexedMessage& message) {
    const auto range = message.info;
    if (logical_ranges_ >= options_.max_ranges ||
        range.len > options_.max_payload_bytes - logical_bytes_)
      return adapter_failure_result<std::optional<QueryBatch>>(
        QueryError::ResourceExhausted, QueryPhase::Execution,
        "GRIB binary logical request budget exhausted", &source_);
    if (range.len > std::numeric_limits<std::size_t>::max() ||
        range.len > std::numeric_limits<std::uint64_t>::max() - sizeof(GribEncodedMessage))
      return adapter_failure_result<std::optional<QueryBatch>>(
        QueryError::ResourceExhausted, QueryPhase::Execution,
        "GRIB binary message exceeds addressable memory", &source_);
    auto charged = source_.reserve_logical_payload(range.len);
    if (!charged.ok())
      return QueryResult<std::optional<QueryBatch>>::failure(charged);
    ++logical_ranges_;
    logical_bytes_ += range.len;
    auto loaded = load_encoded_message(runtime_, producer_source_, range, binary_cancellation_);
    if (!loaded.ok())
      return QueryResult<std::optional<QueryBatch>>::failure(loaded.status());
    if (binary_cancellation_.cancelled())
      return adapter_failure_result<std::optional<QueryBatch>>(
        QueryError::Cancelled, QueryPhase::Execution,
        "GRIB query cursor was cancelled", &source_);
    auto fragment = loaded.value().as<GribEncodedMessage>();
    if (!fragment || fragment->size != range.len)
      return adapter_failure_result<std::optional<QueryBatch>>(
        QueryError::Internal, QueryPhase::Execution,
        "GRIB encoded fragment cache type/size mismatch", &source_);
    std::vector<BinaryValue> output;
    output.emplace_back(fragment->bytes.get(), fragment->bytes.get() + fragment->size);
    runtime_->cache().record_copy(fragment->size);
    std::vector<LogicalArray> columns;
    columns.push_back({{LogicalTypeId::Binary, false, std::nullopt},
      std::move(output), {}});
    return finish_batch(1, std::move(columns));
  }

  QueryResult<std::optional<QueryBatch>> message_batch(
      const GribIndexedMessage& message) const {
    std::vector<LogicalArray> columns;
    columns.reserve(plan_->schema.fields().size());
    for (const auto& field : plan_->schema.fields()) {
      auto column = repeated_attribute(field, message, 1, source_);
      if (!column.ok())
        return QueryResult<std::optional<QueryBatch>>::failure(
          column.status());
      columns.push_back(std::move(column.value()));
    }
    return finish_batch(1, std::move(columns));
  }

  QueryResult<std::optional<QueryBatch>> next_grid() {
    while (!pending_ || pending_offset_ ==
        logical_array_length(pending_->values)) {
      pending_.reset();
      pending_offset_ = 0;
      if (scheduled_decodes_.empty()) {
        schedule_grid_decodes();
      }
      if (scheduled_decodes_.empty() &&
          message_index_ < plan_->messages().size()) {
        pending_message_ = &plan_->index->messages[
          plan_->messages()[message_index_++]];
        auto decode = [&] {
          std::optional<GribSingleMessageDecodeScope> single_message_scope;
          if (plan_->messages().size() == 1)
            single_message_scope.emplace(metadata_budget_);
          auto metadata_scope = metadata_budget_ ?
            std::make_unique<ScopedQueryMetadataBudget>(metadata_budget_) : nullptr;
          ScopedQueryExecutionLease execution_scope(execution_lease_);
          if (plan_->messages().size() == 1 &&
              cancelled_->load(std::memory_order_acquire))
            return QueryResult<GribGridResult>::failure(adapter_failure(
              QueryError::Cancelled, QueryPhase::Execution,
              "GRIB query cursor was cancelled", &source_));
          return decode_cached_grid(source_, producer_source_,
            make_grid_plan(*pending_message_), decoded_field_cache_,
            *grid_budget_, runtime_, native_counters_);
        };
        // Preserve the former future.get() error contract for single-message
        // requests, including errors thrown by request-local accounting.
        auto decoded = plan_->messages().size() == 1
          ? translate_grid_decode_exceptions(decode) : decode();
        if (!decoded.ok())
          return QueryResult<std::optional<QueryBatch>>::failure(
            decoded.status());
        if (logical_array_length(decoded.value().values) == 0) continue;
        pending_ = std::move(decoded.value());
        continue;
      }
      if (scheduled_decodes_.empty())
        return QueryResult<std::optional<QueryBatch>>::success(std::nullopt);
      auto scheduled = std::move(scheduled_decodes_.front());
      scheduled_decodes_.pop_front();
      pending_message_ = scheduled.message;
      auto decoded = translate_grid_decode_exceptions(
        [&] { return scheduled.future.get(); });
      scheduled.permit.reset();
      schedule_grid_decodes();
      if (!decoded.ok())
        return QueryResult<std::optional<QueryBatch>>::failure(
          decoded.status());
      if (logical_array_length(decoded.value().values) == 0) continue;
      pending_ = std::move(decoded.value());
    }

    const auto ni = pending_->domain.shape[1];
    const auto total = logical_array_length(pending_->values);
    if (ni == 0 || total % ni != 0)
      return adapter_failure_result<std::optional<QueryBatch>>(
        QueryError::CorruptPayload, QueryPhase::Execution,
        "GRIB dense grid has an invalid row shape", &source_);
    const auto remaining_grid_rows = (total - pending_offset_) / ni;
    const auto target_cells = std::max<std::uint64_t>(1,
      options_.target_batch_rows);
    const auto target_grid_rows = std::max<std::uint64_t>(1,
      target_cells / ni);
    const auto grid_rows = std::min<std::uint64_t>(remaining_grid_rows,
      target_grid_rows);
    if (grid_rows > std::numeric_limits<std::size_t>::max() / ni)
      return adapter_failure_result<std::optional<QueryBatch>>(
        QueryError::ResourceExhausted, QueryPhase::Execution,
        "GRIB batch row count exceeds addressable memory", &source_);
    const auto count = static_cast<std::size_t>(grid_rows * ni);

    std::vector<LogicalArray> columns;
    columns.reserve(plan_->schema.fields().size());
    for (const auto& field : plan_->schema.fields()) {
      if (field.field_id == grib_query_fields::kLatitude) {
        columns.push_back({field.type,
          grid_axis_slice(pending_->latitude_axis,
            static_cast<std::size_t>(ni), pending_offset_, count, true), {}});
      } else if (field.field_id == grib_query_fields::kLongitude) {
        columns.push_back({field.type,
          grid_axis_slice(pending_->longitude_axis,
            static_cast<std::size_t>(ni), pending_offset_, count, false), {}});
      } else if (field.field_id == grib_query_fields::kValue) {
        if (field.type.id == LogicalTypeId::Float32) {
          columns.push_back({field.type,
            array_slice(std::get<std::vector<float>>(
              pending_->values.values), pending_offset_, count), {}});
        } else {
          columns.push_back({field.type,
            array_slice(std::get<std::vector<double>>(
              pending_->values.values), pending_offset_, count), {}});
        }
      } else {
        auto column = repeated_attribute(field, *pending_message_, count,
          source_);
        if (!column.ok())
          return QueryResult<std::optional<QueryBatch>>::failure(
            column.status());
        columns.push_back(std::move(column.value()));
      }
    }

    auto domain = pending_->domain;
    domain.shape[0] = grid_rows;
    domain.origin[0] += pending_offset_ / ni;
    pending_offset_ += count;
    return finish_batch(count, std::move(columns), std::move(domain));
  }

  std::shared_ptr<const QuerySource> source_owner_;
  const QuerySource& source_;
  std::shared_ptr<const GribAdapterExecutionPlan> plan_;
  QueryExecutionOptions options_;
  std::shared_ptr<GribDecodeExecutor> decode_executor_;
  std::shared_ptr<GribDecodedFieldCache> decoded_field_cache_;
  std::shared_ptr<const QuerySource> producer_source_;
  std::shared_ptr<QueryRuntime> runtime_;
  CancellationToken binary_cancellation_;
  std::uint64_t logical_bytes_{0}, logical_ranges_{0};
  std::shared_ptr<std::atomic_bool> cancelled_;
  std::shared_ptr<QueryMetadataBudget> metadata_budget_;
  std::shared_ptr<QueryExecutionLease> execution_lease_;
  std::shared_ptr<GribGridReadBudget> grid_budget_;
  std::shared_ptr<GribNativeCacheCounters> native_counters_;
  bool native_counters_reported_{false};
  std::size_t message_index_{0};
  std::deque<ScheduledDecode> scheduled_decodes_;
  const GribIndexedMessage* pending_message_{nullptr};
  std::optional<GribGridResult> pending_;
  std::size_t pending_offset_{0};
};

} // namespace

GribFormatQueryAdapter::GribFormatQueryAdapter()
  : GribFormatQueryAdapter(GribParsedIndexCacheConfig{},
      GribDecodedFieldCacheConfig{}) {}

GribFormatQueryAdapter::GribFormatQueryAdapter(
    GribParsedIndexCacheConfig index_cache_config,
    GribDecodedFieldCacheConfig decoded_cache_config,
    std::shared_ptr<QueryRuntime> runtime)
  : parsed_index_cache_(
      std::make_shared<GribParsedIndexCache>(index_cache_config)),
    decoded_field_cache_(
      std::make_shared<GribDecodedFieldCache>(decoded_cache_config)),
    decode_executor_(std::make_shared<GribDecodeExecutor>(
      default_decode_worker_count())),
    runtime_(runtime ? std::move(runtime) : std::make_shared<QueryRuntime>()) {}

const std::string& GribFormatQueryAdapter::id() const {
  static const std::string value = "grib";
  return value;
}

QueryResult<QueryCapabilities> GribFormatQueryAdapter::capabilities(
    const QueryPlanningContext& context, const QueryFragment& fragment) const {
  const auto& source = context.source();
  if (fragment.physical_array)
    return adapter_failure_result<QueryCapabilities>(
      QueryError::UnsupportedCapability, QueryPhase::Planning,
      "GRIB does not support physical nested-array subjects", &source);
  if (!source.manifest().format_id ||
      *source.manifest().format_id != id())
    return adapter_failure_result<QueryCapabilities>(
      QueryError::AdapterUnavailable, QueryPhase::Planning,
      "GRIB Query Adapter requires persisted Format Identity grib", &source);
  return make_grib_query_capabilities();
}

bool GribFormatQueryAdapter::supports_unit_catalog(
    const QueryFragment& fragment) const {
  return fragment.projection.size() == 1 &&
    fragment.projection.front().field_id == grib_query_fields::kOffset &&
    !fragment.physical_array && fragment.slices.empty() &&
    fragment.output.mode == QueryFragment::ResultMode::LogicalValues &&
    !fragment.output.interpolation && fragment.output.type_overrides.empty();
}

bool GribFormatQueryAdapter::supports_domain_index(
    const QueryFragment& fragment) const {
  return grib_domain_key(fragment).has_value();
}

QueryResult<std::shared_ptr<const AdapterDomainIndex>>
GribFormatQueryAdapter::build_domain_index(
    const std::vector<std::shared_ptr<const QuerySource>>& sources,
    const QueryFragment& fragment, const QueryLimits& limits,
    std::uint64_t max_retained_bytes) const {
  using Result = QueryResult<std::shared_ptr<const AdapterDomainIndex>>;
  const auto decline = [] { return Result::success(nullptr); };
  if (!supports_domain_index(fragment) || sources.empty() ||
      sources.size() > limits.max_sources) return decline();
  try {
    // The caller has already completed ordinary full-domain prepare, including
    // its error ordering. This optional derived artifact cannot replace that
    // first validation or make the successful query fail on allocation.
    const auto maximum = std::min(limits.max_layout_bytes, max_retained_bytes);
    std::uint64_t used = 0;
    const auto charge = [&](std::uint64_t amount) {
      if (amount > maximum - used) return false;
      used += amount;
      return true;
    };
    if (!charge(sizeof(GribMessageDomainIndex) + 256) ||
        sources.size() > (maximum - used) /
          sizeof(std::shared_ptr<const GribDomainSource>) ||
        !charge(sources.size() * sizeof(std::shared_ptr<const GribDomainSource>)))
      return decline();
    auto domain = std::make_shared<GribMessageDomainIndex>();
    domain->certified_limits = limits;
    domain->owners.reserve(sources.size());
    std::size_t count = 0;
    for (const auto& source : sources) {
      if (!source || !source->manifest().format_id ||
          *source->manifest().format_id != id()) return decline();
      if (source->manifest().layout_size >
          std::numeric_limits<std::uint64_t>::max() - domain->full_layout_bytes)
        return decline();
      domain->full_layout_bytes += source->manifest().layout_size;
      if (domain->full_layout_bytes > limits.max_layout_bytes) return decline();
      std::optional<std::string> layout_identity;
      auto parsed = parsed_index_cache_->get_or_load(*source, [&] {
        // Ordinary full-source prepare already loaded and verified parsing.
        // If it was evicted, this optional cache must decline rather than add
        // unbudgeted layout reads or a fresh parsing allocation peak.
        return QueryResult<std::shared_ptr<const GribParsedIndex>>::failure(
          QueryStatus::failure(QueryError::UnsupportedCapability,
            QueryPhase::Planning, "GRIB domain build requires retained parsed indexes"));
      }, &layout_identity);
      if (!parsed.ok() || !charge(parsed.value()->memory_bytes) ||
          !charge(sizeof(GribDomainSource) + 256 +
            source->manifest().key.path.size() * 2 +
            (layout_identity ? layout_identity->capacity() : 0)))
        return decline();
      const auto size = parsed.value()->messages.size();
      if (size > std::numeric_limits<std::size_t>::max() - count ||
          size > (maximum - used) / sizeof(GribMessageDomainIndex::Index::Entry) ||
          !charge(size * sizeof(GribMessageDomainIndex::Index::Entry))) return decline();
      for (const auto& message : parsed.value()->messages) {
        GribDomainKey key;
        if (!bind_grib_domain_key(message, key)) return decline();
      }
      count += size;
      auto owner = std::make_shared<GribDomainSource>();
      owner->index = std::move(parsed.value());
      owner->key = source->manifest().key;
      owner->storage_namespace = source->storage_namespace();
      owner->revision = source->manifest().revision;
      owner->payload_size = source->manifest().payload_size;
      owner->layout_size = source->manifest().layout_size;
      owner->layout_identity = std::move(layout_identity);
      domain->owners.push_back(std::move(owner));
    }
    std::vector<GribMessageDomainIndex::Index::Entry> entries;
    entries.reserve(count);
    for (std::size_t source = 0; source < domain->owners.size(); ++source) {
      const auto& messages = domain->owners[source]->index->messages;
      for (std::size_t message = 0; message < messages.size(); ++message) {
        GribDomainKey key;
        bind_grib_domain_key(messages[message], key);
        entries.push_back({key, {source, message}});
      }
    }
    domain->index = GribMessageDomainIndex::Index(std::move(entries),
      [](const auto& a, const auto& b) { return a < b; });
    // Retained capacity can exceed reserve's requested size on a different
    // standard library. Account that surplus before publishing the artifact.
    if (domain->owners.capacity() - sources.size() > (maximum - used) /
        sizeof(std::shared_ptr<const GribDomainSource>) ||
        !charge((domain->owners.capacity() - sources.size()) *
          sizeof(std::shared_ptr<const GribDomainSource>)) ||
        domain->index.capacity() - count > (maximum - used) /
          sizeof(GribMessageDomainIndex::Index::Entry) ||
        !charge((domain->index.capacity() - count) *
          sizeof(GribMessageDomainIndex::Index::Entry))) return decline();
    domain->bytes = used;
    return Result::success(std::move(domain));
  } catch (const std::bad_alloc&) {
    return decline();
  }
}

QueryResult<AdapterPlan> GribFormatQueryAdapter::prepare(
    const QueryPlanningContext& context, const QueryFragment& fragment) const {
  const auto& source = context.source();
  auto available = capabilities(context, fragment);
  if (!available.ok())
    return QueryResult<AdapterPlan>::failure(available.status());
  auto schema = make_grib_logical_schema();
  if (!schema.ok()) return QueryResult<AdapterPlan>::failure(schema.status());
  if (fragment.projection.empty())
    return adapter_failure_result<AdapterPlan>(QueryError::InvalidQuery,
      QueryPhase::Planning, "GRIB query requires a projection", &source);
  if (fragment.predicate) {
    auto valid = validate_expression(fragment.predicate, schema.value());
    if (!valid.ok()) return QueryResult<AdapterPlan>::failure(valid);
  }
  for (const auto& slice : fragment.slices) {
    auto valid = validate_domain_slice(slice, schema.value());
    if (!valid.ok()) return QueryResult<AdapterPlan>::failure(valid);
    if (slice.dimension.field_id != grib_query_fields::kLatitude &&
        slice.dimension.field_id != grib_query_fields::kLongitude)
      return adapter_failure_result<AdapterPlan>(
        QueryError::UnsupportedCapability, QueryPhase::Planning,
        "GRIB supports Domain Slices only on latitude and longitude", &source,
        slice.dimension.field_id);
  }

  auto typed_output = plan_grib_typed_output(fragment);
  if (!typed_output.ok())
    return QueryResult<AdapterPlan>::failure(typed_output.status());
  if (fragment.output.mode == QueryFragment::ResultMode::LogicalValues &&
      std::any_of(fragment.projection.begin(), fragment.projection.end(),
        [](const auto& field) {
          return field.field_id == grib_query_fields::kMessageBytes;
        }))
    return adapter_failure_result<AdapterPlan>(
      QueryError::UnsupportedCapability, QueryPhase::Planning,
      "grib.message.bytes requires BinaryView mode", &source,
      grib_query_fields::kMessageBytes);

  std::shared_ptr<const GribParsedIndex> selected_index;
  std::vector<std::size_t> selected_messages;
  std::shared_ptr<const std::vector<std::size_t>> hinted_messages;
  const auto hint = std::dynamic_pointer_cast<const GribMessageSelection>(context.state());
  const auto key = hint ? grib_domain_key(fragment) : std::nullopt;
  if (hint && key && hint->owner->matches(source) && hint->matches(*key) &&
      same_grib_limits(hint->limits, context.limits())) {
    // This call is an admission/fee observation even on a warm parsed-index
    // hit. Preserve it when bypassing that cache's get_or_load operation.
    auto identity = source.verified_layout_identity();
    if (!identity.ok()) {
      auto detail = identity.status().detail();
      const auto& manifest = source.manifest();
      detail.source = QuerySourceReference{manifest.key.path, manifest.key.version,
        manifest.format_id.value_or("")};
      return QueryResult<AdapterPlan>::failure(QueryStatus::failure(
        identity.status().code(), identity.status().phase(), identity.status().message(),
        std::move(detail)));
    }
    if (identity.value() == hint->owner->layout_identity) {
      selected_index = hint->owner->index;
      hinted_messages = hint->messages;
      query_profile_count("grib.domain.message_hint_hits");
      query_profile_count("grib.domain.message_hint_units", hinted_messages->size());
    }
  }
  if (!selected_index) {
    auto index = parsed_index_cache_->get_or_load(source, [&] {
      auto layout = GribQueryKernel::read_layout(source);
      if (!layout.ok())
        return QueryResult<std::shared_ptr<const GribParsedIndex>>::failure(
          layout.status());
      return parse_index(source, layout.value().records);
    });
    if (!index.ok()) return QueryResult<AdapterPlan>::failure(index.status());
    auto messages = prepare_messages(source, *index.value(), fragment.predicate);
    if (!messages.ok()) return QueryResult<AdapterPlan>::failure(messages.status());
    selected_index = std::move(index.value());
    selected_messages = std::move(messages.value());
  }
  auto projected_schema = output_schema(fragment, typed_output.value(),
    source);
  if (!projected_schema.ok())
    return QueryResult<AdapterPlan>::failure(projected_schema.status());

  auto implementation = std::make_shared<GribAdapterExecutionPlan>(
    projected_schema.value());
  implementation->source = source.manifest().key;
  implementation->storage_namespace = source.storage_namespace();
  implementation->revision = source.manifest().revision;
  implementation->payload_size = source.manifest().payload_size;
  implementation->layout_size = source.manifest().layout_size;
  implementation->output = std::move(typed_output.value());
  implementation->slices = fragment.slices;
  implementation->index = std::move(selected_index);
  implementation->message_indices = std::move(selected_messages);
  implementation->hinted_messages = std::move(hinted_messages);
  implementation->offset_only = supports_unit_catalog(fragment);
  if (fragment.output.mode == QueryFragment::ResultMode::BinaryView) {
    implementation->kind = GribPlanKind::BinaryView;
  } else {
    const bool grid = !fragment.slices.empty() ||
      fragment.output.interpolation.has_value() ||
      std::any_of(fragment.projection.begin(), fragment.projection.end(),
        [](const auto& field) { return is_grid_field(field.field_id); });
    implementation->kind = grid ? GribPlanKind::DenseGrid
                                : GribPlanKind::MessageRows;
  }
  return QueryResult<AdapterPlan>::success(AdapterPlan(
    std::move(implementation), std::move(projected_schema.value()), nullptr,
    {}, fragment.projection));
}

QueryResult<std::unique_ptr<QueryBatchCursor>>
GribFormatQueryAdapter::execute(const QueryExecutionContext& context,
    std::shared_ptr<const AdapterExecutionPlan> plan,
    const QueryExecutionOptions& options) const {
  if (!context.source)
    return adapter_failure_result<std::unique_ptr<QueryBatchCursor>>(
      QueryError::InvalidQuery, QueryPhase::Execution,
      "GRIB execution requires an owned source");
  const auto& source = *context.source;
  auto grib_plan = std::dynamic_pointer_cast<const GribAdapterExecutionPlan>(
    std::move(plan));
  if (!grib_plan)
    return adapter_failure_result<std::unique_ptr<QueryBatchCursor>>(
      QueryError::StalePlan, QueryPhase::Execution,
      "GRIB Query Adapter received a foreign execution plan", &source);
  if (!(grib_plan->source == source.manifest().key) ||
      grib_plan->storage_namespace != source.storage_namespace() ||
      grib_plan->revision != source.manifest().revision ||
      grib_plan->payload_size != source.manifest().payload_size ||
      grib_plan->layout_size != source.manifest().layout_size ||
      !source.manifest().format_id || *source.manifest().format_id != id())
    return adapter_failure_result<std::unique_ptr<QueryBatchCursor>>(
      QueryError::StalePlan, QueryPhase::Execution,
      "GRIB execution plan does not match the fixed File Version", &source);
  if (!context.shared_source ||
      context.shared_source->storage_namespace() != source.storage_namespace() ||
      !(context.shared_source->manifest().key == source.manifest().key) ||
      context.shared_source->manifest().revision != source.manifest().revision ||
      context.shared_source->manifest().payload_size != source.manifest().payload_size ||
      context.shared_source->manifest().layout_size != source.manifest().layout_size ||
      context.shared_source->manifest().format_id != source.manifest().format_id)
    return adapter_failure_result<std::unique_ptr<QueryBatchCursor>>(
      QueryError::StalePlan, QueryPhase::Execution,
      "GRIB producer source does not match the request source", &source);
  std::unique_ptr<QueryBatchCursor> cursor =
    std::make_unique<GribQueryCursor>(context.source, context.shared_source,
      std::move(grib_plan), options, decode_executor_, decoded_field_cache_, runtime_);
  return QueryResult<std::unique_ptr<QueryBatchCursor>>::success(
    std::move(cursor));
}

} // namespace iotdbfs
