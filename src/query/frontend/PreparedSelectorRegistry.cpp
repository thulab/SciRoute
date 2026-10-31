#include "iotdbfs/query/frontend/PreparedSelectorRegistry.h"
#include <openssl/rand.h>
#include <array>
#include <limits>
#include <new>
#include <stdexcept>

namespace iotdbfs {
namespace {
QueryStatus missing() {
  return QueryStatus::failure(QueryError::SourceNotFound, QueryPhase::Validation,
    "prepared selector ID was not found");
}
QueryStatus exhausted(const char* text) {
  return QueryStatus::failure(QueryError::ResourceExhausted,
    QueryPhase::Validation, text);
}
std::string random_token() {
  std::array<unsigned char, 24> bytes{};
  if (RAND_bytes(bytes.data(), static_cast<int>(bytes.size())) != 1)
    throw std::runtime_error("prepared selector ID generation failed");
  constexpr char hex[] = "0123456789abcdef";
  std::string id(48, '0');
  for (std::size_t i = 0; i < bytes.size(); ++i) {
    id[2 * i] = hex[bytes[i] >> 4]; id[2 * i + 1] = hex[bytes[i] & 15];
  }
  return id;
}
bool valid_token(const std::string& id) {
  if (id.size() != 48) return false;
  for (char c : id) if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
  return true;
}
}

struct PreparedSelectorRegistry::Accounting {
  // Declared first so management storage is destroyed before its pool charge.
  std::shared_ptr<HotQueryCache::Reservation> overhead;
  mutable std::mutex mutex;
  Stats stats;
  std::map<uid_t, std::size_t> owners;
};
struct PreparedSelectorRegistry::Lease {
  Payload payload;
  std::shared_ptr<Accounting> accounting;
  std::shared_ptr<HotQueryCache::Reservation> reservation;
  uid_t uid{0};
  std::uint64_t bytes{0};
  bool charged{false};
  ~Lease() {
    // Payload destructors may call back into the registry. Never hold its locks.
    payload.reset();
    if (charged) {
      std::lock_guard<std::mutex> lock(accounting->mutex);
      --accounting->stats.resident_entries;
      accounting->stats.resident_bytes -= bytes;
      auto found = accounting->owners.find(uid);
      if (--found->second == 0) accounting->owners.erase(found);
    }
    reservation.reset();
  }
};

PreparedSelectorRegistry::PreparedSelectorRegistry(std::shared_ptr<HotQueryCache> pool)
  : PreparedSelectorRegistry(std::move(pool), Config{}) {}
PreparedSelectorRegistry::PreparedSelectorRegistry(std::shared_ptr<HotQueryCache> pool,
    Config config) : config_(std::move(config)), pool_(std::move(pool)) {
  if (!pool_ || !config_.max_entries || config_.max_entries > 64 ||
      !config_.max_entries_per_uid || config_.max_entries_per_uid > 16 ||
      !config_.max_bytes || config_.max_bytes > 8ULL * 1024 * 1024 ||
      config_.max_item_bytes < entry_overhead_bytes ||
      config_.max_item_bytes > 1024ULL * 1024 || config_.ttl.count() <= 0 || !config_.clock)
    throw std::invalid_argument("invalid prepared selector registry limits");
  auto reservation = pool_->reserve(HotQueryCache::Kind::Auxiliary, registry_overhead_bytes);
  if (!reservation) throw std::bad_alloc();
  accounting_ = std::make_shared<Accounting>();
  accounting_->overhead = std::move(reservation);
  if (!config_.token) config_.token = random_token;
}
PreparedSelectorRegistry::~PreparedSelectorRegistry() { clear(); }

QueryResult<std::string> PreparedSelectorRegistry::insert(uid_t uid,
    std::uint64_t reserved_bytes, Builder builder) {
  using Result = QueryResult<std::string>;
  try {
    reap();
    if (!builder || reserved_bytes < entry_overhead_bytes ||
        reserved_bytes > config_.max_item_bytes)
      return Result::failure(exhausted("prepared selector item budget exhausted"));
    // Snapshot before reserving/building: a concurrent clear invalidates pending work.
    std::uint64_t generation;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (generation_exhausted_)
        return Result::failure(exhausted("prepared selector registry generation exhausted"));
      generation = generation_;
    }
    auto lease = std::make_shared<Lease>();
    lease->accounting = accounting_; lease->uid = uid; lease->bytes = reserved_bytes;
    {
      std::lock_guard<std::mutex> lock(accounting_->mutex);
      auto found = accounting_->owners.find(uid);
      const auto count = found == accounting_->owners.end() ? 0 : found->second;
      if (accounting_->stats.resident_entries >= config_.max_entries ||
          count >= config_.max_entries_per_uid ||
          reserved_bytes > config_.max_bytes - accounting_->stats.resident_bytes)
        return Result::failure(exhausted("prepared selector registry budget exhausted"));
      auto owner = accounting_->owners.emplace(uid, 0).first;
      lease->reservation = pool_->reserve(HotQueryCache::Kind::Handle, reserved_bytes);
      if (!lease->reservation) {
        if (!owner->second) accounting_->owners.erase(owner);
        return Result::failure(exhausted("shared hot query pool exhausted"));
      }
      ++owner->second; ++accounting_->stats.resident_entries;
      accounting_->stats.resident_bytes += reserved_bytes; lease->charged = true;
    }
    // All caps already include this pending builder. No registry/accounting lock.
    auto built = builder();
    if (!built.ok()) return Result::failure(built.status());
    lease->payload = std::move(built.value());
    if (!lease->payload)
      return Result::failure(QueryStatus::failure(QueryError::InvalidQuery,
        QueryPhase::Validation, "prepared selector builder returned no payload"));
    auto id = config_.token();
    if (!valid_token(id))
      return Result::failure(exhausted("prepared selector ID generation failed"));
    const auto expires = config_.clock() + config_.ttl;
    Payload alias(lease, lease->payload.get());
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (generation_exhausted_ || generation != generation_)
        return Result::failure(QueryStatus::failure(QueryError::Cancelled,
          QueryPhase::Validation, "prepared selector registration was cleared"));
      // Keep alias and lease alive outside the lock even on collision/allocation failure.
      if (!entries_.emplace(id, Entry{uid, alias, expires}).second)
        return Result::failure(exhausted("prepared selector ID collision"));
    }
    return Result::success(std::move(id));
  } catch (const std::bad_alloc&) {
    return Result::failure(exhausted("prepared selector allocation failed"));
  } catch (...) {
    return Result::failure(QueryStatus::failure(QueryError::Internal,
      QueryPhase::Validation, "prepared selector registration failed"));
  }
}

PreparedSelectorRegistry::Payload PreparedSelectorRegistry::inspect(
    uid_t uid, const std::string& id) const {
  const auto now = config_.clock();
  std::lock_guard<std::mutex> lock(mutex_);
  const auto found = entries_.find(id);
  if (found == entries_.end() || found->second.uid != uid || found->second.expires <= now)
    return {};
  return found->second.payload;
}

QueryResult<PreparedSelectorRegistry::Payload> PreparedSelectorRegistry::acquire(
    uid_t uid, const std::string& id, const std::weak_ptr<const void>* expected) {
  using Result = QueryResult<Payload>;
  const auto now = config_.clock();
  Payload retired;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = entries_.find(id);
    if (found == entries_.end() || found->second.uid != uid) return Result::failure(missing());
    if (found->second.expires <= now) {
      retired = std::move(found->second.payload); entries_.erase(found);
    } else {
      // Compare ownership, not addresses: a released ID and allocation may
      // both be reused. A changed scheduling observation must be refreshed
      // before native work, with neither TTL renewal nor a new client error.
      if (expected && (expected->owner_before(found->second.payload) ||
          found->second.payload.owner_before(*expected))) throw ObservationChanged{};
      found->second.expires = now + config_.ttl;
      return Result::success(found->second.payload);
    }
  }
  return Result::failure(missing());
}
bool PreparedSelectorRegistry::erase(uid_t uid, const std::string& id) {
  Payload retired;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = entries_.find(id);
    if (found == entries_.end() || found->second.uid != uid) return false;
    retired = std::move(found->second.payload); entries_.erase(found);
  }
  return true;
}
bool PreparedSelectorRegistry::erase(uid_t uid, const std::string& id,
    const Payload& expected) {
  if (!expected) return false;
  Payload retired;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = entries_.find(id);
    if (found == entries_.end() || found->second.uid != uid) return false;
    const auto& current = found->second.payload;
    if (current.get() != expected.get() || current.owner_before(expected) ||
        expected.owner_before(current)) return false;
    retired = std::move(found->second.payload); entries_.erase(found);
  }
  return true;
}
void PreparedSelectorRegistry::reap() {
  const auto now = config_.clock();
  std::map<std::string, Entry> retired;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = entries_.begin(); it != entries_.end();) {
      if (it->second.expires > now) { ++it; continue; }
      auto expired = it++; retired.insert(entries_.extract(expired));
    }
  }
}
void PreparedSelectorRegistry::clear() {
  std::map<std::string, Entry> retired;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    // Saturation disables future publication instead of wrapping an old epoch.
    if (generation_ == std::numeric_limits<std::uint64_t>::max()) generation_exhausted_ = true;
    else ++generation_;
    retired.swap(entries_);
  }
}
PreparedSelectorRegistry::Stats PreparedSelectorRegistry::stats() const {
  std::lock_guard<std::mutex> lock(accounting_->mutex); return accounting_->stats;
}
} // namespace iotdbfs
