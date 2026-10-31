#include "iotdbfs/query/frontend/PreparedTrainingRegistry.h"
#include <openssl/rand.h>
#include <array>
#include <stdexcept>
#include <vector>

namespace iotdbfs {
namespace {
QueryStatus missing() { return QueryStatus::failure(QueryError::SourceNotFound,
  QueryPhase::Validation, "prepared window ID was not found"); }
QueryStatus exhausted(const char* text) { return QueryStatus::failure(
  QueryError::ResourceExhausted, QueryPhase::Validation, text); }
std::string token() {
  std::array<unsigned char, 24> bytes{};
  if (RAND_bytes(bytes.data(), bytes.size()) != 1)
    throw std::runtime_error("prepared ID generation failed");
  constexpr char hex[] = "0123456789abcdef";
  std::string result;
  for (auto byte : bytes) { result += hex[byte >> 4]; result += hex[byte & 15]; }
  return result;
}
}
struct PreparedTrainingRegistry::Accounting {
  mutable std::mutex mutex;
  Stats stats;
  std::unordered_map<uid_t, std::size_t> owners;
};
struct PreparedTrainingRegistry::Lease {
  std::shared_ptr<const TrainingQueryModule> module;
  std::shared_ptr<Accounting> accounting;
  uid_t uid;
  std::uint64_t bytes{0};
  bool charged{false};
  ~Lease() {
    if (!charged) return;
    // Destroy the metadata before refunding its resident charge.
    module.reset();
    std::lock_guard<std::mutex> lock(accounting->mutex);
    --accounting->stats.resident_entries;
    accounting->stats.resident_bytes -= bytes;
    auto found = accounting->owners.find(uid);
    if (--found->second == 0) accounting->owners.erase(found);
  }
};
PreparedTrainingRegistry::PreparedTrainingRegistry(Config config, RetireRoute retire)
  : config_(std::move(config)), retire_(std::move(retire)), accounting_(std::make_shared<Accounting>()) {
  if (!config_.max_entries || !config_.max_bytes || !config_.max_item_bytes ||
      !config_.max_entries_per_uid || !config_.max_routes_per_entry ||
      config_.ttl.count() <= 0 || !config_.clock || !retire_)
    throw std::invalid_argument("invalid prepared training registry limits");
}
PreparedTrainingRegistry::~PreparedTrainingRegistry() { clear(); }
QueryResult<std::string> PreparedTrainingRegistry::insert(uid_t uid,
    std::shared_ptr<const TrainingQueryModule> module) {
  using Result = QueryResult<std::string>;
  reap();
  if (!module) return Result::failure(missing());
  const auto bytes = module->estimated_metadata_bytes();
  const auto id = token();
  std::shared_ptr<Lease> lease;
  {
    std::lock_guard<std::mutex> lock(accounting_->mutex);
    auto found = accounting_->owners.find(uid);
    const auto count = found == accounting_->owners.end() ? 0 : found->second;
    if (bytes > config_.max_item_bytes || bytes > config_.max_bytes - accounting_->stats.resident_bytes ||
        accounting_->stats.resident_entries >= config_.max_entries || count >= config_.max_entries_per_uid)
      return Result::failure(exhausted("prepared training metadata budget exhausted"));
    // Allocate the lease before charging; a throwing allocation cannot leak a reservation.
    lease = std::make_shared<Lease>();
    lease->module = std::move(module);
    lease->accounting = accounting_;
    lease->uid = uid;
    lease->bytes = bytes;
    ++accounting_->owners[uid];
    ++accounting_->stats.resident_entries;
    accounting_->stats.resident_bytes += bytes;
    lease->charged = true;
  }
  std::shared_ptr<const TrainingQueryModule> alias(lease, lease->module.get());
  std::lock_guard<std::mutex> lock(mutex_);
  auto added = entries_.emplace(id, Entry{uid, std::move(alias), config_.clock() + config_.ttl, {}});
  if (!added.second) return Result::failure(exhausted("prepared ID collision"));
  return Result::success(id);
}
QueryResult<std::shared_ptr<const TrainingQueryModule>> PreparedTrainingRegistry::acquire(
    const std::string& id, uid_t uid) {
  reap();
  std::lock_guard<std::mutex> lock(mutex_);
  auto found = entries_.find(id);
  if (found == entries_.end() || found->second.uid != uid)
    return QueryResult<std::shared_ptr<const TrainingQueryModule>>::failure(missing());
  found->second.expires = config_.clock() + config_.ttl;
  return QueryResult<std::shared_ptr<const TrainingQueryModule>>::success(found->second.module);
}
QueryStatus PreparedTrainingRegistry::track_route(const std::string& id, uid_t uid,
    const std::string& route) {
  reap();
  std::lock_guard<std::mutex> lock(mutex_);
  auto found = entries_.find(id);
  if (found == entries_.end() || found->second.uid != uid) return missing();
  if (found->second.routes.size() >= config_.max_routes_per_entry)
    return exhausted("prepared window result route limit exhausted");
  found->second.routes.insert(route);
  return QueryStatus::success();
}
bool PreparedTrainingRegistry::release_result(const std::string& id, uid_t uid,
    const std::string& route) {
  reap();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = entries_.find(id);
    if (found == entries_.end() || found->second.uid != uid || !found->second.routes.erase(route)) return false;
  }
  retire_(route);
  return true;
}
bool PreparedTrainingRegistry::erase(const std::string& id, uid_t uid) {
  std::unordered_set<std::string> routes;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = entries_.find(id);
    if (found == entries_.end() || found->second.uid != uid) return false;
    routes = std::move(found->second.routes);
    entries_.erase(found);
  }
  for (const auto& route : routes) retire_(route);
  return true;
}
void PreparedTrainingRegistry::reap() {
  std::vector<std::string> routes;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto now = config_.clock();
    for (auto it = entries_.begin(); it != entries_.end();) {
      if (it->second.expires > now) { ++it; continue; }
      routes.insert(routes.end(), it->second.routes.begin(), it->second.routes.end());
      it = entries_.erase(it);
    }
  }
  for (const auto& route : routes) retire_(route);
}
void PreparedTrainingRegistry::clear() {
  std::unordered_map<std::string, Entry> entries;
  { std::lock_guard<std::mutex> lock(mutex_); entries.swap(entries_); }
  for (const auto& entry : entries) for (const auto& route : entry.second.routes) retire_(route);
}
PreparedTrainingRegistry::Stats PreparedTrainingRegistry::stats() const {
  std::lock_guard<std::mutex> lock(accounting_->mutex);
  return accounting_->stats;
}
} // namespace iotdbfs
