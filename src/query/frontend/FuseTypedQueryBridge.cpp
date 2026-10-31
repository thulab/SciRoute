#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "iotdbfs/query/frontend/FuseTypedQueryBridge.h"

#include <cerrno>
#include <stdexcept>
#include <utility>

namespace iotdbfs {
namespace {
QueryStatus failure(QueryError error, std::string message) {
  return QueryStatus::failure(error, QueryPhase::Execution, std::move(message));
}
} // namespace

FuseTypedQueryBridge::FuseTypedQueryBridge(
    std::shared_ptr<TypedQueryFrontend> frontend,
    std::shared_ptr<TypedQueryService> service, std::size_t max_routes,
    std::chrono::milliseconds ttl, ClockFn clock,
    std::chrono::milliseconds wait_timeout)
  : frontend_(std::move(frontend)), service_(std::move(service)),
    max_routes_(max_routes), ttl_(ttl), clock_(std::move(clock)),
    wait_timeout_(wait_timeout) {
  if (!frontend_ || !service_ || !max_routes_ || ttl_.count() <= 0 ||
      wait_timeout_.count() <= 0 || !clock_)
    throw std::invalid_argument("invalid FUSE typed query bridge configuration");
}

void FuseTypedQueryBridge::expire_locked(Clock::time_point now) {
  for (auto it = entries_.begin(); it != entries_.end();) {
    if (!it->second->loading && it->second->expires_at <= now)
      it = entries_.erase(it);
    else ++it;
  }
}

bool FuseTypedQueryBridge::evict_one_locked() {
  auto oldest = entries_.end();
  for (auto it = entries_.begin(); it != entries_.end(); ++it) {
    if (it->second->loading) continue;
    if (oldest == entries_.end() ||
        it->second->sequence < oldest->second->sequence)
      oldest = it;
  }
  if (oldest == entries_.end()) return false;
  entries_.erase(oldest);
  return true;
}

bool FuseTypedQueryBridge::evict_one_reclaimable_locked() {
  auto oldest = entries_.end();
  for (auto it = entries_.begin(); it != entries_.end(); ++it) {
    const auto& entry = it->second;
    if (entry->loading || !entry->handle || entry.use_count() != 1 ||
        entry->handle.use_count() != 1) continue;
    if (oldest == entries_.end() || entry->sequence < oldest->second->sequence)
      oldest = it;
  }
  if (oldest == entries_.end()) return false;
  entries_.erase(oldest);
  return true;
}

QueryResult<std::shared_ptr<FuseTypedQueryBridge::Handle>>
FuseTypedQueryBridge::acquire(const std::string& route,
    const Authorization& authorize, const CatalogSelection* selected,
    const std::optional<QueryPrincipal>& principal) try {
  // An erased or expired registration must never be served from this cache.
  auto registration = frontend_->resolve_with_identity(route);
  if (!registration.ok())
    return QueryResult<std::shared_ptr<Handle>>::failure(registration.status());

  const auto& key = registration.value().registration_id;
  std::shared_ptr<Entry> entry;
  bool producer = false;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    expire_locked(clock_());
    auto it = entries_.find(key);
    if (it == entries_.end()) {
      if (entries_.size() >= max_routes_ && !evict_one_locked()) {
        return QueryResult<std::shared_ptr<Handle>>::failure(failure(
          QueryError::ResourceExhausted, "all query route slots are materializing"));
      }
      entry = std::make_shared<Entry>();
      entry->sequence = ++sequence_;
      entries_.emplace(key, entry);
      producer = true;
    } else {
      entry = it->second;
      if (!entry->ready.wait_for(lock, wait_timeout_,
          [&] { return !entry->loading; }))
        return QueryResult<std::shared_ptr<Handle>>::failure(failure(
          QueryError::DeadlineExceeded, "query result materialization wait timed out"));
      if (!entry->status.ok())
        return QueryResult<std::shared_ptr<Handle>>::failure(entry->status);
      entry->sequence = ++sequence_;
      entry->expires_at = clock_() + ttl_;
    }
  }

  if (!producer) {
    std::unique_ptr<ScopedQueryMetadataBudget> metadata_scope;
    std::shared_ptr<QueryAdmission> admission;
    if (entry->read_context) {
      metadata_scope = std::make_unique<ScopedQueryMetadataBudget>();
      if (!principal) return QueryResult<std::shared_ptr<Handle>>::failure(failure(
        QueryError::PermissionDenied, "owner result requires a caller principal"));
      auto allowed = entry->read_context->admit(*principal);
      if (!allowed.ok()) return QueryResult<std::shared_ptr<Handle>>::failure(allowed.status());
      admission = std::move(allowed.value());
      if (!metadata_scope->status(QueryPhase::Encoding).ok())
        return QueryResult<std::shared_ptr<Handle>>::failure(metadata_scope->status(QueryPhase::Encoding));
    } else if (authorize) {
      auto status = authorize(entry->sources);
      if (!status.ok()) return QueryResult<std::shared_ptr<Handle>>::failure(std::move(status));
    }
    return QueryResult<std::shared_ptr<Handle>>::success(entry->handle);
  }

  QueryStatus status;
  std::shared_ptr<Handle> handle;
  std::vector<QuerySourceReference> sources;
  std::shared_ptr<const QueryReadContext> read_context;
  std::shared_ptr<void> publication_cleanup;
  std::shared_ptr<QueryAdmission> admission;
  try {
    // Allocate the wrapper before publication; installing its handle is noexcept.
    handle = std::make_shared<Handle>();
    auto materialized = service_->materialize_authorized(route, authorize,
      [this] {
        std::lock_guard<std::mutex> lock(mutex_);
        return evict_one_reclaimable_locked();
      }, selected, principal);
    if (!materialized.ok()) {
      status = materialized.status();
    } else {
      admission = std::move(materialized.value().publication_admission);
      publication_cleanup = std::move(materialized.value().publication_cleanup);
      read_context = std::move(materialized.value().read_context);
      sources = std::move(materialized.value().owned_sources);
      *handle = std::move(materialized.value().handle);
    }
  } catch (const QueryMetadataBudgetExceeded& error) {
    status = failure(QueryError::ResourceExhausted, error.what());
  } catch (const std::exception& error) {
    status = failure(QueryError::Internal, error.what());
  } catch (...) {
    status = failure(QueryError::Internal, "query materialization failed");
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    entry->status = status;
    entry->sources = std::move(sources);
    entry->read_context = std::move(read_context);
    entry->handle = handle;
    entry->expires_at = clock_() + ttl_;
    entry->loading = false;
    if (!status.ok()) {
      auto it = entries_.find(key);
      if (it != entries_.end() && it->second == entry)
        entries_.erase(it);
    }
  }
  admission.reset();
  publication_cleanup.reset();
  entry->ready.notify_all();
  if (!status.ok())
    return QueryResult<std::shared_ptr<Handle>>::failure(std::move(status));
  return QueryResult<std::shared_ptr<Handle>>::success(std::move(handle));
} catch (const QueryMetadataBudgetExceeded& error) {
  return QueryResult<std::shared_ptr<Handle>>::failure(
    failure(QueryError::ResourceExhausted, error.what()));
}

QueryResult<std::shared_ptr<FuseTypedQueryBridge::Handle>>
FuseTypedQueryBridge::acquire_exact(const std::string& route,
    const std::string& result_id, const Authorization& authorize,
    const std::optional<QueryPrincipal>& principal) try {
  auto registration = frontend_->resolve_with_identity(route);
  if (!registration.ok())
    return QueryResult<std::shared_ptr<Handle>>::failure(registration.status());

  const auto& key = registration.value().registration_id;
  std::shared_ptr<Handle> handle;
  std::vector<QuerySourceReference> sources;
  std::shared_ptr<const QueryReadContext> read_context;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    expire_locked(clock_());
    auto found = entries_.find(key);
    if (found == entries_.end())
      return QueryResult<std::shared_ptr<Handle>>::failure(failure(
        QueryError::SourceNotFound, "published query result was evicted"));
    const auto entry = found->second;
    if (!entry->ready.wait_for(lock, wait_timeout_,
        [&] { return !entry->loading; }))
      return QueryResult<std::shared_ptr<Handle>>::failure(failure(
        QueryError::DeadlineExceeded, "query result materialization wait timed out"));
    if (!entry->status.ok())
      return QueryResult<std::shared_ptr<Handle>>::failure(entry->status);
    if (!entry->handle || entry->handle->id() != result_id)
      return QueryResult<std::shared_ptr<Handle>>::failure(failure(
        QueryError::SourceNotFound, "published query result ID does not match"));
    entry->sequence = ++sequence_;
    entry->expires_at = clock_() + ttl_;
    handle = entry->handle;
    read_context = entry->read_context;
    if (!read_context) sources = entry->sources;
  }
  std::unique_ptr<ScopedQueryMetadataBudget> metadata_scope;
  std::shared_ptr<QueryAdmission> admission;
  if (read_context) {
    metadata_scope = std::make_unique<ScopedQueryMetadataBudget>();
    if (!principal) return QueryResult<std::shared_ptr<Handle>>::failure(failure(
      QueryError::PermissionDenied, "owner result requires a caller principal"));
    auto allowed = read_context->admit(*principal);
    if (!allowed.ok()) return QueryResult<std::shared_ptr<Handle>>::failure(allowed.status());
    admission = std::move(allowed.value());
    if (!metadata_scope->status(QueryPhase::Encoding).ok())
      return QueryResult<std::shared_ptr<Handle>>::failure(metadata_scope->status(QueryPhase::Encoding));
  } else if (authorize) {
    auto status = authorize(sources);
    if (!status.ok())
      return QueryResult<std::shared_ptr<Handle>>::failure(std::move(status));
  }
  return QueryResult<std::shared_ptr<Handle>>::success(std::move(handle));
} catch (const QueryMetadataBudgetExceeded& error) {
  return QueryResult<std::shared_ptr<Handle>>::failure(
    failure(QueryError::ResourceExhausted, error.what()));
}

void FuseTypedQueryBridge::drop(const std::string& route) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto marker = route.find("?id=");
  if (marker != std::string::npos && route.size() >= marker + 4 + 48)
    entries_.erase(route.substr(marker + 4, 48));
}

std::size_t FuseTypedQueryBridge::cached_routes() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return entries_.size();
}

int query_status_errno(const QueryStatus& status) {
  switch (status.code()) {
    case QueryError::None: return 0;
    case QueryError::InvalidQuery:
    case QueryError::UnknownField:
    case QueryError::TypeMismatch:
    case QueryError::InvalidDomainSlice:
    case QueryError::UnsupportedCapability:
    case QueryError::UnsupportedLayoutVersion: return EINVAL;
    case QueryError::PermissionDenied: return EACCES;
    case QueryError::SourceNotFound:
    case QueryError::LayoutUnavailable: return ENOENT;
    case QueryError::SourceNotPublished:
    case QueryError::StalePlan: return ESTALE;
    case QueryError::AdapterUnavailable: return ENODEV;
    case QueryError::SchemaMismatch:
    case QueryError::CorruptLayout:
    case QueryError::CorruptPayload: return EILSEQ;
    case QueryError::ReadFailure: return EIO;
    case QueryError::ResourceExhausted: return ENOSPC;
    case QueryError::DeadlineExceeded: return ETIMEDOUT;
    case QueryError::Cancelled: return ECANCELED;
    case QueryError::Internal: return EIO;
  }
  return EIO;
}
} // namespace iotdbfs
