#pragma once

#include "iotdbfs/query/frontend/TypedQueryService.h"

#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace iotdbfs {

// Per-mount stat/open rendezvous. One complete IPC result is pinned for a
// bounded interval or until byte pressure evicts it; concurrent stat/open
// calls for the same route join that materialization. Open handles retain the
// bytes after cache eviction.
class FuseTypedQueryBridge {
public:
  using Handle = TypedResultStore::Handle;
  using Authorization = TypedQueryService::SourceAuthorization;
  using Clock = std::chrono::steady_clock;
  using ClockFn = std::function<Clock::time_point()>;

  FuseTypedQueryBridge(std::shared_ptr<TypedQueryFrontend> frontend,
    std::shared_ptr<TypedQueryService> service,
    std::size_t max_routes = 64,
    std::chrono::milliseconds ttl = std::chrono::minutes(5),
    ClockFn clock = Clock::now,
    std::chrono::milliseconds wait_timeout = std::chrono::seconds(30));

  QueryResult<std::shared_ptr<Handle>> acquire(const std::string& route,
    const Authorization& authorize, const CatalogSelection* selected = nullptr,
    const std::optional<QueryPrincipal>& principal = std::nullopt);
  // Opens only the result published for this route. A missing or evicted
  // result is never regenerated under its old path.
  QueryResult<std::shared_ptr<Handle>> acquire_exact(
    const std::string& route, const std::string& result_id,
    const Authorization& authorize,
    const std::optional<QueryPrincipal>& principal = std::nullopt);
  void drop(const std::string& route);
  std::size_t cached_routes() const;

private:
  struct Entry {
    bool loading{true};
    QueryStatus status;
    std::shared_ptr<Handle> handle;
    std::vector<QuerySourceReference> sources;
    std::shared_ptr<const QueryReadContext> read_context;
    Clock::time_point expires_at;
    std::uint64_t sequence{0};
    std::condition_variable ready;
  };

  void expire_locked(Clock::time_point now);
  bool evict_one_locked();
  bool evict_one_reclaimable_locked();

  std::shared_ptr<TypedQueryFrontend> frontend_;
  std::shared_ptr<TypedQueryService> service_;
  std::size_t max_routes_;
  std::chrono::milliseconds ttl_;
  ClockFn clock_;
  std::chrono::milliseconds wait_timeout_;
  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<Entry>> entries_;
  std::uint64_t sequence_{0};
};

int query_status_errno(const QueryStatus& status);

} // namespace iotdbfs
