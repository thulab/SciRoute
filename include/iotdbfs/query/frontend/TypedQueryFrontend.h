#pragma once

#include "iotdbfs/query/core/QueryPlanning.h"
#include "iotdbfs/query/training/arrow/TrainingQueryModule.h"

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>

namespace iotdbfs {

class QueryModule;

// The registration owns an immutable copy of the request. The registered
// training module pins its prepared snapshot, coverage index, and view.
struct TypedQueryExecutionRequest {
  std::shared_ptr<const QueryRequest> request;
  // An immutable route-local adapter policy; absent uses the service default.
  std::shared_ptr<const QueryModule> module;
  // Opt-in fixed-query experiment; ordinary registrations leave this empty.
  std::shared_ptr<const PreparedQuery> prepared;
};

struct WindowExecutionRequest {
  std::shared_ptr<const TrainingQueryModule> module;
  std::shared_ptr<const WindowRequest> request;
};

using FrontendExecutionRequest = std::variant<TypedQueryExecutionRequest,
  WindowExecutionRequest>;

struct ResolvedFrontendRoute {
  std::string registration_id;
  FrontendExecutionRequest request;
};

// A10's native, read-only route registry. Registrations expire after an idle
// TTL; a successful resolve renews the lease. Expiry is reaped on registry
// operations. The FUSE runtime resolves these routes from its callbacks.
class TypedQueryFrontend {
public:
  using Clock = std::chrono::steady_clock;
  using ClockFn = std::function<Clock::time_point()>;

  explicit TypedQueryFrontend(std::size_t max_registrations = 1024,
    std::chrono::milliseconds ttl = std::chrono::minutes(5),
    ClockFn clock = Clock::now);

  // Optional ownership for borrowed immutable declaration components. Every
  // resolved request pointer retains it, independently of registry/route IDs.
  QueryResult<std::string> register_typed(QueryRequest request,
    std::shared_ptr<const QueryModule> module = {},
    std::shared_ptr<const void> declaration_lifetime = {});
  QueryResult<std::string> register_prepared(
    std::shared_ptr<const QueryModule> module,
    std::shared_ptr<const PreparedQuery> prepared);
  QueryResult<std::string> register_windows(
    std::shared_ptr<const TrainingQueryModule> module,
    WindowRequest request);

  QueryResult<FrontendExecutionRequest> resolve(std::string_view route) const;
  QueryResult<ResolvedFrontendRoute> resolve_with_identity(
    std::string_view route) const;

  // Explicitly release a registration; already resolved requests retain their
  // immutable shared ownership and can complete independently.
  bool erase(std::string_view registration_id);
  std::size_t size() const;

private:
  struct Registration {
    FrontendExecutionRequest request;
    Clock::time_point expires_at;
  };

  QueryResult<std::string> register_entry(FrontendExecutionRequest entry,
    std::string_view route_prefix, std::string suffix);
  void expire_locked(Clock::time_point now) const;

  const std::size_t max_registrations_;
  const std::chrono::milliseconds ttl_;
  const ClockFn clock_;
  mutable std::mutex mutex_;
  mutable std::unordered_map<std::string, Registration> registrations_;
};

} // namespace iotdbfs
