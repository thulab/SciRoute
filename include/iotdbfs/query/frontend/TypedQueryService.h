#pragma once

#include "iotdbfs/query/core/QueryModule.h"
#include "iotdbfs/query/core/QueryReadContext.h"
#include "iotdbfs/query/frontend/TypedQueryFrontend.h"
#include "iotdbfs/query/frontend/TypedResultStore.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace iotdbfs {

// A10 native entry point: resolve a registered read-only route, execute one
// bounded query or sample group, finish one IPC stream, then atomically publish
// its bytes. The FUSE bridge pins the result across stat/open without
// re-resolving latest on read.
class TypedQueryService {
public:
  using SourceAuthorization = std::function<QueryStatus(
    const std::vector<QuerySourceReference>&)>;
  using CachePressureRelief = std::function<bool()>;
  struct MaterializedResult {
    std::string id;
    // Strict results own refs; owner results reuse the complete candidate
    // array pinned by read_context.
    std::vector<QuerySourceReference> owned_sources;
    TypedResultStore::Handle handle;
    std::shared_ptr<const QueryReadContext> read_context;
    // Destroy discarded encoded bytes only after the short admission gate.
    std::shared_ptr<void> publication_cleanup;
    std::shared_ptr<QueryAdmission> publication_admission;

    const std::vector<QuerySourceReference>& sources() const {
      return read_context ? read_context->sources() : owned_sources;
    }
  };
  // The effective encoding cap is the minimum of max_encoded_bytes and the
  // store's static per-result admission limits.
  TypedQueryService(std::shared_ptr<const QueryModule> query_module,
    std::shared_ptr<TypedQueryFrontend> frontend,
    std::shared_ptr<TypedResultStore> results,
    std::uint64_t max_encoded_bytes);

  QueryResult<std::string> materialize(std::string_view route) const;
  // Authorization observes exact prepared versions before execution. The
  // returned sources() view is valid while the result owns its storage.
  // Cached opens admit through read_context, or reauthorize owned_sources
  // for strict results, on behalf of the new caller.
  QueryResult<MaterializedResult> materialize_authorized(
    std::string_view route, const SourceAuthorization& authorize,
    const CachePressureRelief& relieve_cache_pressure = {},
    const CatalogSelection* selected = nullptr,
    const std::optional<QueryPrincipal>& principal = std::nullopt) const;
  QueryResult<TypedResultStore::Handle> open_result(
    const std::string& result_id) const;

private:
  std::shared_ptr<const QueryModule> query_module_;
  std::shared_ptr<TypedQueryFrontend> frontend_;
  std::shared_ptr<TypedResultStore> results_;
  std::uint64_t max_encoded_bytes_{0};
};

} // namespace iotdbfs
