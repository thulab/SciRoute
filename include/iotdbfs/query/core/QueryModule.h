#pragma once

#include "iotdbfs/query/core/QueryPlanning.h"

#include <memory>
#include <vector>

namespace iotdbfs {

struct FormatCatalogMatch {
  QuerySourceReference source;
  std::uint64_t revision{0};
  FormatUnitLocator unit;
};
struct FormatCatalogResult {
  std::vector<FormatCatalogMatch> units;
  // Publication dependencies, never a client-supplied authorization proof.
  std::shared_ptr<const QueryReadContext> read_context;
};

// Deep query orchestration Module. prepare() fixes visible File Versions and
// delegates private physical planning to the selected FormatQueryAdapter.
// execute() streams adapter batches and owns residual semantics, projection,
// cross-source schema consistency, resource budgets, and cancellation.
class QueryModule {
public:
  static QueryResult<std::shared_ptr<const QueryModule>> create(
    std::shared_ptr<const PublishedVersionCatalog> catalog,
    std::shared_ptr<const StructuredBlobStorage> storage,
    std::vector<std::shared_ptr<const FormatQueryAdapter>> adapters);

  QueryResult<CatalogSelection> select(const QueryRequest& request) const;
  QueryStatus validate_format_catalog_request(const QueryRequest& request) const;

  // selected is a fresh scan for this request, retained only during this call.
  QueryResult<PreparedQuery> prepare(const QueryRequest& request,
    const CatalogSelection* selected = nullptr) const;
  QueryResult<std::unique_ptr<QueryBatchCursor>> execute(
    const PreparedQuery& query) const;
  // Metadata locators only. Caller authorizes the complete frozen selection
  // and retains an execution lease before preparing/visiting adapter plans.
  QueryResult<FormatCatalogResult> format_catalog(const PreparedQuery& query,
    std::uint64_t max_response_bytes) const;

private:
  class Implementation;

  explicit QueryModule(std::shared_ptr<const Implementation> implementation)
    : implementation_(std::move(implementation)) {}

  std::shared_ptr<const Implementation> implementation_;
};

} // namespace iotdbfs
