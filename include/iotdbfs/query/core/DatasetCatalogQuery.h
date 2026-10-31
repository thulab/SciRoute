#pragma once
#include "iotdbfs/DatasetCatalog.h"
#include "iotdbfs/query/core/QueryPlanning.h"
#include <functional>
#include <memory>

namespace iotdbfs {
struct DatasetCatalogRequest {
  SourceSelector sources;
  std::string format_id{"hdf5"};
  std::optional<std::string> dataset_path;
  QueryExpressionPtr predicate;
  // Half-open UTC seconds. Available only for sources whose GEOS time scale
  // was verified at ingestion and bound to the exact published layout.
  struct UtcTimeRange { std::int64_t lower, upper; };
  std::optional<UtcTimeRange> time_range;
  std::size_t max_sources{1024};
  std::size_t max_datasets{1024};
  std::uint64_t max_catalog_bytes{64ULL * 1024 * 1024};
};
struct DatasetCatalogMatch {
  QuerySourceReference source;
  std::uint64_t revision{0};
  std::string object_id, dataset_path, layout_digest, layout_protocol;
};
struct DatasetCatalogResult {
  std::vector<DatasetCatalogMatch> datasets;
  std::uint64_t selected_sources{0}, candidate_datasets{0}, sql_queries{0};
  std::uint64_t time_candidate_sources{0};
  std::uint64_t catalog_read_bytes{0};
  // In-process publication dependency context; never serialized as client proof.
  std::shared_ptr<const QueryReadContext> read_context;
};
using DatasetCatalogAuthorizer =
    std::function<QueryStatus(const std::vector<QuerySourceReference> &,
                              const CatalogSelection &)>;
QueryStatus validate_dataset_catalog_request(const DatasetCatalogRequest &);
// A missing or mismatched coverage marker is an error, never an empty match.
QueryResult<bool> matches_verified_utc_time(
    const StructuredBlobManifest &, const std::string &dataset_path,
    const DatasetCatalogRequest::UtcTimeRange &);
QueryResult<std::string>
dataset_attribute_name(const FieldRef &,
                       const std::string &prefix = "hdf5.attr:");
QueryResult<LogicalSchema>
build_dataset_attribute_schema(const DatasetCatalogRecord &,
                               const QueryExpressionPtr &,
                               const std::string &prefix = "hdf5.attr:");
QueryResult<bool>
evaluate_dataset_attribute_predicate(const DatasetCatalogRecord &,
                                     const QueryExpressionPtr &,
                                     const std::string &prefix = "hdf5.attr:");
// Immutable, request-scoped program. Grammar/field decoding is compiled once;
// each evaluation binds all referenced attributes before short-circuiting.
// No row pointers or mutable evaluation state survive an evaluate() call.
class DatasetAttributePredicate {
public:
  static QueryResult<DatasetAttributePredicate> compile(
      const QueryExpressionPtr &, const std::string &prefix = "hdf5.attr:");
  QueryResult<bool> evaluate(const DatasetCatalogRecord &) const;
private:
  struct Impl;
  explicit DatasetAttributePredicate(std::shared_ptr<const Impl> impl)
      : impl_(std::move(impl)) {}
  std::shared_ptr<const Impl> impl_;
};
// Reference implementation and common exact evaluator; authorization precedes
// loading attribute artifacts and time pruning. The exact selected catalog
// carries request-local permission evidence; the authorizer still validates
// freshness and may fall back to direct permission reads. Missing/old/incomplete
// indexes fail explicitly.
using DatasetCatalogLoader = std::function<QueryResult<DatasetCatalogArtifact>(
    const StructuredBlobManifest &)>;
QueryResult<DatasetCatalogResult> discover_datasets_reference(
    const PublishedVersionCatalog &, const DatasetCatalogRequest &,
    const DatasetCatalogLoader &, const DatasetCatalogAuthorizer &);
// SQL builder returns a conservative filter (unsupported leaf -> TRUE). Exact
// verification still uses evaluate_dataset_attribute_predicate. Returned names
// describe only SQL joins, not attributes required for validation or residual
// evaluation. A whole-predicate TRUE fallback returns no names. Caller joins
// each returned name using LEFT JOIN on the SAME source/object key.
struct DatasetPredicateSql {
  std::vector<std::string> attributes;
  std::string predicate;
};
QueryResult<DatasetPredicateSql>
compile_dataset_predicate_sql(const QueryExpressionPtr &);
} // namespace iotdbfs
