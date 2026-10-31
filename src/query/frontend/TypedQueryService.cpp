#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "iotdbfs/query/core/QueryBatchBudget.h"
#include "iotdbfs/query/frontend/TypedQueryService.h"

#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/query/encoding/ArrowResultEncoder.h"
#include "iotdbfs/query/encoding/WindowIpcEncoder.h"

#include <algorithm>
#include <stdexcept>
#include <utility>
#include <tuple>
#include <variant>

namespace iotdbfs {
namespace {
QueryStatus invalid(const char* message) {
  return QueryStatus::failure(QueryError::InvalidQuery,
    QueryPhase::Validation, message);
}
class BatchBudgetCursor final : public QueryBatchCursor {
public:
  BatchBudgetCursor(QueryBatchCursor& cursor, std::shared_ptr<QueryBatchBudget> budget)
    : cursor_(cursor), budget_(std::move(budget)) {}
  const LogicalSchema& schema() const override { return cursor_.schema(); }
  QueryResult<std::optional<QueryBatch>> next() override {
    auto next = cursor_.next();
    if (!next.ok() || !next.value()) return next;
    auto reserved = budget_->reserve_rows(next.value()->row_count());
    if (!reserved.ok()) {
      cursor_.cancel();
      return QueryResult<std::optional<QueryBatch>>::failure(reserved);
    }
    return next;
  }
  void cancel() override { cursor_.cancel(); }
private:
  QueryBatchCursor& cursor_;
  std::shared_ptr<QueryBatchBudget> budget_;
};
QueryResult<QueryResultMetadata> result_metadata(const PreparedQuery& prepared,
    std::uint64_t limit) {
  using Result = QueryResult<QueryResultMetadata>;
  QueryResultMetadata metadata;
  std::uint64_t remaining = std::min(ArrowResultEncoder::max_metadata_bytes, limit);
  std::size_t count = 0;
  for (const auto& selected : prepared.sources()) {
    if (!selected.plan) continue;
    const auto& entries = selected.plan->implementation()->result_metadata();
    if (entries.empty()) continue;
    // Reserve one entry for the descriptor count. Check before copying any
    // format-provided strings; a malicious plan cannot force unbounded staging.
    if (entries.size() >= ArrowResultEncoder::max_metadata_entries - metadata.size())
      return Result::failure(QueryStatus::failure(QueryError::ResourceExhausted,
        QueryPhase::Encoding, "prepared result metadata entry limit exceeded"));
    const auto prefix = "iotdbfs.result_descriptor." + std::to_string(count++) + ".";
    for (const auto& entry : entries) {
      if (entry.first.empty() || entry.first.find('\0') != std::string::npos)
        return Result::failure(invalid("adapter result descriptor has an invalid key"));
      if (entry.first.size() > ArrowResultEncoder::max_metadata_key_bytes - prefix.size())
        return Result::failure(QueryStatus::failure(QueryError::ResourceExhausted,
          QueryPhase::Encoding, "prepared result metadata key limit exceeded"));
      const auto key_bytes = prefix.size() + entry.first.size();
      if (key_bytes > remaining || entry.second.size() > remaining - key_bytes)
        return Result::failure(QueryStatus::failure(QueryError::ResourceExhausted,
          QueryPhase::Encoding, "prepared result metadata byte limit exceeded"));
      remaining -= key_bytes + entry.second.size();
      metadata.emplace_back(prefix + entry.first, entry.second);
    }
  }
  if (count) metadata.emplace_back("iotdbfs.result_descriptors", std::to_string(count));
  auto valid = ArrowResultEncoder::validate_metadata(metadata, limit);
  if (!valid.ok()) return Result::failure(valid);
  return Result::success(std::move(metadata));
}
}

TypedQueryService::TypedQueryService(
    std::shared_ptr<const QueryModule> query_module,
    std::shared_ptr<TypedQueryFrontend> frontend,
    std::shared_ptr<TypedResultStore> results,
    std::uint64_t max_encoded_bytes)
  : query_module_(std::move(query_module)), frontend_(std::move(frontend)),
    results_(std::move(results)), max_encoded_bytes_(max_encoded_bytes) {
  if (!frontend_ || !results_ || !max_encoded_bytes_)
    throw std::invalid_argument("typed query service requires frontend, store and byte limit");
  max_encoded_bytes_ = std::min(max_encoded_bytes_, results_->max_publish_bytes());
}

QueryResult<std::string> TypedQueryService::materialize(
    std::string_view route) const {
  auto result = materialize_authorized(route, {});
  if (!result.ok()) return QueryResult<std::string>::failure(result.status());
  return QueryResult<std::string>::success(std::move(result.value().id));
}

QueryResult<TypedQueryService::MaterializedResult>
TypedQueryService::materialize_authorized(std::string_view route,
    const SourceAuthorization& authorize,
    const CachePressureRelief& relieve_cache_pressure,
    const CatalogSelection* selected,
    const std::optional<QueryPrincipal>& principal) const try {
  QueryProfileTimer materialize_timer("query_materialize");
  auto resolved = [&] {
    QueryProfileTimer timer("route_resolve");
    return frontend_->resolve(route);
  }();
  if (!resolved.ok()) return QueryResult<MaterializedResult>::failure(resolved.status());
  QueryResult<BinaryValue> encoded;
  std::vector<QuerySourceReference> sources;
  std::shared_ptr<const QueryReadContext> read_context = selected ? selected->read_context : nullptr;
  std::unique_ptr<ScopedQueryMetadataBudget> metadata_scope;
  std::shared_ptr<QueryExecutionLease> execution_lease;
  std::unique_ptr<ScopedQueryExecutionLease> execution_scope;
  auto admit_execution = [&]() -> QueryStatus {
    if (!read_context) return QueryStatus::success();
    if (!metadata_scope) metadata_scope = std::make_unique<ScopedQueryMetadataBudget>();
    if (!principal) return QueryStatus::failure(QueryError::PermissionDenied,
      QueryPhase::Catalog, "owner query requires a caller principal");
    auto admitted = read_context->admit(*principal);
    if (!admitted.ok()) return admitted.status();
    execution_lease = admitted.value()->execution_lease();
    if (execution_lease) execution_scope = std::make_unique<ScopedQueryExecutionLease>(execution_lease);
    return QueryStatus::success();
  };
  if (std::holds_alternative<TypedQueryExecutionRequest>(resolved.value())) {
    const auto& request = std::get<TypedQueryExecutionRequest>(resolved.value());
    const auto& module = request.module ? request.module : query_module_;
    if (!module) return QueryResult<MaterializedResult>::failure(
      invalid("typed route has no query module"));
    std::shared_ptr<const PreparedQuery> prepared = request.prepared;
    std::optional<CatalogSelection> owned_selection;
    if (!prepared && !selected) {
      QueryMetadataTrace scan_fees;
      auto scanned = [&] {
        ScopedQueryMetadataTrace trace("");
        auto value = module->select(*request.request);
        scan_fees = trace.release();
        return value;
      }();
      if (!scanned.ok()) return QueryResult<MaterializedResult>::failure(scanned.status());
      owned_selection = std::move(scanned.value());
      selected = &*owned_selection;
      read_context = selected->read_context;
      if (read_context) {
        metadata_scope = std::make_unique<ScopedQueryMetadataBudget>();
        replay_query_metadata_trace(scan_fees);
      }
    }
    if (prepared && prepared->read_context()) read_context = prepared->read_context();
    auto admitted = admit_execution();
    if (!admitted.ok()) return QueryResult<MaterializedResult>::failure(admitted);
    if (!prepared) {
      auto planned = [&] {
        QueryProfileTimer timer("typed_prepare");
        return module->prepare(*request.request, selected);
      }();
      if (!planned.ok()) return QueryResult<MaterializedResult>::failure(planned.status());
      prepared = std::make_shared<const PreparedQuery>(std::move(planned.value()));
    } else query_profile_count("typed_prepared_reuse");
    if (!read_context && prepared->read_context()) {
      read_context = prepared->read_context();
      auto admitted = admit_execution();
      if (!admitted.ok()) return QueryResult<MaterializedResult>::failure(admitted);
    }
    if (!read_context) {
      sources.reserve(prepared->sources().size());
      for (const auto& selected : prepared->sources()) {
        const auto& manifest = selected.source->manifest();
        sources.push_back({manifest.key.path, manifest.key.version,
          manifest.format_id.value_or("")});
      }
    }
    if (authorize && !read_context) {
      QueryProfileTimer timer("query_authorize");
      auto status = authorize(sources);
      if (!status.ok()) return QueryResult<MaterializedResult>::failure(
        std::move(status));
    }
    auto metadata = [&] {
      QueryProfileTimer timer("typed_result_metadata");
      return result_metadata(*prepared, max_encoded_bytes_);
    }();
    if (!metadata.ok()) return QueryResult<MaterializedResult>::failure(metadata.status());
    auto cursor = [&] {
      QueryProfileTimer timer("typed_execute_setup");
      return module->execute(*prepared);
    }();
    if (!cursor.ok()) return QueryResult<MaterializedResult>::failure(cursor.status());
    encoded = [&] {
      QueryProfileTimer timer("typed_encode");
      if (active_query_batch_budget) {
        const auto remaining = active_query_batch_budget->remaining_encoded(max_encoded_bytes_);
        if (!remaining) return QueryResult<BinaryValue>::failure(QueryStatus::failure(
          QueryError::ResourceExhausted, QueryPhase::Encoding,
          "typed batch encoded result budget exhausted"));
        BatchBudgetCursor bounded(*cursor.value(), active_query_batch_budget);
        return ArrowResultEncoder::encode_query_cursor(
          bounded.schema(), bounded, remaining, metadata.value());
      }
      return ArrowResultEncoder::encode_query_cursor(
        cursor.value()->schema(), *cursor.value(), max_encoded_bytes_, metadata.value());
    }();
  } else {
    const auto& request = std::get<WindowExecutionRequest>(resolved.value());
    read_context = request.module->snapshot().read_context;
    auto admitted = admit_execution();
    if (!admitted.ok()) return QueryResult<MaterializedResult>::failure(admitted);
    if (!read_context) {
      sources.reserve(request.module->snapshot().manifests.size());
      for (const auto& manifest : request.module->snapshot().manifests) {
        sources.push_back({manifest.key.path, manifest.key.version,
          manifest.format_id.value_or("")});
      }
    }
    if (authorize && !read_context) {
      QueryProfileTimer timer("query_authorize");
      auto status = authorize(sources);
      if (!status.ok()) return QueryResult<MaterializedResult>::failure(
        std::move(status));
    }
    auto batch = [&] {
      QueryProfileTimer timer("window_execute");
      return request.module->execute(*request.request);
    }();
    if (!batch.ok()) return QueryResult<MaterializedResult>::failure(batch.status());
    encoded = [&] {
      QueryProfileTimer timer("ipc_encode");
      return encode_window_batch_ipc(request.module->view(), batch.value(),
        max_encoded_bytes_);
    }();
  }
  if (metadata_scope && !metadata_scope->status(QueryPhase::Encoding).ok())
    return QueryResult<MaterializedResult>::failure(metadata_scope->status(QueryPhase::Encoding));
  if (!encoded.ok()) return QueryResult<MaterializedResult>::failure(encoded.status());
  const auto encoded_bytes = encoded.value().size();
  if (active_query_batch_budget) {
    auto reserved = active_query_batch_budget->reserve_encoded(encoded_bytes, max_encoded_bytes_);
    if (!reserved.ok()) return QueryResult<MaterializedResult>::failure(reserved);
  }
  if (!read_context) {
    // Strict mode retains its existing publication and pressure-relief path.
    for (;;) {
      TypedResultStore::Handle pinned;
      auto published = [&] {
        QueryProfileTimer timer("result_publish");
        return results_->publish_retryable(encoded.value(), &pinned);
      }();
      if (published.ok()) {
        query_profile_count("published_results");
        query_profile_count("published_bytes", encoded_bytes);
        return QueryResult<MaterializedResult>::success(
          {std::move(published.value()), std::move(sources), std::move(pinned), {}, {}, {}});
      }
      if (published.status().code() != QueryError::ResourceExhausted || !relieve_cache_pressure)
        return QueryResult<MaterializedResult>::failure(published.status());
      const bool relieved = [&] {
        QueryProfileTimer timer("result_cache_pressure_relief");
        return relieve_cache_pressure();
      }();
      if (!relieved) return QueryResult<MaterializedResult>::failure(published.status());
    }
  }
  auto prepared_result = results_->prepare_publication(std::move(encoded.value()));
  if (!prepared_result.ok()) return QueryResult<MaterializedResult>::failure(prepared_result.status());
  auto publication = std::make_shared<TypedResultStore::PreparedPublication>(std::move(prepared_result.value()));
  // The bridge drops this request-only cleanup after releasing the final gate.
  // Otherwise a standalone service scope could destroy a large fee ledger
  // while its returned publication admission still owns the namespace lock.
  auto publication_cleanup = std::make_shared<std::tuple<
    std::shared_ptr<TypedResultStore::PreparedPublication>, std::shared_ptr<QueryMetadataBudget>,
    std::shared_ptr<const QueryReadContext>>>(publication, metadata_scope->budget(), read_context);
  // Completed routes need fresh-open authorization evidence, not the full
  // execution/domain ownership graph. Optional detachment must allocate before
  // final admission, and cannot fail an otherwise valid materialization.
  auto result_context = read_context;
  if (read_context) {
    QueryProfileTimer timer("result_reference_prepare");
    if (auto compact = read_context->result_reference()) {
      result_context = std::move(compact);
      query_profile_count("result_reference.detached");
    } else query_profile_count("result_reference.retained");
  }
  unsigned admission_retries = 0;
  for (;;) {
    auto capacity = results_->prepare_capacity(*publication);
    if (!capacity.ok()) {
      if (capacity.code() != QueryError::ResourceExhausted || !relieve_cache_pressure ||
          !relieve_cache_pressure())
        return QueryResult<MaterializedResult>::failure(capacity);
      continue;
    }
    std::shared_ptr<QueryAdmission> admission;
    if (read_context) {
      auto admitted = read_context->admit(*principal);
      if (!admitted.ok()) return QueryResult<MaterializedResult>::failure(admitted.status());
      admission = std::move(admitted.value());
    }
    if (!metadata_scope->status(QueryPhase::Encoding).ok())
      return QueryResult<MaterializedResult>::failure(metadata_scope->status(QueryPhase::Encoding));
    TypedResultStore::Handle pinned;
    auto published = results_->publish_prepared_retryable(*publication, &pinned);
    if (published.ok()) {
      query_profile_count("published_results");
      query_profile_count("published_bytes", encoded_bytes);
      return QueryResult<MaterializedResult>::success(
        {std::move(published.value()), std::move(sources), std::move(pinned),
         std::move(result_context), std::move(publication_cleanup), std::move(admission)});
    }
    // Neither eviction nor discarded IPC destruction belongs inside the gate.
    admission.reset();
    if (published.status().code() != QueryError::ResourceExhausted)
      return QueryResult<MaterializedResult>::failure(published.status());
    if (++admission_retries >= 4)
      return QueryResult<MaterializedResult>::failure(QueryStatus::failure(
        QueryError::ResourceExhausted, QueryPhase::Execution,
        "result publication changed repeatedly during admission"));
    // A prepared publication can lose a race without exhausting capacity.
    // Recheck first; prepare_capacity's failure path alone relieves pressure.
  }
} catch (const QueryMetadataBudgetExceeded& error) {
  return QueryResult<MaterializedResult>::failure(QueryStatus::failure(
    QueryError::ResourceExhausted, QueryPhase::Encoding, error.what()));
}

QueryResult<TypedResultStore::Handle> TypedQueryService::open_result(
    const std::string& result_id) const {
  return results_->open(result_id);
}

} // namespace iotdbfs
