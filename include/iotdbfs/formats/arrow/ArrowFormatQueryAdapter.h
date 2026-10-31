#pragma once

#include "iotdbfs/formats/arrow/ArrowQueryModel.h"
#include "iotdbfs/formats/arrow/ArrowFragmentProvider.h"

namespace iotdbfs {

QueryResult<ArrowPhysicalBinding> arrow_physical_binding(
  std::shared_ptr<const AdapterExecutionPlan> plan);
QueryResult<ArrowDecodeMetrics> arrow_decode_metrics(const QueryBatchCursor& cursor);

// Anonymous positional wide tables: index (UInt64), value (single List), or
// value.N (nested List child N). No time/variable meaning is inferred.
class ArrowFormatQueryAdapter final : public FormatQueryAdapter {
public:
  explicit ArrowFormatQueryAdapter(ArrowDecodeConfig config = {},
    std::shared_ptr<QueryRuntime> runtime = {});
  const std::string& id() const override;
  QueryResult<QueryCapabilities> capabilities(
    const QueryPlanningContext& context, const QueryFragment& fragment) const override;
  QueryResult<AdapterPlan> prepare(const QueryPlanningContext& context,
    const QueryFragment& fragment) const override;
  QueryResult<std::unique_ptr<QueryBatchCursor>> execute(const QueryExecutionContext& context,
    std::shared_ptr<const AdapterExecutionPlan> plan,
    const QueryExecutionOptions& options) const override;
private:
  ArrowDecodeConfig config_;
  std::shared_ptr<QueryRuntime> runtime_;
};

} // namespace iotdbfs
