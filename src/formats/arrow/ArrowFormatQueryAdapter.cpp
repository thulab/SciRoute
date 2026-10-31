#include "iotdbfs/formats/arrow/ArrowFormatQueryAdapter.h"

#include <algorithm>
#include <atomic>
#include <limits>
#include <map>
#include <new>
#include <set>
#include <type_traits>

namespace iotdbfs {
namespace {

template<class T> QueryResult<T> fail(QueryError code, const std::string& message,
    QueryPhase phase = QueryPhase::Planning) {
  return QueryResult<T>::failure(QueryStatus::failure(code, phase, message));
}

struct BindingData {
  std::shared_ptr<const BinaryValue> bytes;
  std::shared_ptr<const ArrowLayoutDocument> layout;
  ArrowPhysicalBinding binding;
  std::uint32_t field{0};
};

bool matches(const StructuredBlobManifest& a, const StructuredBlobManifest& b) {
  return a.key == b.key && a.format_id == b.format_id && a.revision == b.revision &&
    a.payload_size == b.payload_size && a.layout_size == b.layout_size &&
    a.published == b.published && a.phase == b.phase;
}

// Owned only by one planning context. No version-keyed global cache, source
// reader, or planning budget survives in this immutable parsed artifact.
struct ArrowPlanningState final : AdapterPlanningState {
  StructuredBlobManifest manifest;
  std::shared_ptr<const BinaryValue> bytes;
  std::shared_ptr<const ArrowLayoutDocument> layout;
};

QueryResult<BindingData> bind(const QueryPlanningContext& context, const QueryFragment& fragment) {
  const auto& source = context.source();
  const auto& manifest = source.manifest();
  if (manifest.format_id != std::optional<std::string>("arrow"))
    return fail<BindingData>(QueryError::UnsupportedCapability, "source is not Arrow");
  if (!manifest.published || manifest.phase != StructuredBlobPhase::Published)
    return fail<BindingData>(QueryError::SourceNotPublished, "Arrow source is not published");
  if (!fragment.physical_array)
    return fail<BindingData>(QueryError::UnsupportedCapability,
      "Arrow query requires an explicit physical array subject");
  if (fragment.physical_array->values.size() > context.limits().max_ranges)
    return fail<BindingData>(QueryError::ResourceExhausted, "too many Arrow child paths");
  auto valid = validate_physical_array_subject(*fragment.physical_array);
  if (!valid.ok()) return QueryResult<BindingData>::failure(valid);
  if (fragment.output.mode != QueryFragment::ResultMode::LogicalValues ||
      fragment.output.interpolation || !fragment.output.type_overrides.empty())
    return fail<BindingData>(QueryError::UnsupportedCapability,
      "Arrow physical query supports native logical types without interpolation");
  if (!manifest.layout_size)
    return fail<BindingData>(QueryError::LayoutUnavailable, "Arrow layout is required");
  if (manifest.layout_size > ArrowLayoutLimits{}.max_layout_bytes ||
      manifest.layout_size > context.limits().max_layout_bytes)
    return fail<BindingData>(QueryError::ResourceExhausted, "Arrow layout exceeds parser limit");
  std::shared_ptr<const ArrowPlanningState> state;
  if (context.state()) {
    state = std::dynamic_pointer_cast<const ArrowPlanningState>(context.state());
    if (!state || !matches(state->manifest, manifest))
      return fail<BindingData>(QueryError::StalePlan,
        "Arrow planning context no longer matches its source");
  } else {
    auto bytes = source.read_layout(0, manifest.layout_size);
    if (!bytes.ok()) return QueryResult<BindingData>::failure(bytes.status());
    if (bytes.value().size() != manifest.layout_size)
      return fail<BindingData>(QueryError::ReadFailure, "short Arrow layout read");
    auto parsed = parse_arrow_layout(bytes.value(), manifest.payload_size);
    if (!parsed.ok()) return QueryResult<BindingData>::failure(parsed.status());
    auto loaded = std::make_shared<ArrowPlanningState>();
    loaded->manifest = manifest;
    loaded->bytes = std::make_shared<const BinaryValue>(std::move(bytes.value()));
    loaded->layout = std::make_shared<const ArrowLayoutDocument>(std::move(parsed.value()));
    state = std::move(loaded);
    context.set_state(state);
  }
  BindingData result;
  result.bytes = state->bytes;
  result.layout = state->layout;
  result.binding.subject = *fragment.physical_array;
  const auto& subject = result.binding.subject;
  std::optional<std::uint32_t> field;
  for (std::uint32_t i = 0; i < result.layout->fields.size(); ++i) {
    if (result.layout->fields[i].name != subject.field_id) continue;
    if (field) return fail<BindingData>(QueryError::SchemaMismatch,
      "Arrow native field name is ambiguous");
    field = i;
  }
  if (!field) return fail<BindingData>(QueryError::UnknownField,
    "unknown Arrow native field: " + subject.field_id);
  result.field = *field;
  const auto& native = result.layout->fields[*field];
  if (!native.list_depth)
    return fail<BindingData>(QueryError::UnsupportedCapability, native.unsupported_reason);
  auto index = make_arrow_list_index(result.layout, subject.record_batch_index, *field);
  if (!index.ok()) return QueryResult<BindingData>::failure(index.status());
  std::optional<std::uint64_t> length;
  for (const auto& path : subject.values) {
    auto value = resolve_array_value({native.list_depth, PhysicalArrayLeafType::Float32},
      *index.value(), subject.row_index, path);
    if (!value.ok()) return QueryResult<BindingData>::failure(value.status());
    if (value.value().valid) {
      const auto size = value.value().end - value.value().begin;
      if (length && *length != size)
        return fail<BindingData>(QueryError::InvalidQuery,
          "Arrow wide table requires equal lengths for valid selected Lists (ragged input)");
      length = size;
    }
    result.binding.values.push_back(value.value());
  }
  result.binding.length = length.value_or(0);
  return QueryResult<BindingData>::success(std::move(result));
}

void expression_fields(const QueryExpressionPtr& expression, std::set<std::string>& names) {
  if (!expression) return;
  std::visit([&](const auto& node) {
    using T = std::decay_t<decltype(node)>;
    if constexpr (std::is_same_v<T, LogicalExpression>) {
      for (const auto& child : node.operands) expression_fields(child, names);
    } else names.insert(node.field.field_id);
  }, expression->node);
}

class ArrowPlan final : public AdapterExecutionPlan {
public:
  ArrowPlan(std::uint64_t storage_namespace, StructuredBlobManifest manifest, BindingData data, LogicalSchema schema,
      std::uint64_t begin, std::uint64_t end, bool has_residual)
    : storage_namespace(storage_namespace), manifest(std::move(manifest)), data(std::move(data)), schema(std::move(schema)),
      begin(begin), end(end), has_residual(has_residual) {
    this->data.layout.reset(); // Parsed execution metadata belongs to QueryRuntime.
    for (std::size_t i = 0; i < this->data.binding.subject.values.size(); ++i)
      ordinals.emplace(arrow_query_value_field_id(this->data.binding.subject.values[i]), i);
  }
  std::uint64_t storage_namespace;
  StructuredBlobManifest manifest;
  BindingData data;
  LogicalSchema schema;
  std::map<std::string, std::size_t> ordinals;
  std::uint64_t begin, end;
  bool has_residual;
};

class ArrowCursor final : public QueryBatchCursor {
public:
  ArrowCursor(std::shared_ptr<const QuerySource> source, std::shared_ptr<const ArrowPlan> plan,
      QueryExecutionOptions options, ArrowDecodeConfig config, std::shared_ptr<QueryRuntime> runtime)
    : plan_(std::move(plan)), options_(options), provider_(std::move(source), plan_->data.bytes,
      std::move(runtime), options, config), position_(plan_->begin) {}
  const LogicalSchema& schema() const override { return plan_->schema; }
  const ArrowDecodeMetrics& metrics() const { return provider_.metrics(); }
  void cancel() override { cancelled_.store(true); provider_.cancel(); }
  QueryResult<std::optional<QueryBatch>> next() override {
    if (cancelled_.load()) return failure(QueryStatus::failure(QueryError::Cancelled,
      QueryPhase::Execution, "Arrow query cancelled"));
    if (terminal_) return QueryResult<std::optional<QueryBatch>>::failure(*terminal_);
    if (position_ == plan_->end)
      return QueryResult<std::optional<QueryBatch>>::success(std::nullopt);
    try { return render(); }
    catch (const std::bad_alloc&) { return failure(QueryStatus::failure(
      QueryError::ResourceExhausted, QueryPhase::Execution, "Arrow output allocation failed")); }
  }
private:
  QueryResult<std::optional<QueryBatch>> failure(QueryStatus status) {
    terminal_ = std::move(status);
    return QueryResult<std::optional<QueryBatch>>::failure(*terminal_);
  }
  QueryResult<std::optional<QueryBatch>> render() {
    // Conservative per-row accounting includes one validity byte per nullable
    // column; the actual packed bitmap is smaller. Never emit an oversized row.
    std::uint64_t row_bytes = 0;
    for (const auto& field : schema().fields())
      row_bytes += field.type.id == LogicalTypeId::UInt64 ? 8 : 5;
    const auto size = std::min<std::uint64_t>({plan_->end - position_,
      options_.target_batch_rows, options_.target_batch_bytes / row_bytes});
    // Core counts final rows after residual evaluation. Candidate rows must
    // not consume that budget merely because they will be filtered out.
    if (!size || (!plan_->has_residual && size > options_.max_result_rows - produced_))
      return failure(QueryStatus::failure(QueryError::ResourceExhausted,
        QueryPhase::Execution, "Arrow result exceeds row or batch byte budget"));
    std::vector<LogicalArray> columns;
    for (const auto& field : schema().fields()) {
      if (cancelled_.load()) return failure(QueryStatus::failure(QueryError::Cancelled,
        QueryPhase::Execution, "Arrow query cancelled"));
      if (field.field_id == "index") {
        std::vector<std::uint64_t> indices(size);
        for (std::uint64_t i = 0; i < size; ++i) indices[i] = position_ + i;
        columns.push_back({field.type, std::move(indices), {}});
        continue;
      }
      const auto ordinal = plan_->ordinals.find(field.field_id);
      if (ordinal == plan_->ordinals.end()) return failure(QueryStatus::failure(QueryError::Internal,
        QueryPhase::Execution, "Arrow plan column is not bound"));
      const auto& resolved = plan_->data.binding.values[ordinal->second];
      std::vector<float> values(size, 0.f);
      BinaryValue validity((size + 7) / 8, 0);
      if (resolved.valid) {
        std::uint64_t copied = 0;
        while (copied < size) {
          const auto leaf = resolved.begin + position_ + copied;
          auto decoded = provider_.get(plan_->data.binding.subject.record_batch_index,
            plan_->data.field, leaf, size - copied);
          if (!decoded.ok()) return failure(decoded.status());
          const auto& fragment = *decoded.value();
          const auto count = std::min(size - copied, fragment.end - leaf);
          if (!count) return failure(QueryStatus::failure(QueryError::Internal,
            QueryPhase::Execution, "Arrow decoder returned an empty fragment"));
          const auto offset = leaf - fragment.begin;
          std::copy_n(fragment.values.begin() + offset, count, values.begin() + copied);
          for (std::uint64_t i = 0; i < count; ++i) {
            const auto bit = offset + i;
            if (fragment.validity.empty() || (fragment.validity[bit / 8] & (1u << (bit % 8))))
              validity[(copied + i) / 8] |= 1u << ((copied + i) % 8);
          }
          copied += count;
        }
      }
      columns.push_back({field.type, std::move(values), std::move(validity)});
    }
    if (cancelled_.load()) return failure(QueryStatus::failure(QueryError::Cancelled,
      QueryPhase::Execution, "Arrow query cancelled"));
    DomainDescriptor domain{DomainKind::Rows,
      {{{"index"}, {LogicalTypeId::UInt64, false, std::nullopt}, std::nullopt}},
      {size}, {position_}};
    const auto& manifest = plan_->manifest;
    auto batch = QueryBatch::create(schema(), size, std::move(columns), std::move(domain),
      {{manifest.key.path, manifest.key.version, "arrow"}});
    if (!batch.ok()) return failure(batch.status());
    std::uint64_t output_bytes = 0;
    for (const auto& field : schema().fields())
      output_bytes += field.type.id == LogicalTypeId::UInt64 ? size * 8 : size * 4 + (size + 7) / 8;
    provider_.account_output(output_bytes);
    position_ += size;
    produced_ += size;
    return QueryResult<std::optional<QueryBatch>>::success(std::move(batch.value()));
  }
  std::shared_ptr<const ArrowPlan> plan_;
  QueryExecutionOptions options_;
  ArrowFragmentProvider provider_;
  std::uint64_t position_, produced_{0};
  std::atomic<bool> cancelled_{false};
  std::optional<QueryStatus> terminal_;
};

} // namespace

ArrowFormatQueryAdapter::ArrowFormatQueryAdapter(ArrowDecodeConfig config,
    std::shared_ptr<QueryRuntime> runtime)
  : config_(config), runtime_(runtime ? std::move(runtime) : std::make_shared<QueryRuntime>()) {}
const std::string& ArrowFormatQueryAdapter::id() const {
  static const std::string id = "arrow";
  return id;
}

QueryResult<QueryCapabilities> ArrowFormatQueryAdapter::capabilities(
    const QueryPlanningContext& context, const QueryFragment& fragment) const {
  auto data = bind(context, fragment);
  if (!data.ok()) return QueryResult<QueryCapabilities>::failure(data.status());
  return make_arrow_query_capabilities(data.value().binding);
}

QueryResult<AdapterPlan> ArrowFormatQueryAdapter::prepare(
    const QueryPlanningContext& context, const QueryFragment& fragment) const {
  auto data = bind(context, fragment);
  if (!data.ok()) return QueryResult<AdapterPlan>::failure(data.status());
  auto schema = make_arrow_logical_schema(data.value().binding);
  if (!schema.ok()) return QueryResult<AdapterPlan>::failure(schema.status());
  if (fragment.predicate) {
    auto valid = validate_expression(fragment.predicate, schema.value());
    if (!valid.ok()) return QueryResult<AdapterPlan>::failure(valid);
  }
  if (fragment.projection.empty())
    return fail<AdapterPlan>(QueryError::InvalidQuery, "Arrow projection is required");
  std::set<std::string> selected;
  std::vector<LogicalField> output;
  for (const auto& ref : fragment.projection) {
    const auto* field = schema.value().find(ref.field_id);
    if (!field) return fail<AdapterPlan>(QueryError::UnknownField, "unknown Arrow column: " + ref.field_id);
    if (!selected.insert(ref.field_id).second)
      return fail<AdapterPlan>(QueryError::InvalidQuery, "duplicate Arrow projection");
    output.push_back(*field);
  }
  std::set<std::string> required;
  expression_fields(fragment.predicate, required);
  for (const auto& field : schema.value().fields())
    if (required.count(field.field_id) && selected.insert(field.field_id).second)
      output.push_back(field);
  const auto length = data.value().binding.length;
  std::uint64_t begin = 0, end = length;
  std::set<std::string> sliced;
  for (const auto& slice : fragment.slices) {
    auto status = validate_domain_slice(slice, schema.value());
    if (!status.ok()) return QueryResult<AdapterPlan>::failure(status);
    if (!sliced.insert(slice.dimension.field_id).second)
      return fail<AdapterPlan>(QueryError::InvalidDomainSlice, "duplicate Arrow index slice");
    if (slice.lower) {
      auto lower = std::get<std::uint64_t>(slice.lower->value.value());
      if (slice.lower->kind == BoundKind::Exclusive && lower < length) ++lower;
      begin = std::min(lower, length);
    }
    if (slice.upper) {
      auto upper = std::get<std::uint64_t>(slice.upper->value.value());
      if (slice.upper->kind == BoundKind::Inclusive && upper < length) ++upper;
      end = std::min(upper, length);
    }
  }
  end = std::max(begin, end);
  auto output_schema = LogicalSchema::create(std::move(output));
  if (!output_schema.ok()) return QueryResult<AdapterPlan>::failure(output_schema.status());
  auto plan = std::make_shared<const ArrowPlan>(context.source().storage_namespace(), context.source().manifest(), std::move(data.value()),
    output_schema.value(), begin, end, bool(fragment.predicate));
  return QueryResult<AdapterPlan>::success(AdapterPlan(std::move(plan),
    std::move(output_schema.value()), fragment.predicate, {}, fragment.projection));
}

QueryResult<std::unique_ptr<QueryBatchCursor>> ArrowFormatQueryAdapter::execute(
    const QueryExecutionContext& context, std::shared_ptr<const AdapterExecutionPlan> plan,
    const QueryExecutionOptions& options) const {
  if (!context.source || !context.shared_source ||
      context.source->storage_namespace() != context.shared_source->storage_namespace() ||
      !matches(context.source->manifest(), context.shared_source->manifest()))
    return fail<std::unique_ptr<QueryBatchCursor>>(QueryError::StalePlan,
      "Arrow execution source capabilities differ", QueryPhase::Execution);
  const auto& source = *context.source;
  auto arrow = std::dynamic_pointer_cast<const ArrowPlan>(plan);
  if (!arrow || arrow->storage_namespace != source.storage_namespace() ||
      !matches(arrow->manifest, source.manifest()))
    return fail<std::unique_ptr<QueryBatchCursor>>(QueryError::StalePlan,
      "Arrow plan source identity changed", QueryPhase::Execution);
  if (!options.target_batch_rows || !options.target_batch_bytes || !options.max_ranges ||
      !options.max_result_rows || !config_.tile_elements || !config_.max_buffer_bytes ||
      arrow->manifest.layout_size > options.max_layout_bytes)
    return fail<std::unique_ptr<QueryBatchCursor>>(QueryError::ResourceExhausted,
      "Arrow execution limits cannot admit the plan", QueryPhase::Execution);
  auto bytes = source.read_layout(0, arrow->manifest.layout_size);
  if (!bytes.ok()) return QueryResult<std::unique_ptr<QueryBatchCursor>>::failure(bytes.status());
  if (bytes.value().size() != arrow->manifest.layout_size)
    return fail<std::unique_ptr<QueryBatchCursor>>(QueryError::ReadFailure,
      "short Arrow layout read", QueryPhase::Execution);
  if (bytes.value() != *arrow->data.bytes)
    return fail<std::unique_ptr<QueryBatchCursor>>(QueryError::StalePlan,
      "Arrow plan layout/schema identity changed", QueryPhase::Execution);
  auto remaining = options;
  --remaining.max_ranges;
  remaining.max_layout_bytes -= bytes.value().size();
  std::unique_ptr<QueryBatchCursor> cursor = std::make_unique<ArrowCursor>(context.shared_source,
    std::move(arrow), remaining, config_, runtime_);
  return QueryResult<std::unique_ptr<QueryBatchCursor>>::success(std::move(cursor));
}

QueryResult<ArrowPhysicalBinding> arrow_physical_binding(
    std::shared_ptr<const AdapterExecutionPlan> plan) {
  auto arrow = std::dynamic_pointer_cast<const ArrowPlan>(plan);
  if (!arrow) return fail<ArrowPhysicalBinding>(QueryError::InvalidQuery, "not an Arrow plan");
  return QueryResult<ArrowPhysicalBinding>::success(arrow->data.binding);
}

QueryResult<ArrowDecodeMetrics> arrow_decode_metrics(const QueryBatchCursor& cursor) {
  const auto* arrow = dynamic_cast<const ArrowCursor*>(&cursor);
  if (!arrow) return fail<ArrowDecodeMetrics>(QueryError::InvalidQuery, "not an Arrow cursor");
  return QueryResult<ArrowDecodeMetrics>::success(arrow->metrics());
}

} // namespace iotdbfs
