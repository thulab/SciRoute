#include "iotdbfs/formats/arrow/ArrowFormatAdapter.h"
#include "iotdbfs/formats/arrow/ArrowFormatQueryAdapter.h"
#include "iotdbfs/formats/arrow/ArrowFragmentProvider.h"
#include "iotdbfs/query/core/QueryModule.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/compression.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <future>
#include <limits>
#include <mutex>
#include <thread>
#include <stdexcept>

namespace {
using namespace iotdbfs;

void checked(const arrow::Status& s) {
  if (!s.ok()) throw std::runtime_error(s.ToString());
}
template<class T> T checked(arrow::Result<T> r) {
  if (!r.ok()) throw std::runtime_error(r.status().ToString());
  return std::move(r).ValueOrDie();
}

// Row 0: three equal-length values; row 1: null parent; row 2: one valid
// and one null child; row 3: valid empty parent; row 4: ragged children.
std::shared_ptr<arrow::RecordBatch> make_batch(bool large = false,
    bool single = false, float batch_bias = 0, const std::string& field_name = "target") {
  auto pool = arrow::default_memory_pool();
  auto leaf = std::make_shared<arrow::FloatBuilder>(pool);
  std::shared_ptr<arrow::ArrayBuilder> inner;
  if (large) inner = std::make_shared<arrow::LargeListBuilder>(pool, leaf);
  else inner = std::make_shared<arrow::ListBuilder>(pool, leaf);
  std::shared_ptr<arrow::ArrayBuilder> outer;
  if (single) outer = inner;
  else if (large) outer = std::make_shared<arrow::LargeListBuilder>(pool, inner);
  else outer = std::make_shared<arrow::ListBuilder>(pool, inner);
  auto append = [&](std::shared_ptr<arrow::ArrayBuilder> b) {
    if (large) checked(std::static_pointer_cast<arrow::LargeListBuilder>(b)->Append());
    else checked(std::static_pointer_cast<arrow::ListBuilder>(b)->Append());
  };
  auto values = [&](int child, int size) {
    for (int j = 0; j < size; ++j) {
      if (child == 2 && j == 5) checked(leaf->AppendNull());
      else if (child == 2 && j == 6) checked(leaf->Append(std::numeric_limits<float>::quiet_NaN()));
      else checked(leaf->Append(batch_bias + child * 100.f + j));
    }
  };
  append(outer);
  if (single) values(2, 20);
  else for (int c = 0; c < 3; ++c) { append(inner); values(c, 20); }
  checked(outer->AppendNull());
  append(outer);
  if (single) values(0, 20);
  else { append(inner); values(0, 20); checked(inner->AppendNull()); }
  append(outer); // empty row
  append(outer);
  if (single) values(0, 3);
  else { append(inner); values(0, 3); append(inner); values(1, 4); }
  std::shared_ptr<arrow::Array> array;
  checked(outer->Finish(&array));
  return arrow::RecordBatch::Make(arrow::schema({arrow::field(field_name, array->type())}), 5, {array});
}

BinaryValue make_stream(bool large = false, bool single = false,
    arrow::Compression::type compression = arrow::Compression::UNCOMPRESSED,
    const std::string& field_name = "target") {
  auto first = make_batch(large, single, 0, field_name);
  auto sink = checked(arrow::io::BufferOutputStream::Create());
  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  if (compression != arrow::Compression::UNCOMPRESSED)
    options.codec = std::shared_ptr<arrow::util::Codec>(checked(arrow::util::Codec::Create(compression)));
  auto writer = checked(arrow::ipc::MakeStreamWriter(sink, first->schema(), options));
  checked(writer->WriteRecordBatch(*first));
  checked(writer->WriteRecordBatch(*make_batch(large, single, 1000.f, field_name)));
  checked(writer->Close());
  auto bytes = checked(sink->Finish());
  return {bytes->data(), bytes->data() + bytes->size()};
}

class ArrowTestSource final : public QuerySource, public std::enable_shared_from_this<ArrowTestSource> {
public:
  explicit ArrowTestSource(BinaryValue data = make_stream()) : payload(std::move(data)) {
    FormatSource input{payload.size(), [this](std::uint64_t offset, std::uint64_t size)
        -> std::optional<BinaryValue> {
      if (offset > payload.size() || size > payload.size() - offset) return std::nullopt;
      return BinaryValue(payload.begin() + offset, payload.begin() + offset + size);
    }};
    ArrowFormatAdapter ingest;
    auto session = ingest.begin_ingestion(input);
    if (!session) throw std::runtime_error("no ingestion session");
    auto status = session->consume_chunk(0, payload.data(), payload.size());
    if (!status.ok()) throw std::runtime_error(status.error());
    auto result = session->finish();
    if (!result.ok()) throw std::runtime_error(result.error());
    layout = result.value().layout;
    manifest_.key = {"/arrow/test.arrows", 17};
    manifest_.format_id = "arrow";
    manifest_.payload_size = payload.size();
    manifest_.layout_size = layout.size();
    manifest_.published = true;
    manifest_.phase = StructuredBlobPhase::Published;
  }
  const StructuredBlobManifest& manifest() const override { return manifest_; }
  QueryResult<BinaryValue> read_payload(std::uint64_t off, std::uint64_t len) const override {
    { std::lock_guard<std::mutex> lock(trace_mutex); reads.push_back({off, len}); }
    if (on_read) on_read();
    if (fail_payload) return QueryResult<BinaryValue>::failure(QueryStatus::failure(
      QueryError::ReadFailure, QueryPhase::Storage, "injected read failure"));
    auto result = read(payload, off, len);
    if (short_payload && result.ok() && !result.value().empty()) result.value().pop_back();
    return result;
  }
  QueryResult<StructuredBlobPayloadBatch> read_payload_ranges(
      const StructuredBlobPayloadRanges& ranges) const override {
    { std::lock_guard<std::mutex> lock(trace_mutex);
      batch_reads.push_back(ranges);
      for (const auto& range : ranges) reads.push_back({range.offset, range.length});
    }
    if (on_read) on_read();
    if (fail_payload) return QueryResult<StructuredBlobPayloadBatch>::failure(QueryStatus::failure(
      QueryError::ReadFailure, QueryPhase::Storage, "injected batch read failure"));
    StructuredBlobPayloadBatch result;
    result.reserve(ranges.size());
    for (const auto& range : ranges) {
      auto bytes = read(payload, range.offset, range.length);
      if (!bytes.ok()) return QueryResult<StructuredBlobPayloadBatch>::failure(bytes.status());
      result.push_back(std::move(bytes.value()));
    }
    if (short_payload && !result.empty() && !result.front().empty()) result.front().pop_back();
    return QueryResult<StructuredBlobPayloadBatch>::success(std::move(result));
  }
  QueryResult<BinaryValue> read_layout(std::uint64_t off, std::uint64_t len) const override {
    { std::lock_guard<std::mutex> lock(trace_mutex); layout_reads.push_back({off, len}); }
    return read(layout, off, len);
  }
  std::shared_ptr<const ArrowLayoutDocument> document() const {
    auto parsed = parse_arrow_layout(layout, payload.size());
    if (!parsed.ok()) throw std::runtime_error(parsed.status().message());
    return std::make_shared<const ArrowLayoutDocument>(std::move(parsed.value()));
  }
  static QueryResult<BinaryValue> read(const BinaryValue& data, std::uint64_t off, std::uint64_t len) {
    if (off > data.size() || len > data.size() - off)
      return QueryResult<BinaryValue>::failure(QueryStatus::failure(
        QueryError::ReadFailure, QueryPhase::Storage, "invalid test range"));
    return QueryResult<BinaryValue>::success({data.begin() + off, data.begin() + off + len});
  }
  std::shared_ptr<const BinaryValue> layout_bytes() const {
    return std::make_shared<const BinaryValue>(layout);
  }
  mutable std::mutex trace_mutex;
  StructuredBlobManifest manifest_;
  BinaryValue payload, layout;
  mutable std::vector<ArrowBufferSpan> reads;
  mutable std::vector<StructuredBlobPayloadRanges> batch_reads;
  mutable std::vector<ArrowBufferSpan> layout_reads;
  bool fail_payload{false}, short_payload{false};
  std::function<void()> on_read;
};

QueryFragment fragment(bool single = false, std::uint64_t row = 0) {
  QueryFragment q;
  q.physical_array = PhysicalArraySubject{"target", 0, row,
    single ? std::vector<ArrayValuePath>{{{}}} : std::vector<ArrayValuePath>{{{2}}, {{0}}}};
  q.projection = single ? std::vector<FieldRef>{{"index"}, {"value"}} :
    std::vector<FieldRef>{{"index"}, {"value.2"}, {"value.0"}};
  return q;
}
DomainSlice slice(std::uint64_t begin, std::uint64_t end) {
  return {{"index"}, DomainBound{LogicalValue::uint64(begin), BoundKind::Inclusive},
    DomainBound{LogicalValue::uint64(end), BoundKind::Exclusive}};
}
std::vector<QueryBatch> run(ArrowFormatQueryAdapter& adapter, ArrowTestSource& source,
    const QueryFragment& q, QueryExecutionOptions options = {}) {
  auto plan = adapter.prepare(QueryPlanningContext(source), q);
  if (!plan.ok()) throw std::runtime_error(plan.status().message());
  auto cursor = adapter.execute(QueryExecutionContext(source.shared_from_this()), plan.value().implementation(), options);
  if (!cursor.ok()) throw std::runtime_error(cursor.status().message());
  std::vector<QueryBatch> result;
  for (;;) {
    auto next = cursor.value()->next();
    if (!next.ok()) throw std::runtime_error(next.status().message());
    if (!next.value()) break;
    result.push_back(std::move(*next.value()));
  }
  return result;
}
bool valid(const LogicalArray& a, std::size_t row) {
  return a.validity.empty() || (a.validity.at(row / 8) & (1u << (row % 8)));
}
const LogicalArray& column(const QueryBatch& b, const std::string& name) {
  return b.columns().at(*b.schema().index_of(name));
}
} // namespace

TEST(ArrowFormatQueryAdapter, DeclaresAnonymousRowsAndResidualComparisons) {
  auto source_owner = std::make_shared<ArrowTestSource>(); auto& source = *source_owner;
  ArrowFormatQueryAdapter adapter;
  auto c = adapter.capabilities(QueryPlanningContext(source), fragment());
  ASSERT_TRUE(c.ok()) << c.status().message();
  EXPECT_EQ(adapter.id(), "arrow");
  EXPECT_TRUE(validate_query_capabilities(c.value()).ok());
  ASSERT_EQ(c.value().domains().size(), 1u);
  EXPECT_EQ(c.value().domains()[0], DomainKind::Rows);
  ASSERT_EQ(c.value().schema().fields().size(), 3u);
  EXPECT_EQ(c.value().schema().fields()[1].field_id, "value.2");
  EXPECT_EQ(c.value().schema().fields()[2].field_id, "value.0");
  EXPECT_TRUE(source.reads.empty());
}

TEST(ArrowFormatQueryAdapter, PositionalSlicePreservesChildOrderNullAndNan) {
  auto source_owner = std::make_shared<ArrowTestSource>(); auto& source = *source_owner;
  ArrowDecodeConfig config; config.tile_elements = 4;
  ArrowFormatQueryAdapter adapter(config);
  auto q = fragment(); q.slices = {slice(3, 17)};
  auto batches = run(adapter, source, q);
  ASSERT_EQ(batches.size(), 1u);
  const auto& b = batches.front(); ASSERT_EQ(b.row_count(), 14u);
  EXPECT_EQ(std::get<std::vector<std::uint64_t>>(column(b, "index").values).front(), 3u);
  const auto& a = column(b, "value.2");
  EXPECT_FLOAT_EQ(std::get<std::vector<float>>(a.values)[0], 203.f);
  EXPECT_FALSE(valid(a, 2)); EXPECT_TRUE(valid(a, 3));
  EXPECT_TRUE(std::isnan(std::get<std::vector<float>>(a.values)[3]));
  EXPECT_FLOAT_EQ(std::get<std::vector<float>>(column(b, "value.0").values)[13], 16.f);
  ASSERT_TRUE(b.domain()); EXPECT_EQ(b.domain()->kind, DomainKind::Rows);
  EXPECT_EQ(b.sources().front().version, 17);
  const auto doc = source.document(); const auto span = doc->batches[0].arrays[0].leaf_values;
  std::uint64_t read_bytes = 0;
  for (const auto& r : source.reads) {
    if (r.offset >= span.offset && r.offset < span.offset + span.length) {
      EXPECT_LE(r.length, 16u); read_bytes += r.length;
    }
  }
  EXPECT_GT(read_bytes, 0u); EXPECT_LT(read_bytes, span.length);
}

TEST(ArrowFormatQueryAdapter, SingleAndLargeListValuesWorkWithoutSemanticMetadata) {
  for (bool large : {false, true}) for (bool single : {false, true}) {
    auto source_owner = std::make_shared<ArrowTestSource>(make_stream(large, single)); auto& source = *source_owner;
    ArrowFormatQueryAdapter adapter;
    auto q = fragment(single); q.slices = {slice(3, 8)};
    auto batches = run(adapter, source, q);
    ASSERT_EQ(batches.size(), 1u); EXPECT_EQ(batches[0].row_count(), 5u);
    const auto& a = column(batches[0], single ? "value" : "value.2");
    EXPECT_FLOAT_EQ(std::get<std::vector<float>>(a.values)[0], 203.f);
    EXPECT_FALSE(valid(a, 2)); EXPECT_TRUE(valid(a, 3));
  }
}

TEST(ArrowFormatQueryAdapter, ValueOnlyBatchesRetainUnprojectedPositionDomain) {
  auto source_owner = std::make_shared<ArrowTestSource>(); auto& source = *source_owner;
  ArrowFormatQueryAdapter adapter;
  auto q = fragment(); q.projection = {{"value.0"}};
  q.slices = {slice(3, 17)};
  QueryExecutionOptions options; options.target_batch_rows = 5;
  auto batches = run(adapter, source, q, options);
  ASSERT_EQ(batches.size(), 3u);
  for (std::size_t i = 0; i < batches.size(); ++i) {
    const auto& batch = batches[i];
    const auto begin = 3u + 5u * i;
    const auto count = i == 2 ? 4u : 5u;
    ASSERT_EQ(batch.schema().fields().size(), 1u);
    EXPECT_EQ(batch.schema().find("index"), nullptr);
    ASSERT_TRUE(batch.domain());
    EXPECT_EQ(batch.domain()->kind, DomainKind::Rows);
    ASSERT_EQ(batch.domain()->dimensions.size(), 1u);
    EXPECT_EQ(batch.domain()->dimensions[0].field.field_id, "index");
    EXPECT_EQ(batch.domain()->origin, (std::vector<std::uint64_t>{begin}));
    EXPECT_EQ(batch.domain()->shape, (std::vector<std::uint64_t>{count}));
    const auto& values = std::get<std::vector<float>>(column(batch, "value.0").values);
    ASSERT_EQ(values.size(), count);
    EXPECT_FLOAT_EQ(values.front(), static_cast<float>(begin));
    EXPECT_FLOAT_EQ(values.back(), static_cast<float>(begin + count - 1));
  }
}

TEST(ArrowFormatQueryAdapter, ExactBatchBindingDoesNotReadAnotherBatch) {
  auto source_owner = std::make_shared<ArrowTestSource>(); auto& source = *source_owner;
  ArrowFormatQueryAdapter adapter;
  auto q = fragment(); q.physical_array->record_batch_index = 1;
  q.slices = {slice(0, 1)};
  auto batches = run(adapter, source, q);
  EXPECT_FLOAT_EQ(std::get<std::vector<float>>(column(batches.at(0), "value.2").values)[0], 1200.f);
  const auto doc = source.document(); const auto& message = doc->messages[doc->batches[1].message_index];
  for (const auto& read : source.reads) EXPECT_GE(read.offset, message.body_offset);
}

TEST(ArrowFormatQueryAdapter, NullParentShortCircuitsBadChildAndRetainsNullLevel) {
  auto source_owner = std::make_shared<ArrowTestSource>(); auto& source = *source_owner;
  ArrowFormatQueryAdapter adapter;
  auto q = fragment(false, 1); q.physical_array->values = {{{999}}}; q.projection = {{"value.999"}};
  auto plan = adapter.prepare(QueryPlanningContext(source), q); ASSERT_TRUE(plan.ok()) << plan.status().message();
  auto binding = arrow_physical_binding(plan.value().implementation()); ASSERT_TRUE(binding.ok());
  ASSERT_EQ(binding.value().values.size(), 1u);
  EXPECT_EQ(binding.value().values[0].null_list_level, std::optional<std::uint32_t>(0));
  EXPECT_EQ(binding.value().length, 0u);
  auto batches = run(adapter, source, q);
  for (const auto& b : batches) EXPECT_EQ(b.row_count(), 0u);
  EXPECT_TRUE(source.reads.empty());
}

TEST(ArrowFormatQueryAdapter, NullChildUsesValidSiblingLengthAndAllNullColumn) {
  auto source_owner = std::make_shared<ArrowTestSource>(); auto& source = *source_owner;
  ArrowFormatQueryAdapter adapter;
  auto q = fragment(false, 2); q.physical_array->values = {{{1}}, {{0}}};
  q.projection = {{"value.1"}, {"value.0"}};
  auto plan = adapter.prepare(QueryPlanningContext(source), q); ASSERT_TRUE(plan.ok()) << plan.status().message();
  auto binding = arrow_physical_binding(plan.value().implementation()); ASSERT_TRUE(binding.ok());
  EXPECT_EQ(binding.value().length, 20u);
  EXPECT_EQ(binding.value().values[0].null_list_level, std::optional<std::uint32_t>(1));
  auto batches = run(adapter, source, q); ASSERT_EQ(batches.size(), 1u);
  EXPECT_EQ(batches[0].row_count(), 20u);
  for (std::size_t i = 0; i < 20; ++i) EXPECT_FALSE(valid(column(batches[0], "value.1"), i));
}

TEST(ArrowFormatQueryAdapter, RejectsRaggedValidChildrenBeforeReadingFloatValues) {
  auto source_owner = std::make_shared<ArrowTestSource>(); auto& source = *source_owner;
  ArrowFormatQueryAdapter adapter;
  auto q = fragment(false, 4); q.physical_array->values = {{{0}}, {{1}}};
  q.projection = {{"value.0"}, {"value.1"}};
  auto plan = adapter.prepare(QueryPlanningContext(source), q);
  ASSERT_FALSE(plan.ok()); EXPECT_EQ(plan.status().code(), QueryError::InvalidQuery);
  EXPECT_TRUE(source.reads.empty());
}

TEST(ArrowFormatQueryAdapter, EmptySingleListReturnsSchemaWithoutValues) {
  auto source_owner = std::make_shared<ArrowTestSource>(make_stream(false, true)); auto& source = *source_owner;
  ArrowFormatQueryAdapter adapter; auto q = fragment(true, 3);
  auto plan = adapter.prepare(QueryPlanningContext(source), q); ASSERT_TRUE(plan.ok()) << plan.status().message();
  auto cursor = adapter.execute(QueryExecutionContext(source.shared_from_this()), plan.value().implementation(), {}); ASSERT_TRUE(cursor.ok());
  ASSERT_EQ(cursor.value()->schema().fields().size(), 2u);
  auto batches = run(adapter, source, q);
  for (const auto& b : batches) EXPECT_EQ(b.row_count(), 0u);
  EXPECT_TRUE(source.reads.empty());
}

TEST(ArrowFormatQueryAdapter, IndexOnlyProjectionDoesNotReadFloatPayload) {
  auto source_owner = std::make_shared<ArrowTestSource>(); auto& source = *source_owner; ArrowFormatQueryAdapter adapter;
  auto q = fragment(); q.projection = {{"index"}}; q.slices = {slice(7, 11)};
  auto batches = run(adapter, source, q); ASSERT_EQ(batches.size(), 1u);
  EXPECT_EQ(batches[0].row_count(), 4u); EXPECT_TRUE(source.reads.empty());
}

TEST(ArrowFormatQueryAdapter, CarriesHiddenPredicateColumnAndResidualExpression) {
  auto source_owner = std::make_shared<ArrowTestSource>(); auto& source = *source_owner; ArrowFormatQueryAdapter adapter;
  auto q = fragment(); q.projection = {{"value.2"}};
  q.predicate = make_comparison({"value.0"}, ComparisonOperator::Greater, LogicalValue::float32(10));
  auto plan = adapter.prepare(QueryPlanningContext(source), q); ASSERT_TRUE(plan.ok()) << plan.status().message();
  EXPECT_EQ(plan.value().residual_predicate(), q.predicate);
  EXPECT_NE(plan.value().adapter_output_schema().find("value.0"), nullptr);
  ASSERT_EQ(plan.value().final_projection().size(), 1u);
  EXPECT_EQ(plan.value().final_projection()[0].field_id, "value.2");
}

TEST(ArrowFormatQueryAdapter, InclusiveExclusiveBoundsAndBatchesKeepOriginalPositions) {
  auto source_owner = std::make_shared<ArrowTestSource>(); auto& source = *source_owner; ArrowFormatQueryAdapter adapter;
  auto q = fragment(); q.projection = {{"index"}};
  q.slices = {{{"index"}, DomainBound{LogicalValue::uint64(3), BoundKind::Exclusive},
    DomainBound{LogicalValue::uint64(8), BoundKind::Inclusive}}};
  QueryExecutionOptions options; options.target_batch_rows = 2;
  auto batches = run(adapter, source, q, options);
  std::vector<std::uint64_t> indexes;
  for (const auto& b : batches) {
    EXPECT_LE(b.row_count(), 2u);
    const auto& values = std::get<std::vector<std::uint64_t>>(column(b, "index").values);
    indexes.insert(indexes.end(), values.begin(), values.end());
  }
  EXPECT_EQ(indexes, (std::vector<std::uint64_t>{4, 5, 6, 7, 8}));
}

TEST(ArrowFormatQueryAdapter, RejectsUnboundAndInvalidPhysicalAddresses) {
  auto source_owner = std::make_shared<ArrowTestSource>(); auto& source = *source_owner; ArrowFormatQueryAdapter adapter;
  auto q = fragment(); q.physical_array.reset(); EXPECT_FALSE(adapter.prepare(QueryPlanningContext(source), q).ok());
  q = fragment(); q.physical_array->field_id = "missing"; EXPECT_FALSE(adapter.prepare(QueryPlanningContext(source), q).ok());
  q = fragment(); q.physical_array->row_index = 5; EXPECT_FALSE(adapter.prepare(QueryPlanningContext(source), q).ok());
  q = fragment(); q.physical_array->record_batch_index = 2; EXPECT_FALSE(adapter.prepare(QueryPlanningContext(source), q).ok());
  q = fragment(); q.physical_array->values = {{{3}}}; q.projection = {{"value.3"}};
  EXPECT_FALSE(adapter.prepare(QueryPlanningContext(source), q).ok()); EXPECT_TRUE(source.reads.empty());
}

TEST(ArrowFormatQueryAdapter, RejectsStaleVersionAndChangedLayout) {
  auto source_owner = std::make_shared<ArrowTestSource>(); auto& source = *source_owner; ArrowFormatQueryAdapter adapter;
  auto plan = adapter.prepare(QueryPlanningContext(source), fragment()); ASSERT_TRUE(plan.ok());
  ++source.manifest_.key.version;
  auto stale = adapter.execute(QueryExecutionContext(source.shared_from_this()), plan.value().implementation(), {});
  ASSERT_FALSE(stale.ok()); EXPECT_EQ(stale.status().code(), QueryError::StalePlan);
  --source.manifest_.key.version;
  source.layout.back() ^= 1;
  stale = adapter.execute(QueryExecutionContext(source.shared_from_this()), plan.value().implementation(), {});
  ASSERT_FALSE(stale.ok()); EXPECT_EQ(stale.status().code(), QueryError::StalePlan);
}

TEST(ArrowFormatQueryAdapter, PreservesReadFailuresAndDetectsShortReads) {
  for (bool short_read : {false, true}) {
    auto source_owner = std::make_shared<ArrowTestSource>(); auto& source = *source_owner; ArrowFormatQueryAdapter adapter;
    auto plan = adapter.prepare(QueryPlanningContext(source), fragment()); ASSERT_TRUE(plan.ok());
    source.short_payload = short_read; source.fail_payload = !short_read;
    auto cursor = adapter.execute(QueryExecutionContext(source.shared_from_this()), plan.value().implementation(), {}); ASSERT_TRUE(cursor.ok());
    auto next = cursor.value()->next(); ASSERT_FALSE(next.ok());
    EXPECT_EQ(next.status().code(), QueryError::ReadFailure);
  }
}

TEST(ArrowFormatQueryAdapter, CancellationStopsCursorBeforePayloadReads) {
  auto source_owner = std::make_shared<ArrowTestSource>(); auto& source = *source_owner; ArrowFormatQueryAdapter adapter;
  auto plan = adapter.prepare(QueryPlanningContext(source), fragment()); ASSERT_TRUE(plan.ok());
  auto cursor = adapter.execute(QueryExecutionContext(source.shared_from_this()), plan.value().implementation(), {}); ASSERT_TRUE(cursor.ok());
  cursor.value()->cancel();
  auto next = cursor.value()->next(); ASSERT_FALSE(next.ok());
  EXPECT_EQ(next.status().code(), QueryError::Cancelled); EXPECT_TRUE(source.reads.empty());
}

TEST(ArrowFormatQueryAdapter, PayloadBudgetRejectsBeforePhysicalRead) {
  auto source_owner = std::make_shared<ArrowTestSource>(); auto& source = *source_owner; ArrowFormatQueryAdapter adapter;
  auto plan = adapter.prepare(QueryPlanningContext(source), fragment()); ASSERT_TRUE(plan.ok());
  QueryExecutionOptions options; options.max_payload_bytes = 1;
  auto cursor = adapter.execute(QueryExecutionContext(source.shared_from_this()), plan.value().implementation(), options); ASSERT_TRUE(cursor.ok());
  auto next = cursor.value()->next(); ASSERT_FALSE(next.ok());
  EXPECT_EQ(next.status().code(), QueryError::ResourceExhausted); EXPECT_TRUE(source.reads.empty());
}

TEST(ArrowFormatQueryAdapter, CompressedChildrenReuseOneWholePhysicalFloatBuffer) {
  for (auto codec : {arrow::Compression::LZ4_FRAME, arrow::Compression::ZSTD}) {
    auto source_owner = std::make_shared<ArrowTestSource>(make_stream(false, false, codec)); auto& source = *source_owner;
    auto runtime = std::make_shared<QueryRuntime>();
    ArrowFormatQueryAdapter adapter({}, runtime);
    auto q = fragment(); q.slices = {slice(3, 17)};
    auto plan = adapter.prepare(QueryPlanningContext(source), q); ASSERT_TRUE(plan.ok()) << plan.status().message();
    QueryExecutionOptions options; options.target_batch_rows = 3;
    auto cursor = adapter.execute(QueryExecutionContext(source.shared_from_this()), plan.value().implementation(), options); ASSERT_TRUE(cursor.ok());
    std::size_t rows = 0;
    for (;;) {
      auto next = cursor.value()->next(); ASSERT_TRUE(next.ok()) << next.status().message();
      if (!next.value()) break;
      rows += next.value()->row_count();
    }
    EXPECT_EQ(rows, 14u);
    const auto doc = source.document(); const auto& array = doc->batches[0].arrays[0];
    std::size_t float_reads = 0;
    for (const auto& r : source.reads) if (r.offset == array.leaf_values.offset) {
      ++float_reads; EXPECT_EQ(r.length, array.leaf_values.length);
    }
    EXPECT_EQ(float_reads, 1u);
    auto metrics = arrow_decode_metrics(*cursor.value()); ASSERT_TRUE(metrics.ok());
    EXPECT_GE(runtime->stats().read_bytes, array.leaf_values.length);
    EXPECT_GT(metrics.value().logical_bytes, 0u);
    EXPECT_GT(runtime->stats().hits, 0u);
  }
}

TEST(ArrowFragmentProvider, OverlappingRequestsShareCanonicalTileAndNormalizeBitmap) {
  auto source_owner = std::make_shared<ArrowTestSource>(); auto& source = *source_owner;
  ArrowDecodeConfig config; config.tile_elements = 7;
  auto runtime = std::make_shared<QueryRuntime>();
  ArrowFragmentProvider provider(source.shared_from_this(), source.layout_bytes(), runtime, {}, config);
  // Child 2 starts at absolute leaf 40; null leaf45 is bit5 in source byte5.
  auto first = provider.get(0, 0, 43); ASSERT_TRUE(first.ok()) << first.status().message();
  auto overlapping = provider.get(0, 0, 45); ASSERT_TRUE(overlapping.ok());
  EXPECT_EQ(first.value(), overlapping.value());
  EXPECT_EQ(first.value()->begin, 42u); EXPECT_EQ(first.value()->end, 49u);
  ASSERT_EQ(first.value()->values.size(), 7u);
  EXPECT_FLOAT_EQ(first.value()->values[0], 202.f);
  ASSERT_FALSE(first.value()->validity.empty());
  EXPECT_EQ(first.value()->validity[0] & (1u << 3), 0u);
  EXPECT_NE(first.value()->validity[0] & (1u << 4), 0u);
  EXPECT_TRUE(std::isnan(first.value()->values[4]));
  EXPECT_GE(runtime->stats().misses, 1u);
  EXPECT_GE(runtime->stats().hits, 1u);
  auto metrics = provider.metrics();
  provider.cancel();
  auto cancelled = provider.get(0, 0, 43); ASSERT_FALSE(cancelled.ok());
  EXPECT_EQ(cancelled.status().code(), QueryError::Cancelled);
  EXPECT_EQ(provider.metrics().logical_bytes, metrics.logical_bytes);
  EXPECT_FLOAT_EQ(first.value()->values[0], 202.f); // owner survives cancel
}

TEST(ArrowFragmentProvider, TrainingBatchCoalescesAdjacentTilesAndSharesScalarCacheKeys) {
  auto source = std::make_shared<ArrowTestSource>();
  ArrowDecodeConfig config; config.tile_elements = 7;
  auto runtime = std::make_shared<QueryRuntime>();
  ArrowFragmentProvider provider(source, source->layout_bytes(), runtime, {}, config);
  auto batch = provider.get_batch({{0, 0, 0, 7}, {0, 0, 7, 7},
    {0, 0, 14, 6}, {0, 0, 43, 3}, {0, 0, 45, 1}});
  ASSERT_TRUE(batch.ok()) << batch.status().message();
  ASSERT_EQ(batch.value().size(), 5u);
  EXPECT_EQ(batch.value()[3].fragment, batch.value()[4].fragment);
  EXPECT_EQ(batch.value()[3].key_hex, batch.value()[4].key_hex);
  EXPECT_FLOAT_EQ(batch.value()[0].fragment->values.front(), 0.f);
  EXPECT_FLOAT_EQ(batch.value()[1].fragment->values.front(), 7.f);
  EXPECT_FLOAT_EQ(batch.value()[2].fragment->values.front(), 14.f);
  EXPECT_FLOAT_EQ(batch.value()[3].fragment->values.front(), 202.f);
  EXPECT_EQ(batch.value()[3].fragment->validity[0] & (1u << 3), 0u);
  ASSERT_EQ(source->batch_reads.size(), 1u);
  // The source contains multiple physical value and bitmap buffers, which
  // remain distinct spans but travel in one source request.
  EXPECT_GE(source->batch_reads.front().size(), 2u);
  EXPECT_LE(source->batch_reads.front().size(), 4u);
  const auto before = runtime->stats();
  EXPECT_EQ(before.payload_read_calls, 1u);
  EXPECT_EQ(before.decode_count, 4u); // four unique leaves; layout bytes were supplied
  EXPECT_EQ(before.read_ranges, source->batch_reads.front().size());
  auto scalar = provider.get(0, 0, 45);
  ASSERT_TRUE(scalar.ok()) << scalar.status().message();
  EXPECT_EQ(scalar.value(), batch.value()[3].fragment);
  EXPECT_EQ(provider.last_fragment_key(), batch.value()[3].key_hex);
  EXPECT_EQ(runtime->stats().payload_read_calls, before.payload_read_calls);
}

TEST(ArrowFragmentProvider, CompressedTrainingBatchUsesWholeBufferCacheFragment) {
  auto source = std::make_shared<ArrowTestSource>(make_stream(false, false, arrow::Compression::ZSTD));
  auto runtime = std::make_shared<QueryRuntime>();
  ArrowDecodeConfig config; config.tile_elements = 7;
  ArrowFragmentProvider provider(source, source->layout_bytes(), runtime, {}, config);
  auto batch = provider.get_batch({{0, 0, 43, 1}, {0, 0, 45, 1}});
  ASSERT_TRUE(batch.ok()) << batch.status().message();
  ASSERT_EQ(batch.value().size(), 2u);
  EXPECT_EQ(batch.value()[0].fragment, batch.value()[1].fragment);
  EXPECT_EQ(batch.value()[0].key_hex, batch.value()[1].key_hex);
  ASSERT_EQ(source->batch_reads.size(), 1u);
  EXPECT_EQ(runtime->stats().payload_read_calls, 1u);
}

TEST(ArrowFragmentProvider, CompressedTrainingBlocksBatchAndShareScalarIdentity) {
  for (auto codec : {arrow::Compression::LZ4_FRAME, arrow::Compression::ZSTD}) {
    auto source = std::make_shared<ArrowTestSource>(make_stream(false, false, codec));
    auto runtime = std::make_shared<QueryRuntime>();
    ArrowDecodeConfig config; config.tile_elements = 7;
    ArrowFragmentProvider provider(source, source->layout_bytes(), runtime, {}, config);
    auto batch = provider.get_batch({{0, 0, 43, 4}, {1, 0, 43, 4}, {0, 0, 45, 1}});
    ASSERT_TRUE(batch.ok()) << batch.status().message();
    ASSERT_EQ(batch.value().size(), 3u);
    EXPECT_EQ(batch.value()[0].fragment, batch.value()[2].fragment);
    ASSERT_EQ(source->batch_reads.size(), 1u);
    EXPECT_EQ(runtime->stats().payload_read_calls, 1u);
    EXPECT_EQ(runtime->stats().decode_count, 2u);
    EXPECT_FLOAT_EQ(batch.value()[0].fragment->values[43], 203.f);
    EXPECT_FLOAT_EQ(batch.value()[1].fragment->values[43], 1203.f);
    EXPECT_EQ(batch.value()[0].fragment->validity[45/8] & (1u<<(45%8)), 0u);
    EXPECT_TRUE(std::isnan(batch.value()[0].fragment->values[46]));
    ArrowDecodeConfig alternate; alternate.tile_elements = 4096;
    ArrowFragmentProvider scalar(source, source->layout_bytes(), runtime, {}, alternate);
    auto value = scalar.get(1, 0, 43);
    ASSERT_TRUE(value.ok());
    EXPECT_EQ(value.value(), batch.value()[1].fragment);
    EXPECT_EQ(scalar.last_fragment_key(), batch.value()[1].key_hex);
    EXPECT_EQ(runtime->stats().payload_read_calls, 1u);
  }
}

TEST(ArrowFragmentProvider, CompressedBatchShortReadAndCancellationDoNotPublish) {
  auto source = std::make_shared<ArrowTestSource>(make_stream(false, false, arrow::Compression::ZSTD));
  auto runtime = std::make_shared<QueryRuntime>();
  ArrowFragmentProvider provider(source, source->layout_bytes(), runtime);
  source->short_payload = true;
  auto result = provider.get_batch({{0, 0, 43, 1}, {1, 0, 43, 1}});
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::ReadFailure);
  source->short_payload = false;
  auto good = provider.get_batch({{0, 0, 43, 1}, {1, 0, 43, 1}});
  ASSERT_TRUE(good.ok()) << good.status().message();
  auto other = std::make_shared<ArrowTestSource>(make_stream(false, false, arrow::Compression::LZ4_FRAME));
  CancellationToken token;
  ArrowFragmentProvider cancelled(other, other->layout_bytes(), runtime, {}, {}, token);
  other->on_read = [&] { token.cancel(); };
  auto stopped = cancelled.get_batch({{0, 0, 43, 1}, {1, 0, 43, 1}});
  ASSERT_FALSE(stopped.ok());
  EXPECT_EQ(stopped.status().code(), QueryError::Cancelled);
  other->on_read = {};
  ArrowFragmentProvider fresh(other, other->layout_bytes(), runtime);
  ASSERT_TRUE(fresh.get_batch({{0, 0, 43, 1}, {1, 0, 43, 1}}).ok());
}

TEST(ArrowFragmentProvider, FailedTrainingMultiRangeReadDoesNotPublishFragments) {
  auto source = std::make_shared<ArrowTestSource>();
  ArrowDecodeConfig config; config.tile_elements = 7;
  auto runtime = std::make_shared<QueryRuntime>();
  ArrowFragmentProvider provider(source, source->layout_bytes(), runtime, {}, config);
  source->short_payload = true;
  auto failed = provider.get_batch({{0, 0, 0, 7}, {0, 0, 7, 7}});
  ASSERT_FALSE(failed.ok());
  EXPECT_EQ(failed.status().code(), QueryError::ReadFailure);
  source->short_payload = false;
  auto retry = provider.get_batch({{0, 0, 0, 7}, {0, 0, 7, 7}});
  ASSERT_TRUE(retry.ok()) << retry.status().message();
  ASSERT_EQ(retry.value().size(), 2u);
  EXPECT_FLOAT_EQ(retry.value()[1].fragment->values.front(), 7.f);
  EXPECT_EQ(source->batch_reads.size(), 2u);
}

TEST(ArrowFragmentProvider, TrainingBatchRejectsLogicalOverrunBeforeReading) {
  auto source = std::make_shared<ArrowTestSource>();
  auto runtime = std::make_shared<QueryRuntime>();
  QueryExecutionOptions options; options.max_payload_bytes = 29;
  ArrowDecodeConfig config; config.tile_elements = 7;
  ArrowFragmentProvider provider(source, source->layout_bytes(), runtime, options, config);
  auto rejected = provider.get_batch({{0, 0, 0, 7}, {0, 0, 7, 7}});
  ASSERT_FALSE(rejected.ok());
  EXPECT_EQ(rejected.status().code(), QueryError::ResourceExhausted);
  EXPECT_TRUE(source->batch_reads.empty());
  EXPECT_EQ(provider.metrics().logical_bytes, 0u);
  auto single = provider.get_batch({{0, 0, 0, 7}});
  ASSERT_TRUE(single.ok()) << single.status().message();
  EXPECT_EQ(provider.metrics().logical_bytes, 29u);
}

TEST(ArrowFragmentProvider, DifferentProvidersCannotAliasSamePathVersionData) {
  auto first_owner = std::make_shared<ArrowTestSource>(); auto& first = *first_owner;
  auto second_owner = std::make_shared<ArrowTestSource>(); auto& second = *second_owner;
  auto doc = second.document(); const auto off = doc->batches[0].arrays[0].leaf_values.offset;
  // IEEE float32 42.0 in little endian, leaving the valid layout identical.
  second.payload[off] = 0; second.payload[off + 1] = 0;
  second.payload[off + 2] = 0x28; second.payload[off + 3] = 0x42;
  auto runtime = std::make_shared<QueryRuntime>();
  ArrowFragmentProvider a(first.shared_from_this(), first.layout_bytes(), runtime);
  ArrowFragmentProvider b(second.shared_from_this(), second.layout_bytes(), runtime);
  auto left = a.get(0, 0, 0); auto right = b.get(0, 0, 0);
  ASSERT_TRUE(left.ok()); ASSERT_TRUE(right.ok());
  EXPECT_FLOAT_EQ(left.value()->values[0], 0.f);
  EXPECT_FLOAT_EQ(right.value()->values[0], 42.f);
  EXPECT_NE(left.value(), right.value());
}

TEST(ArrowFragmentProvider, RejectsCompressedExpansionOverDecodedBufferLimit) {
  auto source_owner = std::make_shared<ArrowTestSource>(make_stream(false, false, arrow::Compression::ZSTD)); auto& source = *source_owner;
  ArrowDecodeConfig config; config.max_buffer_bytes = 64;
  auto runtime = std::make_shared<QueryRuntime>();
  ArrowFragmentProvider provider(source.shared_from_this(), source.layout_bytes(), runtime, {}, config);
  auto result = provider.get(0, 0, 0);
  ASSERT_FALSE(result.ok()); EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted);
}

namespace {
class ArrowModuleBackend final : public StructuredBlobStorageBackend {
public:
  explicit ArrowModuleBackend(std::shared_ptr<ArrowTestSource> source) : source_(std::move(source)) {}
  StructuredBlobStatus create_manifest(const StructuredBlobManifest&) override { return readonly(); }
  StructuredBlobResult<StructuredBlobManifest> load_manifest(const StructuredBlobVersionKey& key) const override {
    if (!(key == source_->manifest_.key)) return StructuredBlobResult<StructuredBlobManifest>::failure(
      StructuredBlobError::NotFound, "test source absent");
    return StructuredBlobResult<StructuredBlobManifest>::success(source_->manifest_);
  }
  StructuredBlobStatus replace_manifest(const StructuredBlobVersionKey&, std::uint64_t,
      const StructuredBlobManifest&) override { return readonly(); }
  StructuredBlobStatus write_payload_range(const StructuredBlobVersionKey&, std::uint64_t,
      const StructuredBlobBytes&) override { return readonly(); }
  StructuredBlobStatus write_layout(const StructuredBlobVersionKey&, const StructuredBlobBytes&) override { return readonly(); }
  StructuredBlobStatus write_metadata(const StructuredBlobVersionKey&, const MetadataMap&) override { return readonly(); }
  StructuredBlobResult<StructuredBlobBytes> read_payload_range(const StructuredBlobVersionKey&,
      std::uint64_t off, std::uint64_t size) const override { return convert(source_->read_payload(off, size)); }
  StructuredBlobResult<StructuredBlobBytes> read_layout_range(const StructuredBlobVersionKey&,
      std::uint64_t off, std::uint64_t size) const override { return convert(source_->read_layout(off, size)); }
  StructuredBlobStatus erase(const StructuredBlobVersionKey&, std::uint64_t) override { return readonly(); }
private:
  static StructuredBlobStatus readonly() {
    return StructuredBlobStatus::failure(StructuredBlobError::BackendFailure, "read-only test backend");
  }
  static StructuredBlobResult<StructuredBlobBytes> convert(QueryResult<BinaryValue> result) {
    if (!result.ok()) return StructuredBlobResult<StructuredBlobBytes>::failure(
      StructuredBlobError::BackendFailure, result.status().message());
    return StructuredBlobResult<StructuredBlobBytes>::success(std::move(result.value()));
  }
  std::shared_ptr<ArrowTestSource> source_;
};
} // namespace

TEST(ArrowFormatQueryAdapter, QueryModuleAppliesHiddenResidualSqlNullSemanticsAndProjection) {
  auto source = std::make_shared<ArrowTestSource>();
  auto storage = std::make_shared<StructuredBlobStorage>(std::make_shared<ArrowModuleBackend>(source));
  auto module = QueryModule::create(make_in_memory_published_version_catalog({source->manifest_}),
    storage, {std::make_shared<ArrowFormatQueryAdapter>()});
  ASSERT_TRUE(module.ok()) << module.status().message();
  QueryRequest request;
  request.sources.paths = {{PathScopeKind::Exact, source->manifest_.key.path}};
  request.sources.versions = VersionSelection::exact_version(source->manifest_.key.version);
  request.subset = fragment(); request.subset->projection = {{"index"}, {"value.2"}};
  request.subset->slices = {slice(3, 9)};
  request.subset->predicate = make_logical(LogicalOperator::And, {
    make_comparison({"value.0"}, ComparisonOperator::Greater, LogicalValue::float32(3.f)),
    make_is_null({"value.2"}, true)});
  request.limits.target_batch_rows = 2;
  auto plan = module.value()->prepare(request); ASSERT_TRUE(plan.ok()) << plan.status().message();
  auto cursor = module.value()->execute(plan.value()); ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  ASSERT_EQ(cursor.value()->schema().fields().size(), 2u);
  EXPECT_EQ(cursor.value()->schema().fields()[1].field_id, "value.2");
  std::vector<std::uint64_t> positions;
  for (;;) {
    auto next = cursor.value()->next(); ASSERT_TRUE(next.ok()) << next.status().message();
    if (!next.value()) break;
    EXPECT_EQ(next.value()->columns().size(), 2u);
    const auto& indexes = std::get<std::vector<std::uint64_t>>(column(*next.value(), "index").values);
    positions.insert(positions.end(), indexes.begin(), indexes.end());
  }
  EXPECT_EQ(positions, (std::vector<std::uint64_t>{4, 6, 7, 8}));

  // IS NULL on an unprojected value field must still read and evaluate its mask.
  request.subset->projection = {{"index"}, {"value.0"}};
  request.subset->predicate = make_is_null({"value.2"});
  plan = module.value()->prepare(request); ASSERT_TRUE(plan.ok()) << plan.status().message();
  cursor = module.value()->execute(plan.value()); ASSERT_TRUE(cursor.ok());
  positions.clear();
  for (;;) {
    auto next = cursor.value()->next(); ASSERT_TRUE(next.ok()) << next.status().message();
    if (!next.value()) break;
    const auto& indexes = std::get<std::vector<std::uint64_t>>(column(*next.value(), "index").values);
    positions.insert(positions.end(), indexes.begin(), indexes.end());
  }
  EXPECT_EQ(positions, (std::vector<std::uint64_t>{5}));
}

TEST(ArrowFormatQueryAdapter, QueryModulePreservesPositionDomainAfterValueOnlyProjection) {
  auto source = std::make_shared<ArrowTestSource>();
  auto storage = std::make_shared<StructuredBlobStorage>(std::make_shared<ArrowModuleBackend>(source));
  auto module = QueryModule::create(make_in_memory_published_version_catalog({source->manifest_}),
    storage, {std::make_shared<ArrowFormatQueryAdapter>()});
  ASSERT_TRUE(module.ok());
  QueryRequest request;
  request.sources.paths = {{PathScopeKind::Exact, source->manifest_.key.path}};
  request.sources.versions = VersionSelection::exact_version(source->manifest_.key.version);
  request.subset = fragment(); request.subset->projection = {{"value.0"}};
  request.subset->slices = {slice(3, 17)};
  // Core removes this hidden column in final projection. Every candidate
  // satisfies the residual, so the original contiguous domain must survive.
  request.subset->predicate = make_comparison({"index"}, ComparisonOperator::GreaterEqual,
    LogicalValue::uint64(3));
  request.limits.target_batch_rows = 5;
  auto plan = module.value()->prepare(request); ASSERT_TRUE(plan.ok()) << plan.status().message();
  auto cursor = module.value()->execute(plan.value()); ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  std::uint64_t begin = 3;
  std::size_t batches = 0;
  for (;;) {
    auto next = cursor.value()->next(); ASSERT_TRUE(next.ok()) << next.status().message();
    if (!next.value()) break;
    const auto& batch = *next.value();
    ASSERT_EQ(batch.columns().size(), 1u);
    EXPECT_EQ(batch.schema().find("index"), nullptr);
    ASSERT_TRUE(batch.domain());
    EXPECT_EQ(batch.domain()->kind, DomainKind::Rows);
    EXPECT_EQ(batch.domain()->origin, (std::vector<std::uint64_t>{begin}));
    EXPECT_EQ(batch.domain()->shape, (std::vector<std::uint64_t>{batch.row_count()}));
    EXPECT_FLOAT_EQ(std::get<std::vector<float>>(column(batch, "value.0").values).front(),
      static_cast<float>(begin));
    begin += batch.row_count();
    ++batches;
  }
  EXPECT_EQ(begin, 17u);
  EXPECT_EQ(batches, 3u);
}

TEST(ArrowFormatQueryAdapter, RequestChildPathLimitRejectsBeforeReadingLayout) {
  auto source = std::make_shared<ArrowTestSource>();
  auto storage = std::make_shared<StructuredBlobStorage>(std::make_shared<ArrowModuleBackend>(source));
  auto module = QueryModule::create(make_in_memory_published_version_catalog({source->manifest_}),
    storage, {std::make_shared<ArrowFormatQueryAdapter>()});
  ASSERT_TRUE(module.ok());
  QueryRequest request;
  request.sources.paths = {{PathScopeKind::Exact, source->manifest_.key.path}};
  request.sources.versions = VersionSelection::exact_version(source->manifest_.key.version);
  request.subset = fragment();
  request.limits.max_ranges = 1; // The subject contains two distinct paths.
  auto plan = module.value()->prepare(request);
  ASSERT_FALSE(plan.ok());
  EXPECT_EQ(plan.status().code(), QueryError::ResourceExhausted);
  EXPECT_TRUE(source->layout_reads.empty());
  EXPECT_TRUE(source->reads.empty());
}

TEST(ArrowFormatQueryAdapter, PlanningReadsOneLayoutWithinExactLayoutBudget) {
  auto source = std::make_shared<ArrowTestSource>();
  auto storage = std::make_shared<StructuredBlobStorage>(std::make_shared<ArrowModuleBackend>(source));
  auto module = QueryModule::create(make_in_memory_published_version_catalog({source->manifest_}),
    storage, {std::make_shared<ArrowFormatQueryAdapter>()});
  ASSERT_TRUE(module.ok());
  QueryRequest request;
  request.sources.paths = {{PathScopeKind::Exact, source->manifest_.key.path}};
  request.sources.versions = VersionSelection::exact_version(source->manifest_.key.version);
  request.subset = fragment(); request.subset->projection = {{"index"}};
  request.limits.max_layout_bytes = source->layout.size();
  auto plan = module.value()->prepare(request);
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  ASSERT_EQ(source->layout_reads.size(), 1u);
  EXPECT_EQ(source->layout_reads[0].offset, 0u);
  EXPECT_EQ(source->layout_reads[0].length, source->layout.size());
  auto cursor = module.value()->execute(plan.value());
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  // Execution still revalidates the immutable layout against the prepared plan.
  ASSERT_EQ(source->layout_reads.size(), 2u);
  EXPECT_EQ(source->layout_reads[1].length, source->layout.size());
  auto next = cursor.value()->next(); ASSERT_TRUE(next.ok());
  ASSERT_TRUE(next.value()); EXPECT_EQ(next.value()->row_count(), 20u);
  EXPECT_TRUE(source->reads.empty());
}

TEST(ArrowFormatQueryAdapter, DefaultLimitsQueryIngestibleLayoutLargerThan32MiB) {
  // The persisted uint64 offsets plus the ingestion index's 64-byte charge
  // exactly fill its default 32 MiB budget. Native schema/message metadata
  // pushes the serialized layout above 32 MiB without any float payload.
  constexpr std::int64_t row_count = 4194295;
  auto stream = [] {
    auto leaf = std::make_shared<arrow::FloatBuilder>(arrow::default_memory_pool());
    arrow::ListBuilder lists(arrow::default_memory_pool(), leaf);
    checked(lists.Reserve(row_count));
    for (std::int64_t i = 0; i < row_count; ++i) checked(lists.Append());
    std::shared_ptr<arrow::Array> array;
    checked(lists.Finish(&array));
    auto batch = arrow::RecordBatch::Make(
      arrow::schema({arrow::field("target", array->type())}), row_count, {array});
    auto sink = checked(arrow::io::BufferOutputStream::Create());
    auto writer = checked(arrow::ipc::MakeStreamWriter(sink, batch->schema()));
    checked(writer->WriteRecordBatch(*batch));
    checked(writer->Close());
    auto bytes = checked(sink->Finish());
    return BinaryValue(bytes->data(), bytes->data() + bytes->size());
  }();
  auto source = std::make_shared<ArrowTestSource>(std::move(stream));
  ASSERT_GT(source->layout.size(), 32u * 1024u * 1024u);
  ASSERT_LE(source->layout.size(), QueryLimits{}.max_layout_bytes);
  auto storage = std::make_shared<StructuredBlobStorage>(std::make_shared<ArrowModuleBackend>(source));
  auto module = QueryModule::create(make_in_memory_published_version_catalog({source->manifest_}),
    storage, {std::make_shared<ArrowFormatQueryAdapter>()});
  ASSERT_TRUE(module.ok());
  QueryRequest request;
  request.sources.paths = {{PathScopeKind::Exact, source->manifest_.key.path}};
  request.sources.versions = VersionSelection::exact_version(source->manifest_.key.version);
  request.subset = fragment(true); request.subset->projection = {{"index"}};
  auto plan = module.value()->prepare(request);
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  ASSERT_EQ(source->layout_reads.size(), 1u);
  EXPECT_EQ(source->layout_reads[0].length, source->layout.size());
  auto cursor = module.value()->execute(plan.value());
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  ASSERT_EQ(source->layout_reads.size(), 2u);
  auto next = cursor.value()->next(); ASSERT_TRUE(next.ok()) << next.status().message();
  EXPECT_FALSE(next.value());
  EXPECT_TRUE(source->reads.empty());
}

TEST(ArrowFormatQueryAdapter, PlanningContextsIsolateIdenticalManifestKeys) {
  auto first_owner = std::make_shared<ArrowTestSource>(); auto& first = *first_owner;
  auto second_owner = std::make_shared<ArrowTestSource>(make_stream(false, false, arrow::Compression::UNCOMPRESSED, "foobar")); auto& second = *second_owner;
  ASSERT_EQ(first.manifest_.key, second.manifest_.key);
  ASSERT_EQ(first.manifest_.layout_size, second.manifest_.layout_size);
  ASSERT_EQ(first.manifest_.payload_size, second.manifest_.payload_size);
  ASSERT_NE(first.layout, second.layout);
  ArrowFormatQueryAdapter adapter;
  QueryPlanningContext first_context(first), second_context(second);
  auto q = fragment();
  auto capabilities = adapter.capabilities(first_context, q);
  ASSERT_TRUE(capabilities.ok()) << capabilities.status().message();
  auto plan = adapter.prepare(first_context, q);
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  EXPECT_EQ(first.layout_reads.size(), 1u);

  auto wrong_field = adapter.prepare(second_context, q);
  ASSERT_FALSE(wrong_field.ok());
  EXPECT_EQ(wrong_field.status().code(), QueryError::UnknownField);
  q.physical_array->field_id = "foobar";
  capabilities = adapter.capabilities(second_context, q);
  ASSERT_TRUE(capabilities.ok()) << capabilities.status().message();
  plan = adapter.prepare(second_context, q);
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  auto binding = arrow_physical_binding(plan.value().implementation()); ASSERT_TRUE(binding.ok());
  EXPECT_EQ(binding.value().subject.field_id, "foobar");
  EXPECT_EQ(second.layout_reads.size(), 1u);
  EXPECT_TRUE(first.reads.empty()); EXPECT_TRUE(second.reads.empty());
}

TEST(ArrowFormatQueryAdapter, RaisedRequestChildPathLimitIsNotCappedByAdapterDefault) {
  auto source_owner = std::make_shared<ArrowTestSource>(); auto& source = *source_owner;
  ArrowFormatQueryAdapter adapter;
  QueryRequest request;
  request.sources.paths = {{PathScopeKind::Exact, source.manifest_.key.path}};
  request.sources.versions = VersionSelection::exact_version(source.manifest_.key.version);
  request.subset = fragment(false, 1); // Null outer List short-circuits child bounds.
  request.subset->projection = {{"index"}};
  const std::size_t path_count = QueryLimits{}.max_ranges + 1;
  request.limits.max_ranges = path_count;
  auto& paths = request.subset->physical_array->values;
  paths.clear(); paths.reserve(path_count);
  for (std::size_t i = 0; i < path_count; ++i) paths.push_back({{i}});
  auto status = validate_query_request(request);
  ASSERT_TRUE(status.ok()) << status.message();
  // Exercise request validation plus binding directly: full capability-schema
  // cross-validation uses linear field lookups and is unrelated to this cap.
  auto plan = adapter.prepare(QueryPlanningContext(source, request.limits), *request.subset);
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  auto binding = arrow_physical_binding(plan.value().implementation());
  ASSERT_TRUE(binding.ok());
  ASSERT_EQ(binding.value().values.size(), path_count);
  EXPECT_EQ(binding.value().length, 0u);
  for (const auto& value : binding.value().values) {
    EXPECT_FALSE(value.valid);
    EXPECT_EQ(value.null_list_level, std::optional<std::uint32_t>(0));
  }
  ASSERT_EQ(plan.value().adapter_output_schema().fields().size(), 1u);
  EXPECT_EQ(plan.value().adapter_output_schema().fields()[0].field_id, "index");
  EXPECT_TRUE(source.reads.empty());
}

namespace {
template<class Predicate> bool arrow_eventually(Predicate predicate) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  do {
    if (predicate()) return true;
    std::this_thread::yield();
  } while (std::chrono::steady_clock::now() < deadline);
  return predicate();
}
} // namespace

TEST(ArrowFragmentProvider, ReentrantCancellationFromReadCallbackDoesNotDeadlock) {
  auto source = std::make_shared<ArrowTestSource>();
  auto runtime = std::make_shared<QueryRuntime>();
  ArrowDecodeConfig config; config.tile_elements = 4;
  ArrowFragmentProvider provider(source, source->layout_bytes(), runtime, {}, config);
  { auto first = provider.get(0, 0, 0); ASSERT_TRUE(first.ok()); }
  auto owned = provider.get(0, 0, 4); ASSERT_TRUE(owned.ok());
  const auto reads_before = source->reads.size();
  const auto cache_before = runtime->stats().cache_owned_bytes;
  source->on_read = [&] { provider.cancel(); };
  auto result = provider.get(0, 0, 8);
  ASSERT_FALSE(result.ok()); EXPECT_EQ(result.status().code(), QueryError::Cancelled);
  EXPECT_TRUE(arrow_eventually([&] { return runtime->stats().inflight_bytes == 0; }));
  EXPECT_GE(source->reads.size(), reads_before + 1);
  EXPECT_LE(source->reads.size(), reads_before + 2);
  EXPECT_GE(runtime->stats().cache_owned_bytes, cache_before);
  EXPECT_LE(runtime->stats().cache_owned_bytes, cache_before + 17);
  EXPECT_FLOAT_EQ(owned.value()->values[0], 4.f);
}

TEST(ArrowFragmentProvider, IdleCancellationPreservesSharedCacheForOtherRequests) {
  auto source = std::make_shared<ArrowTestSource>();
  auto runtime = std::make_shared<QueryRuntime>();
  ArrowFragmentProvider provider(source, source->layout_bytes(), runtime);
  { auto first = provider.get(0, 0, 0); ASSERT_TRUE(first.ok()); }
  const auto reads_before = source->reads.size();
  const auto cache_before = runtime->stats().cache_owned_bytes;
  provider.cancel();
  auto result = provider.get(0, 0, 0);
  ASSERT_FALSE(result.ok()); EXPECT_EQ(result.status().code(), QueryError::Cancelled);
  EXPECT_EQ(runtime->stats().cache_owned_bytes, cache_before);
  ArrowFragmentProvider other(source, source->layout_bytes(), runtime);
  auto reused = other.get(0, 0, 0); ASSERT_TRUE(reused.ok());
  EXPECT_EQ(source->reads.size(), reads_before);
}

TEST(ArrowFragmentProvider, ProviderAndRuntimeDestructionPreserveExternallyOwnedFragments) {
  auto source = std::make_shared<ArrowTestSource>();
  ArrowDecodeConfig config; config.tile_elements = 4;
  std::shared_ptr<const ArrowDecodedFragment> owned;
  {
    auto runtime = std::make_shared<QueryRuntime>();
    ArrowFragmentProvider provider(source, source->layout_bytes(), runtime, {}, config);
    auto result = provider.get(0, 0, 4); ASSERT_TRUE(result.ok());
    owned = result.value();
    runtime->cache().evict_all();
    EXPECT_GE(runtime->stats().pinned_bytes, owned->resident_bytes());
    provider.cancel();
  }
  ASSERT_TRUE(owned);
  EXPECT_FLOAT_EQ(owned->values[0], 4.f);
  EXPECT_FLOAT_EQ(owned->values[3], 7.f);
}

TEST(ArrowFragmentProvider, ConcurrentCancellationDoesNotWaitForBlockedRead) {
  auto source = std::make_shared<ArrowTestSource>();
  auto runtime = std::make_shared<QueryRuntime>();
  ArrowDecodeConfig config; config.tile_elements = 4;
  ArrowFragmentProvider provider(source, source->layout_bytes(), runtime, {}, config);
  { auto first = provider.get(0, 0, 0); ASSERT_TRUE(first.ok()); }
  const auto reads_before = source->reads.size();
  const auto cache_before = runtime->stats().cache_owned_bytes;
  std::promise<void> entered, release;
  auto entered_future = entered.get_future();
  auto release_future = release.get_future().share();
  std::atomic<bool> read_entered{false};
  source->on_read = [&] {
    if (!read_entered.exchange(true)) { entered.set_value(); release_future.wait(); }
  };
  auto read = std::async(std::launch::async, [&] { return provider.get(0, 0, 4); });
  const auto read_started = entered_future.wait_for(std::chrono::seconds(2));
  auto cancel = std::async(std::launch::async, [&] { provider.cancel(); });
  const auto cancellation_finished = cancel.wait_for(std::chrono::seconds(1));
  release.set_value();
  cancel.get();
  auto result = read.get();
  EXPECT_EQ(read_started, std::future_status::ready);
  EXPECT_EQ(cancellation_finished, std::future_status::ready);
  ASSERT_FALSE(result.ok()); EXPECT_EQ(result.status().code(), QueryError::Cancelled);
  EXPECT_TRUE(arrow_eventually([&] { return runtime->stats().inflight_bytes == 0; }));
  // A complete fragment may win the race with waiter detachment. Shared
  // producer cancellation is separate from this request's cancellation signal.
  EXPECT_GE(runtime->stats().cache_owned_bytes, cache_before);
  EXPECT_LE(runtime->stats().cache_owned_bytes, cache_before + 17);
  EXPECT_GE(source->reads.size(), reads_before + 1);
  EXPECT_LE(source->reads.size(), reads_before + 2);
}

TEST(ArrowFormatQueryAdapter, ExecutionRangeBudgetIncludesLayoutRevalidation) {
  auto source_owner = std::make_shared<ArrowTestSource>(); auto& source = *source_owner; ArrowFormatQueryAdapter adapter;
  auto plan = adapter.prepare(QueryPlanningContext(source), fragment()); ASSERT_TRUE(plan.ok());
  QueryExecutionOptions options; options.max_ranges = 1;
  auto cursor = adapter.execute(QueryExecutionContext(source.shared_from_this()), plan.value().implementation(), options); ASSERT_TRUE(cursor.ok());
  auto next = cursor.value()->next(); ASSERT_FALSE(next.ok());
  EXPECT_EQ(next.status().code(), QueryError::ResourceExhausted);
  EXPECT_TRUE(source.reads.empty());
}

TEST(ArrowFormatQueryAdapter, OversizedOutputRowFailsWithoutPayloadReads) {
  auto source_owner = std::make_shared<ArrowTestSource>(); auto& source = *source_owner; ArrowFormatQueryAdapter adapter;
  auto plan = adapter.prepare(QueryPlanningContext(source), fragment()); ASSERT_TRUE(plan.ok());
  QueryExecutionOptions options; options.target_batch_bytes = 1;
  auto cursor = adapter.execute(QueryExecutionContext(source.shared_from_this()), plan.value().implementation(), options); ASSERT_TRUE(cursor.ok());
  auto next = cursor.value()->next(); ASSERT_FALSE(next.ok());
  EXPECT_EQ(next.status().code(), QueryError::ResourceExhausted);
  EXPECT_TRUE(source.reads.empty());
}

TEST(ArrowFragmentProvider, RetainedOwnersRemainValidAfterTileEviction) {
  auto source_owner = std::make_shared<ArrowTestSource>(); auto& source = *source_owner;
  ArrowDecodeConfig config; config.tile_elements = 4;
  auto runtime = std::make_shared<QueryRuntime>();
  ArrowFragmentProvider provider(source.shared_from_this(), source.layout_bytes(), runtime, {}, config);
  auto first = provider.get(0, 0, 0); ASSERT_TRUE(first.ok());
  auto next = provider.get(0, 0, 4); ASSERT_TRUE(next.ok());
  EXPECT_FLOAT_EQ(first.value()->values[3], 3.f);
  runtime->cache().evict_all();
  auto again = provider.get(0, 0, 0); ASSERT_TRUE(again.ok());
  EXPECT_NE(first.value(), again.value());
  EXPECT_GE(runtime->stats().redecodes, 1u);
  EXPECT_FLOAT_EQ(again.value()->values[3], 3.f);
}

TEST(ArrowRangeReader, InvalidCompressionPrefixAndCodecAreExplicitFailures) {
  auto source_owner = std::make_shared<ArrowTestSource>(make_stream(false, false, arrow::Compression::ZSTD)); auto& source = *source_owner;
  auto doc = source.document(); const auto span = doc->batches[0].arrays[0].leaf_values;
  std::fill_n(source.payload.begin() + span.offset, 8, std::uint8_t(0xff));
  source.payload[span.offset] = 0xfe; // signed -2 is forbidden by Arrow IPC.
  ArrowRangeReader reader(source);
  auto decoded = reader.decode(span, ArrowBufferCompression::Zstd, 4);
  ASSERT_FALSE(decoded.ok()); EXPECT_EQ(decoded.status().code(), QueryError::CorruptPayload);
  const auto reads = source.reads.size();
  auto invalid = reader.decode(span, static_cast<ArrowBufferCompression>(127), 4);
  ASSERT_FALSE(invalid.ok()); EXPECT_EQ(invalid.status().code(), QueryError::CorruptPayload);
  EXPECT_EQ(source.reads.size(), reads);
}

TEST(ArrowRangeReader, RuntimeReadTimingCountsSuccessFailuresAndExceptionsOnlyWhenReading) {
  auto source = std::make_shared<ArrowTestSource>();
  DecodedFragmentCache cache;
  using LoadResult = QueryResult<std::shared_ptr<const DecodedFragment>>;
  source->on_read = [] { std::this_thread::sleep_for(std::chrono::milliseconds(1)); };
  for (int mode = 0; mode < 5; ++mode) {
    source->fail_payload = mode == 1;
    source->short_payload = mode == 2;
    if (mode == 3) source->on_read = [] {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      throw std::bad_alloc();
    };
    auto result = cache.get("read-" + std::to_string(mode), {64, 0},
      [&](const FragmentLoadContext& context) {
        ArrowRangeReader reader(*source, {}, {}, {}, &context);
        // The last attempt is rejected by range validation before source I/O.
        auto read = reader.read({mode == 4 ? source->payload.size() : 0, 1});
        if (mode == 0) EXPECT_TRUE(read.ok());
        else EXPECT_FALSE(read.ok());
        // Discard even a successful read: failed producers retain diagnostics.
        return LoadResult::failure(QueryStatus::failure(
          QueryError::Internal, QueryPhase::Execution, "discard probe output"));
      });
    EXPECT_FALSE(result.ok());
    const auto stats = cache.stats();
    const auto expected_calls = std::uint64_t(std::min(mode + 1, 4));
    EXPECT_EQ(stats.payload_read_calls, expected_calls);
    EXPECT_EQ(stats.read_ranges, expected_calls);
    EXPECT_GE(stats.payload_read_nanoseconds, expected_calls * 1000000);
    EXPECT_GE(stats.fragment_load_nanoseconds, stats.payload_read_nanoseconds);
  }
}

TEST(ArrowFormatQueryAdapter, ResultRowLimitCountsResidualMatchesInsteadOfRawCandidates) {
  auto source = std::make_shared<ArrowTestSource>();
  auto storage = std::make_shared<StructuredBlobStorage>(std::make_shared<ArrowModuleBackend>(source));
  auto module = QueryModule::create(make_in_memory_published_version_catalog({source->manifest_}),
    storage, {std::make_shared<ArrowFormatQueryAdapter>()});
  ASSERT_TRUE(module.ok());
  for (std::size_t batch_rows : {std::size_t(100), std::size_t(1)}) {
    QueryRequest request;
    request.sources.paths = {{PathScopeKind::Exact, source->manifest_.key.path}};
    request.sources.versions = VersionSelection::exact_version(source->manifest_.key.version);
    request.subset = fragment(); request.subset->projection = {{"index"}, {"value.2"}};
    request.subset->predicate = make_comparison({"value.0"}, ComparisonOperator::Equal, LogicalValue::float32(19.f));
    request.limits.max_result_rows = 1;
    request.limits.target_batch_rows = batch_rows;
    auto plan = module.value()->prepare(request); ASSERT_TRUE(plan.ok()) << plan.status().message();
    auto cursor = module.value()->execute(plan.value()); ASSERT_TRUE(cursor.ok()) << cursor.status().message();
    std::vector<std::uint64_t> positions;
    for (;;) {
      auto next = cursor.value()->next(); ASSERT_TRUE(next.ok()) << next.status().message();
      if (!next.value()) break;
      const auto& indexes = std::get<std::vector<std::uint64_t>>(column(*next.value(), "index").values);
      positions.insert(positions.end(), indexes.begin(), indexes.end());
      const auto& values = std::get<std::vector<float>>(column(*next.value(), "value.2").values);
      for (const auto value : values) EXPECT_FLOAT_EQ(value, 219.f);
    }
    EXPECT_EQ(positions, (std::vector<std::uint64_t>{19}));
  }
}

TEST(ArrowFormatQueryAdapter, SeparateOverlappingCursorsAndChannelViewsShareDecoding) {
  for (auto codec : {arrow::Compression::UNCOMPRESSED, arrow::Compression::LZ4_FRAME,
      arrow::Compression::ZSTD}) {
    auto source = std::make_shared<ArrowTestSource>(make_stream(false, false, codec));
    auto runtime = std::make_shared<QueryRuntime>();
    ArrowFormatQueryAdapter adapter({}, runtime);
    auto first = fragment();
    first.slices = {slice(3, 17)};
    first.physical_array->values = {{{2}}};
    first.projection = {{"index"}, {"value.2"}};
    auto one = run(adapter, *source, first);
    ASSERT_FALSE(one.empty());
    const auto before = runtime->stats();
    const auto reads_before = source->reads.size();
    auto second = fragment();
    second.slices = {slice(7, 19)};
    second.physical_array->values = {{{0}}};
    second.projection = {{"index"}, {"value.0"}};
    auto two = run(adapter, *source, second);
    ASSERT_FALSE(two.empty());
    EXPECT_EQ(runtime->stats().producer_count, before.producer_count);
    EXPECT_EQ(runtime->stats().decoded_bytes, before.decoded_bytes);
    EXPECT_EQ(runtime->stats().read_bytes, before.read_bytes);
    EXPECT_GT(runtime->stats().hits, before.hits);
    EXPECT_EQ(source->reads.size(), reads_before);
    const auto& values = std::get<std::vector<float>>(column(two.front(), "value.0").values);
    EXPECT_FLOAT_EQ(values.front(), 7.f);
    EXPECT_FLOAT_EQ(values.back(), 18.f);
  }
}

TEST(ArrowFragmentProvider, SharedHitStillConsumesIndependentLogicalRequestBudget) {
  auto source = std::make_shared<ArrowTestSource>();
  auto runtime = std::make_shared<QueryRuntime>();
  ArrowDecodeConfig config; config.tile_elements = 7;
  ArrowFragmentProvider warm(source, source->layout_bytes(), runtime, {}, config);
  auto warmed = warm.get(0, 0, 43); ASSERT_TRUE(warmed.ok());
  const auto reads = runtime->stats().read_bytes;
  QueryExecutionOptions options; options.max_payload_bytes = 5;
  ArrowFragmentProvider limited(source, source->layout_bytes(), runtime, options, config);
  auto first = limited.get(0, 0, 43); ASSERT_TRUE(first.ok()) << first.status().message();
  EXPECT_EQ(first.value(), warmed.value());
  EXPECT_EQ(limited.metrics().logical_bytes, 5u);
  auto rejected = limited.get(0, 0, 45); ASSERT_FALSE(rejected.ok());
  EXPECT_EQ(rejected.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(runtime->stats().read_bytes, reads);
  ArrowFragmentProvider independent(source, source->layout_bytes(), runtime, options, config);
  EXPECT_TRUE(independent.get(0, 0, 45).ok());
}

TEST(ArrowFragmentProvider, SharedHitStillConsumesIndependentLogicalRangeBudget) {
  auto source = std::make_shared<ArrowTestSource>();
  auto runtime = std::make_shared<QueryRuntime>();
  ArrowFragmentProvider warm(source, source->layout_bytes(), runtime);
  ASSERT_TRUE(warm.get(0, 0, 0).ok());
  QueryExecutionOptions options; options.max_ranges = 1;
  ArrowFragmentProvider limited(source, source->layout_bytes(), runtime, options);
  ASSERT_TRUE(limited.get(0, 0, 0).ok());
  auto rejected = limited.get(0, 0, 1);
  ASSERT_FALSE(rejected.ok());
  EXPECT_EQ(rejected.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(limited.metrics().logical_ranges, 1u);
}

TEST(ArrowFragmentProvider, ReopenedSourcesShareOnlyWithinSameStorageBackendNamespace) {
  auto input = std::make_shared<ArrowTestSource>();
  auto backend = std::make_shared<ArrowModuleBackend>(input);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto reopened_storage = std::make_shared<StructuredBlobStorage>(backend);
  auto other_storage = std::make_shared<StructuredBlobStorage>(std::make_shared<ArrowModuleBackend>(input));
  auto first = open_structured_blob_query_source(storage, input->manifest_.key);
  auto reopened = open_structured_blob_query_source(reopened_storage, input->manifest_.key);
  auto other = open_structured_blob_query_source(other_storage, input->manifest_.key);
  ASSERT_TRUE(first.ok()); ASSERT_TRUE(reopened.ok()); ASSERT_TRUE(other.ok());
  EXPECT_EQ(first.value()->storage_namespace(), reopened.value()->storage_namespace());
  EXPECT_NE(first.value()->storage_namespace(), other.value()->storage_namespace());
  auto runtime = std::make_shared<QueryRuntime>();
  ArrowFragmentProvider a(first.value(), input->layout_bytes(), runtime);
  ArrowFragmentProvider b(reopened.value(), input->layout_bytes(), runtime);
  ArrowFragmentProvider c(other.value(), input->layout_bytes(), runtime);
  auto one = a.get(0, 0, 0); ASSERT_TRUE(one.ok());
  const auto before = runtime->stats();
  auto two = b.get(0, 0, 1); ASSERT_TRUE(two.ok());
  EXPECT_EQ(one.value(), two.value());
  EXPECT_EQ(runtime->stats().producer_count, before.producer_count);
  auto three = c.get(0, 0, 0); ASSERT_TRUE(three.ok());
  EXPECT_NE(one.value(), three.value());
  EXPECT_GT(runtime->stats().producer_count, before.producer_count);
}

TEST(ArrowFragmentProvider, EntryLimitKeepsLeasedLayoutForLaterProvider) {
  auto source = std::make_shared<ArrowTestSource>();
  auto limits = QueryRuntimeConfig{};
  limits.max_entries = 2;
  auto runtime = std::make_shared<QueryRuntime>(limits);
  ArrowDecodeConfig config;
  config.tile_elements = 4;
  ArrowFragmentProvider first(source, source->layout_bytes(), runtime, {}, config);
  {
    auto first_tile = first.get(0, 0, 0);
    ASSERT_TRUE(first_tile.ok()) << first_tile.status().message();
  }
  auto second_tile = first.get(0, 0, 4);
  ASSERT_TRUE(second_tile.ok()) << second_tile.status().message();
  const auto before = runtime->stats();
  EXPECT_EQ(before.evictions, 1u);
  EXPECT_EQ(before.live_bytes, before.cache_owned_bytes);

  ArrowFragmentProvider later(source, source->layout_bytes(), runtime, {}, config);
  auto hit = later.get(0, 0, 4);
  ASSERT_TRUE(hit.ok()) << hit.status().message();
  EXPECT_EQ(hit.value(), second_tile.value());
  EXPECT_EQ(runtime->stats().producer_count, before.producer_count);
  EXPECT_EQ(runtime->stats().read_bytes, before.read_bytes);
}

TEST(ArrowFormatQueryAdapter, PreparedPlanRejectsDifferentBackendNamespaceWithIdenticalManifest) {
  auto input = std::make_shared<ArrowTestSource>();
  auto backend = std::make_shared<ArrowModuleBackend>(input);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto other_storage = std::make_shared<StructuredBlobStorage>(std::make_shared<ArrowModuleBackend>(input));
  auto first = open_structured_blob_query_source(storage, input->manifest_.key);
  auto reopened = open_structured_blob_query_source(storage, input->manifest_.key);
  auto other = open_structured_blob_query_source(other_storage, input->manifest_.key);
  ASSERT_TRUE(first.ok()); ASSERT_TRUE(reopened.ok()); ASSERT_TRUE(other.ok());
  ArrowFormatQueryAdapter adapter;
  auto plan = adapter.prepare(QueryPlanningContext(*first.value()), fragment());
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  auto valid = adapter.execute(QueryExecutionContext(reopened.value()), plan.value().implementation(), {});
  ASSERT_TRUE(valid.ok()) << valid.status().message();
  auto invalid = adapter.execute(QueryExecutionContext(other.value()), plan.value().implementation(), {});
  ASSERT_FALSE(invalid.ok());
  EXPECT_EQ(invalid.status().code(), QueryError::StalePlan);
}

namespace {
// A controlled namespace lets key tests vary exactly one identity component.
class ArrowNamespaceSource final : public QuerySource {
public:
  ArrowNamespaceSource(std::shared_ptr<ArrowTestSource> source, std::uint64_t name_space)
    : source_(std::move(source)), namespace_(name_space) {}
  std::uint64_t storage_namespace() const override { return namespace_; }
  const StructuredBlobManifest& manifest() const override { return source_->manifest(); }
  QueryResult<BinaryValue> read_payload(std::uint64_t off, std::uint64_t len) const override {
    return source_->read_payload(off, len);
  }
  QueryResult<BinaryValue> read_layout(std::uint64_t off, std::uint64_t len) const override {
    return source_->read_layout(off, len);
  }
private:
  std::shared_ptr<ArrowTestSource> source_;
  std::uint64_t namespace_;
};
} // namespace

TEST(ArrowFragmentProvider, RevisionAndExactLayoutIdentityPreventFalseHits) {
  auto first = std::make_shared<ArrowTestSource>();
  auto revised = std::make_shared<ArrowTestSource>();
  revised->manifest_.revision = first->manifest_.revision + 1;
  auto different_layout = std::make_shared<ArrowTestSource>(
    make_stream(false, false, arrow::Compression::UNCOMPRESSED, "foobar"));
  ASSERT_EQ(first->manifest_.payload_size, different_layout->manifest_.payload_size);
  ASSERT_EQ(first->manifest_.layout_size, different_layout->manifest_.layout_size);
  ASSERT_NE(first->layout, different_layout->layout);
  const auto shared_namespace = first->storage_namespace();
  auto revised_alias = std::make_shared<ArrowNamespaceSource>(revised, shared_namespace);
  auto layout_alias = std::make_shared<ArrowNamespaceSource>(different_layout, shared_namespace);
  auto runtime = std::make_shared<QueryRuntime>();
  ArrowFragmentProvider a(first, first->layout_bytes(), runtime);
  ArrowFragmentProvider b(revised_alias, revised->layout_bytes(), runtime);
  ArrowFragmentProvider c(layout_alias, different_layout->layout_bytes(), runtime);
  auto one = a.get(0, 0, 0); ASSERT_TRUE(one.ok());
  const auto initial = runtime->stats().producer_count;
  auto two = b.get(0, 0, 0); ASSERT_TRUE(two.ok());
  EXPECT_NE(one.value(), two.value());
  EXPECT_GT(runtime->stats().producer_count, initial);
  const auto after_revision = runtime->stats().producer_count;
  auto three = c.get(0, 0, 0); ASSERT_TRUE(three.ok());
  EXPECT_NE(one.value(), three.value());
  EXPECT_GT(runtime->stats().producer_count, after_revision);
}

TEST(ArrowFragmentProvider, CancellingInitiatorDoesNotCancelSharedArrowProducerForFollower) {
  auto source = std::make_shared<ArrowTestSource>();
  auto runtime = std::make_shared<QueryRuntime>();
  ArrowDecodeConfig config; config.tile_elements = 4;
  ArrowFragmentProvider initial(source, source->layout_bytes(), runtime, {}, config);
  ArrowFragmentProvider follower(source, source->layout_bytes(), runtime, {}, config);
  std::promise<void> entered, release;
  auto entered_future = entered.get_future();
  auto release_future = release.get_future().share();
  std::atomic<int> callback_count{0};
  source->on_read = [&] {
    if (callback_count.fetch_add(1) == 0) {
      entered.set_value();
      release_future.wait();
    }
  };
  auto first = std::async(std::launch::async, [&] { return initial.get(0, 0, 0); });
  EXPECT_EQ(entered_future.wait_for(std::chrono::seconds(2)), std::future_status::ready);
  const auto waits = runtime->stats().waits;
  auto second = std::async(std::launch::async, [&] { return follower.get(0, 0, 1); });
  EXPECT_TRUE(arrow_eventually([&] { return runtime->stats().waits > waits; }));
  initial.cancel();
  const auto initial_done = first.wait_for(std::chrono::seconds(1));
  release.set_value();
  auto cancelled = first.get();
  auto result = second.get();
  EXPECT_EQ(initial_done, std::future_status::ready);
  ASSERT_FALSE(cancelled.ok()); EXPECT_EQ(cancelled.status().code(), QueryError::Cancelled);
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_FLOAT_EQ(result.value()->values[1], 1.f);
  const auto values_offset = source->document()->batches[0].arrays[0].leaf_values.offset;
  std::size_t float_reads = 0;
  for (const auto& span : source->reads) if (span.offset == values_offset) ++float_reads;
  EXPECT_EQ(float_reads, 1u);
}
