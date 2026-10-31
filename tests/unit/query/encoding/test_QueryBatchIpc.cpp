#include "iotdbfs/query/encoding/ArrowResultEncoder.h"

#include "iotdbfs/query/core/QueryPlanning.h"
#include "iotdbfs/query/core/QueryProfile.h"

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace iotdbfs {
namespace {

template<typename T>
T checked(arrow::Result<T> result) {
  EXPECT_TRUE(result.ok()) << result.status().ToString();
  return std::move(result).ValueOrDie();
}

std::string metadata_value(const std::shared_ptr<const arrow::KeyValueMetadata>& metadata,
    const std::string& key) {
  EXPECT_TRUE(metadata);
  if (!metadata) return {};
  auto value = metadata->Get(key);
  EXPECT_TRUE(value.ok()) << key;
  return value.ok() ? value.ValueOrDie() : std::string{};
}

LogicalSchema sample_schema() {
  auto result = LogicalSchema::create({
    {"index", "Position", {LogicalTypeId::UInt64, false, {}},
      FieldRole::Dimension, {}},
    {"value.2", "Temperature", {LogicalTypeId::Float32, true, {}},
      FieldRole::Measure, "K"},
    {"observed_at", "Time", {LogicalTypeId::TimestampNs, true, "UTC"},
      FieldRole::Attribute, {}},
    {"payload", "Raw", {LogicalTypeId::Binary, true, {}},
      FieldRole::Attribute, {}},
  });
  EXPECT_TRUE(result.ok()) << result.status().message();
  return result.value();
}

QueryBatch batch(const LogicalSchema& fields, std::uint64_t origin,
    std::string path) {
  DomainDescriptor domain{DomainKind::Rows,
    {{{"index"}, {LogicalTypeId::UInt64, false, {}}, {}}},
    {2}, {origin}};
  auto result = QueryBatch::create(fields, 2, {
    {{LogicalTypeId::UInt64, false, {}},
      std::vector<std::uint64_t>{origin, origin + 1}, {}},
    {{LogicalTypeId::Float32, true, {}},
      std::vector<float>{10.5f, -999.0f}, {0b00000001}},
    {{LogicalTypeId::TimestampNs, true, "UTC"},
      std::vector<TimestampNs>{{1000}, {2000}}, {0b00000011}},
    {{LogicalTypeId::Binary, true, {}},
      std::vector<BinaryValue>{{0, 1, 255}, {}}, {0b00000001}},
  }, domain, {{std::move(path), 7, "arrow"}});
  EXPECT_TRUE(result.ok()) << result.status().message();
  return result.value();
}

std::shared_ptr<arrow::ipc::RecordBatchStreamReader> open(const BinaryValue& bytes) {
  auto buffer = std::make_shared<arrow::Buffer>(bytes.data(), bytes.size());
  auto input = std::make_shared<arrow::io::BufferReader>(buffer);
  return checked(arrow::ipc::RecordBatchStreamReader::Open(input));
}

class TwoBatchCursor final : public QueryBatchCursor {
public:
  explicit TwoBatchCursor(bool fail_second = false)
    : fields_(sample_schema()), fail_second_(fail_second) {}

  const LogicalSchema& schema() const override { return fields_; }
  QueryResult<std::optional<QueryBatch>> next() override {
    if (position_ == 1 && fail_second_) {
      ++position_;
      return QueryResult<std::optional<QueryBatch>>::failure(
        QueryStatus::failure(QueryError::ReadFailure, QueryPhase::Execution,
          "fixture read failed"));
    }
    if (position_ >= 2)
      return QueryResult<std::optional<QueryBatch>>::success(std::nullopt);
    const auto origin = position_++ * 2;
    return QueryResult<std::optional<QueryBatch>>::success(
      batch(fields_, origin, "/source/" + std::to_string(origin)));
  }
  void cancel() override { cancelled_ = true; }
  bool cancelled() const { return cancelled_; }

private:
  LogicalSchema fields_;
  std::uint64_t position_{0};
  bool fail_second_{false};
  bool cancelled_{false};
};

TEST(QueryBatchIpc, RetainsValuesMasksFieldMetadataAndPerBatchProvenance) {
  auto fields = sample_schema();
  std::vector<QueryBatch> batches;
  batches.push_back(batch(fields, 3, "/first"));
  batches.push_back(batch(fields, 17, "/second"));

  auto encoded = ArrowResultEncoder::encode_query_batches(fields, batches, 1 << 20);
  ASSERT_TRUE(encoded.ok()) << encoded.status().message();
  if (const char* directory = std::getenv("IOTDBFS_A10_ORACLE_DIR")) {
    std::ofstream output(std::string(directory) + "/query.ipc", std::ios::binary);
    ASSERT_TRUE(output.good());
    output.write(reinterpret_cast<const char*>(encoded.value().data()),
      encoded.value().size());
    ASSERT_TRUE(output.good());
  }
  auto reader = open(encoded.value());
  ASSERT_EQ(metadata_value(reader->schema()->metadata(), "iotdbfs.protocol"),
    "iotdbfs-query-batch-ipc/1");
  ASSERT_EQ(metadata_value(reader->schema()->field(1)->metadata(),
    "iotdbfs.logical_field_id"), "value.2");
  EXPECT_EQ(metadata_value(reader->schema()->field(1)->metadata(), "iotdbfs.role"),
    "measure");
  EXPECT_EQ(metadata_value(reader->schema()->field(1)->metadata(), "iotdbfs.unit"),
    "K");
  EXPECT_EQ(metadata_value(reader->schema()->field(2)->metadata(), "iotdbfs.timezone"),
    "UTC");
  EXPECT_EQ(reader->schema()->field(2)->type()->id(), arrow::Type::TIMESTAMP);

  for (int i = 0; i < 2; ++i) {
    auto next = checked(reader->ReadNext());
    ASSERT_TRUE(next.batch);
    ASSERT_EQ(next.batch->num_rows(), 2);
    ASSERT_TRUE(next.custom_metadata);
    EXPECT_EQ(metadata_value(next.custom_metadata, "iotdbfs.domain.kind"), "rows");
    EXPECT_EQ(metadata_value(next.custom_metadata,
      "iotdbfs.domain.dimension.0.origin"), i == 0 ? "3" : "17");
    EXPECT_EQ(metadata_value(next.custom_metadata, "iotdbfs.source.0.path"),
      i == 0 ? "/first" : "/second");
    EXPECT_EQ(metadata_value(next.custom_metadata, "iotdbfs.source.0.version"),
      "7");
    auto values = std::static_pointer_cast<arrow::FloatArray>(next.batch->column(1));
    EXPECT_FLOAT_EQ(values->Value(0), 10.5f);
    EXPECT_FALSE(values->IsNull(0));
    EXPECT_TRUE(values->IsNull(1));
    auto payload = std::static_pointer_cast<arrow::BinaryArray>(next.batch->column(3));
    EXPECT_EQ(payload->GetView(0).size(), 3);
    EXPECT_TRUE(payload->IsNull(1));
  }
  EXPECT_FALSE(checked(reader->ReadNext()).batch);
}

TEST(QueryBatchIpc, EmptyInputIsSchemaOnlyAndZeroRowBatchIsDistinct) {
  auto fields = sample_schema();
  auto empty = ArrowResultEncoder::encode_query_batches(fields, {}, 1 << 20);
  ASSERT_TRUE(empty.ok()) << empty.status().message();
  auto empty_reader = open(empty.value());
  ASSERT_EQ(empty_reader->schema()->num_fields(), 4);
  EXPECT_FALSE(checked(empty_reader->ReadNext()).batch);

  auto zero = QueryBatch::create(fields, 0, {
    {{LogicalTypeId::UInt64, false, {}}, std::vector<std::uint64_t>{}, {}},
    {{LogicalTypeId::Float32, true, {}}, std::vector<float>{}, {}},
    {{LogicalTypeId::TimestampNs, true, "UTC"}, std::vector<TimestampNs>{}, {}},
    {{LogicalTypeId::Binary, true, {}}, std::vector<BinaryValue>{}, {}},
  }, std::nullopt, {{"/zero", 1, "arrow"}});
  ASSERT_TRUE(zero.ok()) << zero.status().message();
  auto encoded_zero = ArrowResultEncoder::encode_query_batch(zero.value(), 1 << 20);
  ASSERT_TRUE(encoded_zero.ok()) << encoded_zero.status().message();
  auto zero_reader = open(encoded_zero.value());
  auto next = checked(zero_reader->ReadNext());
  ASSERT_TRUE(next.batch);
  EXPECT_EQ(next.batch->num_rows(), 0);
  EXPECT_EQ(metadata_value(next.custom_metadata, "iotdbfs.domain.present"), "0");
  EXPECT_EQ(metadata_value(next.custom_metadata, "iotdbfs.source.0.path"), "/zero");
  EXPECT_FALSE(checked(zero_reader->ReadNext()).batch);
}

TEST(QueryBatchIpc, CursorUsesOneStreamAndRejectsPartialSuccess) {
  TwoBatchCursor cursor;
  auto encoded = ArrowResultEncoder::encode_query_cursor(cursor.schema(), cursor,
    1 << 20);
  ASSERT_TRUE(encoded.ok()) << encoded.status().message();
  auto reader = open(encoded.value());
  EXPECT_TRUE(checked(reader->ReadNext()).batch);
  EXPECT_TRUE(checked(reader->ReadNext()).batch);
  EXPECT_FALSE(checked(reader->ReadNext()).batch);
  EXPECT_FALSE(cursor.cancelled());

  TwoBatchCursor failing(true);
  auto failure = ArrowResultEncoder::encode_query_cursor(failing.schema(), failing,
    1 << 20);
  ASSERT_FALSE(failure.ok());
  EXPECT_EQ(failure.status().code(), QueryError::ReadFailure);
  EXPECT_TRUE(failing.cancelled());
}

TEST(QueryBatchIpc, EnforcesCumulativeByteLimitAndCancelsCursor) {
  TwoBatchCursor baseline;
  auto full = ArrowResultEncoder::encode_query_cursor(baseline.schema(), baseline,
    1 << 20);
  ASSERT_TRUE(full.ok()) << full.status().message();
  ASSERT_GT(full.value().size(), 1u);

  TwoBatchCursor limited;
  auto too_small = ArrowResultEncoder::encode_query_cursor(limited.schema(), limited,
    full.value().size() - 1);
  ASSERT_FALSE(too_small.ok());
  EXPECT_EQ(too_small.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(too_small.status().phase(), QueryPhase::Encoding);
  EXPECT_TRUE(limited.cancelled());

  TwoBatchCursor exact;
  auto exact_limit = ArrowResultEncoder::encode_query_cursor(exact.schema(), exact,
    full.value().size());
  ASSERT_TRUE(exact_limit.ok()) << exact_limit.status().message();
  EXPECT_EQ(exact_limit.value(), full.value());
}

TEST(QueryBatchIpc, SchemaDescriptorsSurviveEmptyStreamsAndStayOutOfBatchMetadata) {
  const QueryResultMetadata metadata{{"iotdbfs.result_descriptors", "1"},
    {"iotdbfs.result_descriptor.0.protocol", "fixture/1"},
    {"iotdbfs.result_descriptor.0.source.path", "/空结果"}};
  auto empty = ArrowResultEncoder::encode_query_batches(sample_schema(), {}, 1 << 20, metadata);
  ASSERT_TRUE(empty.ok()) << empty.status().message();
  auto empty_reader = open(empty.value());
  EXPECT_EQ(metadata_value(empty_reader->schema()->metadata(), "iotdbfs.result_descriptor.0.source.path"), "/空结果");
  EXPECT_FALSE(checked(empty_reader->ReadNext()).batch);
  TwoBatchCursor cursor;
  auto bytes = ArrowResultEncoder::encode_query_cursor(cursor.schema(), cursor, 1 << 20, metadata);
  ASSERT_TRUE(bytes.ok()) << bytes.status().message();
  auto reader = open(bytes.value());
  EXPECT_EQ(metadata_value(reader->schema()->metadata(), "iotdbfs.result_descriptor.0.protocol"), "fixture/1");
  auto first = checked(reader->ReadNext());
  ASSERT_TRUE(first.custom_metadata);
  EXPECT_FALSE(first.custom_metadata->Contains("iotdbfs.result_descriptor.0.protocol"));
  EXPECT_EQ(metadata_value(first.custom_metadata, "iotdbfs.domain.kind"), "rows");
}

TEST(QueryBatchIpc, RejectsMalformedOversizedDescriptorsAndCancelsCursor) {
  for (const QueryResultMetadata metadata : std::vector<QueryResultMetadata>{
      {{"same", "a"}, {"same", "b"}}, {{"iotdbfs.protocol", "spoof"}},
      {{"iotdbfs.result_kind", "spoof"}}, {{"", "x"}},
      {{std::string("bad\0key", 7), "x"}}, {{std::string(257, 'k'), "x"}},
      {{"x", std::string(65536, 'x')}}}) {
    TwoBatchCursor cursor;
    auto result = ArrowResultEncoder::encode_query_cursor(cursor.schema(), cursor, 1 << 20, metadata);
    EXPECT_FALSE(result.ok()); EXPECT_TRUE(cursor.cancelled());
  }
  QueryResultMetadata too_many;
  for (unsigned i = 0; i < 129; ++i) too_many.emplace_back("key" + std::to_string(i), "x");
  EXPECT_FALSE(ArrowResultEncoder::validate_metadata(too_many, 1 << 20).ok());
  too_many.pop_back();
  EXPECT_TRUE(ArrowResultEncoder::validate_metadata(too_many, 1 << 20).ok());
  EXPECT_TRUE(ArrowResultEncoder::validate_metadata({{"k", std::string(65535, 'x')}}, 1 << 20).ok());
  EXPECT_FALSE(ArrowResultEncoder::validate_metadata({{"k", "v"}}, 1).ok());
}

TEST(QueryBatchIpc, EncodedByteLimitIncludesSchemaDescriptors) {
  QueryResultMetadata metadata{{"iotdbfs.result_descriptor.0.notes", std::string(8192, 'x')}};
  TwoBatchCursor full_cursor;
  auto full = ArrowResultEncoder::encode_query_cursor(full_cursor.schema(), full_cursor, 1 << 20, metadata);
  ASSERT_TRUE(full.ok());
  TwoBatchCursor limited;
  auto failed = ArrowResultEncoder::encode_query_cursor(limited.schema(), limited, full.value().size() - 1, metadata);
  ASSERT_FALSE(failed.ok()); EXPECT_EQ(failed.status().code(), QueryError::ResourceExhausted);
  EXPECT_TRUE(limited.cancelled());
  TwoBatchCursor exact;
  auto passed = ArrowResultEncoder::encode_query_cursor(exact.schema(), exact, full.value().size(), metadata);
  ASSERT_TRUE(passed.ok()); EXPECT_EQ(passed.value(), full.value());
}

TEST(QueryBatchIpc, RejectsSchemaChangeBetweenBatches) {
  auto fields = sample_schema();
  auto changed = LogicalSchema::create({
    {"index", "Position", {LogicalTypeId::UInt64, false, {}},
      FieldRole::Dimension, {}},
    {"value.2", "Temperature", {LogicalTypeId::Float32, true, {}},
      FieldRole::Measure, "C"},
    {"observed_at", "Time", {LogicalTypeId::TimestampNs, true, "UTC"},
      FieldRole::Attribute, {}},
    {"payload", "Raw", {LogicalTypeId::Binary, true, {}},
      FieldRole::Attribute, {}},
  });
  ASSERT_TRUE(changed.ok());
  std::vector<QueryBatch> batches;
  batches.push_back(batch(fields, 0, "/first"));
  batches.push_back(batch(changed.value(), 2, "/second"));
  auto encoded = ArrowResultEncoder::encode_query_batches(fields, batches, 1 << 20);
  ASSERT_FALSE(encoded.ok());
  EXPECT_EQ(encoded.status().code(), QueryError::SchemaMismatch);
  EXPECT_EQ(encoded.status().phase(), QueryPhase::Encoding);
}

TEST(QueryBatchIpc, ProfilesCursorExecutionSeparatelyFromSuccessfulIpcEncoding) {
  QueryProfileRecord profile;
  TwoBatchCursor cursor;
  QueryResult<BinaryValue> encoded = [&] {
    ScopedQueryProfile scope(&profile);
    return ArrowResultEncoder::encode_query_cursor(cursor.schema(), cursor, 1 << 20);
  }();
  ASSERT_TRUE(encoded.ok()) << encoded.status().message();
  EXPECT_EQ(profile.counters.at("typed_cursor_next_calls"), 3u);
  EXPECT_EQ(profile.counters.at("typed_cursor_batches"), 2u);
  EXPECT_EQ(profile.counters.at("typed_cursor_rows"), 4u);
  EXPECT_EQ(profile.counters.at("typed_ipc_batches"), 2u);
  EXPECT_EQ(profile.counters.at("typed_ipc_rows"), 4u);
  EXPECT_EQ(profile.counters.at("typed_ipc_bytes"), encoded.value().size());
  for (const auto* stage : {"typed_cursor_execute", "typed_ipc_encode", "typed_ipc_schema",
      "typed_ipc_array_build", "typed_ipc_write", "typed_ipc_finish"})
    EXPECT_EQ(profile.nanoseconds.count(stage), 1u) << stage;
}

TEST(QueryBatchIpc, FailedCursorRetainsAttemptMetricsWithoutClaimingPublishedBytes) {
  QueryProfileRecord profile;
  TwoBatchCursor cursor(true);
  auto encoded = [&] {
    ScopedQueryProfile scope(&profile);
    return ArrowResultEncoder::encode_query_cursor(cursor.schema(), cursor, 1 << 20);
  }();
  ASSERT_FALSE(encoded.ok());
  EXPECT_TRUE(cursor.cancelled());
  EXPECT_EQ(profile.counters.at("typed_cursor_next_calls"), 2u);
  EXPECT_EQ(profile.counters.at("typed_cursor_batches"), 1u);
  EXPECT_EQ(profile.counters.at("typed_ipc_batches"), 1u);
  EXPECT_EQ(profile.counters.count("typed_ipc_bytes"), 0u);
  EXPECT_EQ(profile.nanoseconds.count("typed_ipc_finish"), 0u);
  EXPECT_EQ(profile.nanoseconds.count("typed_cursor_execute"), 1u);
}

} // namespace
} // namespace iotdbfs
