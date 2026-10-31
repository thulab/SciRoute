#include "iotdbfs/formats/arrow/ArrowLayout.h"
#include "iotdbfs/formats/arrow/ArrowFormatAdapter.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <gtest/gtest.h>

#include <limits>
#include <stdexcept>

using namespace iotdbfs;
namespace {
void check(const arrow::Status& status) {
  if (!status.ok()) throw std::runtime_error(status.ToString());
}
template<class T> T unwrap(arrow::Result<T> result) {
  if (!result.ok()) throw std::runtime_error(result.status().ToString());
  return std::move(result).ValueOrDie();
}
BinaryValue stream_bytes(std::shared_ptr<arrow::Schema> schema,
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {}) {
  auto output = unwrap(arrow::io::BufferOutputStream::Create());
  auto writer = unwrap(arrow::ipc::MakeStreamWriter(output, schema));
  for (const auto& batch : batches) check(writer->WriteRecordBatch(*batch));
  check(writer->Close()); auto buffer = unwrap(output->Finish());
  return BinaryValue(buffer->data(), buffer->data() + buffer->size());
}
ArrowLayoutDocument ingest(const BinaryValue& bytes) {
  FormatSource source{bytes.size(), [&bytes](std::uint64_t offset, std::uint64_t size)
      -> std::optional<BinaryValue> {
    if (offset > bytes.size() || size > bytes.size() - offset) return {};
    return BinaryValue(bytes.begin() + offset, bytes.begin() + offset + size);
  }};
  auto session = ArrowFormatAdapter().begin_ingestion(source);
  auto consumed = session->consume_chunk(0, bytes.data(), bytes.size());
  if (!consumed.ok()) throw std::runtime_error(consumed.error());
  auto artifacts = session->finish();
  if (!artifacts.ok()) throw std::runtime_error(artifacts.error());
  auto parsed = parse_arrow_layout(artifacts.value().layout, bytes.size());
  if (!parsed.ok()) throw std::runtime_error(parsed.status().message());
  return std::move(parsed.value());
}
ArrowLayoutDocument empty_layout() { return ingest(stream_bytes(arrow::schema({}))); }
ArrowLayoutDocument nested_layout() {
  auto floats = std::make_shared<arrow::FloatBuilder>();
  auto lists = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), floats);
  arrow::ListBuilder outer(arrow::default_memory_pool(), lists);
  check(outer.Append()); check(lists->Append()); check(floats->AppendValues({1.0f, 2.0f}));
  check(lists->AppendNull()); check(outer.AppendNull()); check(outer.Append());
  auto array = unwrap(outer.Finish());
  auto schema = arrow::schema({arrow::field("target", array->type())});
  auto batch = arrow::RecordBatch::Make(schema, 3, {array});
  return ingest(stream_bytes(schema, {batch, batch}));
}
ArrowLayoutDocument dictionary_layout() {
  arrow::Int8Builder indices; check(indices.AppendValues({0, 1}));
  arrow::StringBuilder strings; check(strings.Append("a")); check(strings.Append("b"));
  auto array = unwrap(arrow::DictionaryArray::FromArrays(
    arrow::dictionary(arrow::int8(), arrow::utf8()), unwrap(indices.Finish()), unwrap(strings.Finish())));
  auto schema = arrow::schema({arrow::field("label", array->type())});
  auto batch = arrow::RecordBatch::Make(schema, 2, {array});
  return ingest(stream_bytes(schema, {batch}));
}
void write_u64(BinaryValue& bytes, std::size_t offset, std::uint64_t value) {
  for (unsigned i = 0; i < 8; ++i) bytes.at(offset + i) = static_cast<std::uint8_t>(value >> (i * 8));
}
}

TEST(ArrowLayout, SchemaOnlyRoundTripAndExplicitStreamContainer) {
  auto doc = empty_layout();
  ASSERT_EQ(doc.messages.size(), 1u); EXPECT_TRUE(doc.batches.empty());
  auto encoded = serialize_arrow_layout(doc); ASSERT_TRUE(encoded.ok()) << encoded.status().message();
  const std::string header = "IOTDBFS-ARROW-LAYOUT/1\n";
  EXPECT_EQ(std::string(encoded.value().begin(), encoded.value().begin() + header.size()), header);
  EXPECT_EQ(encoded.value()[header.size()], 1u);
  auto decoded = parse_arrow_layout(encoded.value(), doc.source_size);
  ASSERT_TRUE(decoded.ok()) << decoded.status().message();
  EXPECT_EQ(decoded.value().messages[0].metadata, doc.messages[0].metadata);
  auto second = serialize_arrow_layout(decoded.value()); ASSERT_TRUE(second.ok());
  EXPECT_EQ(encoded.value(), second.value());
  encoded.value()[header.size()] = 2;
  EXPECT_EQ(parse_arrow_layout(encoded.value(), doc.source_size).status().code(), QueryError::CorruptLayout);
}

TEST(ArrowLayout, IntegerCountersNeverRoundThroughFloatingPoint) {
  auto doc = empty_layout(); doc.index_read_bytes = (std::uint64_t{1} << 53) + 1;
  ArrowLayoutLimits limits; limits.max_index_read_bytes = std::numeric_limits<std::uint64_t>::max();
  auto encoded = serialize_arrow_layout(doc, limits); ASSERT_TRUE(encoded.ok());
  auto decoded = parse_arrow_layout(encoded.value(), doc.source_size, limits); ASSERT_TRUE(decoded.ok());
  EXPECT_EQ(decoded.value().index_read_bytes, doc.index_read_bytes);
}

TEST(ArrowLayout, TruncationHugeCountsAndTrailingBytesAreRejected) {
  auto doc = empty_layout(); auto encoded = serialize_arrow_layout(doc); ASSERT_TRUE(encoded.ok());
  for (std::size_t size = 0; size < encoded.value().size(); ++size) {
    BinaryValue truncated(encoded.value().begin(), encoded.value().begin() + size);
    EXPECT_FALSE(parse_arrow_layout(truncated, doc.source_size).ok()) << size;
  }
  auto huge = encoded.value();
  const std::size_t field_count_offset = std::string("IOTDBFS-ARROW-LAYOUT/1\n").size() + 1 + 8 + 8 + 1 + 8 + 8;
  write_u64(huge, field_count_offset, std::numeric_limits<std::uint64_t>::max());
  EXPECT_EQ(parse_arrow_layout(huge, doc.source_size).status().code(), QueryError::ResourceExhausted);
  encoded.value().push_back(0);
  EXPECT_EQ(parse_arrow_layout(encoded.value(), doc.source_size).status().code(), QueryError::CorruptLayout);
}

TEST(ArrowLayout, UnsupportedVersionAndDifferentBoundSourceAreDistinct) {
  auto doc = empty_layout(); auto encoded = serialize_arrow_layout(doc); ASSERT_TRUE(encoded.ok());
  EXPECT_EQ(parse_arrow_layout(encoded.value(), doc.source_size + 1).status().code(), QueryError::StalePlan);
  encoded.value()[std::string("IOTDBFS-ARROW-LAYOUT/").size()] = '2';
  EXPECT_EQ(parse_arrow_layout(encoded.value(), doc.source_size).status().code(), QueryError::UnsupportedLayoutVersion);
  encoded.value()[std::string("IOTDBFS-ARROW-LAYOUT/").size()] = 'x';
  EXPECT_EQ(parse_arrow_layout(encoded.value(), doc.source_size).status().code(), QueryError::CorruptLayout);
  encoded.value()[std::string("IOTDBFS-ARROW-LAYOUT/").size()] = '1';
  encoded.value()[std::string("IOTDBFS-ARROW-LAYOUT/1").size()] = ' ';
  EXPECT_EQ(parse_arrow_layout(encoded.value(), doc.source_size).status().code(), QueryError::CorruptLayout);
}

TEST(ArrowLayout, MetadataAndStreamSpansAreValidated) {
  auto doc = empty_layout();
  auto bad = doc; bad.messages[0].offset = 8;
  EXPECT_EQ(validate_arrow_layout(bad).code(), QueryError::CorruptLayout);
  bad = doc; bad.messages[0].body_offset += 8;
  EXPECT_EQ(validate_arrow_layout(bad).code(), QueryError::CorruptLayout);
  bad = doc; bad.explicit_eos = !bad.explicit_eos;
  EXPECT_EQ(validate_arrow_layout(bad).code(), QueryError::CorruptLayout);
  bad = doc; bad.messages[0].metadata[0] = 255;
  EXPECT_EQ(validate_arrow_layout(bad).code(), QueryError::CorruptLayout);
}

TEST(ArrowLayout, NestedBatchesRoundTripAndPreserveLeafLocation) {
  auto doc = nested_layout(); ASSERT_EQ(doc.batches.size(), 2u);
  EXPECT_EQ(doc.total_rows, 6u); EXPECT_EQ(doc.batches[1].row_begin, 3u);
  auto encoded = serialize_arrow_layout(doc); ASSERT_TRUE(encoded.ok()) << encoded.status().message();
  auto decoded = parse_arrow_layout(encoded.value(), doc.source_size); ASSERT_TRUE(decoded.ok());
  EXPECT_EQ(decoded.value().batches[0].arrays[0].levels[0].offsets, (std::vector<std::uint64_t>{0, 2, 2, 2}));
  EXPECT_EQ(decoded.value().batches[0].arrays[0].leaf_values.offset, doc.batches[0].arrays[0].leaf_values.offset);
  EXPECT_EQ(decoded.value().batches[0].arrays[0].leaf_count, 2u);
}

TEST(ArrowLayout, IndexOwnsLayoutAndDistinguishesNullLevels) {
  auto doc = std::make_shared<ArrowLayoutDocument>(nested_layout());
  auto index = make_arrow_list_index(doc, 0, 0); ASSERT_TRUE(index.ok()) << index.status().message();
  doc.reset();
  auto valid = resolve_array_value({2, PhysicalArrayLeafType::Float32}, *index.value(), 0, {{0}});
  ASSERT_TRUE(valid.ok()); EXPECT_TRUE(valid.value().valid); EXPECT_EQ(valid.value().end, 2u);
  auto child = resolve_array_value({2, PhysicalArrayLeafType::Float32}, *index.value(), 0, {{1}});
  ASSERT_TRUE(child.ok()); EXPECT_FALSE(child.value().valid); EXPECT_EQ(child.value().null_list_level, 1u);
  auto parent = resolve_array_value({2, PhysicalArrayLeafType::Float32}, *index.value(), 1, {{7}});
  ASSERT_TRUE(parent.ok()); EXPECT_FALSE(parent.value().valid); EXPECT_EQ(parent.value().null_list_level, 0u);
  EXPECT_EQ(resolve_array_value({2, PhysicalArrayLeafType::Float32}, *index.value(), 2, {{0}}).status().code(), QueryError::InvalidQuery);
  EXPECT_EQ(index.value()->list_span(2, 0).status().code(), QueryError::InvalidQuery);
  EXPECT_EQ(index.value()->list_span(0, 3).status().code(), QueryError::InvalidQuery);
}

TEST(ArrowLayout, OffsetValidityAndLeafSpanCorruptionAreRejected) {
  auto doc = nested_layout();
  auto bad = doc; bad.batches[0].arrays[0].levels[0].offsets[1] = 3;
  EXPECT_EQ(validate_arrow_layout(bad).code(), QueryError::CorruptLayout);
  bad = doc; bad.batches[0].arrays[0].levels[1].offsets.back() = 99;
  EXPECT_EQ(validate_arrow_layout(bad).code(), QueryError::CorruptLayout);
  bad = doc; bad.batches[0].arrays[0].levels[0].validity.clear();
  EXPECT_EQ(validate_arrow_layout(bad).code(), QueryError::CorruptLayout);
  bad = doc; bad.batches[0].arrays[0].leaf_values.offset = doc.source_size;
  EXPECT_EQ(validate_arrow_layout(bad).code(), QueryError::CorruptLayout);
  bad = doc; ++bad.total_rows;
  EXPECT_EQ(validate_arrow_layout(bad).code(), QueryError::CorruptLayout);
}

TEST(ArrowLayout, DictionaryEpochAndDeltaReferencesAreValidated) {
  auto doc = dictionary_layout(); ASSERT_EQ(doc.messages.size(), 3u);
  ASSERT_EQ(doc.batches[0].dictionary_epochs.size(), 1u);
  auto encoded = serialize_arrow_layout(doc); ASSERT_TRUE(encoded.ok()) << encoded.status().message();
  auto bad = doc; bad.batches[0].dictionary_epochs.begin()->second = 2;
  EXPECT_EQ(validate_arrow_layout(bad).code(), QueryError::CorruptLayout);
  bad = doc; bad.messages[1].dictionary_delta = true;
  EXPECT_EQ(validate_arrow_layout(bad).code(), QueryError::CorruptLayout);
  bad = doc; bad.messages[1].previous_dictionary = 0;
  EXPECT_EQ(validate_arrow_layout(bad).code(), QueryError::CorruptLayout);
  bad = doc; bad.batches[0].dictionary_epochs.emplace(9123, 1);
  EXPECT_EQ(validate_arrow_layout(bad).code(), QueryError::CorruptLayout);
  // A structurally contiguous stream still needs each schema dictionary
  // defined before its first record batch.
  bad = doc;
  const auto removed = bad.messages[1].body_offset + bad.messages[1].body_length - bad.messages[1].offset;
  bad.messages.erase(bad.messages.begin() + 1);
  bad.source_size -= removed;
  bad.messages[1].offset -= removed; bad.messages[1].body_offset -= removed;
  for (auto& node : bad.messages[1].nodes)
    for (auto& span : node.buffers) span.offset -= removed;
  bad.batches[0].message_index = 1; bad.batches[0].dictionary_epochs.clear();
  EXPECT_EQ(validate_arrow_layout(bad).code(), QueryError::CorruptLayout);
}

TEST(ArrowLayout, MetadataNodeLayoutAndIndexBudgetsAreEnforced) {
  auto doc = nested_layout(); ArrowLayoutLimits limits;
  limits.max_messages = 1;
  EXPECT_EQ(validate_arrow_layout(doc, limits).code(), QueryError::ResourceExhausted);
  limits = {}; limits.max_metadata_bytes = 1;
  EXPECT_EQ(validate_arrow_layout(doc, limits).code(), QueryError::ResourceExhausted);
  limits = {}; limits.max_nodes = 1;
  EXPECT_EQ(validate_arrow_layout(doc, limits).code(), QueryError::ResourceExhausted);
  limits = {}; limits.max_index_bytes = 1;
  EXPECT_EQ(validate_arrow_layout(doc, limits).code(), QueryError::ResourceExhausted);
  limits = {}; limits.max_layout_bytes = 32;
  EXPECT_EQ(serialize_arrow_layout(doc, limits).status().code(), QueryError::ResourceExhausted);
}

TEST(ArrowLayout, NativeMetadataPreventsReinterpretingIndexedBuffers) {
  auto doc = nested_layout();
  auto bad = doc; bad.messages[1].compression = ArrowBufferCompression::Lz4;
  EXPECT_EQ(validate_arrow_layout(bad).code(), QueryError::CorruptLayout);
  bad = doc;
  // Still bounded and ordered, but no longer the exact native offset buffer.
  ASSERT_GT(bad.messages[1].nodes[0].buffers[1].length, 4u);
  bad.messages[1].nodes[0].buffers[1].length -= 4;
  EXPECT_EQ(validate_arrow_layout(bad).code(), QueryError::CorruptLayout);
  bad = doc; bad.fields[0].native_type = "list<item: double>";
  EXPECT_EQ(validate_arrow_layout(bad).code(), QueryError::CorruptLayout);
}

TEST(ArrowLayout, ValidatedImmutableIndexMatchesCheckedFactoryAndRetainsCoordinateChecks) {
  auto doc = std::make_shared<const ArrowLayoutDocument>(nested_layout());
  for (std::uint64_t batch = 0; batch < doc->batches.size(); ++batch) {
    auto checked_index = make_arrow_list_index(doc, batch, 0);
    auto prepared_index = detail::make_validated_arrow_list_index(doc, batch, 0);
    ASSERT_TRUE(checked_index.ok()) << checked_index.status().message();
    ASSERT_TRUE(prepared_index.ok()) << prepared_index.status().message();
    EXPECT_EQ(checked_index.value()->leaf_count(), prepared_index.value()->leaf_count());
    for (std::uint32_t level = 0; level < checked_index.value()->list_depth(); ++level) {
      EXPECT_EQ(checked_index.value()->list_count(level), prepared_index.value()->list_count(level));
      for (std::uint64_t i = 0; i < checked_index.value()->list_count(level); ++i) {
        auto expected = checked_index.value()->list_span(level, i);
        auto actual = prepared_index.value()->list_span(level, i);
        ASSERT_TRUE(expected.ok()); ASSERT_TRUE(actual.ok());
        EXPECT_EQ(actual.value().begin, expected.value().begin);
        EXPECT_EQ(actual.value().end, expected.value().end);
        EXPECT_EQ(actual.value().valid, expected.value().valid);
      }
    }
  }
  EXPECT_EQ(detail::make_validated_arrow_list_index({}, 0, 0).status().code(),
    QueryError::CorruptLayout);
  EXPECT_EQ(detail::make_validated_arrow_list_index(doc, doc->batches.size(), 0).status().code(),
    QueryError::InvalidQuery);
  EXPECT_EQ(detail::make_validated_arrow_list_index(doc, 0, doc->fields.size()).status().code(),
    QueryError::InvalidQuery);
  auto corrupt = std::make_shared<ArrowLayoutDocument>(*doc);
  corrupt->batches[1].arrays[0].levels[0].offsets.back() = 999;
  // The public factory still validates the entire document, including batches
  // other than the requested one; arbitrary caller-owned layouts are untrusted.
  EXPECT_EQ(make_arrow_list_index(corrupt, 0, 0).status().code(), QueryError::CorruptLayout);
}
