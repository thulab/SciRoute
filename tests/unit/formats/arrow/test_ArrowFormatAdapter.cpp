#include "iotdbfs/formats/arrow/ArrowFormatAdapter.h"
#include "iotdbfs/Ingestion.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/compression.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <limits>
#include <stdexcept>

namespace {
using namespace iotdbfs;

void checked(const arrow::Status& status) {
  if (!status.ok()) throw std::runtime_error(status.ToString());
}
template <typename T> T checked(arrow::Result<T> result) {
  if (!result.ok()) throw std::runtime_error(result.status().ToString());
  return std::move(result).ValueOrDie();
}

std::shared_ptr<arrow::RecordBatch> sample_batch() {
  auto pool = arrow::default_memory_pool();
  auto floats = std::make_shared<arrow::FloatBuilder>(pool);
  arrow::ListBuilder flat(pool, floats);
  checked(flat.Append()); checked(floats->AppendValues({1.f, 2.f, 3.f}));
  checked(flat.AppendNull()); checked(flat.Append());
  checked(flat.Append()); checked(floats->AppendValues({4.f, 5.f}));
  std::shared_ptr<arrow::Array> flat_array;
  checked(flat.Finish(&flat_array));

  auto leaves = std::make_shared<arrow::FloatBuilder>(pool);
  auto children = std::make_shared<arrow::ListBuilder>(pool, leaves);
  arrow::ListBuilder nested(pool, children);
  checked(nested.Append());
  checked(children->Append()); checked(leaves->AppendValues({1.f, 2.f}));
  checked(children->AppendNull()); checked(children->Append());
  checked(nested.AppendNull()); checked(nested.Append());
  checked(nested.Append());
  checked(children->Append()); checked(leaves->Append(3.f));
  checked(children->Append()); checked(leaves->AppendValues({4.f, 5.f, 6.f}));
  std::shared_ptr<arrow::Array> nested_array;
  checked(nested.Finish(&nested_array));
  arrow::StringBuilder strings;
  checked(strings.AppendValues({"a", "bb", "", "cccc"}));
  std::shared_ptr<arrow::Array> text;
  checked(strings.Finish(&text));
  auto schema = arrow::schema({arrow::field("single", flat_array->type()),
    arrow::field("multi", nested_array->type()), arrow::field("label", text->type())});
  return arrow::RecordBatch::Make(schema, 4, {flat_array, nested_array, text});
}

StructuredBlobBytes write_batch_stream(const std::shared_ptr<arrow::RecordBatch>& batch,
    std::size_t batch_count = 2,
    arrow::Compression::type compression = arrow::Compression::UNCOMPRESSED) {
  auto sink = checked(arrow::io::BufferOutputStream::Create());
  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  if (compression != arrow::Compression::UNCOMPRESSED)
    options.codec = std::shared_ptr<arrow::util::Codec>(
      checked(arrow::util::Codec::Create(compression)));
  auto writer = checked(arrow::ipc::MakeStreamWriter(sink, batch->schema(), options));
  for (std::size_t n = 0; n < batch_count; ++n) checked(writer->WriteRecordBatch(*batch));
  checked(writer->Close());
  auto buffer = checked(sink->Finish());
  return {buffer->data(), buffer->data() + buffer->size()};
}

StructuredBlobBytes stream_bytes(std::size_t batch_count = 2,
    arrow::Compression::type compression = arrow::Compression::UNCOMPRESSED) {
  return write_batch_stream(sample_batch(), batch_count, compression);
}

FormatSource source_for(const StructuredBlobBytes& bytes) {
  return {bytes.size(), [&bytes](std::uint64_t offset, std::uint64_t length)
      -> std::optional<StructuredBlobBytes> {
    if (offset > bytes.size() || length > bytes.size() - offset) return std::nullopt;
    return StructuredBlobBytes(bytes.begin() + offset, bytes.begin() + offset + length);
  }};
}

FormatResult<FormatIngestionArtifacts> ingest_stream(const StructuredBlobBytes& bytes,
    std::size_t chunk_size = 7, ArrowLayoutLimits limits = {},
    std::optional<FormatSource> source = std::nullopt) {
  ArrowFormatAdapter adapter(limits);
  auto session = adapter.begin_ingestion(source ? *source : source_for(bytes));
  if (!session) return FormatResult<FormatIngestionArtifacts>::failure("missing session");
  for (std::size_t offset = 0; offset < bytes.size(); offset += chunk_size) {
    auto status = session->consume_chunk(offset, bytes.data() + offset,
      std::min(chunk_size, bytes.size() - offset));
    if (!status.ok()) return FormatResult<FormatIngestionArtifacts>::failure(status.error());
  }
  return session->finish();
}

ArrowLayoutDocument layout_for(const StructuredBlobBytes& bytes) {
  auto result = ingest_stream(bytes);
  if (!result.ok()) throw std::runtime_error(result.error());
  auto layout = parse_arrow_layout(result.value().layout, bytes.size());
  if (!layout.ok()) throw std::runtime_error(layout.status().message());
  return layout.value();
}

class MemoryBackend final : public StructuredBlobStorageBackend {
public:
  enum class Fault { None, PayloadWrite, Seal, LayoutWrite, MetadataWrite,
    Publish };

  struct Record {
    StructuredBlobManifest manifest;
    StructuredBlobBytes payload;
    StructuredBlobBytes layout;
  };

  StructuredBlobStatus create_manifest(
    const StructuredBlobManifest& manifest) override {
    if (records_.count(manifest.key)) return failure("already exists");
    records_.emplace(manifest.key, Record{ manifest, {}, {} });
    return StructuredBlobStatus::success();
  }

  StructuredBlobResult<StructuredBlobManifest> load_manifest(
    const StructuredBlobVersionKey& key) const override {
    const auto found = records_.find(key);
    if (found == records_.end()) {
      return StructuredBlobResult<StructuredBlobManifest>::failure(
        StructuredBlobError::NotFound, "not found");
    }
    return StructuredBlobResult<StructuredBlobManifest>::success(
      found->second.manifest);
  }

  StructuredBlobStatus replace_manifest(
    const StructuredBlobVersionKey& key, std::uint64_t expected_revision,
    const StructuredBlobManifest& next) override {
    auto found = records_.find(key);
    if (found == records_.end() ||
        found->second.manifest.revision != expected_revision ||
        next.revision != expected_revision + 1) {
      return failure("invalid manifest transition");
    }
    if (fault_ == Fault::Seal &&
        next.phase == StructuredBlobPhase::PayloadSealed)
      return failure("seal failed");
    if (fault_ == Fault::Publish && next.published)
      return failure("publish failed");
    found->second.manifest = next;
    return StructuredBlobStatus::success();
  }

  StructuredBlobStatus write_payload_range(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    const StructuredBlobBytes& bytes) override {
    auto found = records_.find(key);
    if (found == records_.end()) return failure("not found");
    if (fault_ == Fault::PayloadWrite) return failure("payload write failed");
    const auto end = static_cast<std::size_t>(offset) + bytes.size();
    if (found->second.payload.size() < end)
      found->second.payload.resize(end);
    std::copy(bytes.begin(), bytes.end(),
      found->second.payload.begin() + static_cast<std::size_t>(offset));
    return StructuredBlobStatus::success();
  }

  StructuredBlobStatus write_layout(const StructuredBlobVersionKey& key,
    const StructuredBlobBytes& layout) override {
    auto found = records_.find(key);
    if (found == records_.end()) return failure("not found");
    if (fault_ == Fault::LayoutWrite) return failure("layout write failed");
    found->second.layout = layout;
    return StructuredBlobStatus::success();
  }

  StructuredBlobStatus write_metadata(const StructuredBlobVersionKey& key,
    const MetadataMap&) override {
    if (fault_ == Fault::MetadataWrite)
      return failure("metadata write failed");
    return records_.count(key) ? StructuredBlobStatus::success() :
      failure("not found");
  }

  StructuredBlobResult<StructuredBlobBytes> read_payload_range(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    std::uint64_t length) const override {
    const auto found = records_.find(key);
    if (found == records_.end()) {
      return StructuredBlobResult<StructuredBlobBytes>::failure(
        StructuredBlobError::NotFound, "not found");
    }
    const auto begin = found->second.payload.begin() +
      static_cast<std::size_t>(offset);
    return StructuredBlobResult<StructuredBlobBytes>::success(
      StructuredBlobBytes(begin, begin + static_cast<std::size_t>(length)));
  }

  StructuredBlobResult<StructuredBlobBytes> read_layout_range(
      const StructuredBlobVersionKey& key, std::uint64_t offset,
      std::uint64_t length) const override {
    const auto found = records_.find(key);
    if (found == records_.end()) {
      return StructuredBlobResult<StructuredBlobBytes>::failure(
        StructuredBlobError::NotFound, "not found");
    }
    const auto begin = found->second.layout.begin() +
      static_cast<std::size_t>(offset);
    return StructuredBlobResult<StructuredBlobBytes>::success(
      {begin, begin + static_cast<std::size_t>(length)});
  }

  StructuredBlobStatus erase(const StructuredBlobVersionKey& key,
    std::uint64_t expected_revision) override {
    const auto found = records_.find(key);
    if (found == records_.end() ||
        found->second.manifest.revision != expected_revision)
      return failure("erase failed");
    records_.erase(found);
    return StructuredBlobStatus::success();
  }

  const Record* peek(const StructuredBlobVersionKey& key) const {
    const auto found = records_.find(key);
    return found == records_.end() ? nullptr : &found->second;
  }

  void fail_publish() { fault_ = Fault::Publish; }
  void set_fault(Fault fault) { fault_ = fault; }

private:
  static StructuredBlobStatus failure(const char* message) {
    return StructuredBlobStatus::failure(
      StructuredBlobError::BackendFailure, message);
  }

  std::map<StructuredBlobVersionKey, Record> records_;
  Fault fault_{ Fault::None };
};

} // namespace

TEST(ArrowFormatAdapter, ContentDetectionRequiresACompleteSchema) {
  ArrowFormatAdapter adapter;
  EXPECT_EQ(adapter.id(), "arrow");
  auto bytes = stream_bytes();
  EXPECT_TRUE(adapter.matches(source_for(bytes)));
  for (auto invalid : std::vector<StructuredBlobBytes>{{}, {1,2,3},
      {255,255,255,255,0,0,0,0}, {255,255,255,255,8,0,0,0,0,0,0,0,0,0,0,0}})
    EXPECT_FALSE(adapter.matches(source_for(invalid)));
  bytes.resize(12);
  EXPECT_FALSE(adapter.matches(source_for(bytes)));
}

TEST(ArrowFormatAdapter, LayoutIsIndependentOfEveryChunkBoundary) {
  const auto bytes = stream_bytes();
  StructuredBlobBytes expected;
  for (std::size_t chunk_size : {1u,2u,3u,7u,8u,11u,31u,1024u,65536u}) {
    SCOPED_TRACE(chunk_size);
    auto result = ingest_stream(bytes, chunk_size);
    ASSERT_TRUE(result.ok()) << result.error();
    if (expected.empty()) expected = result.value().layout;
    EXPECT_EQ(result.value().layout, expected);
  }
}

TEST(ArrowFormatAdapter, IndexPreservesBatchesRowsAndNativeUnsupportedFields) {
  const auto bytes = stream_bytes();
  auto layout = layout_for(bytes);
  EXPECT_EQ(layout.source_size, bytes.size());
  EXPECT_EQ(layout.total_rows, 8u);
  EXPECT_TRUE(layout.explicit_eos);
  ASSERT_EQ(layout.fields.size(), 3u);
  EXPECT_EQ(layout.fields[0].name, "single");
  EXPECT_EQ(layout.fields[0].list_depth, 1u);
  EXPECT_EQ(layout.fields[1].list_depth, 2u);
  EXPECT_FALSE(layout.fields[2].native_type.empty());
  EXPECT_FALSE(layout.fields[2].unsupported_reason.empty());
  ASSERT_EQ(layout.batches.size(), 2u);
  EXPECT_EQ(layout.batches[1].row_begin, 4u);
  EXPECT_EQ(layout.batches[1].row_count, 4u);
  ASSERT_EQ(layout.batches[0].arrays.size(), 2u);
  EXPECT_EQ(layout.batches[0].arrays[0].levels[0].offsets,
    (std::vector<std::uint64_t>{0,3,3,3,5}));
  EXPECT_EQ(layout.batches[0].arrays[1].levels[0].offsets,
    (std::vector<std::uint64_t>{0,3,3,3,5}));
  EXPECT_EQ(layout.batches[0].arrays[1].levels[1].offsets,
    (std::vector<std::uint64_t>{0,2,2,2,3,6}));
}

TEST(ArrowFormatAdapter, NullEmptyAndRaggedListsResolveThroughPhysicalContract) {
  const auto bytes = stream_bytes();
  auto layout = std::make_shared<const ArrowLayoutDocument>(layout_for(bytes));
  auto index = make_arrow_list_index(layout, 0, 1);
  ASSERT_TRUE(index.ok());
  PhysicalArrayShape shape{2, PhysicalArrayLeafType::Float32};
  auto parent_null = resolve_array_value(shape, *index.value(), 1, {{7}});
  ASSERT_TRUE(parent_null.ok());
  EXPECT_FALSE(parent_null.value().valid);
  EXPECT_EQ(parent_null.value().null_list_level, 0u);
  auto child_null = resolve_array_value(shape, *index.value(), 0, {{1}});
  ASSERT_TRUE(child_null.ok());
  EXPECT_FALSE(child_null.value().valid);
  EXPECT_EQ(child_null.value().null_list_level, 1u);
  auto empty = resolve_array_value(shape, *index.value(), 0, {{2}});
  ASSERT_TRUE(empty.ok()); EXPECT_TRUE(empty.value().valid);
  EXPECT_EQ(empty.value().begin, empty.value().end);
  auto ragged = resolve_array_value(shape, *index.value(), 3, {{1}});
  ASSERT_TRUE(ragged.ok()); EXPECT_EQ(ragged.value().end - ragged.value().begin, 3u);
  EXPECT_FALSE(resolve_array_value(shape, *index.value(), 0, {{7}}).ok());
}

TEST(ArrowFormatAdapter, SchemaOnlyAcceptsBothExplicitAndImplicitEnd) {
  auto bytes = stream_bytes(0);
  for (bool explicit_eos : {true, false}) {
    if (!explicit_eos) bytes.resize(bytes.size() - 8);
    auto layout = layout_for(bytes);
    EXPECT_EQ(layout.explicit_eos, explicit_eos);
    EXPECT_EQ(layout.total_rows, 0u);
    EXPECT_TRUE(layout.batches.empty());
    EXPECT_EQ(layout.fields.size(), 3u);
    EXPECT_EQ(layout.messages.size(), 1u);
  }
}

TEST(ArrowFormatAdapter, Lz4AndZstdIndexesMatchUncompressedOffsets) {
  for (auto codec : {arrow::Compression::LZ4_FRAME, arrow::Compression::ZSTD}) {
    SCOPED_TRACE(static_cast<int>(codec));
    const auto bytes = stream_bytes(2, codec);
    auto layout = layout_for(bytes);
    ASSERT_EQ(layout.batches.size(), 2u);
    const auto& message = layout.messages[layout.batches[0].message_index];
    EXPECT_EQ(message.compression, codec == arrow::Compression::LZ4_FRAME ?
      ArrowBufferCompression::Lz4 : ArrowBufferCompression::Zstd);
    EXPECT_EQ(layout.batches[0].arrays[1].levels[1].offsets,
      (std::vector<std::uint64_t>{0,2,2,2,3,6}));
    EXPECT_GT(layout.index_read_bytes, 0u);
    EXPECT_GT(layout.index_decoded_bytes, 0u);
  }
}

TEST(ArrowFormatAdapter, StableSourceReadsNeverDecodeFloatValueBuffers) {
  for (auto codec : {arrow::Compression::UNCOMPRESSED, arrow::Compression::LZ4_FRAME,
      arrow::Compression::ZSTD}) {
    const auto bytes = stream_bytes(2, codec);
    std::vector<ArrowBufferSpan> reads;
    auto source = source_for(bytes);
    auto read = source.read;
    source.read = [&](std::uint64_t offset, std::uint64_t length) {
      reads.push_back({offset, length}); return read(offset, length);
    };
    auto result = ingest_stream(bytes, 3, {}, source);
    ASSERT_TRUE(result.ok()) << result.error();
    auto layout = parse_arrow_layout(result.value().layout, bytes.size());
    ASSERT_TRUE(layout.ok());
    ASSERT_FALSE(reads.empty());
    for (const auto& batch : layout.value().batches)
      for (const auto& array : batch.arrays)
        for (const auto& range : reads) {
          const auto& values = array.leaf_values;
          const bool disjoint = range.offset + range.length <= values.offset ||
            range.offset >= values.offset + values.length;
          const bool compressed_prefix = codec != arrow::Compression::UNCOMPRESSED &&
            range.offset == values.offset && range.length <= 8;
          EXPECT_TRUE(disjoint || compressed_prefix);
        }
  }
}

TEST(ArrowFormatAdapter, TruncatedMetadataBodyAndEosAreRejected) {
  const auto bytes = stream_bytes();
  const auto layout = layout_for(bytes);
  const auto& record = layout.messages[layout.batches[0].message_index];
  for (auto end : {std::uint64_t{4}, record.body_offset - 1,
      record.body_offset + record.body_length - 1, std::uint64_t(bytes.size() - 1)}) {
    SCOPED_TRACE(end);
    StructuredBlobBytes truncated(bytes.begin(), bytes.begin() + end);
    EXPECT_FALSE(ingest_stream(truncated).ok());
  }
  auto implicit = bytes; implicit.resize(implicit.size() - 8);
  EXPECT_TRUE(ingest_stream(implicit).ok());
}

TEST(ArrowFormatAdapter, TrailingGarbageAndConcatenatedStreamsAreRejected) {
  const auto bytes = stream_bytes();
  auto garbage = bytes; garbage.push_back(0);
  EXPECT_FALSE(ingest_stream(garbage).ok());
  auto concatenated = bytes; concatenated.insert(concatenated.end(), bytes.begin(), bytes.end());
  EXPECT_FALSE(ingest_stream(concatenated).ok());
}

TEST(ArrowFormatAdapter, CorruptTargetOffsetsAreRejectedBeforePublication) {
  auto bytes = stream_bytes();
  auto layout = layout_for(bytes);
  const auto& message = layout.messages[layout.batches[0].message_index];
  ASSERT_FALSE(message.nodes.empty());
  ASSERT_GE(message.nodes[0].buffers.size(), 2u);
  const auto offset = message.nodes[0].buffers[1].offset;
  ASSERT_LE(offset + 4, bytes.size());
  bytes[offset] = 255; bytes[offset + 1] = 255;
  bytes[offset + 2] = 255; bytes[offset + 3] = 127;
  EXPECT_TRUE(ArrowFormatAdapter().matches(source_for(bytes)));
  EXPECT_FALSE(ingest_stream(bytes).ok());
}

TEST(ArrowFormatAdapter, MetadataIndexAndLayoutBudgetsAreEnforced) {
  const auto bytes = stream_bytes();
  for (unsigned budget = 0; budget < 5; ++budget) {
    SCOPED_TRACE(budget);
    ArrowLayoutLimits limits;
    if (budget == 0) limits.max_metadata_bytes = 1;
    if (budget == 1) limits.max_index_read_bytes = 1;
    if (budget == 2) limits.max_layout_bytes = 1;
    if (budget == 3) limits.max_buffer_bytes = 1;
    if (budget == 4) limits.max_messages = 1;
    EXPECT_FALSE(ingest_stream(bytes, 7, limits).ok());
  }
}

TEST(ArrowFormatAdapter, StableSourceShortAndFailedReadsAreFatal) {
  const auto bytes = stream_bytes();
  for (bool short_read : {true, false}) {
    auto source = source_for(bytes);
    source.read = [short_read](std::uint64_t, std::uint64_t length)
      -> std::optional<StructuredBlobBytes> {
      if (!short_read) return std::nullopt;
      return StructuredBlobBytes(length ? length - 1 : 0);
    };
    EXPECT_FALSE(ingest_stream(bytes, 7, {}, source).ok());
  }
}

TEST(ArrowFormatAdapter, IngestionPersistsIdentityAndUnchangedPayloadAtAnySuffix) {
  auto backend = std::make_shared<MemoryBackend>();
  StructuredBlobStorage storage(backend);
  FormatRegistry formats;
  ASSERT_TRUE(formats.register_adapter(std::make_shared<ArrowFormatAdapter>()));
  IngestionModule ingestion(storage, formats, {11});
  const auto bytes = stream_bytes();
  const StructuredBlobVersionKey key{"/experiment/arbitrary.grib", 1};
  auto result = ingestion.ingest(key, source_for(bytes), [](const IngestionOutcome& outcome) {
    return outcome.format_id == std::optional<std::string>("arrow");
  });
  ASSERT_TRUE(result.ok()) << result.message();
  const auto* record = backend->peek(key);
  ASSERT_NE(record, nullptr);
  EXPECT_TRUE(record->manifest.published);
  EXPECT_EQ(record->manifest.format_id, std::optional<std::string>("arrow"));
  EXPECT_EQ(record->payload, bytes);
  EXPECT_EQ(std::get<std::int64_t>(result.outcome().metadata.at("arrow.rows").value()), 8);
  EXPECT_EQ(std::get<std::int64_t>(result.outcome().metadata.at("arrow.record_batches").value()), 2);
  auto layout = storage.load_layout(key);
  ASSERT_TRUE(layout.ok());
  EXPECT_TRUE(parse_arrow_layout(layout.value(), bytes.size()).ok());
  EXPECT_EQ(formats.resolve_by_id("arrow")->id(), "arrow");
}

TEST(ArrowFormatAdapter, FailedPublicationRollsBackThenOrdinarySmallFilesStillSave) {
  auto backend = std::make_shared<MemoryBackend>();
  StructuredBlobStorage storage(backend);
  FormatRegistry formats;
  ASSERT_TRUE(formats.register_adapter(std::make_shared<ArrowFormatAdapter>()));
  IngestionModule ingestion(storage, formats, {3});
  const auto bytes = stream_bytes();
  const StructuredBlobVersionKey failed{"/rejected.arrow", 1};
  auto result = ingestion.ingest(failed, source_for(bytes), [](const IngestionOutcome&) { return false; });
  EXPECT_FALSE(result.ok()); EXPECT_EQ(backend->peek(failed), nullptr);
  for (const auto& small : std::vector<StructuredBlobBytes>{{}, {'a','b','c'}}) {
    const StructuredBlobVersionKey key{"/ordinary.arrow", static_cast<std::int64_t>(small.size() + 2)};
    auto saved = ingestion.ingest(key, source_for(small), [](const IngestionOutcome&) { return true; });
    ASSERT_TRUE(saved.ok()) << saved.message();
    EXPECT_FALSE(saved.outcome().format_id);
    ASSERT_NE(backend->peek(key), nullptr);
    EXPECT_EQ(backend->peek(key)->payload, small);
    EXPECT_TRUE(backend->peek(key)->manifest.published);
  }
}

TEST(ArrowFormatAdapter, LargeListRetainsInt64OffsetsAndLeafNullValidity) {
  auto values = std::make_shared<arrow::FloatBuilder>();
  arrow::LargeListBuilder lists(arrow::default_memory_pool(), values);
  checked(lists.Append()); checked(values->Append(10.f));
  checked(values->AppendNull()); checked(values->Append(30.f)); checked(lists.Append());
  std::shared_ptr<arrow::Array> array; checked(lists.Finish(&array));
  auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("large", array->type())}),
    2, {array});
  auto bytes = write_batch_stream(batch, 1);
  auto layout = layout_for(bytes);
  ASSERT_EQ(layout.batches[0].arrays.size(), 1u);
  const auto& index = layout.batches[0].arrays[0];
  EXPECT_EQ(index.levels[0].offsets, (std::vector<std::uint64_t>{0,3,3}));
  EXPECT_EQ(index.leaf_null_count, 1u);
  EXPECT_GT(index.leaf_validity.length, 0u);
  EXPECT_EQ(layout.fields[0].list_depth, 1u);
}

TEST(ArrowFormatAdapter, ZeroRowRecordBatchRetainsItsPhysicalBatchIdentity) {
  auto bytes = write_batch_stream(sample_batch()->Slice(0, 0), 1);
  auto layout = layout_for(bytes);
  EXPECT_EQ(layout.total_rows, 0u);
  ASSERT_EQ(layout.batches.size(), 1u);
  EXPECT_EQ(layout.batches[0].row_count, 0u);
  EXPECT_EQ(layout.batches[0].arrays[0].levels[0].offsets,
    (std::vector<std::uint64_t>{0}));
}

TEST(ArrowFormatAdapter, DictionaryMessagesAndBatchEpochAreIndexed) {
  arrow::Int8Builder codes; checked(codes.AppendValues({0,1,0,1}));
  std::shared_ptr<arrow::Array> indices; checked(codes.Finish(&indices));
  arrow::StringBuilder strings; checked(strings.AppendValues({"first", "second"}));
  std::shared_ptr<arrow::Array> dictionary; checked(strings.Finish(&dictionary));
  auto encoded = checked(arrow::DictionaryArray::FromArrays(
    arrow::dictionary(arrow::int8(), arrow::utf8()), indices, dictionary));
  const auto sample = sample_batch();
  auto batch = arrow::RecordBatch::Make(arrow::schema({sample->schema()->field(0),
      arrow::field("category", encoded->type())}), 4, {sample->column(0), encoded});
  const auto bytes = write_batch_stream(batch, 2);
  const auto layout = layout_for(bytes);
  ASSERT_EQ(layout.batches.size(), 2u);
  EXPECT_EQ(layout.batches[0].dictionary_epochs.size(), 1u);
  EXPECT_EQ(layout.batches[1].dictionary_epochs, layout.batches[0].dictionary_epochs);
  std::size_t dictionaries = 0;
  for (const auto& message : layout.messages) {
    if (message.kind != ArrowMessageKind::Dictionary) continue;
    ++dictionaries;
    EXPECT_TRUE(message.dictionary_id.has_value());
    EXPECT_FALSE(message.dictionary_delta);
    EXPECT_FALSE(message.previous_dictionary.has_value());
  }
  EXPECT_EQ(dictionaries, 1u);
  EXPECT_FALSE(layout.fields[1].unsupported_reason.empty());
}

TEST(ArrowFormatAdapter, CompressedOffsetSizePrefixCannotBypassBufferBudget) {
  auto bytes = stream_bytes(1, arrow::Compression::ZSTD);
  const auto layout = layout_for(bytes);
  const auto& message = layout.messages[layout.batches[0].message_index];
  const auto span = message.nodes[0].buffers[1];
  ASSERT_GE(span.length, 8u);
  for (unsigned i = 0; i < 7; ++i) bytes[span.offset + i] = 255;
  bytes[span.offset + 7] = 127;
  EXPECT_FALSE(ingest_stream(bytes).ok());
}

TEST(ArrowFormatAdapter, DetectionSourceFailureNeverPublishesPassThrough) {
  auto backend = std::make_shared<MemoryBackend>();
  StructuredBlobStorage storage(backend);
  FormatRegistry formats;
  ASSERT_TRUE(formats.register_adapter(std::make_shared<ArrowFormatAdapter>()));
  IngestionModule ingestion(storage, formats, {7});
  const auto bytes = stream_bytes();
  auto source = source_for(bytes);
  source.read = [](std::uint64_t, std::uint64_t) -> std::optional<StructuredBlobBytes> {
    return std::nullopt;
  };
  const StructuredBlobVersionKey key{"/failed.arrow", 1};
  const auto result = ingestion.ingest(key, source, [](const IngestionOutcome&) { return true; });
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error(), IngestionError::SourceReadFailure);
  EXPECT_EQ(backend->peek(key), nullptr);
}

TEST(ArrowFormatAdapter, DictionaryDeltaLinksToBaseAndReplacementStartsNewEpoch) {
  const auto sample = sample_batch();
  auto make_batch = [&](const std::vector<std::string>& labels) {
    arrow::Int8Builder codes; checked(codes.AppendValues({0,1,0,1}));
    std::shared_ptr<arrow::Array> indices; checked(codes.Finish(&indices));
    arrow::StringBuilder strings; checked(strings.AppendValues(labels));
    std::shared_ptr<arrow::Array> dictionary; checked(strings.Finish(&dictionary));
    auto encoded = checked(arrow::DictionaryArray::FromArrays(
      arrow::dictionary(arrow::int8(), arrow::utf8()), indices, dictionary));
    return arrow::RecordBatch::Make(arrow::schema({sample->schema()->field(0),
      arrow::field("category", encoded->type())}), 4, {sample->column(0), encoded});
  };
  auto base = make_batch({"first", "second"});
  auto delta = make_batch({"first", "second", "third"});
  auto replacement = make_batch({"other", "words"});
  auto sink = checked(arrow::io::BufferOutputStream::Create());
  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  options.emit_dictionary_deltas = true;
  auto writer = checked(arrow::ipc::MakeStreamWriter(sink, base->schema(), options));
  checked(writer->WriteRecordBatch(*base)); checked(writer->WriteRecordBatch(*delta));
  checked(writer->WriteRecordBatch(*replacement)); checked(writer->Close());
  auto buffer = checked(sink->Finish());
  StructuredBlobBytes bytes(buffer->data(), buffer->data() + buffer->size());
  auto layout = layout_for(bytes);
  std::vector<std::uint64_t> dictionary_messages;
  for (std::size_t i = 0; i < layout.messages.size(); ++i)
    if (layout.messages[i].kind == ArrowMessageKind::Dictionary) dictionary_messages.push_back(i);
  ASSERT_EQ(dictionary_messages.size(), 3u);
  ASSERT_EQ(layout.batches.size(), 3u);
  EXPECT_FALSE(layout.messages[dictionary_messages[0]].dictionary_delta);
  EXPECT_TRUE(layout.messages[dictionary_messages[1]].dictionary_delta);
  EXPECT_EQ(layout.messages[dictionary_messages[1]].previous_dictionary, dictionary_messages[0]);
  EXPECT_FALSE(layout.messages[dictionary_messages[2]].dictionary_delta);
  EXPECT_FALSE(layout.messages[dictionary_messages[2]].previous_dictionary.has_value());
  for (std::size_t i = 0; i < 3; ++i) {
    ASSERT_EQ(layout.batches[i].dictionary_epochs.size(), 1u);
    EXPECT_EQ(layout.batches[i].dictionary_epochs.begin()->second, dictionary_messages[i]);
  }
}

TEST(ArrowFormatAdapter, Uint64MaximumIsRetainedAsNativeTypeWithoutValueStatistics) {
  arrow::UInt64Builder values;
  checked(values.Append(std::numeric_limits<std::uint64_t>::max()));
  std::shared_ptr<arrow::Array> array; checked(values.Finish(&array));
  auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("opaque", array->type())}),
    1, {array});
  auto bytes = write_batch_stream(batch, 1);
  auto result = ingest_stream(bytes);
  ASSERT_TRUE(result.ok()) << result.error();
  auto layout = parse_arrow_layout(result.value().layout, bytes.size());
  ASSERT_TRUE(layout.ok());
  EXPECT_EQ(layout.value().fields[0].list_depth, 0u);
  EXPECT_FALSE(layout.value().fields[0].unsupported_reason.empty());
  EXPECT_EQ(layout.value().fields[0].native_type, "uint64");
  EXPECT_TRUE(layout.value().batches[0].arrays.empty());
  EXPECT_EQ(layout.value().index_read_bytes, 0u);
  for (const auto& entry : result.value().metadata) {
    EXPECT_EQ(entry.first.find("min"), std::string::npos);
    EXPECT_EQ(entry.first.find("max"), std::string::npos);
  }
}

TEST(ArrowFormatAdapter, XattrsUsePersistedIdentityAndTypedMetadata) {
  const auto bytes = stream_bytes();
  auto result = ingest_stream(bytes);
  ASSERT_TRUE(result.ok()) << result.error();
  auto encoded = serialize_metadata(result.value().metadata, 65536);
  ASSERT_TRUE(encoded.ok());
  Inode inode;
  inode.format_id = "arrow";
  inode.metadata = encoded.value();
  ArrowFormatAdapter adapter;
  EXPECT_EQ(adapter.get_xattr(1, "/renamed.bin", inode, "user.arrow.rows"), "8");
  EXPECT_EQ(adapter.get_xattr(1, "/renamed.bin", inode, "user.arrow.container"), "stream");
  EXPECT_FALSE(adapter.get_xattr(1, "/renamed.bin", inode, "user.arrow.unknown"));
  EXPECT_EQ(adapter.list_xattrs(1, "/renamed.bin", inode).size(), 5u);
  inode.format_id.clear();
  EXPECT_TRUE(adapter.list_xattrs(1, "/renamed.arrow", inode).empty());
}

TEST(ArrowFormatAdapter, LegacySchemaIsRecognizedButIngestionRejectsItsFraming) {
  const auto batch = sample_batch();
  auto sink = checked(arrow::io::BufferOutputStream::Create());
  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  options.write_legacy_ipc_format = true;
  auto writer = checked(arrow::ipc::MakeStreamWriter(sink, batch->schema(), options));
  checked(writer->WriteRecordBatch(*batch)); checked(writer->Close());
  const auto buffer = checked(sink->Finish());
  const StructuredBlobBytes bytes(buffer->data(), buffer->data() + buffer->size());
  ASSERT_TRUE(ArrowFormatAdapter().matches(source_for(bytes)));
  const auto result = ingest_stream(bytes);
  ASSERT_FALSE(result.ok());
  EXPECT_NE(result.error().find("UnsupportedCapability"), std::string::npos) << result.error();
  EXPECT_NE(result.error().find("framing"), std::string::npos) << result.error();
}

TEST(ArrowFormatAdapter, RecognizedSchemaWithTruncatedBatchCannotFallBackToPlainBytes) {
  auto bytes = stream_bytes();
  const auto layout = layout_for(bytes);
  const auto& record = layout.messages[layout.batches[0].message_index];
  bytes.resize(record.body_offset + record.body_length - 1);
  ASSERT_TRUE(ArrowFormatAdapter().matches(source_for(bytes)));
  auto backend = std::make_shared<MemoryBackend>();
  StructuredBlobStorage storage(backend);
  FormatRegistry formats;
  ASSERT_TRUE(formats.register_adapter(std::make_shared<ArrowFormatAdapter>()));
  IngestionModule ingestion(storage, formats, {11});
  const StructuredBlobVersionKey key{"/truncated.data", 1};
  bool publication_called = false;
  const auto result = ingestion.ingest(key, source_for(bytes), [&](const IngestionOutcome&) {
    publication_called = true;
    return true;
  });
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error(), IngestionError::AdapterFailure);
  EXPECT_FALSE(publication_called);
  EXPECT_EQ(backend->peek(key), nullptr);
}

TEST(ArrowFormatAdapter, ArtifactProofChecksSizeFramingAndNativeMetadata) {
  const auto bytes = stream_bytes();
  const auto result = ingest_stream(bytes);
  ASSERT_TRUE(result.ok()) << result.error();
  ArrowFormatAdapter adapter;
  EXPECT_TRUE(adapter.validate_layout_artifact(result.value().layout, bytes.size()).ok());
  EXPECT_FALSE(adapter.validate_layout_artifact(result.value().layout, bytes.size() + 1).ok());
  auto truncated = result.value().layout;
  truncated.pop_back();
  EXPECT_FALSE(adapter.validate_layout_artifact(truncated, bytes.size()).ok());
  auto trailing = result.value().layout;
  trailing.push_back(0);
  EXPECT_FALSE(adapter.validate_layout_artifact(trailing, bytes.size()).ok());

  const auto document = parse_arrow_layout(result.value().layout, bytes.size());
  ASSERT_TRUE(document.ok());
  ASSERT_FALSE(document.value().messages.empty());
  const auto& metadata = document.value().messages.front().metadata;
  ASSERT_GE(metadata.size(), 4u);
  auto corrupt = result.value().layout;
  const auto position = std::search(corrupt.begin(), corrupt.end(), metadata.begin(), metadata.end());
  ASSERT_NE(position, corrupt.end());
  // Invalid FlatBuffer root offset: validation must use the native verifier.
  std::fill(position, position + 4, 0xff);
  EXPECT_FALSE(adapter.validate_layout_artifact(corrupt, bytes.size()).ok());
  ArrowLayoutLimits limits;
  limits.max_layout_bytes = result.value().layout.size() - 1;
  EXPECT_FALSE(ArrowFormatAdapter(limits).validate_layout_artifact(
    result.value().layout, bytes.size()).ok());
}
