#include "iotdbfs/formats/hdf5/Hdf5FormatQueryAdapter.h"
#include "iotdbfs/query/core/QueryRuntime.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include <set>

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <future>
#include <cmath>
#include <limits>
#include <mutex>
#include <thread>
#include <zlib.h>

namespace {
using namespace iotdbfs;
using namespace std::chrono_literals;

// Deliberately uses serialized physical metadata rather than the native library:
// every byte range and value is known independently of the production decoder.
class CacheSource final : public QuerySource {
public:
  explicit CacheSource(std::uint64_t gap = 0, float bias = 0, std::uint64_t chunk_count = 8) {
    Hdf5DatasetLayout dataset;
    dataset.descriptor = {"/a", LogicalTypeId::Float32, {chunk_count * 4}, true, {}};
    dataset.storage = Hdf5StorageLayout::Chunked;
    dataset.byte_order = Hdf5ByteOrder::Little;
    dataset.native_type_class = 1; dataset.element_bytes = 4;
    dataset.fill_status = Hdf5FillStatus::Default; dataset.fill_value = {0,0,0,0};
    dataset.fill_time = 2; dataset.allocation_status = 2;
    dataset.chunk_shape = {4}; dataset.index_complete = true;
    payload.resize(64 + chunk_count * (16 + gap));
    for (std::uint64_t chunk = 0; chunk < chunk_count; ++chunk) {
      const auto offset = 64 + chunk * (16 + gap);
      dataset.chunks.push_back({{chunk * 4}, offset, 16, 0});
      for (std::uint64_t i = 0; i < 4; ++i) {
        const float value = float(chunk * 4 + i) + .25f + bias;
        std::uint32_t bits; std::memcpy(&bits, &value, sizeof(bits));
        for (unsigned byte = 0; byte < 4; ++byte)
          payload[offset + i * 4 + byte] = static_cast<std::uint8_t>(bits >> (byte * 8));
      }
    }
    dataset.allocated_chunk_count = dataset.chunks.size();
    document.source_size = payload.size(); document.root_object_id = "root";
    document.objects = {{"root", Hdf5ObjectKind::Group, {}},
      {"values", Hdf5ObjectKind::Dataset, dataset}};
    document.links = {{"root", "a", Hdf5LinkKind::Hard, "values", {}},
      {"root", "alias", Hdf5LinkKind::Hard, "values", {}}};
    manifest_.key = {"/shared-cache.h5", 17}; manifest_.format_id = "hdf5";
    manifest_.payload_size = payload.size(); manifest_.revision = 3;
    manifest_.published = true; manifest_.phase = StructuredBlobPhase::Published;
    encode();
  }
  void encode() {
    auto result = serialize_hdf5_layout(document);
    if (!result.ok()) throw std::runtime_error(result.status().message());
    layout = std::move(result.value()); manifest_.layout_size = layout.size();
  }
  std::uint64_t storage_namespace() const override {
    return namespace_override ? namespace_override : QuerySource::storage_namespace();
  }
  const StructuredBlobManifest& manifest() const override { return manifest_; }
  PayloadReadCapabilities payload_read_capabilities() const override { return capabilities; }
  static QueryResult<BinaryValue> read(const BinaryValue& bytes, std::uint64_t offset,
      std::uint64_t length) {
    if (offset > bytes.size() || length > bytes.size() - offset)
      return failure<BinaryValue>();
    return QueryResult<BinaryValue>::success({bytes.begin() + offset, bytes.begin() + offset + length});
  }
  template<class T> static QueryResult<T> failure() {
    return QueryResult<T>::failure(QueryStatus::failure(QueryError::ReadFailure,
      QueryPhase::Storage, "injected H06 source failure"));
  }
  QueryResult<BinaryValue> read_payload(std::uint64_t offset, std::uint64_t length) const override {
    ++calls; ++ranges; bytes += length;
    if (on_read) on_read();
    if (fail.load()) return failure<BinaryValue>();
    auto result = read(payload, offset, length);
    if (short_read.load() && result.ok() && !result.value().empty()) result.value().pop_back();
    return result;
  }
  QueryResult<StructuredBlobPayloadBatch> read_payload_ranges(
      const StructuredBlobPayloadRanges& requested) const override {
    ++calls; ++batch_calls; ranges += requested.size();
    for (const auto& range : requested) bytes += range.length;
    {
      std::lock_guard<std::mutex> lock(trace_mutex);
      trace.push_back(requested);
    }
    if (on_read) on_read();
    if (fail.load()) return failure<StructuredBlobPayloadBatch>();
    StructuredBlobPayloadBatch result;
    for (const auto& range : requested) {
      auto part = read(payload, range.offset, range.length);
      if (!part.ok()) return failure<StructuredBlobPayloadBatch>();
      result.push_back(std::move(part.value()));
    }
    if (short_read.load() && !result.empty() && !result.back().empty()) result.back().pop_back();
    return QueryResult<StructuredBlobPayloadBatch>::success(std::move(result));
  }
  QueryResult<BinaryValue> read_layout(std::uint64_t offset, std::uint64_t length) const override {
    ++layout_calls; layout_bytes += length;
    if (on_layout_read) on_layout_read(offset);
    if (fail_layout.load()) return failure<BinaryValue>();
    if (short_layout.load()) return QueryResult<BinaryValue>::success({});
    return read(layout, offset, length);
  }
  BinaryValue payload, layout;
  Hdf5LayoutDocument document;
  StructuredBlobManifest manifest_;
  PayloadReadCapabilities capabilities{true, 64, 8ULL * 1024 * 1024};
  std::uint64_t namespace_override{0};
  mutable std::atomic<std::uint64_t> calls{0}, batch_calls{0}, ranges{0}, bytes{0};
  mutable std::atomic<std::uint64_t> layout_calls{0}, layout_bytes{0};
  std::atomic<bool> fail{false}, short_read{false};
  std::atomic<bool> fail_layout{false}, short_layout{false};
  std::function<void()> on_read;
  std::function<void(std::uint64_t)> on_layout_read;
  mutable std::mutex trace_mutex;
  mutable std::vector<StructuredBlobPayloadRanges> trace;
};
QueryFragment query(std::uint64_t begin = 0, std::uint64_t end = 32,
    const std::string& path = "/a") {
  QueryFragment result;
  result.projection = {make_hdf5_value_field(path).value()};
  result.slices = {{hdf5_axis_field(0),
    DomainBound{LogicalValue::uint64(begin), BoundKind::Inclusive},
    DomainBound{LogicalValue::uint64(end), BoundKind::Exclusive}}};
  return result;
}
using CursorResult = QueryResult<std::unique_ptr<QueryBatchCursor>>;
CursorResult cursor(const std::shared_ptr<CacheSource>& source,
    const std::shared_ptr<QueryRuntime>& runtime, QueryFragment fragment = query(),
    Hdf5QueryKernelConfig config = {}, QueryExecutionOptions options = {}) {
  Hdf5FormatQueryAdapter adapter(config, runtime);
  auto plan = adapter.prepare(QueryPlanningContext(*source), fragment);
  if (!plan.ok()) return CursorResult::failure(plan.status());
  return adapter.execute(QueryExecutionContext(source), plan.value().implementation(), options);
}
QueryResult<std::vector<float>> consume(QueryBatchCursor& cursor) {
  std::vector<float> values;
  for (;;) {
    auto next = cursor.next();
    if (!next.ok()) return QueryResult<std::vector<float>>::failure(next.status());
    if (!next.value()) break;
    const auto& column = std::get<std::vector<float>>(next.value()->columns()[0].values);
    values.insert(values.end(), column.begin(), column.end());
  }
  return QueryResult<std::vector<float>>::success(std::move(values));
}
void expect_values(const std::vector<float>& values, std::uint64_t begin,
    std::uint64_t end, float bias = 0) {
  ASSERT_EQ(values.size(), end - begin);
  for (std::size_t i = 0; i < values.size(); ++i)
    EXPECT_FLOAT_EQ(values[i], float(begin + i) + .25f + bias);
}
class InlineDataset final : public Hdf5DatasetProvider {
public:
  explicit InlineDataset(Hdf5DatasetDescriptor value) : descriptor(std::move(value)) {}
  QueryResult<Hdf5DatasetDescriptor> describe(const std::string&) const override {
    return QueryResult<Hdf5DatasetDescriptor>::success(descriptor);
  }
  Hdf5DatasetDescriptor descriptor;
};
CursorResult scientific_cursor(const std::shared_ptr<CacheSource>& source,
    Hdf5DatasetLayout dataset, Hdf5CfBinding cf,
    const std::shared_ptr<QueryRuntime>& runtime,
    QueryExecutionOptions options = {}, Hdf5QueryKernelConfig config = {}) {
  InlineDataset provider(dataset.descriptor);
  QueryFragment fragment;
  fragment.projection = {make_hdf5_science_value_field("/a").value()};
  fragment.slices = {{hdf5_axis_field(0),
    DomainBound{LogicalValue::uint64(0), BoundKind::Inclusive},
    DomainBound{LogicalValue::uint64(dataset.descriptor.shape[0]), BoundKind::Exclusive}}};
  auto binding = bind_hdf5_query(provider, fragment, {}, std::nullopt, std::move(cf));
  if (!binding.ok()) return CursorResult::failure(binding.status());
  return make_hdf5_query_cursor(source, std::move(binding.value()),
    std::move(dataset), options, config, runtime, "science-test");
}
Hdf5DatasetLayout contiguous_numeric(const std::shared_ptr<CacheSource>& source,
    LogicalTypeId type, std::uint64_t rows, std::uint64_t native_bytes) {
  auto dataset = *source->document.objects[1].dataset;
  dataset.descriptor.value_type = type;
  dataset.descriptor.shape = {rows};
  dataset.storage = Hdf5StorageLayout::Contiguous;
  dataset.element_bytes = native_bytes;
  dataset.contiguous_offset = 64;
  dataset.contiguous_size = rows * native_bytes;
  dataset.chunk_shape.clear(); dataset.chunks.clear();
  dataset.allocated_chunk_count = 0;
  source->payload.assign(64 + rows * native_bytes, 0);
  source->manifest_.payload_size = source->payload.size();
  return dataset;
}
std::shared_ptr<CacheSource> multirow_chunks(std::uint64_t chunk_count,
    std::uint64_t chunk_rows = 4) {
  auto source = std::make_shared<CacheSource>(0, 0, chunk_count);
  auto& dataset = *source->document.objects[1].dataset;
  const auto columns = chunk_count * 4;
  dataset.descriptor.shape = {4, columns};
  dataset.chunk_shape = {chunk_rows, 4};
  dataset.chunks.clear();
  const auto bands = (4 + chunk_rows - 1) / chunk_rows;
  const auto chunk_bytes = chunk_rows * 16;
  source->payload.assign(64 + bands * chunk_count * chunk_bytes, 0);
  for (std::uint64_t band = 0; band < bands; ++band)
    for (std::uint64_t chunk = 0; chunk < chunk_count; ++chunk) {
      const auto offset = 64 + (band * chunk_count + chunk) * chunk_bytes;
      dataset.chunks.push_back({{band * chunk_rows, chunk * 4}, offset, chunk_bytes, 0});
      for (std::uint64_t row = 0; row < chunk_rows; ++row)
        for (std::uint64_t column = 0; column < 4; ++column) {
          const auto full_row = band * chunk_rows + row;
          const float value = full_row < 4 ?
            float(full_row * columns + chunk * 4 + column) + .25f : 0;
          std::uint32_t bits; std::memcpy(&bits, &value, sizeof(bits));
          for (unsigned byte = 0; byte < 4; ++byte)
            source->payload[offset + (row * 4 + column) * 4 + byte] =
              static_cast<std::uint8_t>(bits >> (byte * 8));
        }
    }
  dataset.allocated_chunk_count = dataset.chunks.size();
  source->document.source_size = source->payload.size();
  source->manifest_.payload_size = source->payload.size();
  source->encode();
  return source;
}
class Gate {
public:
  void wait() {
    std::unique_lock<std::mutex> lock(mutex_); ++started_; ready_.notify_all();
    ready_.wait_for(lock, 5s, [&] { return open_; });
  }
  bool started(unsigned count = 1) {
    std::unique_lock<std::mutex> lock(mutex_);
    return ready_.wait_for(lock, 2s, [&] { return started_ >= count; });
  }
  void open() { std::lock_guard<std::mutex> lock(mutex_); open_ = true; ready_.notify_all(); }
private:
  std::mutex mutex_; std::condition_variable ready_; unsigned started_{0}; bool open_{false};
};
template<class F> bool eventually(F&& predicate) {
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  do { if (predicate()) return true; std::this_thread::yield(); }
  while (std::chrono::steady_clock::now() < deadline);
  return predicate();
}
} // namespace

TEST(Hdf5LayoutCache, WarmRequestsReuseValidatedDirectoryAndPagesWithoutReads) {
  auto source = std::make_shared<CacheSource>();
  Hdf5FormatQueryAdapter adapter;
  QueryProfileRecord profile;
  ScopedQueryProfile scope(&profile);
  auto first = adapter.prepare(QueryPlanningContext(*source), query(1, 11));
  ASSERT_TRUE(first.ok()) << first.status().message();
  EXPECT_EQ(source->layout_calls, 3U);
  EXPECT_EQ(profile.counters["hdf5.layout.cache.page.misses"], 1U);
  const auto parsed_ns = profile.nanoseconds["hdf5.layout.pages.parse"];
  EXPECT_GT(parsed_ns, 0U);
  auto cursor = adapter.execute(QueryExecutionContext(source), first.value().implementation(), {});
  ASSERT_TRUE(cursor.ok()); ASSERT_TRUE(consume(*cursor.value()).ok());
  EXPECT_EQ(source->layout_calls, 3U);
  const auto bytes = source->layout_bytes.load();
  auto second = adapter.prepare(QueryPlanningContext(*source), query(3, 9, "/alias"));
  ASSERT_TRUE(second.ok()) << second.status().message();
  EXPECT_EQ(source->layout_calls, 3U);
  EXPECT_EQ(profile.counters["hdf5.layout.cache.page.hits"], 1U);
  EXPECT_EQ(profile.nanoseconds["hdf5.layout.pages.parse"], parsed_ns);
  cursor = adapter.execute(QueryExecutionContext(source), second.value().implementation(), {});
  ASSERT_TRUE(cursor.ok());
  auto values = consume(*cursor.value()); ASSERT_TRUE(values.ok());
  expect_values(values.value(), 3, 9);
  EXPECT_EQ(source->layout_calls, 3U);
  EXPECT_EQ(source->layout_bytes.load(), bytes);
}

TEST(Hdf5LayoutCache, WrongSliceTypeIsAttributedBeforeLayoutReads) {
  auto source = std::make_shared<CacheSource>();
  Hdf5FormatQueryAdapter adapter;
  auto fragment = query();
  fragment.slices[0].lower = DomainBound{LogicalValue::int64(1), BoundKind::Inclusive};
  auto plan = adapter.prepare(QueryPlanningContext(*source), fragment);
  ASSERT_FALSE(plan.ok());
  EXPECT_EQ(plan.status().code(), QueryError::TypeMismatch);
  EXPECT_EQ(plan.status().phase(), QueryPhase::Validation);
  ASSERT_TRUE(plan.status().detail().source);
  EXPECT_EQ(plan.status().detail().source->path, source->manifest().key.path);
  ASSERT_TRUE(plan.status().detail().field_id);
  EXPECT_EQ(*plan.status().detail().field_id, "hdf5.axis.0");
  EXPECT_EQ(source->layout_calls, 0U);
}

TEST(Hdf5LayoutCache, PlanningErrorsIdentifyExactSourceAndOperation) {
  auto source = std::make_shared<CacheSource>();
  Hdf5FormatQueryAdapter adapter;
  auto missing = adapter.prepare(QueryPlanningContext(*source), query(0, 4, "/missing"));
  ASSERT_FALSE(missing.ok());
  EXPECT_EQ(missing.status().code(), QueryError::UnknownField);
  ASSERT_TRUE(missing.status().detail().source);
  EXPECT_EQ(missing.status().detail().source->path, source->manifest().key.path);
  EXPECT_EQ(missing.status().detail().source->version, source->manifest().key.version);
  ASSERT_TRUE(missing.status().detail().operation);
  EXPECT_EQ(*missing.status().detail().operation, "hdf5.prepare");
}

TEST(Hdf5LayoutCache, ThrownLayoutReadIsAttributedAsReadFailure) {
  auto source = std::make_shared<CacheSource>();
  source->on_layout_read = [](std::uint64_t) {
    throw std::runtime_error("injected layout callback failure");
  };
  Hdf5FormatQueryAdapter adapter;
  auto plan = adapter.prepare(QueryPlanningContext(*source), query());
  ASSERT_FALSE(plan.ok());
  EXPECT_EQ(plan.status().code(), QueryError::ReadFailure);
  ASSERT_TRUE(plan.status().detail().source);
  EXPECT_EQ(plan.status().detail().source->path, source->manifest().key.path);
  ASSERT_TRUE(plan.status().detail().operation);
  EXPECT_EQ(*plan.status().detail().operation, "read_layout");
}

TEST(Hdf5LayoutCache, UnexpectedSemanticExceptionIsInternalWithSource) {
  class ThrowingProfile final : public Hdf5SemanticProfile {
  public:
    std::string id() const override { return "test-throwing-profile"; }
    bool declares(const Hdf5LayoutDocument&) const override {
      throw std::runtime_error("injected semantic failure");
    }
    QueryResult<Hdf5CoordinateIndex> inspect_coordinate(
        const Hdf5LayoutObject&) const override {
      return QueryResult<Hdf5CoordinateIndex>::failure(QueryStatus::failure(
        QueryError::Internal, QueryPhase::Planning, "unused"));
    }
    QueryResult<Hdf5CfBinding> compile(const Hdf5LayoutDocument&,
        const std::string&) const override {
      return QueryResult<Hdf5CfBinding>::failure(QueryStatus::failure(
        QueryError::Internal, QueryPhase::Planning, "unused"));
    }
  };
  auto source = std::make_shared<CacheSource>();
  Hdf5FormatQueryAdapter adapter({}, {}, {}, {},
    {std::make_shared<ThrowingProfile>()});
  auto fragment = query();
  fragment.projection = {make_hdf5_science_value_field("/a").value()};
  auto plan = adapter.prepare(QueryPlanningContext(*source), fragment);
  ASSERT_FALSE(plan.ok());
  EXPECT_EQ(plan.status().code(), QueryError::Internal);
  ASSERT_TRUE(plan.status().detail().source);
  EXPECT_EQ(plan.status().detail().source->path, source->manifest().key.path);
}

TEST(Hdf5LayoutCache, CursorReadFailureIdentifiesExactSource) {
  auto source = std::make_shared<CacheSource>();
  Hdf5FormatQueryAdapter adapter;
  auto plan = adapter.prepare(QueryPlanningContext(*source), query());
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  auto opened = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), {});
  ASSERT_TRUE(opened.ok()) << opened.status().message();
  source->fail = true;
  auto next = opened.value()->next();
  ASSERT_FALSE(next.ok());
  EXPECT_EQ(next.status().code(), QueryError::ReadFailure);
  ASSERT_TRUE(next.status().detail().source);
  EXPECT_EQ(next.status().detail().source->path, source->manifest().key.path);
  EXPECT_EQ(next.status().detail().source->version, source->manifest().key.version);
  EXPECT_EQ(next.status().detail().operation, "hdf5.cursor.next");
}

TEST(Hdf5LayoutCache, PreparedSelectionRejectsChangedTransport) {
  auto source = std::make_shared<CacheSource>();
  Hdf5FormatQueryAdapter adapter;
  auto plan = adapter.prepare(QueryPlanningContext(*source), query());
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  source->capabilities.max_bytes /= 2;
  auto cursor = adapter.execute(QueryExecutionContext(source),
    plan.value().implementation(), {});
  ASSERT_FALSE(cursor.ok());
  EXPECT_EQ(cursor.status().code(), QueryError::StalePlan);
}

TEST(Hdf5LayoutCache, PreparedSelectionRejectsDifferentDecoderLimits) {
  auto source = std::make_shared<CacheSource>();
  Hdf5FormatQueryAdapter planner;
  auto plan = planner.prepare(QueryPlanningContext(*source), query());
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  Hdf5QueryKernelConfig stricter;
  stricter.max_decoded_chunk_bytes /= 2;
  Hdf5FormatQueryAdapter executor(stricter);
  auto cursor = executor.execute(QueryExecutionContext(source),
    plan.value().implementation(), {});
  ASSERT_FALSE(cursor.ok());
  EXPECT_EQ(cursor.status().code(), QueryError::StalePlan);
}

TEST(Hdf5LayoutCache, ReusesPagesAcrossSelectionsAndReadsNewPagesOnDemand) {
  auto source = std::make_shared<CacheSource>(0, 0, 600);
  Hdf5FormatQueryAdapter adapter;
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), query(0, 4)).ok());
  EXPECT_EQ(source->layout_calls, 3U);
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), query(2048, 2052)).ok());
  EXPECT_EQ(source->layout_calls, 4U); // Only the different page is read.
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), query(4, 8, "/alias")).ok());
  EXPECT_EQ(source->layout_calls, 4U);
}

TEST(Hdf5LayoutCache, NamespacePathVersionRevisionAndSizesIsolateDirectories) {
  auto baseline = std::make_shared<CacheSource>();
  Hdf5FormatQueryAdapter adapter;
  auto original = adapter.prepare(QueryPlanningContext(*baseline), query());
  ASSERT_TRUE(original.ok());
  for (unsigned change = 0; change < 5; ++change) {
    auto other = std::make_shared<CacheSource>();
    if (change) other->namespace_override = baseline->storage_namespace();
    if (change == 1) other->manifest_.key.path += ".other";
    if (change == 2) ++other->manifest_.key.version;
    if (change == 3) ++other->manifest_.revision;
    if (change == 4) { other->document.parser_version = "1.14.7.1"; other->encode(); }
    if (change == 4) ASSERT_NE(other->layout.size(), baseline->layout.size());
    else ASSERT_EQ(other->layout.size(), baseline->layout.size());
    auto plan = adapter.prepare(QueryPlanningContext(*other), query());
    ASSERT_TRUE(plan.ok()) << plan.status().message();
    EXPECT_EQ(other->layout_calls, 3U) << change;
    auto stale = adapter.execute(QueryExecutionContext(other), original.value().implementation(), {});
    ASSERT_FALSE(stale.ok()); EXPECT_EQ(stale.status().code(), QueryError::StalePlan);
  }
  // Older exact versions remain valid independently after newer versions warm.
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*baseline), query()).ok());
  EXPECT_EQ(baseline->layout_calls, 3U);
}

TEST(Hdf5LayoutCache, ColdMissDoesNotCacheFailedHeaderReads) {
  auto source = std::make_shared<CacheSource>();
  Hdf5FormatQueryAdapter adapter;
  source->fail_layout = true;
  auto failed = adapter.prepare(QueryPlanningContext(*source), query());
  ASSERT_FALSE(failed.ok()); EXPECT_EQ(failed.status().code(), QueryError::ReadFailure);
  source->fail_layout = false; source->short_layout = true;
  failed = adapter.prepare(QueryPlanningContext(*source), query());
  ASSERT_FALSE(failed.ok()); EXPECT_EQ(failed.status().code(), QueryError::CorruptLayout);
  source->short_layout = false;
  auto recovered = adapter.prepare(QueryPlanningContext(*source), query());
  ASSERT_TRUE(recovered.ok()) << recovered.status().message();
  EXPECT_EQ(source->layout_calls, 5U);
}

TEST(Hdf5LayoutCache, LogicalDirectoryAndPageBudgetsStillApplyOnHits) {
  auto source = std::make_shared<CacheSource>();
  Hdf5FormatQueryAdapter adapter;
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), query()).ok());
  const auto logical_bytes = source->layout_bytes.load();
  for (unsigned variant = 0; variant < 3; ++variant) {
    QueryLimits limits;
    if (variant == 0) limits.max_layout_bytes = logical_bytes - 1;
    if (variant == 1) limits.max_ranges = 2;
    if (variant == 2) limits.max_layout_bytes = kHdf5LayoutHeaderBytes;
    auto plan = adapter.prepare(QueryPlanningContext(*source, limits), query());
    ASSERT_FALSE(plan.ok()) << variant;
    EXPECT_EQ(plan.status().code(), QueryError::ResourceExhausted);
  }
  EXPECT_EQ(source->layout_calls, 3U); // Cache hits preserve logical budgets without I/O.
  auto plan = adapter.prepare(QueryPlanningContext(*source), query()); ASSERT_TRUE(plan.ok());
  QueryExecutionOptions options; options.max_layout_bytes = 0;
  auto execution = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), options);
  ASSERT_TRUE(execution.ok()) << execution.status().message();
  EXPECT_EQ(source->layout_calls, 3U);
}

TEST(Hdf5LayoutCache, CorruptPagesAreNotRetainedAndCanBeRetried) {
  auto source = std::make_shared<CacheSource>();
  Hdf5FormatQueryAdapter adapter;
  source->layout.back() ^= 1;
  auto failed = adapter.prepare(QueryPlanningContext(*source), query());
  ASSERT_FALSE(failed.ok()); EXPECT_EQ(failed.status().code(), QueryError::CorruptLayout);
  EXPECT_EQ(source->layout_calls, 3U);
  source->layout.back() ^= 1;
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), query()).ok());
  EXPECT_EQ(source->layout_calls, 4U); // Valid directory survives; corrupt page does not.
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), query()).ok());
  EXPECT_EQ(source->layout_calls, 4U);
}

TEST(Hdf5LayoutCache, EntryAndByteBoundsEvictWithoutBreakingPinnedPlans) {
  for (const auto bounds : {Hdf5LayoutCacheConfig{0, 4096}, Hdf5LayoutCacheConfig{1, 4096},
      Hdf5LayoutCacheConfig{64ULL * 1024 * 1024, 0}, Hdf5LayoutCacheConfig{64ULL * 1024 * 1024, 1}}) {
    auto source = std::make_shared<CacheSource>();
    Hdf5FormatQueryAdapter adapter({}, {}, {}, bounds);
    auto first = adapter.prepare(QueryPlanningContext(*source), query()); ASSERT_TRUE(first.ok());
    auto second = adapter.prepare(QueryPlanningContext(*source), query()); ASSERT_TRUE(second.ok());
    EXPECT_EQ(source->layout_calls, 6U);
    auto cursor = adapter.execute(QueryExecutionContext(source), first.value().implementation(), {});
    ASSERT_TRUE(cursor.ok()); auto values = consume(*cursor.value()); ASSERT_TRUE(values.ok());
    expect_values(values.value(), 0, 32);
  }
}

TEST(Hdf5LayoutCache, ConcurrentColdAndWarmPlanningPublishesOnlyCompleteEntries) {
  auto source = std::make_shared<CacheSource>();
  Hdf5FormatQueryAdapter adapter;
  Gate gate;
  source->on_layout_read = [&](std::uint64_t offset) { if (!offset) gate.wait(); };
  std::vector<std::future<QueryResult<AdapterPlan>>> requests;
  for (unsigned i = 0; i < 8; ++i)
    requests.push_back(std::async(std::launch::async, [&] {
      return adapter.prepare(QueryPlanningContext(*source), query());
    }));
  const bool all_started = gate.started(8);
  gate.open();
  EXPECT_TRUE(all_started);
  for (auto& request : requests) {
    auto result = request.get(); ASSERT_TRUE(result.ok()) << result.status().message();
  }
  source->on_layout_read = {};
  const auto reads = source->layout_calls.load();
  EXPECT_GE(reads, 10U); EXPECT_LE(reads, 24U);
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), query(1, 9)).ok());
  EXPECT_EQ(source->layout_calls, reads);
}

TEST(Hdf5LayoutCache, PositiveByteBudgetEvictsOlderSourcesUnderPressure) {
  auto source = std::make_shared<CacheSource>();
  auto directory = read_hdf5_layout_directory([&](auto offset, auto length) {
    return CacheSource::read(source->layout, offset, length);
  }, source->layout.size(), source->payload.size());
  ASSERT_TRUE(directory.ok());
  Hdf5LayoutCacheConfig bounds;
  bounds.max_bytes = 4 * (directory.value().retained_bytes() + source->layout.size() + 2048);
  Hdf5FormatQueryAdapter adapter({}, {}, {}, bounds);
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), query()).ok());
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), query()).ok());
  ASSERT_EQ(source->layout_calls, 3U); // A whole request fits and is reusable.
  for (unsigned i = 0; i < 32; ++i) {
    auto other = std::make_shared<CacheSource>();
    ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*other), query()).ok());
  }
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), query()).ok());
  EXPECT_EQ(source->layout_calls, 6U); // Byte pressure evicted both old entries.
}

TEST(Hdf5LayoutCache, EntryPressureEvictsLeastRecentlyUsedSource) {
  auto first = std::make_shared<CacheSource>();
  auto second = std::make_shared<CacheSource>();
  auto third = std::make_shared<CacheSource>();
  Hdf5FormatQueryAdapter adapter({}, {}, {}, {64ULL * 1024 * 1024, 4});
  for (const auto& source : {first, second, first, third, first})
    ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), query()).ok());
  EXPECT_EQ(first->layout_calls, 3U); // The refreshed first source survived.
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*second), query()).ok());
  EXPECT_EQ(second->layout_calls, 6U); // Its older directory and page were evicted.
}

TEST(Hdf5SharedCache, OverlappingQueriesAndHardLinkAliasesReuseDecodedChunks) {
  auto source = std::make_shared<CacheSource>(); auto runtime = std::make_shared<QueryRuntime>();
  auto first = cursor(source, runtime, query(1, 11)); ASSERT_TRUE(first.ok()) << first.status().message();
  auto values = consume(*first.value()); ASSERT_TRUE(values.ok()); expect_values(values.value(), 1, 11);
  EXPECT_EQ(runtime->stats().decode_count, 3U);
  const auto reads = source->calls.load();
  auto second = cursor(source, runtime, query(3, 9, "/alias")); ASSERT_TRUE(second.ok());
  values = consume(*second.value()); ASSERT_TRUE(values.ok()); expect_values(values.value(), 3, 9);
  EXPECT_EQ(source->calls.load(), reads); EXPECT_EQ(runtime->stats().decode_count, 3U);
  EXPECT_GT(runtime->stats().hits, 0U);
}

TEST(Hdf5SharedCache, NamespaceVersionRevisionAndLayoutSizeArePartOfIdentity) {
  auto runtime = std::make_shared<QueryRuntime>(); auto baseline = std::make_shared<CacheSource>();
  auto run = [&](const std::shared_ptr<CacheSource>& source, float bias) {
    auto result = cursor(source, runtime, query(0, 4)); ASSERT_TRUE(result.ok()) << result.status().message();
    auto values = consume(*result.value()); ASSERT_TRUE(values.ok()) << values.status().message();
    expect_values(values.value(), 0, 4, bias);
    EXPECT_GT(source->calls.load(), 0U);
  };
  run(baseline, 0);
  auto other_namespace = std::make_shared<CacheSource>(0, 100); run(other_namespace, 100);
  auto other_version = std::make_shared<CacheSource>(0, 200);
  other_version->namespace_override = baseline->storage_namespace(); ++other_version->manifest_.key.version;
  run(other_version, 200);
  auto other_revision = std::make_shared<CacheSource>(0, 300);
  other_revision->namespace_override = baseline->storage_namespace(); ++other_revision->manifest_.revision;
  run(other_revision, 300);
  auto other_layout = std::make_shared<CacheSource>(0, 400);
  other_layout->namespace_override = baseline->storage_namespace();
  other_layout->document.parser_version = "1.14.7.1"; other_layout->encode();
  ASSERT_EQ(other_layout->manifest_.payload_size, baseline->manifest_.payload_size);
  ASSERT_NE(other_layout->manifest_.layout_size, baseline->manifest_.layout_size);
  run(other_layout, 400);
  EXPECT_EQ(runtime->stats().decode_count, 5U);
}

TEST(Hdf5SharedCache, RequestPayloadBudgetIsEnforcedOnCacheHits) {
  auto source = std::make_shared<CacheSource>(); auto runtime = std::make_shared<QueryRuntime>();
  auto first = cursor(source, runtime, query(0, 4)); ASSERT_TRUE(first.ok());
  ASSERT_TRUE(consume(*first.value()).ok()); const auto reads = source->calls.load();
  QueryExecutionOptions limits; limits.max_payload_bytes = 15;
  auto second = cursor(source, runtime, query(0, 4), {}, limits);
  if (second.ok()) {
    auto result = consume(*second.value()); ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted);
  } else EXPECT_EQ(second.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(source->calls.load(), reads);
  auto third = cursor(source, runtime, query(0, 4)); ASSERT_TRUE(third.ok());
  ASSERT_TRUE(consume(*third.value()).ok()); EXPECT_EQ(source->calls.load(), reads);
}

TEST(Hdf5SharedCache, AdjacentChunksMergeWhileDisjointChunksUseOneMultiRangeCall) {
  for (const auto gap : {0U, 16U}) {
    auto source = std::make_shared<CacheSource>(gap); auto runtime = std::make_shared<QueryRuntime>();
    auto result = cursor(source, runtime); ASSERT_TRUE(result.ok()) << result.status().message();
    auto values = consume(*result.value()); ASSERT_TRUE(values.ok()) << values.status().message();
    expect_values(values.value(), 0, 32);
    EXPECT_EQ(source->calls.load(), 1U); EXPECT_EQ(source->bytes.load(), 128U);
    EXPECT_EQ(source->ranges.load(), gap ? 8U : 1U);
    if (gap) EXPECT_EQ(source->batch_calls.load(), 1U);
    EXPECT_EQ(runtime->stats().decode_count, 8U);
    EXPECT_EQ(runtime->stats().read_ranges, gap ? 8U : 1U);
  }
}

TEST(Hdf5SharedCache, ConfiguredFragmentAndByteBoundsSplitReadWaves) {
  for (const bool byte_bound : {false, true}) {
    auto source = std::make_shared<CacheSource>(8); auto runtime = std::make_shared<QueryRuntime>();
    Hdf5QueryKernelConfig config;
    if (byte_bound) config.max_batch_encoded_bytes = 32;
    else config.max_batch_fragments = 2;
    auto result = cursor(source, runtime, query(), config); ASSERT_TRUE(result.ok());
    auto values = consume(*result.value()); ASSERT_TRUE(values.ok()) << values.status().message();
    expect_values(values.value(), 0, 32); EXPECT_EQ(source->calls.load(), 4U);
    EXPECT_EQ(source->ranges.load(), 8U); EXPECT_EQ(source->bytes.load(), 128U);
    for (const auto& call : source->trace) {
      std::uint64_t size = 0; for (const auto& range : call) size += range.length;
      EXPECT_LE(size, 32U); EXPECT_LE(call.size(), 2U);
    }
  }
}

TEST(Hdf5SharedCache, ShortOrFailedBatchPublishesNoFragmentsAndCanRetry) {
  for (const bool short_read : {false, true}) {
    auto source = std::make_shared<CacheSource>(8); auto runtime = std::make_shared<QueryRuntime>();
    source->short_read = short_read; source->fail = !short_read;
    auto result = cursor(source, runtime); ASSERT_TRUE(result.ok());
    auto failed = consume(*result.value()); ASSERT_FALSE(failed.ok());
    EXPECT_EQ(failed.status().code(), QueryError::ReadFailure);
    EXPECT_EQ(runtime->stats().cache_owned_bytes, 0U);
    source->short_read = false; source->fail = false;
    auto retry = cursor(source, runtime); ASSERT_TRUE(retry.ok());
    auto values = consume(*retry.value()); ASSERT_TRUE(values.ok()) << values.status().message();
    expect_values(values.value(), 0, 32);
  }
}

TEST(Hdf5SharedCache, ConcurrentIdenticalQueriesHaveOneProducer) {
  auto source = std::make_shared<CacheSource>(); auto runtime = std::make_shared<QueryRuntime>();
  Gate gate; source->on_read = [&] { gate.wait(); };
  auto a = cursor(source, runtime, query(0, 4)); ASSERT_TRUE(a.ok());
  auto b = cursor(source, runtime, query(0, 4)); ASSERT_TRUE(b.ok());
  auto first = std::async(std::launch::async, [&] { return consume(*a.value()); });
  EXPECT_TRUE(gate.started()); const auto waits = runtime->stats().waits;
  auto second = std::async(std::launch::async, [&] { return consume(*b.value()); });
  EXPECT_TRUE(eventually([&] { return runtime->stats().waits > waits; }));
  gate.open(); auto av = first.get(); auto bv = second.get();
  ASSERT_TRUE(av.ok()) << av.status().message(); ASSERT_TRUE(bv.ok()) << bv.status().message();
  expect_values(av.value(), 0, 4); expect_values(bv.value(), 0, 4);
  EXPECT_EQ(source->calls.load(), 1U); EXPECT_EQ(runtime->stats().decode_count, 1U);
}

TEST(Hdf5SharedCache, CancellingFirstWaiterDoesNotCancelOtherQuery) {
  auto source = std::make_shared<CacheSource>(); auto runtime = std::make_shared<QueryRuntime>();
  Gate gate; source->on_read = [&] { gate.wait(); };
  auto a = cursor(source, runtime, query(0, 4)); ASSERT_TRUE(a.ok());
  auto b = cursor(source, runtime, query(0, 4)); ASSERT_TRUE(b.ok());
  auto first = std::async(std::launch::async, [&] { return consume(*a.value()); });
  EXPECT_TRUE(gate.started()); const auto waits = runtime->stats().waits;
  auto second = std::async(std::launch::async, [&] { return consume(*b.value()); });
  EXPECT_TRUE(eventually([&] { return runtime->stats().waits > waits; }));
  a.value()->cancel(); const auto first_state = first.wait_for(1s);
  gate.open(); auto av = first.get(); auto bv = second.get();
  EXPECT_EQ(first_state, std::future_status::ready);
  ASSERT_FALSE(av.ok()); EXPECT_EQ(av.status().code(), QueryError::Cancelled);
  ASSERT_TRUE(bv.ok()) << bv.status().message(); expect_values(bv.value(), 0, 4);
  EXPECT_EQ(source->calls.load(), 1U); EXPECT_EQ(runtime->stats().decode_count, 1U);
}

TEST(Hdf5SharedCache, LastWaiterCancellationAllowsImmediateFreshLoad) {
  auto source = std::make_shared<CacheSource>(); auto runtime = std::make_shared<QueryRuntime>();
  Gate gate; std::atomic<unsigned> calls{0};
  source->on_read = [&] { if (++calls == 1) gate.wait(); };
  auto a = cursor(source, runtime, query(0, 4)); ASSERT_TRUE(a.ok());
  auto first = std::async(std::launch::async, [&] { return consume(*a.value()); });
  EXPECT_TRUE(gate.started()); a.value()->cancel();
  const auto first_state = first.wait_for(1s);
  auto b = cursor(source, runtime, query(0, 4));
  EXPECT_TRUE(b.ok());
  if (!b.ok()) { gate.open(); first.get(); return; }
  auto second = std::async(std::launch::async, [&] { return consume(*b.value()); });
  const auto second_state = second.wait_for(1s);
  gate.open(); auto av = first.get(); auto bv = second.get();
  EXPECT_EQ(first_state, std::future_status::ready); EXPECT_EQ(second_state, std::future_status::ready);
  ASSERT_FALSE(av.ok()); EXPECT_EQ(av.status().code(), QueryError::Cancelled);
  ASSERT_TRUE(bv.ok()) << bv.status().message(); expect_values(bv.value(), 0, 4);
  EXPECT_EQ(source->calls.load(), 2U);
  auto again = cursor(source, runtime, query(0, 4)); ASSERT_TRUE(again.ok());
  ASSERT_TRUE(consume(*again.value()).ok()); EXPECT_EQ(source->calls.load(), 2U);
}

TEST(Hdf5SharedCache, RuntimeInflightLimitBoundsBatchAdmissionWithoutTimeout) {
  auto source = std::make_shared<CacheSource>(8);
  QueryRuntimeConfig config; config.max_inflight = 2; config.wait_timeout_ms = 250;
  auto runtime = std::make_shared<QueryRuntime>(config);
  auto result = cursor(source, runtime); ASSERT_TRUE(result.ok());
  auto values = consume(*result.value()); ASSERT_TRUE(values.ok()) << values.status().message();
  expect_values(values.value(), 0, 32);
  EXPECT_EQ(runtime->stats().decode_count, 8U);
  EXPECT_LE(runtime->stats().peak_bytes, config.memory_budget_bytes);
}

TEST(Hdf5SharedCache, DefaultAdapterKeepsMoreThan1024TinyChunksWithinByteBudget) {
  auto source = std::make_shared<CacheSource>(0, 0, 1100);
  Hdf5FormatQueryAdapter adapter;
  auto run = [&]() {
    auto plan = adapter.prepare(QueryPlanningContext(*source), query(0, 4400));
    if (!plan.ok()) return QueryResult<std::vector<float>>::failure(plan.status());
    auto opened = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), {});
    if (!opened.ok()) return QueryResult<std::vector<float>>::failure(opened.status());
    return consume(*opened.value());
  };
  auto first = run(); ASSERT_TRUE(first.ok()) << first.status().message();
  expect_values(first.value(), 0, 4400); const auto calls = source->calls.load();
  auto second = run(); ASSERT_TRUE(second.ok()) << second.status().message();
  expect_values(second.value(), 0, 4400); EXPECT_EQ(source->calls.load(), calls);
}

TEST(Hdf5SharedCache, ChunkMajorAssemblyReusesWideRowsAcrossBatchBoundaries) {
  for (const std::uint64_t chunk_count : {2ULL, 65ULL}) {
    SCOPED_TRACE(chunk_count);
    auto source = multirow_chunks(chunk_count);
    QueryRuntimeConfig limits;
    limits.max_entries = 1; // Force eviction from the shared decoded cache.
    auto runtime = std::make_shared<QueryRuntime>(limits);
    QueryExecutionOptions options;
    options.target_batch_rows = chunk_count * 8; // Two rows per batch.
    auto opened = cursor(source, runtime, query(0, 4), {}, options);
    ASSERT_TRUE(opened.ok()) << opened.status().message();
    auto values = consume(*opened.value());
    ASSERT_TRUE(values.ok()) << values.status().message();
    expect_values(values.value(), 0, chunk_count * 16);
    EXPECT_EQ(source->ranges.load(), chunk_count);
    EXPECT_EQ(runtime->stats().decode_count, chunk_count);
  }
}

TEST(Hdf5SharedCache, ChunkMajorPrefetchSkipsRetainedEarlierBand) {
  auto source = multirow_chunks(8, 3);
  auto runtime = std::make_shared<QueryRuntime>();
  QueryExecutionOptions options;
  options.target_batch_rows = 16; // Two rows per batch; second crosses band.
  auto opened = cursor(source, runtime, query(0, 4), {}, options);
  ASSERT_TRUE(opened.ok()) << opened.status().message();
  auto values = consume(*opened.value());
  ASSERT_TRUE(values.ok()) << values.status().message();
  expect_values(values.value(), 0, 128);
  EXPECT_EQ(runtime->stats().decode_count, 16U);
  EXPECT_LE(source->ranges.load(), 4U);
  EXPECT_LE(source->calls.load(), 4U); // Compact waves, not eight singles.
}

TEST(Hdf5SharedCache, ChunkMajorLeaseRetentionRespectsOutputBufferBudget) {
  auto source = multirow_chunks(2);
  QueryRuntimeConfig limits; limits.max_entries = 1;
  auto runtime = std::make_shared<QueryRuntime>(limits);
  Hdf5QueryKernelConfig config; config.max_buffer_bytes = 128;
  QueryExecutionOptions options; options.target_batch_rows = 16;
  auto opened = cursor(source, runtime, query(0, 4), config, options);
  ASSERT_TRUE(opened.ok()) << opened.status().message();
  auto values = consume(*opened.value());
  ASSERT_TRUE(values.ok()) << values.status().message();
  expect_values(values.value(), 0, 32);
  EXPECT_EQ(runtime->stats().decode_count, 4U); // One chunk can be leased at a time.
}

TEST(Hdf5SharedCache, StricterDecoderCapsCannotBorrowAnAdmittedChunk) {
  auto source = std::make_shared<CacheSource>(); auto runtime = std::make_shared<QueryRuntime>();
  auto initial = cursor(source, runtime, query(0, 4)); ASSERT_TRUE(initial.ok());
  ASSERT_TRUE(consume(*initial.value()).ok()); const auto reads = source->calls.load();
  Hdf5QueryKernelConfig config; config.max_decoded_chunk_bytes = 8;
  auto limited = cursor(source, runtime, query(0, 4), config);
  if (limited.ok()) {
    auto values = consume(*limited.value()); ASSERT_FALSE(values.ok());
    EXPECT_EQ(values.status().code(), QueryError::ResourceExhausted);
  } else EXPECT_EQ(limited.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(source->calls.load(), reads);
}

TEST(Hdf5SharedCache, ReadWaveParallelismIsObservableAndBounded) {
  for (const unsigned parallelism : {1U, 2U, 4U}) {
    auto source = std::make_shared<CacheSource>(8); auto runtime = std::make_shared<QueryRuntime>();
    source->capabilities.max_bytes = 16;
    Gate gate; std::atomic<unsigned> active{0}, peak{0};
    source->on_read = [&] {
      const auto count = ++active; auto old = peak.load();
      while (count > old && !peak.compare_exchange_weak(old, count)) {}
      gate.wait(); --active;
    };
    Hdf5QueryKernelConfig config; config.read_parallelism = parallelism;
    auto result = cursor(source, runtime, query(), config); ASSERT_TRUE(result.ok());
    auto pending = std::async(std::launch::async, [&] { return consume(*result.value()); });
    const auto all_started = gate.started(parallelism); gate.open();
    auto values = pending.get();
    EXPECT_TRUE(all_started); ASSERT_TRUE(values.ok()) << values.status().message();
    expect_values(values.value(), 0, 32); EXPECT_EQ(source->calls.load(), 8U);
    EXPECT_EQ(peak.load(), parallelism); EXPECT_EQ(active.load(), 0U);
    EXPECT_EQ(runtime->stats().payload_reads_peak, parallelism);
  }
}

TEST(Hdf5SharedCache, RepeatedSlabsChargeEachLogicalChunkOnlyOncePerRequest) {
  auto source = std::make_shared<CacheSource>(); auto runtime = std::make_shared<QueryRuntime>();
  QueryExecutionOptions limits; limits.target_batch_rows = 1;
  limits.max_payload_bytes = 16; limits.max_ranges = 2; // One header and one distinct chunk.
  auto result = cursor(source, runtime, query(0, 4), {}, limits); ASSERT_TRUE(result.ok());
  auto values = consume(*result.value()); ASSERT_TRUE(values.ok()) << values.status().message();
  expect_values(values.value(), 0, 4); EXPECT_EQ(source->calls.load(), 1U);
  EXPECT_EQ(runtime->stats().decode_count, 1U);
}

TEST(Hdf5SharedCache, SparseFillIsReusableWithoutPhysicalPayloadReads) {
  auto source = std::make_shared<CacheSource>(); auto runtime = std::make_shared<QueryRuntime>();
  auto& dataset = *source->document.objects[1].dataset;
  dataset.chunks.erase(dataset.chunks.begin()); --dataset.allocated_chunk_count;
  dataset.allocation_status = 1; dataset.fill_status = Hdf5FillStatus::Explicit;
  dataset.fill_value = {0, 0, 128, 191}; source->encode(); // -1.0 in canonical LE.
  QueryExecutionOptions limits; limits.max_payload_bytes = 0; limits.max_ranges = 1;
  for (unsigned pass = 0; pass < 2; ++pass) {
    auto result = cursor(source, runtime, query(0, 4), {}, limits); ASSERT_TRUE(result.ok()) << result.status().message();
    auto values = consume(*result.value()); ASSERT_TRUE(values.ok()) << values.status().message();
    ASSERT_EQ(values.value().size(), 4U);
    for (const auto value : values.value()) EXPECT_FLOAT_EQ(value, -1.f);
  }
  EXPECT_EQ(source->calls.load(), 0U); EXPECT_EQ(runtime->stats().read_bytes, 0U);
  EXPECT_GT(runtime->stats().hits, 0U);
}

TEST(Hdf5SharedCache, MissingChunksShareOneDatasetFillFragment) {
  auto source = std::make_shared<CacheSource>();
  auto runtime = std::make_shared<QueryRuntime>();
  auto& dataset = *source->document.objects[1].dataset;
  dataset.chunks.erase(dataset.chunks.begin(), dataset.chunks.begin() + 3);
  dataset.allocated_chunk_count = dataset.chunks.size();
  dataset.allocation_status = 1;
  dataset.fill_status = Hdf5FillStatus::Explicit;
  dataset.fill_value = {0, 0, 128, 191}; // -1.0 in canonical LE.
  source->encode();
  Hdf5FragmentMetrics metrics;
  Hdf5QueryKernelConfig config;
  config.metrics_observer = [&](const Hdf5FragmentMetrics& current) { metrics = current; };
  QueryExecutionOptions limits;
  limits.max_payload_bytes = 0;
  limits.max_ranges = 1;
  for (unsigned pass = 0; pass < 2; ++pass) {
    auto result = cursor(source, runtime, query(0, 12), config, limits);
    ASSERT_TRUE(result.ok()) << result.status().message();
    auto values = consume(*result.value());
    ASSERT_TRUE(values.ok()) << values.status().message();
    ASSERT_EQ(values.value().size(), 12U);
    for (const auto value : values.value()) EXPECT_FLOAT_EQ(value, -1.f);
    EXPECT_EQ(metrics.fill_bytes, pass == 0 ? 16U : 0U);
  }
  EXPECT_EQ(source->calls.load(), 0U);
  EXPECT_EQ(runtime->stats().misses, 1U);
}

TEST(Hdf5SharedCache, UnallocatedContiguousTailKeepsItsOwnFillSize) {
  auto source = std::make_shared<CacheSource>();
  auto runtime = std::make_shared<QueryRuntime>();
  auto& dataset = *source->document.objects[1].dataset;
  dataset.storage = Hdf5StorageLayout::Contiguous;
  dataset.descriptor.shape = {10};
  dataset.contiguous_offset.reset();
  dataset.contiguous_size = 0;
  dataset.chunk_shape.clear();
  dataset.chunks.clear();
  dataset.allocated_chunk_count = 0;
  dataset.allocation_status = 0;
  dataset.fill_status = Hdf5FillStatus::Explicit;
  dataset.fill_value = {0, 0, 128, 191};
  source->encode();
  Hdf5QueryKernelConfig config;
  config.contiguous_tile_elements = 4;
  for (const auto [begin, end] : {std::pair{8U, 10U}, std::pair{0U, 4U}}) {
    auto result = cursor(source, runtime, query(begin, end), config);
    ASSERT_TRUE(result.ok()) << result.status().message();
    auto values = consume(*result.value());
    ASSERT_TRUE(values.ok()) << values.status().message();
    ASSERT_EQ(values.value().size(), end - begin);
    for (const auto value : values.value()) EXPECT_FLOAT_EQ(value, -1.f);
  }
  EXPECT_EQ(runtime->stats().misses, 2U);
}

TEST(Hdf5SharedCache, CompressedChunksReuseOneDecodeAcrossQueries) {
  auto source = std::make_shared<CacheSource>(); auto runtime = std::make_shared<QueryRuntime>();
  auto& dataset = *source->document.objects[1].dataset;
  BinaryValue encoded(64);
  for (auto& chunk : dataset.chunks) {
    uLongf length = compressBound(chunk.encoded_size); BinaryValue compressed(length);
    ASSERT_EQ(compress2(compressed.data(), &length, source->payload.data() + chunk.offset,
      chunk.encoded_size, 6), Z_OK);
    compressed.resize(length); chunk.offset = encoded.size(); chunk.encoded_size = length;
    encoded.insert(encoded.end(), compressed.begin(), compressed.end());
  }
  dataset.filters = {{1, 1, {6}}}; source->payload = std::move(encoded);
  source->document.source_size = source->payload.size();
  source->manifest_.payload_size = source->payload.size(); source->encode();
  QueryProfileRecord profile;
  ScopedQueryProfile scope(&profile);
  std::set<std::string> decoded_keys;
  std::mutex observed_mu;
  std::uint64_t decoded_bytes = 0;
  Hdf5QueryKernelConfig config;
  config.decoded_fragment_observer = [&](const std::string& key, std::uint64_t bytes) {
    std::lock_guard<std::mutex> lock(observed_mu);
    decoded_keys.insert(key); decoded_bytes += bytes;
    // Diagnostic exceptions must not alter the result or cache publication.
    throw std::runtime_error("ignored observer failure");
  };
  auto first = cursor(source, runtime, query(), config); ASSERT_TRUE(first.ok());
  auto values = consume(*first.value()); ASSERT_TRUE(values.ok()) << values.status().message();
  expect_values(values.value(), 0, 32);
  EXPECT_EQ(runtime->stats().decompressions, 8U);
  EXPECT_EQ(decoded_keys.size(), 8U);
  EXPECT_EQ(decoded_bytes, 128U);
  const auto detailed = runtime->stats();
  EXPECT_GT(detailed.decode_inflate_nanoseconds, 0U);
  EXPECT_EQ(detailed.decode_filter_nanoseconds, detailed.decode_inflate_nanoseconds);
  EXPECT_EQ(detailed.decode_shuffle_nanoseconds, 0U);
  EXPECT_EQ(detailed.payload_read_bytes, source->bytes.load());
  EXPECT_EQ(detailed.payload_read_ranges, source->ranges.load());
  // The runtime also counts the layout fragment; the native query contributes 128 bytes.
  EXPECT_GE(detailed.copy_bytes, 128U);
  EXPECT_GT(profile.nanoseconds["hdf5.assemble"], 0U);
  EXPECT_GT(profile.nanoseconds["hdf5.assemble.copy"], 0U);
  EXPECT_EQ(profile.counters["hdf5.output.rows"], 32U);
  EXPECT_EQ(profile.counters["hdf5.output.bytes"], 128U);
  EXPECT_EQ(profile.counters["hdf5.layout.cache.identity.calls"], 1U);
  EXPECT_EQ(profile.counters["hdf5.layout.identity.calls"], 0U);
  EXPECT_EQ(profile.counters["hdf5.selection.validation_calls"], 1U);
  EXPECT_EQ(profile.counters["hdf5.selection.decoder_compiles"], 1U);
  const auto calls = source->calls.load();
  auto second = cursor(source, runtime, query(1, 31, "/alias"), config); ASSERT_TRUE(second.ok());
  values = consume(*second.value()); ASSERT_TRUE(values.ok()); expect_values(values.value(), 1, 31);
  EXPECT_EQ(runtime->stats().decompressions, 8U); EXPECT_EQ(runtime->stats().decode_count, 8U);
  EXPECT_EQ(source->calls.load(), calls);
  EXPECT_EQ(decoded_keys.size(), 8U);
  EXPECT_EQ(decoded_bytes, 128U);
  EXPECT_EQ(runtime->stats().decode_inflate_nanoseconds, detailed.decode_inflate_nanoseconds);
  EXPECT_EQ(runtime->stats().copy_bytes - detailed.copy_bytes, 120U);
  EXPECT_EQ(profile.counters["hdf5.selection.validation_calls"], 2U);
  EXPECT_EQ(profile.counters["hdf5.selection.decoder_compiles"], 2U);
}

TEST(Hdf5SharedCache, ScientificFloatKernelsKeepMaskAndSeparateScaleOffset) {
  for (const auto type : {LogicalTypeId::Float32, LogicalTypeId::Float64}) {
    SCOPED_TRACE(static_cast<int>(type));
    auto source = std::make_shared<CacheSource>();
    const auto width = type == LogicalTypeId::Float32 ? 4U : 8U;
    auto dataset = contiguous_numeric(source, type, 4, width);
    const double one_plus = 1.0 + std::ldexp(1.0, -27);
    const double inputs[] = {one_plus, 0, -999, 2};
    for (std::size_t i = 0; i < 4; ++i) {
      if (type == LogicalTypeId::Float32) {
        const float value = static_cast<float>(inputs[i]);
        std::memcpy(source->payload.data() + 64 + i * width, &value, width);
      } else {
        std::memcpy(source->payload.data() + 64 + i * width, &inputs[i], width);
      }
    }
    Hdf5CfBinding cf;
    cf.scale_factor = type == LogicalTypeId::Float64 ?
      1.0 - std::ldexp(1.0, -27) : 0.5;
    cf.add_offset = type == LogicalTypeId::Float64 ? -1.0 : 10.0;
    cf.missing_values = {-999};
    auto opened = scientific_cursor(source, dataset, cf, std::make_shared<QueryRuntime>());
    ASSERT_TRUE(opened.ok()) << opened.status().message();
    auto next = opened.value()->next(); ASSERT_TRUE(next.ok()) << next.status().message();
    ASSERT_TRUE(next.value());
    const auto& values = std::get<std::vector<double>>(next.value()->columns()[0].values);
    const auto& mask = next.value()->columns()[0].validity;
    ASSERT_EQ(values.size(), 4U);
    EXPECT_EQ(mask[0] & 0x0fU, 0x0bU);
    EXPECT_DOUBLE_EQ(values[2], 0.0);
    if (type == LogicalTypeId::Float64) EXPECT_EQ(values[0], 0.0); // FMA would yield -2^-54.
    else EXPECT_DOUBLE_EQ(values[3], 11.0);
  }
}

TEST(Hdf5SharedCache, LargeInt16ScientificLookupMatchesScalarAndRespectsBudget) {
  constexpr std::uint64_t rows = 524288;
  auto source = std::make_shared<CacheSource>();
  auto dataset = contiguous_numeric(source, LogicalTypeId::Int32, rows, 2);
  const std::int16_t pattern[] = {-999, 0, 4, 20};
  for (std::uint64_t i = 0; i < rows; ++i)
    std::memcpy(source->payload.data() + 64 + i * 2, &pattern[i % 4], 2);
  Hdf5CfBinding cf; cf.scale_factor = .5; cf.add_offset = 10;
  cf.valid_min = 0; cf.valid_max = 10; cf.missing_values = {-999};
  QueryExecutionOptions options; options.target_batch_rows = 4096;
  QueryProfileRecord lookup_profile;
  std::vector<double> lookup_values, scalar_values;
  std::vector<std::uint8_t> lookup_validity, scalar_validity;
  auto run = [&](std::uint64_t buffer_bytes, QueryProfileRecord& profile,
                 std::vector<double>& values, std::vector<std::uint8_t>& valid) {
    ScopedQueryProfile scope(&profile);
    Hdf5QueryKernelConfig config; config.max_buffer_bytes = buffer_bytes;
    auto opened = scientific_cursor(source, dataset, cf,
      std::make_shared<QueryRuntime>(), options, config);
    ASSERT_TRUE(opened.ok()) << opened.status().message();
    for (;;) {
      auto next = opened.value()->next();
      ASSERT_TRUE(next.ok()) << next.status().message();
      if (!next.value()) break;
      const auto& column = next.value()->columns()[0];
      const auto& part = std::get<std::vector<double>>(column.values);
      values.insert(values.end(), part.begin(), part.end());
      for (std::uint64_t i = 0; i < part.size(); ++i)
        valid.push_back((column.validity[i / 8] >> (i % 8)) & 1U);
    }
  };
  run(8ULL * 1024 * 1024, lookup_profile, lookup_values, lookup_validity);
  QueryProfileRecord scalar_profile;
  run(512ULL * 1024, scalar_profile, scalar_values, scalar_validity);
  ASSERT_EQ(lookup_values.size(), rows);
  EXPECT_EQ(lookup_values, scalar_values);
  EXPECT_EQ(lookup_validity, scalar_validity);
  EXPECT_EQ(lookup_profile.counters["hdf5.science.int16_lookup.builds"], 1U);
  EXPECT_EQ(scalar_profile.counters["hdf5.science.int16_lookup.builds"], 0U);
  ::testing::Test::RecordProperty("lookup_build_ns",
    std::to_string(lookup_profile.nanoseconds["hdf5.science.int16_lookup.build"]));
  ::testing::Test::RecordProperty("lookup_conversion_ns",
    std::to_string(lookup_profile.nanoseconds["hdf5.assemble.scientific_conversion"]));
  ::testing::Test::RecordProperty("scalar_conversion_ns",
    std::to_string(scalar_profile.nanoseconds["hdf5.assemble.scientific_conversion"]));
  for (std::size_t i = 0; i < rows; ++i) {
    EXPECT_EQ(lookup_validity[i], i % 4 == 1 || i % 4 == 2);
    if (i % 4 == 1) EXPECT_DOUBLE_EQ(lookup_values[i], 10.0);
    if (i % 4 == 2) EXPECT_DOUBLE_EQ(lookup_values[i], 12.0);
  }
}

TEST(Hdf5SharedCache, Int16LookupReportsOnlyOverflowPresentInTheSelection) {
  constexpr std::uint64_t rows = 524288;
  Hdf5CfBinding cf; cf.scale_factor = std::numeric_limits<double>::max();
  QueryExecutionOptions options; options.target_batch_rows = rows;
  Hdf5QueryKernelConfig config; config.max_buffer_bytes = 8ULL * 1024 * 1024;
  for (bool overflow : {false, true}) {
    auto source = std::make_shared<CacheSource>();
    auto dataset = contiguous_numeric(source, LogicalTypeId::Int32, rows, 2);
    if (overflow) {
      const std::int16_t value = 3;
      std::memcpy(source->payload.data() + 64, &value, 2);
    }
    auto opened = scientific_cursor(source, dataset, cf,
      std::make_shared<QueryRuntime>(), options, config);
    ASSERT_TRUE(opened.ok()) << opened.status().message();
    auto next = opened.value()->next();
    if (overflow) {
      ASSERT_FALSE(next.ok());
      EXPECT_EQ(next.status().code(), QueryError::CorruptPayload);
    } else {
      ASSERT_TRUE(next.ok()) << next.status().message();
      ASSERT_TRUE(next.value());
      const auto& values = std::get<std::vector<double>>(next.value()->columns()[0].values);
      ASSERT_EQ(values.size(), rows);
      EXPECT_EQ(values[0], 0.0);
      EXPECT_EQ(values.back(), 0.0);
    }
  }
}

TEST(Hdf5SharedCache, ContiguousTileGranularityIsPartOfCacheIdentity) {
  auto source = std::make_shared<CacheSource>(); auto runtime = std::make_shared<QueryRuntime>();
  auto& dataset = *source->document.objects[1].dataset;
  dataset.storage = Hdf5StorageLayout::Contiguous; dataset.chunk_shape.clear();
  dataset.chunks.clear(); dataset.allocated_chunk_count = 0;
  dataset.contiguous_offset = 64; dataset.contiguous_size = 128; source->encode();
  for (const auto granularity : {4U, 8U}) {
    Hdf5QueryKernelConfig config; config.contiguous_tile_elements = granularity;
    auto result = cursor(source, runtime, query(0, 8), config); ASSERT_TRUE(result.ok());
    auto values = consume(*result.value()); ASSERT_TRUE(values.ok()) << values.status().message();
    expect_values(values.value(), 0, 8);
  }
  EXPECT_EQ(runtime->stats().decode_count, 3U);
}

TEST(Hdf5SharedCache, LargeRawChunkFitsDefaultBufferWithBoundedTransportSplits) {
  constexpr std::uint64_t chunk_bytes = 32ULL * 1024 * 1024;
  auto source = std::make_shared<CacheSource>(); auto runtime = std::make_shared<QueryRuntime>();
  auto& dataset = *source->document.objects[1].dataset;
  dataset.descriptor.shape = {chunk_bytes / sizeof(float)};
  dataset.chunk_shape = dataset.descriptor.shape;
  dataset.chunks = {{{0}, 64, chunk_bytes, 0}}; dataset.allocated_chunk_count = 1;
  // Keep the first four independently specified fixture values. The rest of
  // this allocated chunk is outside the requested rectangle but still decoded.
  source->payload.resize(64 + chunk_bytes);
  source->document.source_size = source->payload.size();
  source->manifest_.payload_size = source->payload.size(); source->encode();
  auto result = cursor(source, runtime, query(0, 4)); ASSERT_TRUE(result.ok()) << result.status().message();
  auto values = consume(*result.value()); ASSERT_TRUE(values.ok()) << values.status().message();
  expect_values(values.value(), 0, 4);
  EXPECT_EQ(source->calls.load(), 4U); EXPECT_EQ(source->bytes.load(), chunk_bytes);
  EXPECT_EQ(runtime->stats().decode_count, 1U);
  EXPECT_EQ(runtime->stats().decoded_bytes, chunk_bytes);
  EXPECT_LE(runtime->stats().peak_bytes, 64ULL * 1024 * 1024);
}

TEST(Hdf5SharedCache, TransportWorkingSetShrinksOutputSlabsBeforeReading) {
  auto source = std::make_shared<CacheSource>(); auto runtime = std::make_shared<QueryRuntime>();
  auto& dataset = *source->document.objects[1].dataset;
  dataset.descriptor.shape = {16}; dataset.chunk_shape = {16};
  dataset.chunks = {{{0}, 64, 64, 0}}; dataset.allocated_chunk_count = 1;
  source->capabilities.max_bytes = 16; source->encode();
  Hdf5QueryKernelConfig config; config.max_buffer_bytes = 100;
  auto result = cursor(source, runtime, query(0, 16), config);
  ASSERT_TRUE(result.ok()) << result.status().message();
  std::vector<float> values;
  for (;;) {
    auto next = result.value()->next(); ASSERT_TRUE(next.ok()) << next.status().message();
    if (!next.value()) break;
    // 64 bytes of encoded/decoded storage plus one 16-byte transport response
    // leave at most 20 bytes for the current projected Float32 output slab.
    EXPECT_LE(next.value()->row_count(), 5U);
    const auto& column = std::get<std::vector<float>>(next.value()->columns()[0].values);
    values.insert(values.end(), column.begin(), column.end());
  }
  expect_values(values, 0, 16);
  EXPECT_EQ(source->calls.load(), 4U); EXPECT_EQ(source->bytes.load(), 64U);
  EXPECT_EQ(runtime->stats().decode_count, 1U);
}
