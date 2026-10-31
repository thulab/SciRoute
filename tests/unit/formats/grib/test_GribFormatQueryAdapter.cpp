#include "iotdbfs/formats/grib/GribFormatQueryAdapter.h"
#include "iotdbfs/formats/grib/GribLayout.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "iotdbfs/query/core/QueryStorageReadControl.h"
#include "iotdbfs/query/core/QueryReadContext.h"

#include <gtest/gtest.h>

#include <eccodes.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace iotdbfs;

namespace {

struct GribFixtureData {
  StructuredBlobBytes payload;
  StructuredBlobBytes layout;
};

struct PayloadReadConcurrency {
  void enter() {
    const auto current = active.fetch_add(1, std::memory_order_acq_rel) + 1;
    auto observed = maximum.load(std::memory_order_relaxed);
    while (observed < current && !maximum.compare_exchange_weak(observed,
        current, std::memory_order_relaxed)) {}
  }

  void leave() { active.fetch_sub(1, std::memory_order_acq_rel); }

  std::atomic<std::size_t> active{0};
  std::atomic<std::size_t> maximum{0};
};

class MemoryQuerySource final : public QuerySource {
public:
  MemoryQuerySource(GribFixtureData data, std::int64_t version = 11,
      std::optional<std::string> format_id = std::string("grib"),
      std::chrono::milliseconds layout_read_delay = {},
      std::chrono::milliseconds payload_read_delay = {},
      std::shared_ptr<PayloadReadConcurrency> payload_concurrency =
        std::make_shared<PayloadReadConcurrency>())
    : payload_(std::move(data.payload)), layout_(std::move(data.layout)),
      layout_read_delay_(layout_read_delay),
      payload_read_delay_(payload_read_delay),
      payload_concurrency_(std::move(payload_concurrency)) {
    static std::atomic<std::uint64_t> next_source_id{0};
    const auto source_id = next_source_id.fetch_add(1,
      std::memory_order_relaxed);
    manifest_.key = {"/typed/grid-" + std::to_string(version) + "-" +
      std::to_string(source_id) + ".grib2", version};
    manifest_.format_id = std::move(format_id);
    manifest_.payload_size = payload_.size();
    manifest_.layout_size = layout_.size();
    manifest_.phase = StructuredBlobPhase::Published;
    manifest_.published = true;
  }

  const StructuredBlobManifest& manifest() const override {
    return manifest_;
  }

  QueryResult<StructuredBlobBytes> read_payload(std::uint64_t offset,
      std::uint64_t length) const override {
    if (emit_payload_fees_) {
      require_query_metadata("grib-grid/control/" + manifest_.key.path +
        "/" + std::to_string(manifest_.key.version) + "/" +
        std::to_string(storage_namespace()) + "/" +
        std::to_string(manifest_.revision), payload_metadata_fee_bytes_);
      require_query_payload_demand(length, 1);
    }
    payload_read_count_.fetch_add(1, std::memory_order_relaxed);
    std::shared_ptr<void> storage_reservation;
    if (transport_temporary_bytes_) {
      try {
        storage_reservation = reserve_storage_read_temporary(transport_temporary_bytes_);
        record_storage_read_io(transport_temporary_bytes_, 1);
        ++physical_block_reads_;
      } catch (const QueryStorageReadInterrupted& error) {
        return QueryResult<StructuredBlobBytes>::failure(error.status());
      }
    }
    auto failures = payload_failures_remaining_.load(
      std::memory_order_relaxed);
    while (failures > 0 && !payload_failures_remaining_.compare_exchange_weak(
        failures, failures - 1, std::memory_order_acq_rel)) {}
    if (failures > 0)
      return QueryResult<StructuredBlobBytes>::failure(QueryStatus::failure(
        QueryError::ReadFailure, QueryPhase::Storage,
        "injected payload read failure"));
    payload_concurrency_->enter();
    if (payload_read_delay_.count() > 0)
      std::this_thread::sleep_for(payload_read_delay_);
    auto result = read(payload_, offset, length);
    payload_concurrency_->leave();
    return result;
  }

  QueryResult<StructuredBlobBytes> read_layout(std::uint64_t offset,
      std::uint64_t length) const override {
    layout_read_count_.fetch_add(1, std::memory_order_relaxed);
    if (layout_read_delay_.count() > 0)
      std::this_thread::sleep_for(layout_read_delay_);
    if (fail_layout_)
      return QueryResult<StructuredBlobBytes>::failure(QueryStatus::failure(
        QueryError::ReadFailure, QueryPhase::Storage,
        "layout read failed for " + manifest_.key.path));
    return read(layout_, offset, length);
  }

  QueryResult<std::optional<std::string>> verified_layout_identity() const override {
    ++identity_checks_;
    if (identity_fee_bytes_)
      require_query_metadata("test-grib-identity/" + manifest_.key.path,
        identity_fee_bytes_);
    if (fail_identity_)
      return QueryResult<std::optional<std::string>>::failure(QueryStatus::failure(
        QueryError::StalePlan, QueryPhase::Planning, "source proof is no longer valid"));
    return QueryResult<std::optional<std::string>>::success(layout_identity_);
  }

  void set_layout_identity(std::string value) { layout_identity_ = std::move(value); }
  void fail_identity() { fail_identity_ = true; }
  void emit_identity_fees(std::uint64_t bytes = 16) { identity_fee_bytes_ = bytes; }
  void fail_layout() { fail_layout_ = true; }
  std::size_t identity_checks() const { return identity_checks_.load(); }

  std::size_t layout_read_count() const {
    return layout_read_count_.load(std::memory_order_relaxed);
  }

  std::size_t max_active_payload_reads() const {
    return payload_concurrency_->maximum.load(std::memory_order_relaxed);
  }

  std::size_t payload_read_count() const {
    return payload_read_count_.load(std::memory_order_relaxed);
  }

  void emit_payload_fees(std::uint64_t metadata_bytes = 16) {
    emit_payload_fees_ = true;
    payload_metadata_fee_bytes_ = metadata_bytes;
  }
  void require_transport_temporary(std::uint64_t bytes) { transport_temporary_bytes_ = bytes; }
  std::size_t physical_block_reads() const { return physical_block_reads_.load(); }
  std::size_t active_payload_reads() const { return payload_concurrency_->active.load(); }

  void fail_next_payload_read() {
    payload_failures_remaining_.fetch_add(1, std::memory_order_relaxed);
  }

  void set_identity(StructuredBlobVersionKey key, std::uint64_t ns,
      std::uint64_t revision = 0) {
    manifest_.key = std::move(key);
    namespace_override_ = ns;
    manifest_.revision = revision;
  }

  std::uint64_t storage_namespace() const override {
    return namespace_override_.value_or(QuerySource::storage_namespace());
  }

private:
  static QueryResult<StructuredBlobBytes> read(
      const StructuredBlobBytes& bytes, std::uint64_t offset,
      std::uint64_t length) {
    if (offset > bytes.size() || length > bytes.size() - offset)
      return QueryResult<StructuredBlobBytes>::failure(QueryStatus::failure(
        QueryError::ReadFailure, QueryPhase::Storage,
        "test source range is invalid"));
    const auto begin = bytes.begin() + static_cast<std::size_t>(offset);
    return QueryResult<StructuredBlobBytes>::success(
      {begin, begin + static_cast<std::size_t>(length)});
  }

  StructuredBlobManifest manifest_;
  StructuredBlobBytes payload_;
  StructuredBlobBytes layout_;
  std::chrono::milliseconds layout_read_delay_;
  std::chrono::milliseconds payload_read_delay_;
  std::shared_ptr<PayloadReadConcurrency> payload_concurrency_;
  mutable std::atomic<std::size_t> layout_read_count_{0};
  mutable std::atomic<std::size_t> payload_read_count_{0};
  mutable std::atomic<std::size_t> payload_failures_remaining_{0};
  std::optional<std::uint64_t> namespace_override_;
  std::optional<std::string> layout_identity_;
  std::uint64_t identity_fee_bytes_{0};
  bool fail_identity_{false};
  bool fail_layout_{false};
  bool emit_payload_fees_{false};
  std::uint64_t payload_metadata_fee_bytes_{16};
  std::uint64_t transport_temporary_bytes_{0};
  mutable std::atomic<std::size_t> physical_block_reads_{0};
  mutable std::atomic<std::size_t> identity_checks_{0};
};

// Request accounting runs where decode/render runs. Payload transport may use
// an independent shared producer, so its thread identity is not a decode probe.
class GridRequestProbe final : public QuerySource {
public:
  GridRequestProbe(std::shared_ptr<const QuerySource> producer,
      std::function<QueryStatus(std::uint64_t)> reserve)
    : producer_(std::move(producer)), reserve_(std::move(reserve)) {}
  const StructuredBlobManifest& manifest() const override {
    return producer_->manifest();
  }
  std::uint64_t storage_namespace() const override {
    return producer_->storage_namespace();
  }
  QueryStatus reserve_logical_payload(std::uint64_t bytes) const override {
    ++reserve_calls;
    return reserve_(bytes);
  }
  QueryResult<StructuredBlobBytes> read_payload(std::uint64_t offset,
      std::uint64_t length) const override {
    return producer_->read_payload(offset, length);
  }
  QueryResult<StructuredBlobBytes> read_layout(std::uint64_t offset,
      std::uint64_t length) const override {
    return producer_->read_layout(offset, length);
  }
  mutable std::atomic<std::size_t> reserve_calls{0};
private:
  std::shared_ptr<const QuerySource> producer_;
  std::function<QueryStatus(std::uint64_t)> reserve_;
};

struct EncodedMessage {
  StructuredBlobBytes bytes;
  std::string layout_record;
};

EncodedMessage regular_grid_message(std::uint64_t offset, long ni, long nj,
    const std::vector<double>& values,
    std::optional<long> param_id = std::nullopt,
    std::optional<long> data_date = std::nullopt,
    std::optional<long> data_time = std::nullopt) {
  std::unique_ptr<codes_handle, decltype(&codes_handle_delete)> handle(
    codes_grib_handle_new_from_samples(nullptr, "regular_ll_sfc_grib2"),
    codes_handle_delete);
  EXPECT_NE(handle, nullptr);
  if (!handle) return {};
  EXPECT_EQ(codes_set_long(handle.get(), "Ni", ni), CODES_SUCCESS);
  EXPECT_EQ(codes_set_long(handle.get(), "Nj", nj), CODES_SUCCESS);
  EXPECT_EQ(codes_set_double(handle.get(),
    "latitudeOfFirstGridPointInDegrees", nj - 1), CODES_SUCCESS);
  EXPECT_EQ(codes_set_double(handle.get(),
    "latitudeOfLastGridPointInDegrees", 0), CODES_SUCCESS);
  EXPECT_EQ(codes_set_double(handle.get(),
    "longitudeOfFirstGridPointInDegrees", 0), CODES_SUCCESS);
  EXPECT_EQ(codes_set_double(handle.get(),
    "longitudeOfLastGridPointInDegrees", ni - 1), CODES_SUCCESS);
  EXPECT_EQ(codes_set_double(handle.get(),
    "iDirectionIncrementInDegrees", 1), CODES_SUCCESS);
  EXPECT_EQ(codes_set_double(handle.get(),
    "jDirectionIncrementInDegrees", 1), CODES_SUCCESS);
  EXPECT_EQ(codes_set_double_array(handle.get(), "values", values.data(),
    values.size()), CODES_SUCCESS);
  if (param_id)
    EXPECT_EQ(codes_set_long(handle.get(), "paramId", *param_id), CODES_SUCCESS);
  if (data_date)
    EXPECT_EQ(codes_set_long(handle.get(), "dataDate", *data_date), CODES_SUCCESS);
  if (data_time)
    EXPECT_EQ(codes_set_long(handle.get(), "dataTime", *data_time), CODES_SUCCESS);

  const void* message = nullptr;
  std::size_t size = 0;
  EXPECT_EQ(codes_get_message(handle.get(), &message, &size), CODES_SUCCESS);
  const auto* begin = static_cast<const std::uint8_t*>(message);
  EncodedMessage encoded;
  encoded.bytes.assign(begin, begin + size);
  encoded.layout_record = grib_utils::build_index_line(handle.get(), offset,
    size, 2);
  return encoded;
}

GribFixtureData one_message(long ni = 2, long nj = 2,
    std::vector<double> values = {1, 3, 5, 7}) {
  auto message = regular_grid_message(0, ni, nj, values);
  const auto layout = grib_layout_header() + message.layout_record;
  return {std::move(message.bytes), {layout.begin(), layout.end()}};
}

GribFixtureData two_messages() {
  auto first = regular_grid_message(0, 2, 2, {1, 3, 5, 7});
  auto second = regular_grid_message(first.bytes.size(), 2, 2,
    {2, 4, 6, 8});
  StructuredBlobBytes payload = first.bytes;
  payload.insert(payload.end(), second.bytes.begin(), second.bytes.end());
  const auto layout = grib_layout_header() + first.layout_record +
    second.layout_record;
  return {std::move(payload), {layout.begin(), layout.end()}};
}

GribFixtureData typed_index_message() {
  const auto layout = grib_layout_header() +
    R"({"sn":"clwmr","tl":"isobaricInhPa","lv":50,"st":3,"sr":"3","u":"kg kg-1","of":0,"ln":1,"ed":2})" +
    "\n";
  return {{0}, {layout.begin(), layout.end()}};
}

QueryResult<std::vector<QueryBatch>> execute(GribFormatQueryAdapter& adapter,
    std::shared_ptr<const QuerySource> source, const QueryFragment& fragment,
    std::size_t target_batch_rows = 65536) {
  auto prepared = adapter.prepare(QueryPlanningContext(*source), fragment);
  if (!prepared.ok())
    return QueryResult<std::vector<QueryBatch>>::failure(prepared.status());
  QueryExecutionOptions options;
  options.target_batch_rows = target_batch_rows;
  auto cursor = adapter.execute(QueryExecutionContext(source), prepared.value().implementation(),
    options);
  if (!cursor.ok())
    return QueryResult<std::vector<QueryBatch>>::failure(cursor.status());
  std::vector<QueryBatch> batches;
  for (;;) {
    auto next = cursor.value()->next();
    if (!next.ok())
      return QueryResult<std::vector<QueryBatch>>::failure(next.status());
    if (!next.value()) break;
    batches.push_back(std::move(*next.value()));
  }
  return QueryResult<std::vector<QueryBatch>>::success(std::move(batches));
}

DomainSlice closed_slice(const char* field, double lower, double upper) {
  return {{field},
    DomainBound{LogicalValue::float64(lower), BoundKind::Inclusive},
    DomainBound{LogicalValue::float64(upper), BoundKind::Inclusive}};
}

GribFixtureData domain_index_records(const std::vector<std::int64_t>& dates) {
  std::string layout = grib_layout_header();
  for (std::size_t i = 0; i < dates.size(); ++i)
    layout += R"({"sn":"t","tl":"isobaricInhPa","lv":500,"st":3,"sr":"3","u":"K","of":)" +
      std::to_string(i) + R"(,"ln":1,"ed":2,"pi":130,"vd":)" +
      std::to_string(dates[i]) + R"(,"vt":600})" + "\n";
  return {StructuredBlobBytes(dates.size(), 0), {layout.begin(), layout.end()}};
}

std::vector<std::size_t> source_ordinals(const AdapterDomainSelection& selection) {
  std::vector<std::size_t> result;
  for (const auto& match : selection) result.push_back(match.source_ordinal);
  return result;
}

QueryFragment domain_index_fragment(std::int64_t date = 20180101) {
  using namespace grib_query_fields;
  QueryFragment fragment;
  fragment.projection = {{kOffset}, {kShortName}, {kParamId}};
  fragment.predicate = make_logical(LogicalOperator::And, {
    make_comparison({kShortName}, ComparisonOperator::Equal, LogicalValue::utf8("t")),
    make_comparison({kTypeOfLevel}, ComparisonOperator::Equal, LogicalValue::utf8("isobaricInhPa")),
    make_comparison({kLevel}, ComparisonOperator::Equal, LogicalValue::int64(500)),
    make_comparison({kStep}, ComparisonOperator::Equal, LogicalValue::int64(3)),
    make_comparison({kValidityDate}, ComparisonOperator::Equal, LogicalValue::int64(date)),
    make_comparison({kValidityTime}, ComparisonOperator::Equal, LogicalValue::int64(600))});
  return fragment;
}

} // namespace

TEST(GribFormatQueryAdapter, DomainIndexRetainsOnlyParsingAndPreservesSourceOrder) {
  GribFormatQueryAdapter adapter;
  const auto fragment = domain_index_fragment();
  auto first = std::make_shared<MemoryQuerySource>(domain_index_records({20180102, 20180102}));
  auto second = std::make_shared<MemoryQuerySource>(domain_index_records({20180101}));
  auto third = std::make_shared<MemoryQuerySource>(domain_index_records({20180102}));
  std::vector<std::shared_ptr<const QuerySource>> sources{first, second, third};
  for (const auto& source : sources)
    ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), fragment).ok());
  auto built = adapter.build_domain_index(sources, fragment, {}, 64ULL * 1024 * 1024);
  ASSERT_TRUE(built.ok());
  ASSERT_NE(built.value(), nullptr);
  EXPECT_GT(built.value()->retained_bytes(), 0u);
  const auto reads = first->layout_read_count();
  auto initial = built.value()->select(fragment, {});
  ASSERT_TRUE(initial.ok()); ASSERT_TRUE(initial.value());
  EXPECT_EQ(source_ordinals(*initial.value()), (std::vector<std::size_t>{1}));
  auto changed = built.value()->select(domain_index_fragment(20180102), {});
  ASSERT_TRUE(changed.ok()); ASSERT_TRUE(changed.value());
  EXPECT_EQ(source_ordinals(*changed.value()), (std::vector<std::size_t>{0, 2}));
  EXPECT_EQ(first->layout_read_count(), reads);
  auto none = built.value()->select(domain_index_fragment(20300101), {});
  ASSERT_TRUE(none.ok()); ASSERT_TRUE(none.value()); EXPECT_TRUE(none.value()->empty());
  std::weak_ptr<MemoryQuerySource> weak = first;
  first.reset(); second.reset(); third.reset(); sources.clear();
  EXPECT_TRUE(weak.expired());
  changed = built.value()->select(domain_index_fragment(20180102), {});
  ASSERT_TRUE(changed.ok()); ASSERT_TRUE(changed.value());
  EXPECT_EQ(source_ordinals(*changed.value()), (std::vector<std::size_t>{0, 2}));
}

TEST(GribFormatQueryAdapter, DomainIndexDeclinesUncertifiedFragmentForms) {
  GribFormatQueryAdapter adapter;
  const auto original = domain_index_fragment();
  ASSERT_TRUE(adapter.supports_domain_index(original));
  std::vector<QueryFragment> unsupported;
  auto fragment = original; fragment.projection = {{grib_query_fields::kValue}};
  unsupported.push_back(fragment);
  fragment = original; fragment.output.mode = QueryFragment::ResultMode::BinaryView;
  fragment.projection = {{grib_query_fields::kMessageBytes}}; unsupported.push_back(fragment);
  fragment = original; fragment.slices.push_back(closed_slice(grib_query_fields::kLatitude, 0, 1));
  unsupported.push_back(fragment);
  fragment = original; fragment.output.interpolation.emplace(); unsupported.push_back(fragment);
  fragment = original; fragment.physical_array.emplace(); unsupported.push_back(fragment);
  fragment = original; fragment.output.type_overrides.push_back({{grib_query_fields::kParamId}, LogicalTypeId::Float64});
  unsupported.push_back(fragment);
  fragment = original; fragment.projection.clear(); unsupported.push_back(fragment);
  fragment = original; fragment.projection.push_back(fragment.projection.front()); unsupported.push_back(fragment);
  fragment = original; fragment.predicate = make_comparison({grib_query_fields::kLevel},
    ComparisonOperator::Equal, LogicalValue::int64(500)); unsupported.push_back(fragment);
  fragment = original; fragment.predicate = make_logical(LogicalOperator::Or,
    {original.predicate, original.predicate}); unsupported.push_back(fragment);
  fragment = original; fragment.predicate = make_logical(LogicalOperator::And,
    {original.predicate, make_comparison({grib_query_fields::kParamId},
      ComparisonOperator::Equal, LogicalValue::int64(130))}); unsupported.push_back(fragment);
  fragment = original; fragment.predicate = make_logical(LogicalOperator::And,
    {original.predicate, make_comparison({grib_query_fields::kLevel},
      ComparisonOperator::Equal, LogicalValue::null(LogicalTypeId::Int64))}); unsupported.push_back(fragment);
  auto source = std::make_shared<MemoryQuerySource>(domain_index_records({20180101}));
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), original).ok());
  auto built = adapter.build_domain_index({source}, original, {}, 64ULL * 1024 * 1024);
  ASSERT_TRUE(built.ok()); ASSERT_NE(built.value(), nullptr);
  for (const auto& rejected : unsupported) {
    EXPECT_FALSE(adapter.supports_domain_index(rejected));
    auto selection = built.value()->select(rejected, {});
    ASSERT_TRUE(selection.ok()); EXPECT_FALSE(selection.value());
  }
}

TEST(GribFormatQueryAdapter, DomainIndexDeclinesWholeDomainWithMissingTuple) {
  GribFormatQueryAdapter adapter;
  const auto fragment = domain_index_fragment();
  auto data = domain_index_records({20180102});
  std::string layout(data.layout.begin(), data.layout.end());
  const auto level = layout.find(R"("lv":500,)");
  ASSERT_NE(level, std::string::npos); layout.erase(level, 9);
  data.layout.assign(layout.begin(), layout.end());
  auto source = std::make_shared<MemoryQuerySource>(std::move(data));
  // A missing optional level is Unknown, hence an ordinary empty result. It
  // still cannot certify a complete tuple for all possible cached lookups.
  auto prepared = adapter.prepare(QueryPlanningContext(*source), fragment);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  auto built = adapter.build_domain_index({source}, fragment, {}, 64ULL * 1024 * 1024);
  ASSERT_TRUE(built.ok()); EXPECT_EQ(built.value(), nullptr);
}

TEST(GribFormatQueryAdapter, DomainIndexNeverHidesNonmatchingRecordErrors) {
  GribFormatQueryAdapter adapter;
  const auto fragment = domain_index_fragment(20300101);
  auto data = domain_index_records({20180102});
  std::string layout(data.layout.begin(), data.layout.end());
  const auto validity = layout.find(R"(,"vd":20180102)");
  ASSERT_NE(validity, std::string::npos); layout.erase(validity, 14);
  data.layout.assign(layout.begin(), layout.end());
  auto source = std::make_shared<MemoryQuerySource>(std::move(data));
  auto prepared = adapter.prepare(QueryPlanningContext(*source), fragment);
  ASSERT_FALSE(prepared.ok()); EXPECT_EQ(prepared.status().code(), QueryError::CorruptLayout);
  auto built = adapter.build_domain_index({source}, fragment, {}, 64ULL * 1024 * 1024);
  ASSERT_TRUE(built.ok()); EXPECT_EQ(built.value(), nullptr);
}

TEST(GribFormatQueryAdapter, DomainIndexDeclinesChangedLimitsAndInsufficientRetentionBudget) {
  GribFormatQueryAdapter adapter;
  const auto fragment = domain_index_fragment();
  auto source = std::make_shared<MemoryQuerySource>(domain_index_records({20180101}));
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), fragment).ok());
  QueryLimits limits;
  auto built = adapter.build_domain_index({source}, fragment, limits, 64ULL * 1024 * 1024);
  ASSERT_TRUE(built.ok()); ASSERT_NE(built.value(), nullptr);
  auto different = limits; --different.max_ranges;
  auto result = built.value()->select(fragment, different);
  ASSERT_TRUE(result.ok()); EXPECT_FALSE(result.value());
  different = limits; different.max_layout_bytes = source->manifest().layout_size - 1;
  result = built.value()->select(fragment, different);
  ASSERT_TRUE(result.ok()); EXPECT_FALSE(result.value());
  different = limits; different.max_layout_bytes = 1;
  auto small = adapter.build_domain_index({source}, fragment, different, 64ULL * 1024 * 1024);
  ASSERT_TRUE(small.ok()); EXPECT_EQ(small.value(), nullptr);
  auto no_reservation = adapter.build_domain_index({source}, fragment, limits, 1);
  ASSERT_TRUE(no_reservation.ok()); EXPECT_EQ(no_reservation.value(), nullptr);
  auto below_retained = adapter.build_domain_index({source}, fragment, limits,
    built.value()->retained_bytes() - 1);
  ASSERT_TRUE(below_retained.ok()); EXPECT_EQ(below_retained.value(), nullptr);
  auto enough_reserved = adapter.build_domain_index({source}, fragment, limits,
    built.value()->retained_bytes());
  ASSERT_TRUE(enough_reserved.ok()); ASSERT_NE(enough_reserved.value(), nullptr);
  result = enough_reserved.value()->select(fragment, limits);
  ASSERT_TRUE(result.ok()); ASSERT_TRUE(result.value());
  EXPECT_EQ(source_ordinals(*result.value()), (std::vector<std::size_t>{0}));
  auto empty = adapter.build_domain_index({}, fragment, limits, 64ULL * 1024 * 1024);
  ASSERT_TRUE(empty.ok()); EXPECT_EQ(empty.value(), nullptr);
}

TEST(GribFormatQueryAdapter, DomainIndexOptionalBuildNeverReloadsEvictedOrDisabledParsing) {
  const auto fragment = domain_index_fragment();
  for (const auto versions : {std::size_t{1}, std::size_t{0}}) {
    GribParsedIndexCacheConfig config;
    config.max_versions = versions;
    GribFormatQueryAdapter adapter(config);
    auto first = std::make_shared<MemoryQuerySource>(domain_index_records({20180101}));
    auto second = std::make_shared<MemoryQuerySource>(domain_index_records({20180101}));
    ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*first), fragment).ok());
    ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*second), fragment).ok());
    const auto first_reads = first->layout_read_count();
    const auto second_reads = second->layout_read_count();
    auto built = adapter.build_domain_index({first, second}, fragment, {}, 64ULL * 1024 * 1024);
    ASSERT_TRUE(built.ok()); EXPECT_EQ(built.value(), nullptr);
    EXPECT_EQ(first->layout_read_count(), first_reads);
    EXPECT_EQ(second->layout_read_count(), second_reads);
  }
}

TEST(GribFormatQueryAdapter, DomainMessageHintReusesExactPostingsWithoutParsedLookupOrScan) {
  GribFormatQueryAdapter adapter;
  auto source = std::make_shared<MemoryQuerySource>(
    domain_index_records({20180101, 20180102, 20180101, 20180102, 20180101}));
  source->set_layout_identity("layout-proof-a");
  auto fragment = domain_index_fragment();
  fragment.projection = {{grib_query_fields::kOffset}};
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), fragment).ok());
  auto built = adapter.build_domain_index({source}, fragment, {}, 64ULL * 1024 * 1024);
  ASSERT_TRUE(built.ok()); ASSERT_NE(built.value(), nullptr);
  // Destroy the original query AST: the hint must own its small key.
  auto selected = built.value()->select(domain_index_fragment(), {});
  ASSERT_TRUE(selected.ok()); ASSERT_TRUE(selected.value());
  ASSERT_EQ(selected.value()->size(), 1u);
  QueryPlanningContext context(*source);
  context.set_state(selected.value()->front().planning_state);
  const auto identity_checks = source->identity_checks();
  const auto layout_reads = source->layout_read_count();
  QueryProfileRecord profile;
  ScopedQueryProfile scope(&profile);
  auto plan = adapter.prepare(context, fragment);
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  EXPECT_EQ(profile.counters["grib.domain.message_hint_hits"], 1u);
  EXPECT_EQ(profile.counters["grib.domain.message_hint_units"], 3u);
  EXPECT_EQ(profile.counters["grib.message.predicate_evaluations"], 0u);
  EXPECT_EQ(profile.counters["grib.index.cache.hits"], 0u);
  EXPECT_EQ(profile.counters["grib.index.cache.misses"], 0u);
  EXPECT_EQ(source->identity_checks(), identity_checks + 1);
  EXPECT_EQ(source->layout_read_count(), layout_reads);
  std::vector<std::uint64_t> offsets;
  auto status = plan.value().implementation()->visit_unit_locators(
    [&](const FormatUnitLocator& locator) {
      offsets.push_back(locator.offset);
      EXPECT_EQ(locator.length, 1u);
      EXPECT_EQ(locator.unit_id, "message:" + std::to_string(locator.offset) + ":1");
      return QueryStatus::success();
    });
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(offsets, (std::vector<std::uint64_t>{0, 2, 4}));
  EXPECT_EQ(source->payload_read_count(), 0u);
  auto stop = plan.value().implementation()->visit_unit_locators(
    [](const FormatUnitLocator&) {
      return QueryStatus::failure(QueryError::Cancelled, QueryPhase::Execution,
        "visitor cancelled");
    });
  EXPECT_EQ(stop.code(), QueryError::Cancelled);
  EXPECT_EQ(plan.value().implementation()->visit_unit_locators({}).code(),
    QueryError::InvalidQuery);
}

TEST(GribFormatQueryAdapter, DomainMessageHintDeclinesChangedSourcePredicateLimitsOrLayout) {
  GribFormatQueryAdapter adapter;
  auto data = domain_index_records({20180101, 20180102, 20180101});
  auto source = std::make_shared<MemoryQuerySource>(data);
  source->set_layout_identity("layout-a");
  const auto fragment = domain_index_fragment();
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), fragment).ok());
  auto built = adapter.build_domain_index({source}, fragment, {}, 64ULL * 1024 * 1024);
  ASSERT_TRUE(built.ok()); ASSERT_NE(built.value(), nullptr);
  auto selected = built.value()->select(fragment, {});
  ASSERT_TRUE(selected.ok()); ASSERT_TRUE(selected.value());
  auto hint = selected.value()->front().planning_state;
  const auto check_fallback = [&](const MemoryQuerySource& target,
      const QueryFragment& changed, QueryLimits limits) {
    QueryPlanningContext context(target, limits);
    context.set_state(hint);
    QueryProfileRecord profile;
    ScopedQueryProfile scope(&profile);
    auto plan = adapter.prepare(context, changed);
    EXPECT_TRUE(plan.ok()) << plan.status().message();
    EXPECT_EQ(profile.counters["grib.domain.message_hint_hits"], 0u);
    EXPECT_EQ(profile.counters["grib.message.predicate_evaluations"], 3u);
    return plan;
  };
  auto different = std::make_shared<MemoryQuerySource>(data);
  different->set_layout_identity("layout-a");
  check_fallback(*different, fragment, {});
  // Same namespace/path/version and content proof, different revision.
  different->set_identity(source->manifest().key, source->storage_namespace(), 1);
  check_fallback(*different, fragment, {});
  check_fallback(*source, domain_index_fragment(20180102), {});
  auto limits = QueryLimits{}; --limits.max_ranges;
  check_fallback(*source, fragment, limits);
  const auto reads = source->layout_read_count();
  source->set_layout_identity("layout-b");
  check_fallback(*source, fragment, {});
  EXPECT_EQ(source->layout_read_count(), reads + 1);
}

TEST(GribFormatQueryAdapter, DomainMessageHintPreservesIdentityFailuresBudgetAndProjectionValidation) {
  GribFormatQueryAdapter adapter;
  auto source = std::make_shared<MemoryQuerySource>(domain_index_records({20180101}));
  source->emit_identity_fees();
  const auto fragment = domain_index_fragment();
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), fragment).ok());
  auto built = adapter.build_domain_index({source}, fragment, {}, 64ULL * 1024 * 1024);
  ASSERT_TRUE(built.ok()); ASSERT_NE(built.value(), nullptr);
  auto selected = built.value()->select(fragment, {});
  ASSERT_TRUE(selected.ok()); ASSERT_TRUE(selected.value());
  QueryPlanningContext context(*source);
  context.set_state(selected.value()->front().planning_state);
  std::uint64_t ordinary_bytes = 0;
  {
    ScopedQueryMetadataBudget budget;
    ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), fragment).ok());
    ordinary_bytes = budget.budget()->consumed_bytes();
  }
  {
    ScopedQueryMetadataBudget budget;
    ASSERT_TRUE(adapter.prepare(context, fragment).ok());
    EXPECT_EQ(budget.budget()->consumed_bytes(), ordinary_bytes);
  }
  {
    ScopedQueryMetadataBudget budget;
    budget.budget()->observe_unique(QueryMetadataBudget::maximum_bytes - 1);
    EXPECT_THROW(adapter.prepare(context, fragment), QueryMetadataBudgetExceeded);
    EXPECT_FALSE(budget.budget()->status().ok());
  }
  auto invalid = fragment;
  invalid.projection.push_back({"grib.message.unknown"});
  auto rejected = adapter.prepare(context, invalid);
  ASSERT_FALSE(rejected.ok()); EXPECT_EQ(rejected.status().code(), QueryError::UnknownField);
  source->fail_identity();
  rejected = adapter.prepare(context, fragment);
  ASSERT_FALSE(rejected.ok()); EXPECT_EQ(rejected.status().code(), QueryError::StalePlan);
  ASSERT_TRUE(rejected.status().detail().source);
  EXPECT_EQ(rejected.status().detail().source->path, source->manifest().key.path);
}

TEST(GribFormatQueryAdapter, UnitLocatorVisitorAcceptsOnlyOffsetMessageRowsAndSupportsEmpty) {
  GribFormatQueryAdapter adapter;
  auto source = std::make_shared<MemoryQuerySource>(domain_index_records({20180101}));
  auto fragment = domain_index_fragment(20300101);
  fragment.projection = {{grib_query_fields::kOffset}};
  EXPECT_TRUE(adapter.supports_unit_catalog(fragment));
  auto plan = adapter.prepare(QueryPlanningContext(*source), fragment);
  ASSERT_TRUE(plan.ok());
  std::size_t count = 0;
  auto visitor = [&](const FormatUnitLocator&) { ++count; return QueryStatus::success(); };
  EXPECT_TRUE(plan.value().implementation()->visit_unit_locators(visitor).ok());
  EXPECT_EQ(count, 0u);
  for (const auto& projection : std::vector<decltype(fragment.projection)>{
      {{grib_query_fields::kOffset}, {grib_query_fields::kShortName}},
      {{grib_query_fields::kValue}}}) {
    fragment.projection = projection;
    EXPECT_FALSE(adapter.supports_unit_catalog(fragment));
    plan = adapter.prepare(QueryPlanningContext(*source), fragment);
    ASSERT_TRUE(plan.ok()) << plan.status().message();
    EXPECT_EQ(plan.value().implementation()->visit_unit_locators(visitor).code(),
      QueryError::UnsupportedCapability);
  }
  fragment.projection = {{grib_query_fields::kMessageBytes}};
  fragment.output.mode = QueryFragment::ResultMode::BinaryView;
  EXPECT_FALSE(adapter.supports_unit_catalog(fragment));
  plan = adapter.prepare(QueryPlanningContext(*source), fragment);
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  EXPECT_EQ(plan.value().implementation()->visit_unit_locators(visitor).code(),
    QueryError::UnsupportedCapability);
  EXPECT_EQ(source->payload_read_count(), 0u);
}

TEST(GribFormatQueryAdapter, SelectsEveryMatchingMessageAsBinaryView) {
  const auto data = two_messages();
  const auto expected_payload = data.payload;
  auto source_owner = std::make_shared<MemoryQuerySource>(data);
  auto& source = *source_owner;
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kMessageBytes}};
  fragment.predicate = make_comparison({grib_query_fields::kEdition},
    ComparisonOperator::Equal, LogicalValue::int32(2));
  fragment.output.mode = QueryFragment::ResultMode::BinaryView;

  auto result = execute(adapter, source_owner, fragment);
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().size(), 2u);
  std::size_t offset = 0;
  for (const auto& batch : result.value()) {
    ASSERT_EQ(batch.row_count(), 1u);
    ASSERT_EQ(batch.columns().size(), 1u);
    const auto& message = std::get<std::vector<BinaryValue>>(
      batch.columns()[0].values)[0];
    EXPECT_EQ(message,
      BinaryValue(expected_payload.begin() + offset,
        expected_payload.begin() + offset + message.size()));
    offset += message.size();
    ASSERT_EQ(batch.sources().size(), 1u);
    EXPECT_EQ(batch.sources()[0].version, 11);
  }
  EXPECT_EQ(offset, expected_payload.size());
}

namespace {
QueryFragment binary_fragment() {
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kMessageBytes}};
  fragment.output.mode = QueryFragment::ResultMode::BinaryView;
  return fragment;
}

QueryResult<std::unique_ptr<QueryBatchCursor>> binary_cursor(
    GribFormatQueryAdapter& adapter, std::shared_ptr<const QuerySource> source,
    QueryExecutionOptions options = {}) {
  auto prepared = adapter.prepare(QueryPlanningContext(*source), binary_fragment());
  if (!prepared.ok())
    return QueryResult<std::unique_ptr<QueryBatchCursor>>::failure(prepared.status());
  return adapter.execute(QueryExecutionContext(source),
    prepared.value().implementation(), options);
}
}

TEST(GribFormatQueryAdapter, BinaryCacheReusesBytesAndReturnsOwnedOutputs) {
  auto source = std::make_shared<MemoryQuerySource>(one_message());
  auto runtime = std::make_shared<QueryRuntime>();
  GribFormatQueryAdapter adapter({}, {}, runtime);
  auto first = execute(adapter, source, binary_fragment());
  ASSERT_TRUE(first.ok()) << first.status().message();
  auto expected = std::get<std::vector<BinaryValue>>(first.value()[0].columns()[0].values)[0];
  // Destroying/mutating a returned batch cannot change the cached message.
  first.value().clear();
  for (int i = 0; i < 4; ++i) {
    auto result = execute(adapter, source, binary_fragment());
    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(std::get<std::vector<BinaryValue>>(result.value()[0].columns()[0].values)[0], expected);
  }
  EXPECT_EQ(source->payload_read_count(), 1u);
  auto stats = runtime->stats();
  EXPECT_EQ(stats.misses, 1u);
  EXPECT_EQ(stats.hits, 4u);
  EXPECT_EQ(stats.read_ranges, 1u);
  EXPECT_EQ(stats.payload_read_calls, 1u);
  EXPECT_EQ(stats.read_bytes, expected.size());
  EXPECT_EQ(stats.copy_bytes, expected.size() * 6);
  EXPECT_EQ(stats.decode_count, 0u);
}

TEST(GribFormatQueryAdapter, BinaryCacheIsolatesNamespaceVersionAndRevision) {
  auto runtime = std::make_shared<QueryRuntime>();
  const auto initial = one_message();
  std::vector<std::shared_ptr<MemoryQuerySource>> sources;
  for (int i = 0; i < 4; ++i) {
    auto source = std::make_shared<MemoryQuerySource>(initial);
    source->set_identity({"/same.grib2", i == 2 ? 12 : 11},
      i == 1 ? 99 : 98, i == 3 ? 1 : 0);
    sources.push_back(source);
    // Separate adapters avoid involving the legacy path/version index cache.
    GribFormatQueryAdapter adapter({}, {}, runtime);
    ASSERT_TRUE(execute(adapter, source, binary_fragment()).ok());
  }
  GribFormatQueryAdapter another({}, {}, runtime);
  ASSERT_TRUE(execute(another, sources.front(), binary_fragment()).ok());
  for (const auto& source : sources) EXPECT_EQ(source->payload_read_count(), 1u);
  EXPECT_EQ(runtime->stats().misses, 4u);
  EXPECT_EQ(runtime->stats().hits, 1u);
}

TEST(GribFormatQueryAdapter, BinarySameAdapterNamespacesUseTheirOwnMessageDirectory) {
  auto a = std::make_shared<MemoryQuerySource>(one_message());
  auto b = std::make_shared<MemoryQuerySource>(two_messages());
  a->set_identity({"/same.grib2", 11}, 90);
  b->set_identity({"/same.grib2", 11}, 91);
  GribFormatQueryAdapter adapter;
  auto one = execute(adapter, a, binary_fragment());
  ASSERT_TRUE(one.ok());
  ASSERT_EQ(one.value().size(), 1u);
  auto two = execute(adapter, b, binary_fragment());
  ASSERT_TRUE(two.ok()) << two.status().message();
  ASSERT_EQ(two.value().size(), 2u);
  EXPECT_EQ(a->payload_read_count(), 1u);
  EXPECT_EQ(b->payload_read_count(), 2u);
  auto cached = execute(adapter, b, binary_fragment());
  ASSERT_TRUE(cached.ok());
  EXPECT_EQ(cached.value().size(), 2u);
  EXPECT_EQ(b->payload_read_count(), 2u);
}

TEST(GribFormatQueryAdapter, BinaryCacheEvictionReloadsUnderByteBudget) {
  const auto data = one_message();
  QueryRuntimeConfig config;
  config.memory_budget_bytes = data.payload.size() * 2 + 128;
  auto runtime = std::make_shared<QueryRuntime>(config);
  GribFormatQueryAdapter adapter({}, {}, runtime);
  auto a = std::make_shared<MemoryQuerySource>(data);
  auto b = std::make_shared<MemoryQuerySource>(data);
  ASSERT_TRUE(execute(adapter, a, binary_fragment()).ok());
  ASSERT_TRUE(execute(adapter, b, binary_fragment()).ok());
  ASSERT_TRUE(execute(adapter, a, binary_fragment()).ok());
  EXPECT_EQ(a->payload_read_count(), 2u);
  EXPECT_EQ(b->payload_read_count(), 1u);
  EXPECT_GE(runtime->stats().evictions, 2u);
  EXPECT_LE(runtime->stats().peak_bytes, config.memory_budget_bytes);
}

TEST(GribFormatQueryAdapter, BinaryCacheHitStillEnforcesLogicalBytesAndRanges) {
  auto source = std::make_shared<MemoryQuerySource>(two_messages());
  GribFormatQueryAdapter adapter;
  ASSERT_TRUE(execute(adapter, source, binary_fragment()).ok());
  EXPECT_EQ(source->payload_read_count(), 2u);
  QueryExecutionOptions options;
  options.max_payload_bytes = 1;
  auto denied = binary_cursor(adapter, source, options);
  ASSERT_TRUE(denied.ok());
  auto next = denied.value()->next();
  EXPECT_EQ(next.status().code(), QueryError::ResourceExhausted);
  options.max_payload_bytes = source->manifest().payload_size;
  options.max_ranges = 1;
  auto bounded = binary_cursor(adapter, source, options);
  ASSERT_TRUE(bounded.ok());
  ASSERT_TRUE(bounded.value()->next().ok());
  next = bounded.value()->next();
  EXPECT_EQ(next.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(source->payload_read_count(), 2u);
}

TEST(GribFormatQueryAdapter, BinaryCacheFailedReadIsNotRetained) {
  auto source = std::make_shared<MemoryQuerySource>(one_message());
  GribFormatQueryAdapter adapter;
  source->fail_next_payload_read();
  auto failed = execute(adapter, source, binary_fragment());
  ASSERT_FALSE(failed.ok());
  EXPECT_EQ(failed.status().code(), QueryError::ReadFailure);
  ASSERT_TRUE(execute(adapter, source, binary_fragment()).ok());
  ASSERT_TRUE(execute(adapter, source, binary_fragment()).ok());
  EXPECT_EQ(source->payload_read_count(), 2u);
}

TEST(GribFormatQueryAdapter, BinaryCacheUsesIndependentOwnedProducerSource) {
  const auto data = one_message();
  auto request = std::make_shared<MemoryQuerySource>(data);
  auto producer = std::make_shared<MemoryQuerySource>(data);
  request->set_identity({"/shared.grib2", 11}, 123);
  producer->set_identity({"/shared.grib2", 11}, 123);
  GribFormatQueryAdapter adapter;
  auto plan = adapter.prepare(QueryPlanningContext(*request), binary_fragment());
  ASSERT_TRUE(plan.ok());
  auto cursor = adapter.execute(QueryExecutionContext(request, producer),
    plan.value().implementation(), {});
  ASSERT_TRUE(cursor.ok());
  ASSERT_TRUE(cursor.value()->next().ok());
  EXPECT_EQ(request->payload_read_count(), 0u);
  EXPECT_EQ(producer->payload_read_count(), 1u);
  // A wrong producer must never receive I/O or seed the shared cache.
  auto wrong = std::make_shared<MemoryQuerySource>(data);
  wrong->set_identity({"/shared.grib2", 11}, 124);
  auto denied = adapter.execute(QueryExecutionContext(request, wrong),
    plan.value().implementation(), {});
  ASSERT_FALSE(denied.ok());
  EXPECT_EQ(denied.status().code(), QueryError::StalePlan);
  EXPECT_EQ(wrong->payload_read_count(), 0u);
}

TEST(GribFormatQueryAdapter, BinaryServiceReadLimitRejectsBeforePayloadCall) {
  QueryRuntimeConfig config;
  config.max_read_bytes_per_load = 1;
  auto runtime = std::make_shared<QueryRuntime>(config);
  auto source = std::make_shared<MemoryQuerySource>(one_message());
  GribFormatQueryAdapter adapter({}, {}, runtime);
  auto result = execute(adapter, source, binary_fragment());
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(source->payload_read_count(), 0u);
  EXPECT_EQ(runtime->stats().payload_read_calls, 0u);
}

TEST(GribFormatQueryAdapter, BinaryCacheConcurrentWaitersShareOnePayloadRead) {
  auto source = std::make_shared<MemoryQuerySource>(one_message(), 11,
    std::string("grib"), std::chrono::milliseconds{}, std::chrono::milliseconds(100));
  GribFormatQueryAdapter adapter;
  auto a = binary_cursor(adapter, source);
  auto b = binary_cursor(adapter, source);
  ASSERT_TRUE(a.ok()); ASSERT_TRUE(b.ok());
  std::atomic<bool> success{true};
  std::thread first([&] { if (!a.value()->next().ok()) success = false; });
  std::thread second([&] { if (!b.value()->next().ok()) success = false; });
  first.join(); second.join();
  EXPECT_TRUE(success.load());
  EXPECT_EQ(source->payload_read_count(), 1u);
}

TEST(GribFormatQueryAdapter, BinaryCancellationDoesNotCancelAnotherWaiter) {
  auto source = std::make_shared<MemoryQuerySource>(one_message(), 11,
    std::string("grib"), std::chrono::milliseconds{}, std::chrono::milliseconds(100));
  auto runtime = std::make_shared<QueryRuntime>();
  GribFormatQueryAdapter adapter({}, {}, runtime);
  auto a = binary_cursor(adapter, source);
  auto b = binary_cursor(adapter, source);
  ASSERT_TRUE(a.ok()); ASSERT_TRUE(b.ok());
  std::atomic<QueryError> first_error{QueryError::None};
  std::thread first([&] { first_error = a.value()->next().status().code(); });
  while (!source->payload_read_count()) std::this_thread::yield();
  std::thread second([&] {
    auto result = b.value()->next();
    EXPECT_TRUE(result.ok()) << result.status().message();
  });
  // Cache wait accounting establishes that the second caller has joined.
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (!runtime->stats().waits && std::chrono::steady_clock::now() < deadline)
    std::this_thread::yield();
  EXPECT_GT(runtime->stats().waits, 0u);
  a.value()->cancel();
  first.join(); second.join();
  EXPECT_EQ(first_error.load(), QueryError::Cancelled);
  EXPECT_EQ(source->payload_read_count(), 1u);
  ASSERT_TRUE(execute(adapter, source, binary_fragment()).ok());
  EXPECT_EQ(source->payload_read_count(), 1u);
}

TEST(GribFormatQueryAdapter, ProjectsMessageRowsAfterTypedLogicalPredicate) {
  auto source_owner = std::make_shared<MemoryQuerySource>(two_messages());
  auto& source = *source_owner;
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kEdition}};
  fragment.predicate = make_logical(LogicalOperator::And, {
    make_in({grib_query_fields::kEdition},
      {LogicalValue::int32(1), LogicalValue::int32(2)}),
    make_logical(LogicalOperator::Not, {
      make_comparison({grib_query_fields::kEdition},
        ComparisonOperator::Equal, LogicalValue::int32(1))})});

  auto result = execute(adapter, source_owner, fragment);
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().size(), 2u);
  for (const auto& batch : result.value()) {
    EXPECT_EQ(batch.row_count(), 1u);
    EXPECT_FALSE(batch.domain());
    EXPECT_EQ(std::get<std::vector<std::int32_t>>(
      batch.columns()[0].values), (std::vector<std::int32_t>{2}));
  }
}

TEST(GribFormatQueryAdapter, SelectsExactValidityAndParameterWithMessageOffset) {
  auto first = regular_grid_message(0, 2, 2, {1, 3, 5, 7},
    130, 20180101, 0);
  auto second = regular_grid_message(first.bytes.size(), 2, 2,
    {2, 4, 6, 8}, 130, 20180102, 600);
  GribFixtureData data;
  data.payload = first.bytes;
  data.payload.insert(data.payload.end(), second.bytes.begin(),
    second.bytes.end());
  const auto layout = grib_layout_header() + first.layout_record +
    second.layout_record;
  data.layout.assign(layout.begin(), layout.end());
  auto source = std::make_shared<MemoryQuerySource>(std::move(data));
  GribFormatQueryAdapter adapter;

  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kOffset},
    {grib_query_fields::kParamId},
    {grib_query_fields::kValidityDate},
    {grib_query_fields::kValidityTime},
    {grib_query_fields::kValue}};
  fragment.predicate = make_logical(LogicalOperator::And, {
    make_comparison({grib_query_fields::kParamId},
      ComparisonOperator::Equal, LogicalValue::int64(130)),
    make_comparison({grib_query_fields::kValidityDate},
      ComparisonOperator::Equal, LogicalValue::int64(20180102)),
    make_comparison({grib_query_fields::kValidityTime},
      ComparisonOperator::Equal, LogicalValue::int64(600)),
    make_comparison({grib_query_fields::kOffset},
      ComparisonOperator::Equal,
      LogicalValue::uint64(first.bytes.size()))});

  auto result = execute(adapter, source, fragment, 2);
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().size(), 2u);
  std::vector<float> values;
  for (const auto& batch : result.value()) {
    EXPECT_EQ(batch.row_count(), 2u);
    EXPECT_TRUE(batch.domain());
    EXPECT_EQ(std::get<std::vector<std::uint64_t>>(
      batch.columns()[0].values),
      (std::vector<std::uint64_t>(2, first.bytes.size())));
    EXPECT_EQ(std::get<std::vector<std::int64_t>>(
      batch.columns()[1].values), (std::vector<std::int64_t>{130, 130}));
    EXPECT_EQ(std::get<std::vector<std::int64_t>>(
      batch.columns()[2].values),
      (std::vector<std::int64_t>{20180102, 20180102}));
    EXPECT_EQ(std::get<std::vector<std::int64_t>>(
      batch.columns()[3].values), (std::vector<std::int64_t>{600, 600}));
    const auto& part = std::get<std::vector<float>>(
      batch.columns()[4].values);
    values.insert(values.end(), part.begin(), part.end());
  }
  EXPECT_EQ(values, (std::vector<float>{2, 4, 6, 8}));
}

TEST(GribFormatQueryAdapter, OldLayoutRejectsNewIndexedPredicates) {
  auto source = std::make_shared<MemoryQuerySource>(typed_index_message());
  GribFormatQueryAdapter adapter;
  for (const char* field : {grib_query_fields::kParamId,
      grib_query_fields::kValidityDate,
      grib_query_fields::kValidityTime}) {
    QueryFragment fragment;
    fragment.projection = {{grib_query_fields::kOffset}};
    fragment.predicate = make_comparison({field},
      ComparisonOperator::Equal, LogicalValue::int64(1));
    auto result = execute(adapter, source, fragment);
    ASSERT_FALSE(result.ok()) << field;
    EXPECT_EQ(result.status().code(), QueryError::CorruptLayout) << field;
  }
  QueryFragment physical;
  physical.projection = {{grib_query_fields::kOffset}};
  physical.predicate = make_comparison({grib_query_fields::kOffset},
    ComparisonOperator::Equal, LogicalValue::uint64(0));
  auto selected = execute(adapter, source, physical);
  ASSERT_TRUE(selected.ok()) << selected.status().message();
  ASSERT_EQ(selected.value().size(), 1u);
  EXPECT_EQ(std::get<std::vector<std::uint64_t>>(
    selected.value()[0].columns()[0].values),
    (std::vector<std::uint64_t>{0}));
}

TEST(GribFormatQueryAdapter, StreamsTypedGridInWholeLongitudeRows) {
  auto source_owner = std::make_shared<MemoryQuerySource>(one_message());
  auto& source = *source_owner;
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kLatitude},
    {grib_query_fields::kLongitude}, {grib_query_fields::kValue},
    {grib_query_fields::kEdition}};
  fragment.output.type_overrides = {
    {{grib_query_fields::kValue}, LogicalTypeId::Float64}};

  auto result = execute(adapter, source_owner, fragment, 2);
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().size(), 2u);
  std::vector<double> values;
  for (std::size_t index = 0; index < result.value().size(); ++index) {
    const auto& batch = result.value()[index];
    ASSERT_EQ(batch.row_count(), 2u);
    ASSERT_TRUE(batch.domain());
    EXPECT_EQ(batch.domain()->shape,
      (std::vector<std::uint64_t>{1, 2}));
    EXPECT_EQ(batch.domain()->origin,
      (std::vector<std::uint64_t>{index, 0}));
    const auto& part = std::get<std::vector<double>>(
      batch.columns()[2].values);
    values.insert(values.end(), part.begin(), part.end());
    EXPECT_EQ(std::get<std::vector<std::int32_t>>(
      batch.columns()[3].values), (std::vector<std::int32_t>{2, 2}));
  }
  EXPECT_EQ(values, (std::vector<double>{1, 3, 5, 7}));
}

TEST(GribFormatQueryAdapter, UsesExactClosedSlicesIncludingOneCell) {
  auto source_owner = std::make_shared<MemoryQuerySource>(one_message());
  auto& source = *source_owner;
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kLatitude},
    {grib_query_fields::kLongitude}, {grib_query_fields::kValue}};
  fragment.slices = {
    closed_slice(grib_query_fields::kLatitude, 1, 1),
    closed_slice(grib_query_fields::kLongitude, 0, 0)};

  auto result = execute(adapter, source_owner, fragment);
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().size(), 1u);
  const auto& batch = result.value()[0];
  EXPECT_EQ(batch.row_count(), 1u);
  ASSERT_TRUE(batch.domain());
  EXPECT_EQ(batch.domain()->shape,
    (std::vector<std::uint64_t>{1, 1}));
  EXPECT_EQ(std::get<std::vector<double>>(batch.columns()[0].values),
    (std::vector<double>{1}));
  EXPECT_EQ(std::get<std::vector<double>>(batch.columns()[1].values),
    (std::vector<double>{0}));
  EXPECT_EQ(std::get<std::vector<float>>(batch.columns()[2].values),
    (std::vector<float>{1}));
}

TEST(GribFormatQueryAdapter, AppliesDomainSliceBeforeBilinearInterpolation) {
  auto source_owner = std::make_shared<MemoryQuerySource>(one_message(3, 3,
    {0, 10, 20, 100, 110, 120, 200, 210, 220}));
  auto& source = *source_owner;
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  fragment.slices = {
    closed_slice(grib_query_fields::kLatitude, 1, 2),
    closed_slice(grib_query_fields::kLongitude, 0, 1)};
  fragment.output.type_overrides = {
    {{grib_query_fields::kValue}, LogicalTypeId::Float64}};
  fragment.output.interpolation = QueryFragment::GridInterpolation{
    QueryFragment::InterpolationMethod::Bilinear,
    {{{grib_query_fields::kLatitude}, 3, std::nullopt},
     {{grib_query_fields::kLongitude}, 3, std::nullopt}},
    QueryFragment::BorderMode::Clamp};

  auto result = execute(adapter, source_owner, fragment);
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().size(), 1u);
  ASSERT_TRUE(result.value()[0].domain());
  EXPECT_EQ(result.value()[0].domain()->shape,
    (std::vector<std::uint64_t>{3, 3}));
  EXPECT_EQ(std::get<std::vector<double>>(
    result.value()[0].columns()[0].values),
    (std::vector<double>{0, 5, 10, 50, 55, 60, 100, 105, 110}));
}

TEST(GribFormatQueryAdapter, InterpolatesWithTypedAxisSpacing) {
  auto source_owner = std::make_shared<MemoryQuerySource>(one_message(3, 3,
    {0, 10, 20, 100, 110, 120, 200, 210, 220}));
  auto& source = *source_owner;
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  fragment.output.type_overrides = {
    {{grib_query_fields::kValue}, LogicalTypeId::Float64}};
  fragment.output.interpolation = QueryFragment::GridInterpolation{
    QueryFragment::InterpolationMethod::Bilinear,
    {{{grib_query_fields::kLatitude}, std::nullopt,
       LogicalValue::float64(0.5)},
     {{grib_query_fields::kLongitude}, std::nullopt,
       LogicalValue::float64(0.5)}},
    QueryFragment::BorderMode::Clamp};

  auto result = execute(adapter, source_owner, fragment);
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().size(), 1u);
  ASSERT_TRUE(result.value()[0].domain());
  EXPECT_EQ(result.value()[0].domain()->shape,
    (std::vector<std::uint64_t>{5, 5}));
  EXPECT_EQ(std::get<std::vector<double>>(
    result.value()[0].columns()[0].values),
    (std::vector<double>{
      0, 5, 10, 15, 20,
      50, 55, 60, 65, 70,
      100, 105, 110, 115, 120,
      150, 155, 160, 165, 170,
      200, 205, 210, 215, 220}));
}

TEST(GribFormatQueryAdapter, LongitudeSliceAcrossZeroSelectsBothGridEdges) {
  std::vector<double> values(2 * 360);
  for (long j = 0; j < 2; ++j)
    for (long i = 0; i < 360; ++i)
      values[static_cast<std::size_t>(j * 360 + i)] = j * 1000 + i;
  auto source_owner = std::make_shared<MemoryQuerySource>(one_message(360, 2, std::move(values)));
  auto& source = *source_owner;
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kLongitude},
    {grib_query_fields::kValue}};
  fragment.slices = {
    closed_slice(grib_query_fields::kLongitude, -10, 10)};
  fragment.output.type_overrides = {
    {{grib_query_fields::kValue}, LogicalTypeId::Float64}};

  auto result = execute(adapter, source_owner, fragment);
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().size(), 1u);
  const auto& batch = result.value()[0];
  ASSERT_TRUE(batch.domain());
  EXPECT_EQ(batch.domain()->shape,
    (std::vector<std::uint64_t>{2, 21}));
  const std::vector<double> expected_longitudes{350, 351, 352, 353, 354,
    355, 356, 357, 358, 359, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  const auto& longitudes =
    std::get<std::vector<double>>(batch.columns()[0].values);
  EXPECT_EQ(expected_longitudes,
    std::vector<double>(longitudes.begin(), longitudes.begin() + 21));
  EXPECT_EQ(batch.row_count(), 42u);
}

TEST(GribFormatQueryAdapter, RejectsForeignIdentityAndUnversionedLayout) {
  GribFormatQueryAdapter adapter;
  auto foreign_owner = std::make_shared<MemoryQuerySource>(one_message(), 11, std::string("arrow"));
  auto& foreign = *foreign_owner;
  EXPECT_EQ(adapter.capabilities(QueryPlanningContext(foreign), QueryFragment{}).status().code(),
    QueryError::AdapterUnavailable);

  auto data = one_message();
  const std::string unversioned(data.layout.begin() +
    static_cast<std::ptrdiff_t>(grib_layout_header().size()),
    data.layout.end());
  data.layout.assign(unversioned.begin(), unversioned.end());
  auto legacy_owner = std::make_shared<MemoryQuerySource>(std::move(data));
  auto& legacy = *legacy_owner;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  EXPECT_EQ(adapter.prepare(QueryPlanningContext(legacy), fragment).status().code(),
    QueryError::CorruptLayout);

  auto current_owner = std::make_shared<MemoryQuerySource>(one_message(), 12);
  auto& current = *current_owner;
  auto prepared = adapter.prepare(QueryPlanningContext(current), fragment);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  auto another_version_owner = std::make_shared<MemoryQuerySource>(one_message(), 13);
  auto& another_version = *another_version_owner;
  QueryExecutionOptions options;
  EXPECT_EQ(adapter.execute(QueryExecutionContext(another_version_owner),
    prepared.value().implementation(), options).status().code(),
    QueryError::StalePlan);
}

TEST(GribFormatQueryAdapter, ReusesParsedIndexForTheSameFileVersion) {
  auto source_owner = std::make_shared<MemoryQuerySource>(two_messages(), 21);
  auto& source = *source_owner;
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kEdition}};
  fragment.predicate = make_comparison({grib_query_fields::kEdition},
    ComparisonOperator::Equal, LogicalValue::int32(2));

  auto first = adapter.prepare(QueryPlanningContext(source), fragment);
  ASSERT_TRUE(first.ok()) << first.status().message();
  auto second_fragment = fragment;
  second_fragment.predicate = make_comparison({grib_query_fields::kEdition},
    ComparisonOperator::Equal, LogicalValue::int32(1));
  auto second = adapter.prepare(QueryPlanningContext(source), second_fragment);
  ASSERT_TRUE(second.ok()) << second.status().message();

  EXPECT_EQ(source.layout_read_count(), 1u);
}

TEST(GribFormatQueryAdapter, DefaultIndexCacheRetainsWarmMultiFileDiscovery) {
  const auto fixture = one_message();
  std::vector<std::shared_ptr<MemoryQuerySource>> sources;
  for (std::int64_t i = 0; i < 128; ++i)
    sources.push_back(std::make_shared<MemoryQuerySource>(fixture, 1000 + i));
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kEdition}};
  QueryProfileRecord profile;
  ScopedQueryProfile scope(&profile);
  for (int pass = 0; pass < 3; ++pass) {
    for (const auto& source : sources)
      ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), fragment).ok());
    // Real discovery is followed by a selected-source read, which changes LRU.
    for (std::size_t i : {std::size_t{0}, std::size_t{1}})
      ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*sources[i]), fragment).ok());
  }
  for (const auto& source : sources)
    EXPECT_EQ(source->layout_read_count(), 1u);
  EXPECT_EQ(profile.counters["grib.index.cache.misses"], 128u);
  EXPECT_EQ(profile.counters["grib.index.cache.hits"], 262u);
  EXPECT_EQ(profile.counters["grib.index.cache.evictions"], 0u);
  EXPECT_LT(profile.counters["grib.index.cache.loaded_bytes"], 64u * 1024 * 1024);
}

TEST(GribFormatQueryAdapter, ParsedIndexProfileIdentifiesVersionLimitThrashing) {
  const auto fixture = one_message();
  std::vector<std::shared_ptr<MemoryQuerySource>> sources;
  for (std::int64_t i = 0; i < 32; ++i)
    sources.push_back(std::make_shared<MemoryQuerySource>(fixture, 2000 + i));
  GribFormatQueryAdapter adapter({16, 64 * 1024 * 1024});
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kEdition}};
  for (const auto& source : sources)
    ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), fragment).ok());
  QueryProfileRecord profile;
  {
    ScopedQueryProfile scope(&profile);
    for (const auto& source : sources)
      ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), fragment).ok());
  }
  EXPECT_EQ(profile.counters["grib.index.cache.misses"], 32u);
  EXPECT_EQ(profile.counters["grib.index.cache.evictions"], 32u);
  EXPECT_EQ(profile.counters["grib.index.cache.version_limit_evictions"], 32u);
  EXPECT_EQ(profile.counters["grib.index.cache.byte_limit_evictions"], 0u);
  for (const auto& source : sources) EXPECT_EQ(source->layout_read_count(), 2u);
}

TEST(GribFormatQueryAdapter, ParsedIndexByteEvictionsAndDisablingRemainObservable) {
  auto source = std::make_shared<MemoryQuerySource>(one_message(), 3000);
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kEdition}};
  QueryProfileRecord profile;
  ScopedQueryProfile scope(&profile);
  GribFormatQueryAdapter small({65536, 1});
  ASSERT_TRUE(small.prepare(QueryPlanningContext(*source), fragment).ok());
  ASSERT_TRUE(small.prepare(QueryPlanningContext(*source), fragment).ok());
  EXPECT_EQ(source->layout_read_count(), 2u);
  EXPECT_EQ(profile.counters["grib.index.cache.byte_limit_evictions"], 2u);
  EXPECT_EQ(profile.counters["grib.index.cache.version_limit_evictions"], 0u);
  GribFormatQueryAdapter disabled({0, 64 * 1024 * 1024});
  ASSERT_TRUE(disabled.prepare(QueryPlanningContext(*source), fragment).ok());
  EXPECT_EQ(profile.counters["grib.index.cache.disabled"], 1u);
}

TEST(GribFormatQueryAdapter, DoesNotShareParsedIndexAcrossFileVersions) {
  auto first_source_owner = std::make_shared<MemoryQuerySource>(one_message(), 31);
  auto& first_source = *first_source_owner;
  auto second_source_owner = std::make_shared<MemoryQuerySource>(two_messages(), 32);
  auto& second_source = *second_source_owner;
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kEdition}};

  auto first = adapter.prepare(QueryPlanningContext(first_source), fragment);
  ASSERT_TRUE(first.ok()) << first.status().message();
  auto second = adapter.prepare(QueryPlanningContext(second_source), fragment);
  ASSERT_TRUE(second.ok()) << second.status().message();

  EXPECT_EQ(first_source.layout_read_count(), 1u);
  EXPECT_EQ(second_source.layout_read_count(), 1u);
}

TEST(GribFormatQueryAdapter, EvictsLeastRecentlyUsedParsedIndexByVersion) {
  auto first_source_owner = std::make_shared<MemoryQuerySource>(one_message(), 41);
  auto& first_source = *first_source_owner;
  auto second_source_owner = std::make_shared<MemoryQuerySource>(two_messages(), 42);
  auto& second_source = *second_source_owner;
  GribFormatQueryAdapter adapter({1, 64 * 1024 * 1024});
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kEdition}};

  auto first_plan = adapter.prepare(QueryPlanningContext(first_source), fragment);
  ASSERT_TRUE(first_plan.ok()) << first_plan.status().message();
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(second_source), fragment).ok());
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(first_source), fragment).ok());

  EXPECT_EQ(first_source.layout_read_count(), 2u);
  EXPECT_EQ(second_source.layout_read_count(), 1u);

  QueryExecutionOptions options;
  auto cursor = adapter.execute(QueryExecutionContext(first_source_owner),
    first_plan.value().implementation(), options);
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  auto batch = cursor.value()->next();
  ASSERT_TRUE(batch.ok()) << batch.status().message();
  ASSERT_TRUE(batch.value());
  EXPECT_EQ(batch.value()->row_count(), 1u);
}

TEST(GribFormatQueryAdapter, DoesNotRetainIndexBeyondMemoryBudget) {
  auto source_owner = std::make_shared<MemoryQuerySource>(one_message(), 51);
  auto& source = *source_owner;
  GribFormatQueryAdapter adapter({16, 1});
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kEdition}};

  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(source), fragment).ok());
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(source), fragment).ok());

  EXPECT_EQ(source.layout_read_count(), 2u);
}

TEST(GribFormatQueryAdapter, ParsesAndProjectsTypedIndexFieldsOnce) {
  auto source_owner = std::make_shared<MemoryQuerySource>(typed_index_message(), 61);
  auto& source = *source_owner;
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kShortName},
    {grib_query_fields::kLevel}, {grib_query_fields::kStep},
    {grib_query_fields::kEdition}};
  fragment.predicate = make_logical(LogicalOperator::And, {
    make_comparison({grib_query_fields::kShortName},
      ComparisonOperator::Equal, LogicalValue::utf8("clwmr")),
    make_comparison({grib_query_fields::kLevel},
      ComparisonOperator::Equal, LogicalValue::int64(50)),
    make_comparison({grib_query_fields::kStep},
      ComparisonOperator::Equal, LogicalValue::int64(3))});

  auto result = execute(adapter, source_owner, fragment);
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().size(), 1u);
  const auto& columns = result.value()[0].columns();
  EXPECT_EQ(std::get<std::vector<std::string>>(columns[0].values),
    (std::vector<std::string>{"clwmr"}));
  EXPECT_EQ(std::get<std::vector<std::int64_t>>(columns[1].values),
    (std::vector<std::int64_t>{50}));
  EXPECT_EQ(std::get<std::vector<std::int64_t>>(columns[2].values),
    (std::vector<std::int64_t>{3}));
  EXPECT_EQ(std::get<std::vector<std::int32_t>>(columns[3].values),
    (std::vector<std::int32_t>{2}));
}

TEST(GribFormatQueryAdapter, CoalescesConcurrentIndexLoadsPerFileVersion) {
  auto source_owner = std::make_shared<MemoryQuerySource>(two_messages(), 71, std::string("grib"),
    std::chrono::milliseconds(25));
  auto& source = *source_owner;
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kEdition}};
  std::atomic<std::size_t> failures{0};
  std::vector<std::thread> workers;
  for (std::size_t index = 0; index < 8; ++index) {
    workers.emplace_back([&] {
      if (!adapter.prepare(QueryPlanningContext(source), fragment).ok())
        failures.fetch_add(1, std::memory_order_relaxed);
    });
  }
  for (auto& worker : workers) worker.join();

  EXPECT_EQ(failures.load(std::memory_order_relaxed), 0u);
  EXPECT_EQ(source.layout_read_count(), 1u);
}

TEST(GribFormatQueryAdapter,
    DecodesMessagesConcurrentlyAndEmitsThemInLayoutOrder) {
  auto source_owner = std::make_shared<MemoryQuerySource>(two_messages(), 81, std::string("grib"), std::chrono::milliseconds{},
    std::chrono::milliseconds(40));
  auto& source = *source_owner;
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};

  auto result = execute(adapter, source_owner, fragment);
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().size(), 2u);
  EXPECT_GE(source.max_active_payload_reads(), 2u);
  EXPECT_EQ(std::get<std::vector<float>>(
    result.value()[0].columns()[0].values),
    (std::vector<float>{1, 3, 5, 7}));
  EXPECT_EQ(std::get<std::vector<float>>(
    result.value()[1].columns()[0].values),
    (std::vector<float>{2, 4, 6, 8}));
}

TEST(GribFormatQueryAdapter, SingleSelectedMessageRunsOnPublicQueryWorkerColdAndWarm) {
  // The source has two messages; only the selected count may choose the path.
  auto producer = std::make_shared<MemoryQuerySource>(two_messages(), 8201);
  QueryRuntimeConfig config;
  config.query_tasks = {1, 16, 1};
  auto runtime = std::make_shared<QueryRuntime>(config);
  GribFormatQueryAdapter adapter({}, {}, runtime);
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  fragment.predicate = make_comparison({grib_query_fields::kOffset},
    ComparisonOperator::Equal, LogicalValue::uint64(0));
  auto prepared = adapter.prepare(QueryPlanningContext(*producer), fragment);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  for (unsigned pass = 0; pass < 2; ++pass) {
    QueryTaskScheduler::Submission task;
    task.task = [&](const CancellationToken&) {
      const auto worker = std::this_thread::get_id();
      auto request = std::make_shared<GridRequestProbe>(producer,
        [worker](std::uint64_t) {
          EXPECT_EQ(std::this_thread::get_id(), worker);
          return QueryStatus::success();
        });
      auto cursor = adapter.execute(QueryExecutionContext(request, producer),
        prepared.value().implementation(), {});
      if (!cursor.ok()) return cursor.status();
      EXPECT_EQ(request->reserve_calls.load(), 0u);
      auto batch = cursor.value()->next();
      if (!batch.ok()) return batch.status();
      EXPECT_TRUE(batch.value());
      if (batch.value()) {
        EXPECT_EQ(std::get<std::vector<float>>(batch.value()->columns()[0].values),
          (std::vector<float>{1, 3, 5, 7}));
        EXPECT_EQ(batch.value()->sources().size(), 1u);
        if (!batch.value()->sources().empty())
          EXPECT_EQ(batch.value()->sources()[0].version, 8201);
      }
      auto end = cursor.value()->next();
      if (!end.ok()) return end.status();
      EXPECT_FALSE(end.value());
      EXPECT_EQ(request->reserve_calls.load(), 1u);
      return QueryStatus::success();
    };
    auto submitted = runtime->query_tasks().submit(std::move(task));
    ASSERT_TRUE(submitted.ok()) << submitted.status().message();
    auto status = submitted.value().get();
    ASSERT_TRUE(status.ok()) << status.message();
  }
  EXPECT_EQ(producer->payload_read_count(), 1u);
}

TEST(GribFormatQueryAdapter, SingleMessageCancelledBeforeNextDoesNotStartDecode) {
  auto source = std::make_shared<MemoryQuerySource>(one_message(), 8202);
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  auto prepared = adapter.prepare(QueryPlanningContext(*source), fragment);
  ASSERT_TRUE(prepared.ok());
  auto cursor = adapter.execute(QueryExecutionContext(source),
    prepared.value().implementation(), {});
  ASSERT_TRUE(cursor.ok());
  cursor.value()->cancel();
  auto result = cursor.value()->next();
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::Cancelled);
  EXPECT_EQ(result.status().phase(), QueryPhase::Execution);
  EXPECT_EQ(result.status().message(), "GRIB query cursor was cancelled");
  ASSERT_TRUE(result.status().detail().source);
  EXPECT_EQ(result.status().detail().source->path, source->manifest().key.path);
  cursor.value().reset();
  EXPECT_EQ(source->payload_read_count(), 0u);
  // Cancellation never poisons the shared producer or the next request.
  auto survivor = execute(adapter, source, fragment);
  ASSERT_TRUE(survivor.ok()) << survivor.status().message();
  EXPECT_EQ(source->payload_read_count(), 1u);
}

TEST(GribFormatQueryAdapter, SingleMessageRestoresCapturedRequestContextAndCallerTls) {
  auto producer = std::make_shared<MemoryQuerySource>(one_message(), 8203);
  producer->emit_payload_fees(23);
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  auto prepared = adapter.prepare(QueryPlanningContext(*producer), fragment);
  ASSERT_TRUE(prepared.ok());
  auto captured_budget = std::make_shared<QueryMetadataBudget>();
  auto captured_lease = std::make_shared<QueryExecutionLease>();
  auto caller_budget = std::make_shared<QueryMetadataBudget>();
  caller_budget->observe_unique(QueryMetadataBudget::maximum_bytes);
  auto caller_lease = std::make_shared<QueryExecutionLease>();
  for (bool capture_budget : {true, false}) {
    auto request = std::make_shared<GridRequestProbe>(producer,
      [&, capture_budget](std::uint64_t) {
        EXPECT_EQ(current_query_metadata_budget(), capture_budget ? captured_budget : nullptr);
        EXPECT_EQ(current_query_execution_lease(), captured_lease);
        EXPECT_EQ(active_query_metadata_trace, nullptr);
        EXPECT_EQ(active_query_profile, nullptr);
        EXPECT_EQ(active_storage_read_control, nullptr);
        return QueryStatus::success();
      });
    std::unique_ptr<QueryBatchCursor> cursor;
    {
      ScopedQueryMetadataProducer no_ambient_budget;
      std::unique_ptr<ScopedQueryMetadataBudget> scope;
      if (capture_budget) scope = std::make_unique<ScopedQueryMetadataBudget>(captured_budget);
      ScopedQueryExecutionLease lease(captured_lease);
      auto started = adapter.execute(QueryExecutionContext(request, producer),
        prepared.value().implementation(), {});
      ASSERT_TRUE(started.ok());
      cursor = std::move(started.value());
    }
    ScopedQueryMetadataBudget budget(caller_budget);
    ScopedQueryExecutionLease lease(caller_lease);
    QueryProfileRecord record;
    ScopedQueryProfile profile(&record);
    ScopedQueryStorageReadControl control([] {
      throw std::runtime_error("unrelated caller storage control");
    }, {});
    ScopedQueryMetadataTrace collector("", [](std::uint64_t) {
      throw QueryMetadataBudgetExceeded("unrelated caller collector");
    });
    const auto* storage_control = active_storage_read_control;
    auto batch = cursor->next();
    ASSERT_TRUE(batch.ok()) << batch.status().message();
    ASSERT_TRUE(batch.value());
    EXPECT_EQ(current_query_metadata_budget(), caller_budget);
    EXPECT_EQ(current_query_execution_lease(), caller_lease);
    EXPECT_EQ(active_query_metadata_trace, &collector);
    EXPECT_EQ(active_query_profile, &record);
    EXPECT_EQ(active_storage_read_control, storage_control);
    EXPECT_FALSE(collector.failed());
    EXPECT_TRUE(collector.release().empty());
    EXPECT_EQ(caller_budget->consumed_bytes(), QueryMetadataBudget::maximum_bytes);
  }
  EXPECT_EQ(captured_budget->consumed_bytes(), 23u);
  EXPECT_EQ(producer->payload_read_count(), 1u);
}

TEST(GribFormatQueryAdapter, SingleMessageCancelledWaiterDoesNotPoisonConcurrentSurvivor) {
  auto producer = std::make_shared<MemoryQuerySource>(one_message(), 8205,
    std::string("grib"), std::chrono::milliseconds{}, std::chrono::milliseconds(100));
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  auto prepared = adapter.prepare(QueryPlanningContext(*producer), fragment);
  ASSERT_TRUE(prepared.ok());
  auto first = adapter.execute(QueryExecutionContext(producer),
    prepared.value().implementation(), {});
  auto second = adapter.execute(QueryExecutionContext(producer),
    prepared.value().implementation(), {});
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  auto pending = std::async(std::launch::async,
    [&] { return first.value()->next(); });
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (!producer->active_payload_reads() && std::chrono::steady_clock::now() < deadline)
    std::this_thread::yield();
  ASSERT_GT(producer->active_payload_reads(), 0u);
  auto survivor = std::async(std::launch::async,
    [&] { return second.value()->next(); });
  first.value()->cancel();
  // Existing cancellation is cooperative at the cursor entry: an already
  // running native read may finish. Its shared producer remains useful.
  auto in_flight = pending.get();
  EXPECT_TRUE(in_flight.ok()) << in_flight.status().message();
  auto batch = survivor.get();
  ASSERT_TRUE(batch.ok()) << batch.status().message();
  ASSERT_TRUE(batch.value());
  EXPECT_EQ(std::get<std::vector<float>>(batch.value()->columns()[0].values),
    (std::vector<float>{1, 3, 5, 7}));
  auto cancelled = first.value()->next();
  EXPECT_EQ(cancelled.status().code(), QueryError::Cancelled);
  EXPECT_EQ(producer->payload_read_count(), 1u);
}

TEST(GribFormatQueryAdapter, SingleMessagePreservesDecodeExceptionStatusAndRestoresContext) {
  auto producer = std::make_shared<MemoryQuerySource>(one_message(), 8204);
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  auto prepared = adapter.prepare(QueryPlanningContext(*producer), fragment);
  ASSERT_TRUE(prepared.ok());
  for (bool standard : {true, false}) {
    auto request = std::make_shared<GridRequestProbe>(producer,
      [standard](std::uint64_t) -> QueryStatus {
        if (standard) throw std::runtime_error("injected request accounting failure");
        throw 7;
      });
    auto cursor = adapter.execute(QueryExecutionContext(request, producer),
      prepared.value().implementation(), {});
    ASSERT_TRUE(cursor.ok());
    ScopedQueryMetadataBudget caller_budget;
    auto caller_lease = std::make_shared<QueryExecutionLease>();
    ScopedQueryExecutionLease lease(caller_lease);
    QueryProfileRecord record;
    ScopedQueryProfile profile(&record);
    ScopedQueryStorageReadControl control({}, {});
    ScopedQueryMetadataTrace collector("");
    const auto* storage_control = active_storage_read_control;
    auto failed = cursor.value()->next();
    ASSERT_FALSE(failed.ok());
    EXPECT_EQ(failed.status().code(), QueryError::Internal);
    EXPECT_EQ(failed.status().phase(), QueryPhase::Execution);
    EXPECT_EQ(failed.status().message(), standard
      ? "GRIB parallel decode failed: injected request accounting failure"
      : "GRIB parallel decode failed with an unknown exception");
    ASSERT_TRUE(failed.status().detail().source);
    EXPECT_EQ(failed.status().detail().source->path, producer->manifest().key.path);
    EXPECT_EQ(failed.status().detail().source->version, 8204);
    EXPECT_EQ(current_query_metadata_budget(), caller_budget.budget());
    EXPECT_EQ(current_query_execution_lease(), caller_lease);
    EXPECT_EQ(active_query_metadata_trace, &collector);
    EXPECT_EQ(active_query_profile, &record);
    EXPECT_EQ(active_storage_read_control, storage_control);
  }
  EXPECT_EQ(producer->payload_read_count(), 0u);
}

TEST(GribFormatQueryAdapter,
    ReusesOneDecodedNativeFieldAcrossDifferentCrops) {
  struct RestoreMessageCacheCapacity {
    ~RestoreMessageCacheCapacity() {
      grib_utils::configure_message_cache(32);
    }
  } restore;
  grib_utils::configure_message_cache(1);

  auto source_owner = std::make_shared<MemoryQuerySource>(one_message(), 82);
  auto& source = *source_owner;
  auto eviction_source_owner = std::make_shared<MemoryQuerySource>(one_message(), 83);
  auto& eviction_source = *eviction_source_owner;
  GribFormatQueryAdapter adapter;

  QueryFragment northwest;
  northwest.projection = {{grib_query_fields::kValue}};
  northwest.slices = {
    closed_slice(grib_query_fields::kLatitude, 1, 1),
    closed_slice(grib_query_fields::kLongitude, 0, 0)};
  auto first = execute(adapter, source_owner, northwest);
  ASSERT_TRUE(first.ok()) << first.status().message();
  ASSERT_EQ(first.value().size(), 1u);
  EXPECT_EQ(std::get<std::vector<float>>(
    first.value()[0].columns()[0].values), (std::vector<float>{1}));

  QueryFragment full;
  full.projection = {{grib_query_fields::kValue}};
  ASSERT_TRUE(execute(adapter, eviction_source_owner, full).ok());

  QueryFragment southeast;
  southeast.projection = {{grib_query_fields::kValue}};
  southeast.slices = {
    closed_slice(grib_query_fields::kLatitude, 0, 0),
    closed_slice(grib_query_fields::kLongitude, 1, 1)};
  auto second = execute(adapter, source_owner, southeast);
  ASSERT_TRUE(second.ok()) << second.status().message();
  ASSERT_EQ(second.value().size(), 1u);
  EXPECT_EQ(std::get<std::vector<float>>(
    second.value()[0].columns()[0].values), (std::vector<float>{7}));

  EXPECT_EQ(source.payload_read_count(), 1u);
  EXPECT_EQ(eviction_source.payload_read_count(), 1u);
}

TEST(GribFormatQueryAdapter,
    CoalescesConcurrentDecodedFieldLoadsPerMessage) {
  auto source_owner = std::make_shared<MemoryQuerySource>(one_message(), 84, std::string("grib"), std::chrono::milliseconds{},
    std::chrono::milliseconds(60));
  auto& source = *source_owner;
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  auto prepared = adapter.prepare(QueryPlanningContext(source), fragment);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();

  QueryExecutionOptions options;
  auto first = adapter.execute(QueryExecutionContext(source_owner), prepared.value().implementation(),
    options);
  auto second = adapter.execute(QueryExecutionContext(source_owner), prepared.value().implementation(),
    options);
  ASSERT_TRUE(first.ok()) << first.status().message();
  ASSERT_TRUE(second.ok()) << second.status().message();
  auto first_pending = std::async(std::launch::async,
    [&] { return first.value()->next(); });
  auto second_pending = std::async(std::launch::async,
    [&] { return second.value()->next(); });
  auto first_batch = first_pending.get();
  auto second_batch = second_pending.get();
  ASSERT_TRUE(first_batch.ok()) << first_batch.status().message();
  ASSERT_TRUE(second_batch.ok()) << second_batch.status().message();
  ASSERT_TRUE(first_batch.value());
  ASSERT_TRUE(second_batch.value());

  EXPECT_EQ(source.payload_read_count(), 1u);
  EXPECT_EQ(source.max_active_payload_reads(), 1u);
}

TEST(GribFormatQueryAdapter, DoesNotRetainDecodedFieldsBeyondMemoryBudget) {
  struct RestoreMessageCacheCapacity {
    ~RestoreMessageCacheCapacity() {
      grib_utils::configure_message_cache(32);
    }
  } restore;
  grib_utils::configure_message_cache(1);

  auto source_owner = std::make_shared<MemoryQuerySource>(one_message(), 85);
  auto& source = *source_owner;
  auto eviction_source_owner = std::make_shared<MemoryQuerySource>(one_message(), 86);
  auto& eviction_source = *eviction_source_owner;
  GribFormatQueryAdapter adapter(GribParsedIndexCacheConfig{},
    GribDecodedFieldCacheConfig{16, 1});
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};

  ASSERT_TRUE(execute(adapter, source_owner, fragment).ok());
  ASSERT_TRUE(execute(adapter, eviction_source_owner, fragment).ok());
  ASSERT_TRUE(execute(adapter, source_owner, fragment).ok());

  // The native field is evicted, while its bounded encoded runtime fragment remains.
  EXPECT_EQ(source.payload_read_count(), 1u);
  EXPECT_EQ(eviction_source.payload_read_count(), 1u);
}

TEST(GribFormatQueryAdapter, RetriesDecodedFieldAfterLoadFailure) {
  auto source_owner = std::make_shared<MemoryQuerySource>(one_message(), 87);
  auto& source = *source_owner;
  source.fail_next_payload_read();
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};

  auto failed = execute(adapter, source_owner, fragment);
  ASSERT_FALSE(failed.ok());
  EXPECT_EQ(failed.status().code(), QueryError::ReadFailure);
  auto retried = execute(adapter, source_owner, fragment);
  ASSERT_TRUE(retried.ok()) << retried.status().message();

  EXPECT_EQ(source.payload_read_count(), 2u);
}

TEST(GribFormatQueryAdapter,
    PrefetchesDifferentFileVersionsThroughTheSharedDecodePool) {
  auto concurrency = std::make_shared<PayloadReadConcurrency>();
  auto first_owner = std::make_shared<MemoryQuerySource>(two_messages(), 91, std::string("grib"), std::chrono::milliseconds{},
    std::chrono::milliseconds(40), concurrency);
  auto& first = *first_owner;
  auto second_owner = std::make_shared<MemoryQuerySource>(two_messages(), 92, std::string("grib"), std::chrono::milliseconds{},
    std::chrono::milliseconds(40), concurrency);
  auto& second = *second_owner;
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  auto first_plan = adapter.prepare(QueryPlanningContext(first), fragment);
  auto second_plan = adapter.prepare(QueryPlanningContext(second), fragment);
  ASSERT_TRUE(first_plan.ok()) << first_plan.status().message();
  ASSERT_TRUE(second_plan.ok()) << second_plan.status().message();
  QueryExecutionOptions options;
  auto first_cursor = adapter.execute(QueryExecutionContext(first_owner), first_plan.value().implementation(),
    options);
  auto second_cursor = adapter.execute(QueryExecutionContext(second_owner),
    second_plan.value().implementation(), options);
  ASSERT_TRUE(first_cursor.ok()) << first_cursor.status().message();
  ASSERT_TRUE(second_cursor.ok()) << second_cursor.status().message();

  auto first_batch = first_cursor.value()->next();
  auto second_batch = second_cursor.value()->next();
  ASSERT_TRUE(first_batch.ok()) << first_batch.status().message();
  ASSERT_TRUE(second_batch.ok()) << second_batch.status().message();
  ASSERT_TRUE(first_batch.value());
  ASSERT_TRUE(second_batch.value());
  EXPECT_GE(concurrency->maximum.load(std::memory_order_relaxed), 2u);
}

TEST(GribFormatQueryAdapter,
    ACursorWithoutAPrefetchPermitStillDecodesOnDemand) {
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  QueryExecutionOptions options;
  std::vector<std::shared_ptr<MemoryQuerySource>> sources;
  std::vector<std::unique_ptr<QueryBatchCursor>> cursors;
  for (std::int64_t version = 100; version < 109; ++version) {
    sources.push_back(std::make_shared<MemoryQuerySource>(two_messages(),
      version));
    auto plan = adapter.prepare(QueryPlanningContext(*sources.back()), fragment);
    ASSERT_TRUE(plan.ok()) << plan.status().message();
    auto cursor = adapter.execute(QueryExecutionContext(sources.back()),
      plan.value().implementation(), options);
    ASSERT_TRUE(cursor.ok()) << cursor.status().message();
    cursors.push_back(std::move(cursor.value()));
  }

  auto batch = cursors.back()->next();
  ASSERT_TRUE(batch.ok()) << batch.status().message();
  ASSERT_TRUE(batch.value());
  EXPECT_EQ(batch.value()->row_count(), 4u);
}

TEST(GribFormatQueryAdapter, CursorRetainsItsSourceAfterExecutionContextEnds) {
  auto source = std::make_shared<MemoryQuerySource>(one_message(), 200);
  std::weak_ptr<const QuerySource> weak = source;
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  auto plan = adapter.prepare(QueryPlanningContext(*source), fragment);
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  auto cursor = adapter.execute(QueryExecutionContext(source),
    plan.value().implementation(), {});
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  source.reset();
  EXPECT_FALSE(weak.expired());
  auto batch = cursor.value()->next();
  ASSERT_TRUE(batch.ok()) << batch.status().message();
  ASSERT_TRUE(batch.value());
  cursor.value().reset();
  EXPECT_TRUE(weak.expired());
}


TEST(GribFormatQueryAdapter, SharesOnlyParsedLayoutAcrossVerifiedVersions) {
  auto first = std::make_shared<MemoryQuerySource>(one_message(), 501);
  auto second = std::make_shared<MemoryQuerySource>(one_message(2, 2, {9, 10, 11, 12}), 502);
  first->set_layout_identity("namespace/incarnation/layout-id/grib-rule-1");
  second->set_layout_identity("namespace/incarnation/layout-id/grib-rule-1");
  GribFormatQueryAdapter adapter;
  auto one = execute(adapter, first, binary_fragment());
  ASSERT_TRUE(one.ok()) << one.status().message();
  auto two = execute(adapter, second, binary_fragment());
  ASSERT_TRUE(two.ok()) << two.status().message();
  EXPECT_EQ(first->layout_read_count(), 1u);
  EXPECT_EQ(second->layout_read_count(), 0u);
  EXPECT_EQ(first->payload_read_count(), 1u);
  EXPECT_EQ(second->payload_read_count(), 1u);
  ASSERT_EQ(one.value().size(), two.value().size());
  EXPECT_NE(std::get<std::vector<BinaryValue>>(one.value().front().columns().back().values),
    std::get<std::vector<BinaryValue>>(two.value().front().columns().back().values));
}

TEST(GribFormatQueryAdapter, RechecksProofBeforeWarmParsedLayoutHit) {
  auto source = std::make_shared<MemoryQuerySource>(one_message(), 503);
  source->set_layout_identity("namespace/incarnation/layout-id/grib-rule-1");
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kEdition}};
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), fragment).ok());
  source->fail_identity();
  auto rejected = adapter.prepare(QueryPlanningContext(*source), fragment);
  ASSERT_FALSE(rejected.ok());
  EXPECT_EQ(rejected.status().code(), QueryError::StalePlan);
  ASSERT_TRUE(rejected.status().detail().source);
  EXPECT_EQ(rejected.status().detail().source->path, source->manifest().key.path);
  EXPECT_EQ(source->identity_checks(), 2u);
  EXPECT_EQ(source->layout_read_count(), 1u);
}

TEST(GribFormatQueryAdapter, SharedLayoutKeyRetainsSizeAndNamespaceRuleIsolation) {
  auto first = std::make_shared<MemoryQuerySource>(one_message(), 504);
  auto larger = std::make_shared<MemoryQuerySource>(two_messages(), 505);
  auto other_incarnation = std::make_shared<MemoryQuerySource>(one_message(), 506);
  first->set_layout_identity("namespace/incarnation-a/layout-id/rule-1");
  larger->set_layout_identity("namespace/incarnation-a/layout-id/rule-1");
  other_incarnation->set_layout_identity("namespace/incarnation-b/layout-id/rule-1");
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kEdition}};
  for (const auto& source : {first, larger, other_incarnation})
    ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*source), fragment).ok());
  EXPECT_EQ(first->layout_read_count(), 1u);
  EXPECT_EQ(larger->layout_read_count(), 1u);
  EXPECT_EQ(other_incarnation->layout_read_count(), 1u);
}

TEST(GribFormatQueryAdapter, SharedLayoutSingleflightFailureUsesEachRequestSource) {
  auto first = std::make_shared<MemoryQuerySource>(one_message(), 507,
    std::string("grib"), std::chrono::milliseconds(100));
  auto second = std::make_shared<MemoryQuerySource>(one_message(), 508);
  first->set_layout_identity("namespace/incarnation/layout-id/rule-1");
  second->set_layout_identity("namespace/incarnation/layout-id/rule-1");
  first->fail_layout();
  second->fail_layout();
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kEdition}};
  auto pending = std::async(std::launch::async, [&] {
    return adapter.prepare(QueryPlanningContext(*first), fragment);
  });
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (first->layout_read_count() == 0 && std::chrono::steady_clock::now() < deadline)
    std::this_thread::yield();
  ASSERT_GT(first->layout_read_count(), 0u);
  auto own_failure = adapter.prepare(QueryPlanningContext(*second), fragment);
  auto producer_failure = pending.get();
  ASSERT_FALSE(producer_failure.ok());
  ASSERT_FALSE(own_failure.ok());
  ASSERT_TRUE(own_failure.status().detail().source);
  EXPECT_EQ(own_failure.status().detail().source->path, second->manifest().key.path);
  EXPECT_NE(own_failure.status().message().find(second->manifest().key.path), std::string::npos);
  EXPECT_EQ(own_failure.status().message().find(first->manifest().key.path), std::string::npos);
  EXPECT_EQ(second->layout_read_count(), 1u);
}

TEST(GribFormatQueryAdapter, WarmNumericGridStillEnforcesLogicalPayloadLimit) {
  auto source = std::make_shared<MemoryQuerySource>(one_message(), 9001);
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  ASSERT_TRUE(execute(adapter, source, fragment).ok());
  auto prepared = adapter.prepare(QueryPlanningContext(*source), fragment);
  ASSERT_TRUE(prepared.ok());
  QueryExecutionOptions options;
  options.max_payload_bytes = 1;
  auto cursor = adapter.execute(QueryExecutionContext(source),
    prepared.value().implementation(), options);
  ASSERT_TRUE(cursor.ok());
  auto rejected = cursor.value()->next();
  ASSERT_FALSE(rejected.ok());
  EXPECT_EQ(rejected.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(source->payload_read_count(), 1u);
}


TEST(GribFormatQueryAdapter, WarmNumericGridReplaysFeesIntoEachRequestLedger) {
  auto source = std::make_shared<MemoryQuerySource>(one_message(), 9002);
  source->emit_payload_fees();
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  ASSERT_TRUE(execute(adapter, source, fragment).ok());
  auto ledger = std::make_shared<QueryMetadataBudget>();
  ledger->observe("v04-payload-bytes/already-used", QueryMetadataBudget::maximum_payload_bytes - 1);
  ScopedQueryMetadataBudget scope(ledger);
  auto rejected = execute(adapter, source, fragment);
  ASSERT_FALSE(rejected.ok());
  EXPECT_EQ(rejected.status().code(), QueryError::ResourceExhausted);
  EXPECT_FALSE(ledger->status().ok());
  EXPECT_EQ(source->payload_read_count(), 1u);
}


TEST(GribFormatQueryAdapter, NumericGridFeesReuseSameLedgerAndDoNotRetainIt) {
  auto source = std::make_shared<MemoryQuerySource>(one_message(), 9003);
  source->emit_payload_fees();
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  auto ledger = std::make_shared<QueryMetadataBudget>();
  std::weak_ptr<QueryMetadataBudget> weak = ledger;
  ledger->observe("v04-payload-bytes/already-used", QueryMetadataBudget::maximum_payload_bytes - source->manifest().payload_size);
  {
    ScopedQueryMetadataBudget scope(ledger);
    ASSERT_TRUE(execute(adapter, source, fragment).ok());
    EXPECT_EQ(ledger->consumed_bytes(), 16u);
    ASSERT_TRUE(execute(adapter, source, fragment).ok());
    EXPECT_EQ(ledger->consumed_bytes(), 16u);
    EXPECT_TRUE(ledger->status().ok());
  }
  ledger.reset();
  EXPECT_TRUE(weak.expired());
  {
    ScopedQueryMetadataBudget fresh;
    ASSERT_TRUE(execute(adapter, source, fragment).ok());
    EXPECT_EQ(fresh.budget()->consumed_bytes(), 16u);
  }
  {
    ScopedQueryMetadataBudget failed;
    failed.budget()->observe_unique(QueryMetadataBudget::maximum_bytes - 1);
    auto rejected = execute(adapter, source, fragment);
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), QueryError::ResourceExhausted);
  }
  {
    ScopedQueryMetadataBudget after_failed_waiter;
    ASSERT_TRUE(execute(adapter, source, fragment).ok());
    EXPECT_EQ(after_failed_waiter.budget()->consumed_bytes(), 16u);
  }
  EXPECT_EQ(source->payload_read_count(), 1u);
}

TEST(GribFormatQueryAdapter, NumericCachesIsolateNamespaceAndRevision) {
  std::vector<std::shared_ptr<MemoryQuerySource>> sources{
    std::make_shared<MemoryQuerySource>(one_message(), 9004),
    std::make_shared<MemoryQuerySource>(one_message(2, 2, {9, 11, 13, 15}), 9004),
    std::make_shared<MemoryQuerySource>(one_message(2, 2, {17, 19, 21, 23}), 9004)};
  sources[0]->set_identity({"/same-grid.grib2", 9004}, 901, 0);
  sources[1]->set_identity({"/same-grid.grib2", 9004}, 902, 0);
  sources[2]->set_identity({"/same-grid.grib2", 9004}, 901, 1);
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  for (std::size_t i = 0; i < sources.size(); ++i) {
    auto result = execute(adapter, sources[i], fragment);
    ASSERT_TRUE(result.ok()) << result.status().message();
    ASSERT_EQ(result.value().size(), 1u);
    EXPECT_EQ(std::get<std::vector<float>>(result.value()[0].columns()[0].values).front(), 1 + 8 * i);
    EXPECT_EQ(sources[i]->payload_read_count(), 1u);
  }
}

TEST(GribFormatQueryAdapter, NumericColdReadRespectsAggregateRuntimeStorageTemporaryBudget) {
  auto source = std::make_shared<MemoryQuerySource>(one_message(), 9005);
  source->require_transport_temporary(1024 * 1024);
  QueryRuntimeConfig limits;
  limits.memory_budget_bytes = 512 * 1024;
  auto runtime = std::make_shared<QueryRuntime>(limits);
  GribFormatQueryAdapter adapter({}, {}, runtime);
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  auto rejected = execute(adapter, source, fragment);
  ASSERT_FALSE(rejected.ok());
  EXPECT_EQ(rejected.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(source->physical_block_reads(), 0u);
  EXPECT_LE(runtime->stats().peak_bytes, limits.memory_budget_bytes);
  auto roomy_runtime = std::make_shared<QueryRuntime>();
  GribFormatQueryAdapter roomy({}, {}, roomy_runtime);
  ASSERT_TRUE(execute(roomy, source, fragment).ok());
  EXPECT_EQ(source->physical_block_reads(), 1u);
  EXPECT_GE(roomy_runtime->stats().read_bytes, 1024u * 1024);
  EXPECT_GE(roomy_runtime->stats().peak_bytes, 1024u * 1024);
}

TEST(GribFormatQueryAdapter, NumericTimedOutEncodedProducerKeepsExecutionLeaseUntilSourceReadEnds) {
  auto source = std::make_shared<MemoryQuerySource>(one_message(), 9006,
    std::string("grib"), std::chrono::milliseconds{}, std::chrono::milliseconds(100));
  QueryRuntimeConfig limits;
  limits.wait_timeout_ms = 10;
  auto runtime = std::make_shared<QueryRuntime>(limits);
  GribFormatQueryAdapter adapter({}, {}, runtime);
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  auto prepared = adapter.prepare(QueryPlanningContext(*source), fragment);
  ASSERT_TRUE(prepared.ok());
  auto lease = std::make_shared<QueryExecutionLease>();
  std::weak_ptr<QueryExecutionLease> weak = lease;
  std::unique_ptr<QueryBatchCursor> cursor;
  {
    ScopedQueryExecutionLease scope(lease);
    auto started = adapter.execute(QueryExecutionContext(source), prepared.value().implementation(), {});
    ASSERT_TRUE(started.ok());
    cursor = std::move(started.value());
  }
  lease.reset();
  auto result = cursor->next();
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(static_cast<int>(result.status().code()), static_cast<int>(QueryError::ResourceExhausted))
    << result.status().message();
  cursor.reset();
  EXPECT_FALSE(weak.expired());
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (!weak.expired() && std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(source->active_payload_reads(), 0u);
}


TEST(GribFormatQueryAdapter, NumericRequestReservesOnceAndUsesIndependentProducer) {
  class RequestSource final : public QuerySource {
  public:
    explicit RequestSource(std::shared_ptr<const QuerySource> source) : source_(std::move(source)) {}
    const StructuredBlobManifest& manifest() const override { return source_->manifest(); }
    std::uint64_t storage_namespace() const override { return source_->storage_namespace(); }
    QueryStatus reserve_logical_payload(std::uint64_t length) const override {
      if (reserved_ || length != manifest().payload_size)
        return QueryStatus::failure(QueryError::ResourceExhausted, QueryPhase::Execution,
          "test request permits exactly one full-message demand");
      reserved_ = true;
      return QueryStatus::success();
    }
    QueryResult<StructuredBlobBytes> read_payload(std::uint64_t, std::uint64_t) const override {
      return QueryResult<StructuredBlobBytes>::failure(QueryStatus::failure(QueryError::ReadFailure,
        QueryPhase::Storage, "shared numeric producer borrowed request source"));
    }
    QueryResult<StructuredBlobBytes> read_layout(std::uint64_t offset, std::uint64_t length) const override {
      return source_->read_layout(offset, length);
    }
    bool reserved() const { return reserved_; }
  private:
    std::shared_ptr<const QuerySource> source_;
    mutable bool reserved_{false};
  };
  auto producer = std::make_shared<MemoryQuerySource>(one_message(), 9007);
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  auto prepared = adapter.prepare(QueryPlanningContext(*producer), fragment);
  ASSERT_TRUE(prepared.ok());
  for (unsigned request = 0; request < 2; ++request) {
    auto source = std::make_shared<RequestSource>(producer);
    auto cursor = adapter.execute(QueryExecutionContext(source, producer), prepared.value().implementation(), {});
    ASSERT_TRUE(cursor.ok());
    auto batch = cursor.value()->next();
    ASSERT_TRUE(batch.ok()) << batch.status().message();
    ASSERT_TRUE(batch.value());
    EXPECT_TRUE(source->reserved());
  }
  EXPECT_EQ(producer->payload_read_count(), 1u);
}

TEST(GribFormatQueryAdapter, NativeContentSharesAcrossPathsVersionsAndRevisions) {
  const auto data = one_message();
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  QueryProfileRecord profile;
  ScopedQueryProfile scope(&profile);
  std::vector<std::shared_ptr<MemoryQuerySource>> sources;
  for (int i = 0; i < 4; ++i) {
    auto source = std::make_shared<MemoryQuerySource>(data);
    source->set_identity({i < 2 ? "/shared.grib2" : "/other.grib2", 11 + i},
      10001, i);
    auto result = execute(adapter, source, fragment);
    ASSERT_TRUE(result.ok()) << result.status().message();
    ASSERT_EQ(result.value().size(), 1u);
    EXPECT_EQ(std::get<std::vector<float>>(result.value()[0].columns()[0].values),
      (std::vector<float>{1, 3, 5, 7}));
    ASSERT_EQ(result.value()[0].sources().size(), 1u);
    EXPECT_EQ(result.value()[0].sources()[0].path, source->manifest().key.path);
    EXPECT_EQ(result.value()[0].sources()[0].version, 11 + i);
    // Every new exact source must supply its own verified message bytes.
    EXPECT_EQ(source->payload_read_count(), 1u);
    sources.push_back(std::move(source));
  }
  // Destroying an owned output cannot invalidate the shared native field.
  auto output = execute(adapter, sources.front(), fragment);
  ASSERT_TRUE(output.ok());
  output.value().clear();
  auto again = execute(adapter, sources.back(), fragment);
  ASSERT_TRUE(again.ok());
  EXPECT_EQ(std::get<std::vector<float>>(again.value()[0].columns()[0].values)[0], 1);
  EXPECT_EQ(profile.counters["grib.native_field_cache.loads"], 1u);
  EXPECT_GE(profile.counters["grib.native_field_cache.content_hits"], 3u);
  for (const auto& source : sources) EXPECT_EQ(source->payload_read_count(), 1u);
}

TEST(GribFormatQueryAdapter, NativeCanonicalFieldSharesAcrossPrecisionAndCrops) {
  const auto data = one_message(2, 2, {1.25, 3.5, 5.75, 7.125});
  auto first = std::make_shared<MemoryQuerySource>(data);
  auto second = std::make_shared<MemoryQuerySource>(data);
  first->set_identity({"/precision-a.grib2", 1}, 10002);
  second->set_identity({"/precision-b.grib2", 2}, 10002);
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  QueryProfileRecord profile;
  ScopedQueryProfile scope(&profile);
  auto f32 = execute(adapter, first, fragment);
  ASSERT_TRUE(f32.ok()) << f32.status().message();
  EXPECT_EQ(std::get<std::vector<float>>(f32.value()[0].columns()[0].values),
    (std::vector<float>{1.25f, 3.5f, 5.75f, 7.125f}));
  fragment.output.type_overrides = {
    {{grib_query_fields::kValue}, LogicalTypeId::Float64}};
  fragment.slices = {
    closed_slice(grib_query_fields::kLatitude, 0, 0),
    closed_slice(grib_query_fields::kLongitude, 1, 1)};
  auto f64 = execute(adapter, second, fragment);
  ASSERT_TRUE(f64.ok()) << f64.status().message();
  EXPECT_EQ(std::get<std::vector<double>>(f64.value()[0].columns()[0].values),
    (std::vector<double>{7.125}));
  EXPECT_EQ(profile.counters["grib.native_field_cache.loads"], 1u);
  EXPECT_EQ(first->payload_read_count(), 1u);
  EXPECT_EQ(second->payload_read_count(), 1u);
}

TEST(GribFormatQueryAdapter, NativeContentIdentitySeparatesDifferentBytesAndNamespaces) {
  const auto data = one_message();
  std::vector<std::shared_ptr<MemoryQuerySource>> sources{
    std::make_shared<MemoryQuerySource>(data),
    std::make_shared<MemoryQuerySource>(one_message(2, 2, {9, 11, 13, 15})),
    std::make_shared<MemoryQuerySource>(data)};
  for (std::size_t i = 0; i < sources.size(); ++i)
    sources[i]->set_identity({"/content.grib2", 100 + static_cast<std::int64_t>(i)},
      i == 2 ? 10004 : 10003);
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  QueryProfileRecord profile;
  ScopedQueryProfile scope(&profile);
  for (std::size_t i = 0; i < sources.size(); ++i) {
    auto result = execute(adapter, sources[i], fragment);
    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(std::get<std::vector<float>>(result.value()[0].columns()[0].values)[0],
      i == 1 ? 9 : 1);
  }
  EXPECT_EQ(profile.counters["grib.native_field_cache.loads"], 3u);
}

TEST(GribFormatQueryAdapter, NativeSharedContentKeepsFeesQualifiedByExactSource) {
  const auto data = one_message();
  auto first = std::make_shared<MemoryQuerySource>(data);
  auto second = std::make_shared<MemoryQuerySource>(data);
  first->set_identity({"/fee.grib2", 1}, 10005);
  second->set_identity({"/fee.grib2", 2}, 10005);
  first->emit_payload_fees(16);
  second->emit_payload_fees(31);
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  ASSERT_TRUE(execute(adapter, first, fragment).ok());
  ASSERT_TRUE(execute(adapter, second, fragment).ok());
  auto ledger = std::make_shared<QueryMetadataBudget>();
  std::weak_ptr<QueryMetadataBudget> weak = ledger;
  {
    ScopedQueryMetadataBudget scope(ledger);
    ASSERT_TRUE(execute(adapter, first, fragment).ok());
    EXPECT_EQ(ledger->consumed_bytes(), 16u);
    ASSERT_TRUE(execute(adapter, second, fragment).ok());
    EXPECT_EQ(ledger->consumed_bytes(), 47u);
    ASSERT_TRUE(execute(adapter, first, fragment).ok());
    EXPECT_EQ(ledger->consumed_bytes(), 47u);
  }
  ledger.reset();
  EXPECT_TRUE(weak.expired());
  {
    ScopedQueryMetadataBudget fresh;
    ASSERT_TRUE(execute(adapter, second, fragment).ok());
    EXPECT_EQ(fresh.budget()->consumed_bytes(), 31u);
  }
  {
    ScopedQueryMetadataBudget bounded;
    bounded.budget()->observe("v04-payload-bytes/preused",
      QueryMetadataBudget::maximum_payload_bytes - data.payload.size());
    ASSERT_TRUE(execute(adapter, first, fragment).ok());
    auto rejected = execute(adapter, second, fragment);
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), QueryError::ResourceExhausted);
  }
  // The failed request cannot poison content or source aliases for fresh callers.
  ASSERT_TRUE(execute(adapter, second, fragment).ok());
  EXPECT_EQ(first->payload_read_count(), 1u);
  EXPECT_EQ(second->payload_read_count(), 1u);
}

TEST(GribFormatQueryAdapter, NativeContentHitCannotBypassNewSourceReadFailure) {
  const auto data = one_message();
  auto first = std::make_shared<MemoryQuerySource>(data);
  auto second = std::make_shared<MemoryQuerySource>(data);
  first->set_identity({"/read-success.grib2", 1}, 10006);
  second->set_identity({"/read-retry.grib2", 2}, 10006);
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  QueryProfileRecord profile;
  ScopedQueryProfile scope(&profile);
  ASSERT_TRUE(execute(adapter, first, fragment).ok());
  second->fail_next_payload_read();
  auto failed = execute(adapter, second, fragment);
  ASSERT_FALSE(failed.ok());
  EXPECT_EQ(failed.status().code(), QueryError::ReadFailure);
  ASSERT_TRUE(execute(adapter, second, fragment).ok());
  ASSERT_TRUE(execute(adapter, second, fragment).ok());
  EXPECT_EQ(first->payload_read_count(), 1u);
  EXPECT_EQ(second->payload_read_count(), 2u);
  EXPECT_EQ(profile.counters["grib.native_field_cache.loads"], 1u);
}

TEST(GribFormatQueryAdapter, NativeContentConcurrentAliasesDecodeOnlyOnce) {
  const auto data = one_message(128, 64, std::vector<double>(128 * 64, 3));
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  std::vector<std::shared_ptr<MemoryQuerySource>> sources;
  for (int i = 0; i < 8; ++i) {
    auto source = std::make_shared<MemoryQuerySource>(data, i, std::string("grib"),
      std::chrono::milliseconds{}, std::chrono::milliseconds(20));
    source->set_identity({"/parallel-" + std::to_string(i) + ".grib2", i}, 10007);
    sources.push_back(std::move(source));
  }
  std::atomic<bool> start{false};
  std::vector<std::future<std::pair<bool, QueryProfileRecord>>> workers;
  for (const auto& source : sources) {
    workers.push_back(std::async(std::launch::async, [&, source] {
      while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
      QueryProfileRecord profile;
      ScopedQueryProfile scope(&profile);
      auto result = execute(adapter, source, fragment);
      return std::make_pair(result.ok() && result.value().size() == 1 &&
        result.value()[0].row_count() == 128 * 64, std::move(profile));
    }));
  }
  start.store(true, std::memory_order_release);
  std::uint64_t loads = 0;
  for (auto& worker : workers) {
    auto result = worker.get();
    EXPECT_TRUE(result.first);
    loads += result.second.counters["grib.native_field_cache.loads"];
  }
  EXPECT_EQ(loads, 1u);
  for (const auto& source : sources) EXPECT_EQ(source->payload_read_count(), 1u);
}

TEST(GribFormatQueryAdapter, NativeTwoQueueProtectsRepeatedFieldsFromColdScan) {
  constexpr std::size_t budget = 1024 * 1024;
  GribFormatQueryAdapter adapter({}, GribDecodedFieldCacheConfig{3, budget});
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  std::vector<std::shared_ptr<MemoryQuerySource>> sources;
  for (int i = 0; i < 12; ++i) {
    auto source = std::make_shared<MemoryQuerySource>(
      one_message(16, 16, std::vector<double>(16 * 16, i + 1)));
    source->set_identity({"/scan-" + std::to_string(i) + ".grib2", i}, 10008);
    sources.push_back(std::move(source));
  }
  for (int pass = 0; pass < 2; ++pass)
    for (int i = 0; i < 2; ++i)
      ASSERT_TRUE(execute(adapter, sources[i], fragment).ok());
  for (std::size_t i = 2; i < sources.size(); ++i)
    ASSERT_TRUE(execute(adapter, sources[i], fragment).ok());
  QueryProfileRecord profile;
  ScopedQueryProfile scope(&profile);
  for (int i = 0; i < 2; ++i) {
    auto result = execute(adapter, sources[i], fragment);
    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(std::get<std::vector<float>>(result.value()[0].columns()[0].values)[0], i + 1);
  }
  EXPECT_EQ(profile.counters["grib.native_field_cache.loads"], 0u);
  EXPECT_EQ(profile.counters["grib.native_field_cache.hits"], 2u);
  EXPECT_LE(profile.counters["grib.native_field_cache.resident_bytes"], budget);
  EXPECT_LE(profile.counters["grib.native_field_cache.resident_entries"], 3u);
  EXPECT_LE(profile.counters["grib.native_field_cache.aliases"], 256u);
  EXPECT_LE(profile.counters["grib.native_field_cache.ghosts"], 256u);
}

TEST(GribFormatQueryAdapter, NativeSharedContentBoundsAliasesAcrossManyVersions) {
  constexpr std::size_t budget = 1024 * 1024;
  GribFormatQueryAdapter adapter({}, GribDecodedFieldCacheConfig{16, budget});
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  const auto data = one_message();
  QueryProfileRecord profile;
  ScopedQueryProfile scope(&profile);
  for (int i = 0; i < 300; ++i) {
    auto source = std::make_shared<MemoryQuerySource>(data);
    source->set_identity({"/many-versions.grib2", i}, 10009);
    auto result = execute(adapter, source, fragment);
    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(result.value()[0].sources()[0].version, i);
  }
  EXPECT_EQ(profile.counters["grib.native_field_cache.loads"], 1u);
  EXPECT_GT(profile.counters["grib.native_field_cache.alias_evictions"], 0u);
  EXPECT_LE(profile.counters["grib.native_field_cache.aliases"], 256u);
  EXPECT_LE(profile.counters["grib.native_field_cache.ghosts"], 256u);
  EXPECT_LE(profile.counters["grib.native_field_cache.resident_bytes"], budget);
}

TEST(GribFormatQueryAdapter, NativeDecodeFailureIsNotRetainedAsAContentResult) {
  auto data = one_message();
  auto invalid_data = data;
  invalid_data.payload.assign(invalid_data.payload.size(), 0);
  auto invalid = std::make_shared<MemoryQuerySource>(std::move(invalid_data));
  auto valid = std::make_shared<MemoryQuerySource>(std::move(data));
  invalid->set_identity({"/decode-retry.grib2", 1}, 10010, 0);
  valid->set_identity({"/decode-retry.grib2", 1}, 10010, 1);
  GribFormatQueryAdapter adapter;
  QueryFragment fragment;
  fragment.projection = {{grib_query_fields::kValue}};
  QueryProfileRecord profile;
  ScopedQueryProfile scope(&profile);
  for (int attempt = 0; attempt < 2; ++attempt) {
    auto failed = execute(adapter, invalid, fragment);
    ASSERT_FALSE(failed.ok());
  }
  auto succeeded = execute(adapter, valid, fragment);
  ASSERT_TRUE(succeeded.ok()) << succeeded.status().message();
  EXPECT_EQ(std::get<std::vector<float>>(succeeded.value()[0].columns()[0].values),
    (std::vector<float>{1, 3, 5, 7}));
  EXPECT_GE(profile.counters["grib.native_field_cache.load_failures"], 2u);
  // The bounded encoded cache may retain the invalid bytes. It must not turn
  // a failed native decode into a successful field or poison another revision.
  EXPECT_EQ(valid->payload_read_count(), 1u);
}
