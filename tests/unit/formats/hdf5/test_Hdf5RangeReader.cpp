#include "iotdbfs/formats/hdf5/Hdf5RangeReader.h"
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>

using namespace iotdbfs;
namespace {
class RangeSource final : public QuerySource {
public:
  RangeSource() { manifest_.payload_size = 256; }
  const StructuredBlobManifest& manifest() const override { return manifest_; }
  PayloadReadCapabilities payload_read_capabilities() const override { return caps; }
  QueryResult<StructuredBlobBytes> read_payload(std::uint64_t off,
      std::uint64_t n) const override { return read({{off,n}}, false); }
  QueryResult<StructuredBlobPayloadBatch> read_payload_ranges(
      const StructuredBlobPayloadRanges& ranges) const override {
    auto first = read(ranges, true);
    if (!first.ok()) return QueryResult<StructuredBlobPayloadBatch>::failure(first.status());
    StructuredBlobPayloadBatch values;
    for (const auto& r : ranges) values.push_back(bytes(r.offset, r.length));
    if (wrong_count && !values.empty()) values.pop_back();
    return QueryResult<StructuredBlobPayloadBatch>::success(std::move(values));
  }
  QueryResult<StructuredBlobBytes> read_layout(std::uint64_t, std::uint64_t) const override {
    return QueryResult<StructuredBlobBytes>::success({});
  }
  static BinaryValue bytes(std::uint64_t off, std::uint64_t n) {
    BinaryValue b(n); for (std::size_t i=0;i<n;++i) b[i]=(off+i)&255; return b;
  }
  QueryResult<StructuredBlobBytes> read(const StructuredBlobPayloadRanges& r, bool multi) const {
    {
      std::lock_guard<std::mutex> lock(mutex);
      calls.push_back(r); if (multi) ++multi_calls;
    }
    const auto n = ++active;
    auto p=peak.load(); while (p<n && !peak.compare_exchange_weak(p,n)) {}
    if (on_read) on_read();
    if (pause_ms) std::this_thread::sleep_for(std::chrono::milliseconds(pause_ms));
    --active;
    if (throws) throw std::runtime_error("read failure");
    auto value=bytes(r.front().offset,r.front().length);
    if (short_read && !value.empty()) value.pop_back();
    if (excess_capacity) value.reserve(value.size()+1);
    return QueryResult<StructuredBlobBytes>::success(std::move(value));
  }
  StructuredBlobManifest manifest_;
  PayloadReadCapabilities caps{true,64,8};
  bool short_read{false}, wrong_count{false}, throws{false};
  bool cancel_producer{false}, excess_capacity{false};
  std::function<void()> on_read;
  unsigned pause_ms{0};
  mutable std::mutex mutex;
  mutable std::vector<StructuredBlobPayloadRanges> calls;
  mutable unsigned multi_calls{0};
  mutable std::atomic<unsigned> active{0},peak{0};
};
class ReadFragment final : public DecodedFragment {
public:
  std::vector<BinaryValue> data;
  std::uint64_t resident_bytes() const override {
    std::uint64_t n=0; for(const auto& b:data)n+=b.capacity();return n;
  }
};
QueryResult<FragmentLease> load(DecodedFragmentCache& cache,
    std::shared_ptr<RangeSource> source, StructuredBlobPayloadRanges spans,
    Hdf5RangeReadConfig config={}) {
  std::uint64_t total=0;for(const auto& span:spans)total+=span.length;
  const auto memory=hdf5_range_read_temporary_bytes(spans,config);
  EXPECT_TRUE(memory.ok());
  return cache.get("test",{total,memory.ok()?memory.value():0},
    [source,spans,config](const FragmentLoadContext& context)
      ->QueryResult<std::shared_ptr<const DecodedFragment>> {
      if (source->cancel_producer)
        source->on_read = [token=context.cancellation()] { token.cancel(); };
      auto read=read_hdf5_payload_ranges(source,spans,context,config);
      if(!read.ok())return QueryResult<std::shared_ptr<const DecodedFragment>>::failure(read.status());
      auto value=std::make_shared<ReadFragment>();value->data=std::move(read.value());
      return QueryResult<std::shared_ptr<const DecodedFragment>>::success(value);
    });
}
TEST(Hdf5RangeReaderTest, MergesOverlappingAdjacentAndPreservesSpanOrder) {
  DecodedFragmentCache cache;
  auto source=std::make_shared<RangeSource>(); source->caps.max_bytes=64;
  StructuredBlobPayloadRanges spans{{20,3},{5,7},{0,7},{12,2}};
  auto result=load(cache,source,spans);ASSERT_TRUE(result.ok())<<result.status().message();
  const auto fragment=result.value().as<ReadFragment>();ASSERT_TRUE(fragment);
  for(std::size_t i=0;i<spans.size();++i)
    EXPECT_EQ(fragment->data[i],RangeSource::bytes(spans[i].offset,spans[i].length));
  ASSERT_EQ(source->calls.size(),1);EXPECT_EQ(source->multi_calls,1);
  ASSERT_EQ(source->calls[0].size(),2);
  EXPECT_EQ(source->calls[0][0].offset,0);EXPECT_EQ(source->calls[0][0].length,14);
  EXPECT_EQ(cache.stats().read_bytes,17);EXPECT_EQ(cache.stats().read_ranges,2);
  EXPECT_EQ(cache.stats().copy_bytes,19);EXPECT_EQ(cache.stats().payload_read_calls,1);
}
TEST(Hdf5RangeReaderTest, SplitsOversizedSpansAndBoundsParallelism) {
  DecodedFragmentCache cache; auto source=std::make_shared<RangeSource>();source->pause_ms=10;
  auto result=load(cache,source,{{1,51}}, {4,8,64});ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.value().as<ReadFragment>()->data[0],RangeSource::bytes(1,51));
  EXPECT_EQ(source->calls.size(),7);EXPECT_LE(source->peak,4);EXPECT_GE(source->peak,2);
  EXPECT_EQ(source->active,0);EXPECT_EQ(cache.stats().read_bytes,51);
  EXPECT_EQ(cache.stats().payload_reads_active,0);
}
TEST(Hdf5RangeReaderTest, ScalarCapabilityNeverUsesMultiRange) {
  DecodedFragmentCache cache;auto source=std::make_shared<RangeSource>();source->caps={false,64,8};
  auto result=load(cache,source,{{0,3},{20,3},{40,3}}, {2,8,64});ASSERT_TRUE(result.ok());
  EXPECT_EQ(source->multi_calls,0);EXPECT_EQ(source->calls.size(),3);
}
TEST(Hdf5RangeReaderTest, HonorsAdvertisedRangeAndByteBounds) {
  DecodedFragmentCache cache;auto source=std::make_shared<RangeSource>();source->caps={true,2,7};
  auto result=load(cache,source,{{0,3},{10,3},{20,3},{30,3},{40,3}});ASSERT_TRUE(result.ok());
  EXPECT_EQ(source->calls.size(),3);
  for(const auto& call:source->calls){EXPECT_LE(call.size(),2);std::size_t n=0;for(const auto& r:call)n+=r.length;EXPECT_LE(n,7);}
}
TEST(Hdf5RangeReaderTest, ErrorsDrainConcurrentReadsAndBalanceInstrumentation) {
  for(unsigned kind=0;kind<3;++kind){
    DecodedFragmentCache cache;auto source=std::make_shared<RangeSource>();source->pause_ms=5;
    source->short_read=kind==0;source->throws=kind==1;source->wrong_count=kind==2;
    auto result=load(cache,source,kind==2?StructuredBlobPayloadRanges{{0,2},{10,2},{20,2},{30,2},{40,2}}:StructuredBlobPayloadRanges{{0,40}}, {4,8,64});
    ASSERT_FALSE(result.ok());EXPECT_EQ(result.status().code(),QueryError::ReadFailure);
    EXPECT_EQ(source->active,0);EXPECT_EQ(cache.stats().payload_reads_active,0);
  }
}
TEST(Hdf5RangeReaderTest, CancelledProducerDrainsWaveAndDoesNotPublishPartialBytes) {
  DecodedFragmentCache cache; auto source=std::make_shared<RangeSource>();
  source->cancel_producer=true;source->pause_ms=10;
  auto result=load(cache,source,{{0,100}},{4,8,64});
  ASSERT_FALSE(result.ok());EXPECT_EQ(result.status().code(),QueryError::Cancelled);
  EXPECT_EQ(source->active,0);EXPECT_EQ(cache.stats().payload_reads_active,0);
  EXPECT_LE(source->calls.size(),4);EXPECT_EQ(cache.stats().cache_owned_bytes,0);
}
TEST(Hdf5RangeReaderTest, ServiceIoBudgetAndUnexpectedCapacityRejectWithoutPublication) {
  QueryRuntimeConfig limits;limits.max_read_bytes_per_load=12;
  DecodedFragmentCache cache(limits);auto source=std::make_shared<RangeSource>();
  auto result=load(cache,source,{{0,50}},{4,8,64});
  ASSERT_FALSE(result.ok());EXPECT_EQ(result.status().code(),QueryError::ResourceExhausted);
  EXPECT_LE(cache.stats().read_bytes,12);EXPECT_EQ(source->active,0);
  DecodedFragmentCache cache2;source=std::make_shared<RangeSource>();source->excess_capacity=true;
  result=load(cache2,source,{{0,6}});ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(),QueryError::ResourceExhausted);
}
TEST(Hdf5RangeReaderTest, TinyTransportGranulesCannotAllocateUnboundedPlanningDescriptors) {
  DecodedFragmentCache cache;
  auto source = std::make_shared<RangeSource>();
  source->caps = {false, 1, 1};
  auto result = load(cache, source, {{0, 100}}, {1, 8, 64, 8});
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted);
  EXPECT_TRUE(source->calls.empty());
  EXPECT_EQ(cache.stats().read_bytes, 0U);
  StructuredBlobPayloadRanges spans(9, {0, 1});
  EXPECT_FALSE(hdf5_range_read_temporary_bytes(spans, {1, 8, 64, 8}).ok());
}
TEST(Hdf5RangeReaderTest, InvalidLimitsAndSourceBoundsFailBeforeIo) {
  EXPECT_FALSE(hdf5_range_read_temporary_bytes({{0,3}},{3,8,64}).ok());
  EXPECT_FALSE(hdf5_range_read_temporary_bytes({{UINT64_MAX,3}}).ok());
  EXPECT_FALSE(hdf5_range_read_temporary_bytes({{0,0}}).ok());
  auto memory=hdf5_range_read_temporary_bytes({{0,30},{40,20}},{4,8,64});
  ASSERT_TRUE(memory.ok());EXPECT_EQ(memory.value(),82);
  DecodedFragmentCache cache;auto source=std::make_shared<RangeSource>();
  EXPECT_FALSE(load(cache,source,{{250,7}}).ok());EXPECT_TRUE(source->calls.empty());
}
} // namespace
