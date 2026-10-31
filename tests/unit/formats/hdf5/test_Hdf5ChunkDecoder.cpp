#include "iotdbfs/formats/hdf5/Hdf5ChunkDecoder.h"
#include <gtest/gtest.h>
#include <zlib.h>
#include <algorithm>
#include <cstring>
#include <future>
#include <limits>

using namespace iotdbfs;
namespace {
Hdf5DatasetLayout dataset(std::uint64_t elements = 128) {
  Hdf5DatasetLayout d;
  d.descriptor = {"/a", LogicalTypeId::Float32, {elements}, true, {}};
  d.storage = Hdf5StorageLayout::Chunked;
  d.byte_order = Hdf5ByteOrder::Little;
  d.element_bytes = 4;
  d.chunk_shape = {elements};
  return d;
}
std::vector<Hdf5FilterDescription> pipeline() {
  return {{2, 1, {4}}, {1, 1, {6}}, {3, 0, {}}};
}
BinaryValue data(std::size_t count = 128, std::size_t width = 4) {
  BinaryValue bytes(count * width);
  for (std::size_t i = 0; i < bytes.size(); ++i) bytes[i] = (i * 127 + i / 31) & 255;
  return bytes;
}
BinaryValue native(BinaryValue bytes, std::size_t width = 4, bool file_big = false) {
  const std::uint16_t one = 1;
  const bool host_big = *reinterpret_cast<const std::uint8_t*>(&one) != 1;
  if (file_big != host_big)
    for (std::size_t i = 0; i < bytes.size(); i += width)
      std::reverse(bytes.begin() + i, bytes.begin() + i + width);
  return bytes;
}
BinaryValue shuffled(const BinaryValue& bytes, std::size_t width) {
  BinaryValue result(bytes.size());
  const auto n = bytes.size() / width;
  for (std::size_t b = 0; b < width; ++b)
    for (std::size_t i = 0; i < n; ++i) result[b * n + i] = bytes[i * width + b];
  return result;
}
BinaryValue compressed(const BinaryValue& bytes) {
  uLongf n = compressBound(bytes.size());
  BinaryValue result(n);
  EXPECT_EQ(compress2(result.data(), &n, bytes.data(), bytes.size(), 6), Z_OK);
  result.resize(n); return result;
}
BinaryValue checksummed(BinaryValue bytes) {
  // Independent slow reference: reduce after every word; do not reproduce the
  // production decoder's block accumulation and overflow handling.
  std::uint32_t a = 0, b = 0;
  for (std::size_t p = 0; p < bytes.size(); p += 2) {
    const auto word = (static_cast<std::uint32_t>(bytes[p]) << 8) |
      (p + 1 < bytes.size() ? bytes[p + 1] : 0U);
    a += word; a = (a & 65535) + (a >> 16);
    b += a; b = (b & 65535) + (b >> 16);
  }
  a = (a & 65535) + (a >> 16); b = (b & 65535) + (b >> 16);
  const auto sum = (b << 16) | a;
  for (int j = 0; j < 4; ++j) bytes.push_back(sum >> (8 * j));
  return bytes;
}
BinaryValue encode(BinaryValue bytes, const std::vector<Hdf5FilterDescription>& filters,
  std::uint32_t mask) {
  for (std::size_t i = 0; i < filters.size(); ++i) {
    if (mask & (1U << i)) continue;
    if (filters[i].id == 2) bytes = shuffled(bytes, filters[i].parameters[0]);
    if (filters[i].id == 1) bytes = compressed(bytes);
    if (filters[i].id == 3) bytes = checksummed(std::move(bytes));
  }
  return bytes;
}
void corrupt(const std::shared_ptr<const Hdf5ChunkDecoder>& decoder, BinaryValue bytes, std::uint32_t mask = 0) {
  auto result = decoder->decode(std::move(bytes), mask);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::CorruptPayload) << result.status().message();
}
} // namespace

TEST(Hdf5ChunkDecoder, EveryCanonicalPipelineSubsetAndEveryMaskRoundTrips) {
  const auto all = pipeline();
  for (unsigned selected = 0; selected < 8; ++selected) {
    auto d = dataset();
    for (unsigned i = 0; i < 3; ++i) if (selected & (1U << i)) d.filters.push_back(all[i]);
    auto decoder = Hdf5ChunkDecoder::compile(d);
    ASSERT_TRUE(decoder.ok());
    for (unsigned mask = 0; mask < (1U << d.filters.size()); ++mask) {
      SCOPED_TRACE(std::to_string(selected) + ":" + std::to_string(mask));
      auto decoded = decoder.value()->decode(encode(data(), d.filters, mask), mask);
      ASSERT_TRUE(decoded.ok()) << decoded.status().message();
      EXPECT_EQ(decoded.value(), native(data()));
    }
  }
}
TEST(Hdf5ChunkDecoder, Float64BigEndianPreservesAllBits) {
  auto d = dataset(8);
  d.descriptor.value_type = LogicalTypeId::Float64; d.element_bytes = 8;
  d.byte_order = Hdf5ByteOrder::Big; d.filters = pipeline(); d.filters[0].parameters = {8};
  // Arbitrary bit patterns include NaN payloads, infinity and negative zero.
  BinaryValue bytes{0x7f,0xf8,0,0,0,0,0,1, 0xff,0xf0,0,0,0,0,0,0, 0x80,0,0,0,0,0,0,0};
  bytes.resize(64);
  auto decoder = Hdf5ChunkDecoder::compile(d);
  ASSERT_TRUE(decoder.ok());
  auto result = decoder.value()->decode(encode(bytes, d.filters, 0), 0);
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result.value(), native(bytes, 8, true));
}
TEST(Hdf5ChunkDecoder, RejectsUnknownDuplicateAndReorderedPipelinesEvenWhenSkipped) {
  for (const auto& f : std::vector<std::vector<Hdf5FilterDescription>>{
      {{99,0,{}}}, {{2,1,{4}},{2,1,{4}}}, {{1,1,{6}},{2,1,{4}}},
      {{2,1,{4}},{3,0,{}},{1,1,{6}}}, {{3,0,{}},{3,0,{}}}, {{1,2,{6}}}}) {
    auto d = dataset(); d.filters = f;
    const auto result = Hdf5ChunkDecoder::compile(d);
    EXPECT_FALSE(result.ok()); EXPECT_EQ(result.status().code(), QueryError::UnsupportedCapability);
  }
}
TEST(Hdf5ChunkDecoder, RejectsNoncanonicalParameters) {
  for (const auto& f : std::vector<Hdf5FilterDescription>{
      {2,1,{}}, {2,1,{8}}, {2,1,{4,0}}, {1,1,{}}, {1,1,{10}}, {1,1,{6,0}}, {3,0,{0}}}) {
    auto d = dataset(); d.filters = {f};
    EXPECT_EQ(Hdf5ChunkDecoder::compile(d).status().code(), QueryError::UnsupportedCapability);
  }
}
TEST(Hdf5ChunkDecoder, RejectsInvalidRepresentationAndOversizedChunksBeforeAllocation) {
  auto d = dataset(); d.chunk_shape = {0};
  EXPECT_EQ(Hdf5ChunkDecoder::compile(d).status().code(), QueryError::ResourceExhausted);
  d = dataset(); d.chunk_shape = {std::numeric_limits<std::uint64_t>::max()};
  EXPECT_EQ(Hdf5ChunkDecoder::compile(d).status().code(), QueryError::ResourceExhausted);
  d = dataset(); d.element_bytes = 8;
  EXPECT_EQ(Hdf5ChunkDecoder::compile(d).status().code(), QueryError::UnsupportedCapability);
  d = dataset(); d.byte_order = Hdf5ByteOrder::Other;
  EXPECT_EQ(Hdf5ChunkDecoder::compile(d).status().code(), QueryError::UnsupportedCapability);
  Hdf5ChunkDecoderConfig c; c.max_decoded_bytes = 511;
  EXPECT_EQ(Hdf5ChunkDecoder::compile(dataset(), c).status().code(), QueryError::ResourceExhausted);
  c.max_decoded_bytes = 0;
  EXPECT_EQ(Hdf5ChunkDecoder::compile(dataset(), c).status().code(), QueryError::InvalidQuery);
}
TEST(Hdf5ChunkDecoder, RejectsUnknownMaskBitsBeforeAnyDecode) {
  auto d = dataset(); d.filters = pipeline();
  auto decoder = Hdf5ChunkDecoder::compile(d).value();
  EXPECT_EQ(decoder->working_set_bytes(100, 8).status().code(), QueryError::CorruptPayload);
  corrupt(decoder, {}, 8);
  auto none = Hdf5ChunkDecoder::compile(dataset()).value();
  corrupt(none, data(), 1);
}
TEST(Hdf5ChunkDecoder, FletcherCanonicalKnownBytesAndStrictCorruption) {
  auto d = dataset(1); d.filters = {{3,0,{}}};
  const auto decoder = Hdf5ChunkDecoder::compile(d).value();
  // BE words 0x0102,0x0304 -> sum1=0x0406 sum2=0x0508, LE checksum.
  const BinaryValue encoded{1,2,3,4,6,4,8,5};
  auto result = decoder->decode(encoded, 0);
  ASSERT_TRUE(result.ok()); EXPECT_EQ(result.value(), native({1,2,3,4}));
  auto swapped = encoded; std::swap(swapped[4],swapped[5]); std::swap(swapped[6],swapped[7]);
  corrupt(decoder, swapped); // no pre-1.6.3 compatibility acceptance
  auto damaged = encoded; damaged[0] ^= 1; corrupt(decoder, damaged);
  corrupt(decoder, {1,2,3});
}
TEST(Hdf5ChunkDecoder, FletcherChecksOddLengthCompressedPayload) {
  auto d = dataset(); d.filters = {{1,1,{6}},{3,0,{}}};
  auto bytes = data();
  BinaryValue encoded;
  // Obtain an odd compressed length so the checksum's final padded byte is exercised.
  for (unsigned i = 0; i < 256; ++i) {
    bytes[0] = i; encoded = compressed(bytes);
    if (encoded.size() % 2) break;
  }
  ASSERT_EQ(encoded.size() % 2, 1U);
  auto result = Hdf5ChunkDecoder::compile(d).value()->decode(checksummed(encoded), 0);
  ASSERT_TRUE(result.ok()) << result.status().message(); EXPECT_EQ(result.value(), native(bytes));
}
TEST(Hdf5ChunkDecoder, FletcherCrossesReductionBoundaries) {
  auto d = dataset(65536); d.filters = {{3,0,{}}};
  auto bytes = data(65536);
  auto result = Hdf5ChunkDecoder::compile(d).value()->decode(checksummed(bytes), 0);
  ASSERT_TRUE(result.ok()); EXPECT_EQ(result.value(), native(bytes));
}
TEST(Hdf5ChunkDecoder, RejectsDeflateTruncationTrailingAndConcatenatedStreams) {
  auto d = dataset(); d.filters = {{1,1,{6}}};
  const auto decoder = Hdf5ChunkDecoder::compile(d).value();
  const auto good = compressed(data());
  for (std::size_t n = 0; n < good.size(); ++n) {
    SCOPED_TRACE(n); corrupt(decoder, BinaryValue(good.begin(), good.begin() + n));
  }
  auto tail = good; tail.push_back(0); corrupt(decoder, tail);
  tail = good; tail.insert(tail.end(), good.begin(), good.end()); corrupt(decoder, tail);
  tail = good; tail[tail.size()-1] ^= 1; corrupt(decoder, tail);
}
TEST(Hdf5ChunkDecoder, RejectsDeflateUnderflowAndBombBeyondDeclaredOutput) {
  auto d = dataset(); d.filters = {{1,1,{6}}};
  const auto decoder = Hdf5ChunkDecoder::compile(d).value();
  corrupt(decoder, compressed(BinaryValue(511, 0)));
  corrupt(decoder, compressed(BinaryValue(513, 0)));
  corrupt(decoder, compressed(BinaryValue(8 * 1024 * 1024, 0)));
}
TEST(Hdf5ChunkDecoder, RejectsDeflatePresetDictionary) {
  z_stream z{}; ASSERT_EQ(deflateInit(&z, 6), Z_OK);
  const BinaryValue dictionary{1,2,3,4}; ASSERT_EQ(deflateSetDictionary(&z,dictionary.data(),dictionary.size()),Z_OK);
  auto bytes = data(); BinaryValue encoded(2048);
  z.next_in = bytes.data(); z.avail_in = bytes.size(); z.next_out = encoded.data(); z.avail_out = encoded.size();
  ASSERT_EQ(deflate(&z,Z_FINISH),Z_STREAM_END); encoded.resize(z.total_out); deflateEnd(&z);
  auto d = dataset(); d.filters = {{1,1,{6}}};
  corrupt(Hdf5ChunkDecoder::compile(d).value(), encoded);
}
TEST(Hdf5ChunkDecoder, RejectsRawAndShuffleSizeMismatch) {
  auto d = dataset();
  const auto raw = Hdf5ChunkDecoder::compile(d).value();
  corrupt(raw, BinaryValue(511)); corrupt(raw, BinaryValue(513));
  d.filters = {{2,1,{4}}}; auto shuffle = Hdf5ChunkDecoder::compile(d).value();
  corrupt(shuffle, BinaryValue(511)); corrupt(shuffle, BinaryValue(513));
}
TEST(Hdf5ChunkDecoder, ZlibAllocatorHasHardScratchLimitAndCanRecover) {
  auto d = dataset(); d.filters = {{1,1,{6}}}; Hdf5ChunkDecoderConfig c;
  c.max_zlib_scratch_bytes = 1;
  auto bounded = Hdf5ChunkDecoder::compile(d,c).value();
  for (int i = 0; i < 3; ++i)
    EXPECT_EQ(bounded->decode(compressed(data()),0).status().code(), QueryError::ResourceExhausted);
  // Enough for inflate state, insufficient for its subsequently allocated
  // 32 KiB sliding window after the first output block.
  d = dataset(65536); d.filters = {{1,1,{6}}}; c.max_zlib_scratch_bytes = 10000;
  auto later = Hdf5ChunkDecoder::compile(d,c).value();
  EXPECT_EQ(later->decode(compressed(data(65536)),0).status().code(), QueryError::ResourceExhausted);
  auto normal = Hdf5ChunkDecoder::compile(d).value()->decode(compressed(data(65536)),0);
  EXPECT_TRUE(normal.ok());
}
TEST(Hdf5ChunkDecoder, WorkingSetCountsEncodedCapacityAndMaskSpecificPeak) {
  auto d = dataset(); d.filters = pipeline(); Hdf5ChunkDecoderConfig c;
  c.max_zlib_scratch_bytes = 100;
  auto decoder = Hdf5ChunkDecoder::compile(d,c).value();
  EXPECT_EQ(decoder->working_set_bytes(1000,7).value(),1000U);
  EXPECT_EQ(decoder->working_set_bytes(1000,6).value(),1512U); // shuffle only
  EXPECT_EQ(decoder->working_set_bytes(1000,0).value(),1613U);
  EXPECT_EQ(decoder->working_set_bytes(1,0).value(),1024U); // unshuffle peak dominates
  EXPECT_EQ(decoder->working_set_bytes(std::numeric_limits<std::uint64_t>::max(),0).status().code(),QueryError::ResourceExhausted);
}
TEST(Hdf5ChunkDecoder, CancelBeforeReadAndInsideEachTransform) {
  auto d = dataset(65536); d.filters = pipeline();
  auto decoder = Hdf5ChunkDecoder::compile(d).value();
  auto bytes = encode(data(65536), d.filters, 0);
  EXPECT_EQ(decoder->decode(bytes,0,[]{return true;}).status().code(),QueryError::Cancelled);
  for (const auto& filters : std::vector<std::vector<Hdf5FilterDescription>>{
      {{3,0,{}}},{{1,1,{6}}},{{2,1,{4}}}}) {
    d.filters = filters; auto dec = Hdf5ChunkDecoder::compile(d).value(); unsigned calls=0;
    auto result = dec->decode(encode(data(65536),filters,0),0,[&]{return ++calls > 2;});
    EXPECT_EQ(result.status().code(),QueryError::Cancelled);
  }
  d.filters.clear(); d.byte_order = Hdf5ByteOrder::Big;
  unsigned calls=0; auto result=Hdf5ChunkDecoder::compile(d).value()->decode(data(65536),0,[&]{return ++calls > 2;});
  EXPECT_EQ(result.status().code(),QueryError::Cancelled);
}
TEST(Hdf5ChunkDecoder, CallbackExceptionsBecomeInternalAndDecoderRemainsReusable) {
  auto d = dataset(); d.filters = pipeline(); auto decoder=Hdf5ChunkDecoder::compile(d).value();
  EXPECT_EQ(decoder->decode(encode(data(),d.filters,0),0,[]{throw 1;return false;}).status().code(),QueryError::Internal);
  EXPECT_TRUE(decoder->decode(encode(data(),d.filters,0),0).ok());
}
TEST(Hdf5ChunkDecoder, ImmutableDecoderSupportsConcurrentReuse) {
  auto d = dataset(); d.filters = pipeline(); auto decoder=Hdf5ChunkDecoder::compile(d).value();
  std::vector<std::future<bool>> work;
  for (unsigned mask=0;mask<8;++mask) work.push_back(std::async(std::launch::async,[=]{
    for (int n=0;n<20;++n) {
      auto result=decoder->decode(encode(data(),d.filters,mask),mask);
      if (!result.ok() || result.value()!=native(data())) return false;
    } return true;
  }));
  for (auto& f:work) EXPECT_TRUE(f.get());
}

TEST(Hdf5ChunkDecoder, GwoscChecksumBeforeDeflateSupportsEveryMask) {
  auto d = dataset(); d.filters = {{3,0,{}},{1,1,{1}}};
  Hdf5ChunkDecoderConfig config; config.max_zlib_scratch_bytes = 100;
  auto bounded = Hdf5ChunkDecoder::compile(d,config);
  ASSERT_TRUE(bounded.ok());
  EXPECT_EQ(bounded.value()->working_set_bytes(1000,0).value(),1617U);
  EXPECT_EQ(bounded.value()->working_set_bytes(1000,1).value(),1613U);
  EXPECT_EQ(bounded.value()->working_set_bytes(1000,2).value(),1000U);
  const auto decoder = Hdf5ChunkDecoder::compile(d).value();
  for (unsigned mask=0; mask<4; ++mask) {
    SCOPED_TRACE(mask);
    auto result = decoder->decode(encode(data(),d.filters,mask),mask);
    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(result.value(),native(data()));
  }
  // Decoding preserves the exact intermediate allocation capacity after
  // dropping the four checksum bytes; the provider charges capacity.
  auto result = decoder->decode(encode(data(),d.filters,0),0);
  ASSERT_TRUE(result.ok()); EXPECT_EQ(result.value().capacity(),516U);
}
TEST(Hdf5ChunkDecoder, GwoscChecksumBeforeDeflateRejectsCorruptIntermediate) {
  auto d = dataset(); d.filters = {{3,0,{}},{1,1,{1}}};
  const auto decoder = Hdf5ChunkDecoder::compile(d).value();
  auto damaged = checksummed(data()); damaged.back() ^= 1;
  corrupt(decoder,compressed(damaged));
  corrupt(decoder,compressed(data())); // missing inner checksum
  damaged = checksummed(data()); damaged.push_back(0);
  corrupt(decoder,compressed(damaged));
  damaged = encode(data(),d.filters,0); damaged.push_back(0);
  corrupt(decoder,damaged);
  damaged = encode(data(),d.filters,0); damaged.pop_back();
  corrupt(decoder,damaged);
  // The same bytes interpreted under a different mask must not silently pass.
  corrupt(decoder,encode(data(),d.filters,0),1);
  corrupt(decoder,encode(data(),d.filters,1),0);
}

TEST(Hdf5ChunkDecoder, DetailedStagesRespectFilterMasksAndRecordFailedAttempts) {
  auto d = dataset(65536);
  d.filters = pipeline();
  const std::uint16_t one = 1;
  d.byte_order = *reinterpret_cast<const std::uint8_t*>(&one) ? Hdf5ByteOrder::Big : Hdf5ByteOrder::Little;
  auto decoder = Hdf5ChunkDecoder::compile(d); ASSERT_TRUE(decoder.ok());
  const auto original = data(65536);
  FragmentDecodeTimings stages;
  auto decoded = decoder.value()->decode(encode(original, d.filters, 0), 0, {}, &stages);
  ASSERT_TRUE(decoded.ok());
  EXPECT_GT(stages.inflate_nanoseconds, 0u);
  EXPECT_GT(stages.shuffle_nanoseconds, 0u);
  EXPECT_GT(stages.checksum_nanoseconds, 0u);
  EXPECT_GT(stages.conversion_nanoseconds, 0u);
  FragmentDecodeTimings skipped;
  auto plain = decoder.value()->decode(original, 7, {}, &skipped); ASSERT_TRUE(plain.ok());
  EXPECT_EQ(skipped.inflate_nanoseconds, 0u);
  EXPECT_EQ(skipped.shuffle_nanoseconds, 0u);
  EXPECT_EQ(skipped.checksum_nanoseconds, 0u);
  EXPECT_GT(skipped.conversion_nanoseconds, 0u);
  auto broken = encode(original, d.filters, 0); broken.back() ^= 1;
  FragmentDecodeTimings failed;
  auto error = decoder.value()->decode(std::move(broken), 0, {}, &failed);
  EXPECT_EQ(error.status().code(), QueryError::CorruptPayload);
  EXPECT_GT(failed.checksum_nanoseconds, 0u);
  EXPECT_EQ(failed.inflate_nanoseconds, 0u);
  EXPECT_EQ(failed.conversion_nanoseconds, 0u);
}

TEST(Hdf5ChunkDecoder, RankFourInt16UsesTwoByteShuffleAndNativeEndian) {
  auto d = dataset(24);
  d.descriptor.value_type = LogicalTypeId::Int32;
  d.descriptor.shape = {1,2,3,4}; d.chunk_shape = d.descriptor.shape;
  d.element_bytes = 2; d.byte_order = Hdf5ByteOrder::Big;
  d.filters = {{2,1,{2}}, {1,1,{6}}, {3,0,{}}};
  auto decoder = Hdf5ChunkDecoder::compile(d); ASSERT_TRUE(decoder.ok()) << decoder.status().message();
  EXPECT_EQ(decoder.value()->decoded_bytes(),48u);
  auto bytes = data(24,2);
  auto result = decoder.value()->decode(encode(bytes,d.filters,0),0);
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result.value(),native(bytes,2,true));
}
