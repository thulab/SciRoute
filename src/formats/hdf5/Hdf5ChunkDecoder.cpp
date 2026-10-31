#include "iotdbfs/formats/hdf5/Hdf5ChunkDecoder.h"

#include <zlib.h>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>

namespace iotdbfs {
namespace {
QueryStatus error(QueryError code, const char* text) {
  return QueryStatus::failure(code, QueryPhase::Execution, text);
}
struct Failure { QueryStatus status; };
class StageTimer {
public:
  explicit StageTimer(std::uint64_t* counter) noexcept : counter_(counter),
    start_(counter ? Clock::now() : Clock::time_point{}) {}
  ~StageTimer() noexcept { if (counter_) *counter_ += static_cast<std::uint64_t>(
    std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start_).count()); }
private:
  using Clock = std::chrono::steady_clock;
  std::uint64_t* counter_;
  Clock::time_point start_;
};
[[noreturn]] void fail(QueryError code, const char* text) {
  throw Failure{error(code, text)};
}
void check_cancel(const std::function<bool()>& cancelled) {
  bool stop = false;
  try { stop = cancelled && cancelled(); }
  catch (...) { fail(QueryError::Internal, "HDF5 chunk decoder cancellation callback failed"); }
  if (stop) fail(QueryError::Cancelled, "HDF5 chunk decode cancelled");
}
std::uint64_t add(std::uint64_t a, std::uint64_t b) {
  if (b > std::numeric_limits<std::uint64_t>::max() - a)
    fail(QueryError::ResourceExhausted, "HDF5 decoder working set overflow");
  return a + b;
}
constexpr std::size_t block = 65536;
struct AllocationBudget {
  // max_align_t preserves the alignment expected by zlib allocation clients.
  struct alignas(std::max_align_t) Header { std::size_t bytes; };
  std::uint64_t limit, live{0};
  bool exhausted{false};
  static voidpf allocate(voidpf opaque, uInt items, uInt size) noexcept {
    auto& b = *static_cast<AllocationBudget*>(opaque);
    if (size && items > (std::numeric_limits<std::size_t>::max() - sizeof(Header)) / size) {
      b.exhausted = true; return nullptr;
    }
    const auto bytes = static_cast<std::size_t>(items) * size + sizeof(Header);
    if (b.live > b.limit || bytes > b.limit - b.live) {
      b.exhausted = true; return nullptr;
    }
    auto* h = static_cast<Header*>(std::calloc(1, bytes));
    if (!h) { b.exhausted = true; return nullptr; }
    h->bytes = bytes; b.live += bytes;
    return h + 1;
  }
  static void release(voidpf opaque, voidpf address) noexcept {
    if (!address) return;
    auto& b = *static_cast<AllocationBudget*>(opaque);
    auto* h = static_cast<Header*>(address) - 1;
    b.live -= h->bytes; std::free(h);
  }
};
struct Inflate {
  z_stream stream{};
  bool initialized{false};
  ~Inflate() { if (initialized) inflateEnd(&stream); }
};
BinaryValue inflate_exact(const BinaryValue& input, std::size_t expected,
  std::uint64_t scratch, const std::function<bool()>& cancelled) {
  AllocationBudget budget{scratch};
  Inflate state;
  state.stream.zalloc = AllocationBudget::allocate;
  state.stream.zfree = AllocationBudget::release;
  state.stream.opaque = &budget;
  const int init = inflateInit(&state.stream);
  if (init != Z_OK)
    fail(init == Z_MEM_ERROR ? QueryError::ResourceExhausted : QueryError::Internal,
      "cannot initialize bounded HDF5 DEFLATE decoder");
  state.initialized = true;
  BinaryValue output(expected);
  // The budget uses vector capacity, not only the requested length. Common
  // vector implementations allocate exactly this size; reject any excess.
  if (output.capacity() != expected)
    fail(QueryError::ResourceExhausted, "HDF5 decoder allocation exceeded reserved capacity");
  std::size_t supplied = 0, produced = 0;
  std::uint8_t overflow = 0;
  for (;;) {
    check_cancel(cancelled);
    auto& z = state.stream;
    if (!z.avail_in && supplied < input.size()) {
      const auto n = std::min(block, input.size() - supplied);
      z.next_in = const_cast<Bytef*>(input.data() + supplied);
      z.avail_in = static_cast<uInt>(n); supplied += n;
    }
    const bool full = produced == expected;
    const auto capacity = full ? std::size_t{1} : std::min(block, expected - produced);
    z.next_out = full ? &overflow : output.data() + produced;
    z.avail_out = static_cast<uInt>(capacity);
    const auto before_in = z.avail_in;
    const int rc = inflate(&z, Z_NO_FLUSH);
    const auto emitted = capacity - z.avail_out;
    if (full && emitted)
      fail(QueryError::CorruptPayload, "HDF5 DEFLATE output exceeds declared chunk bytes");
    produced += emitted;
    if (rc == Z_STREAM_END) {
      if (produced != expected || z.avail_in || supplied != input.size())
        fail(QueryError::CorruptPayload, "HDF5 DEFLATE length mismatch or trailing bytes");
      check_cancel(cancelled);
      return output;
    }
    if (rc == Z_MEM_ERROR || budget.exhausted)
      fail(QueryError::ResourceExhausted, "HDF5 DEFLATE scratch budget exhausted");
    if (rc != Z_OK || (!emitted && before_in == z.avail_in))
      fail(QueryError::CorruptPayload, "invalid or truncated HDF5 DEFLATE stream");
  }
}
std::uint32_t checksum(const BinaryValue& input, std::size_t size,
  const std::function<bool()>& cancelled) {
  // HDF5 1.14.6 H5_checksum_fletcher32: big-endian 16-bit words,
  // odd trailing byte padded on the right; checksum stored little-endian.
  std::uint32_t a = 0, b = 0;
  std::size_t pos = 0;
  while (pos + 1 < size) {
    check_cancel(cancelled);
    const auto end = pos + std::min<std::size_t>(360, (size - pos) / 2) * 2;
    while (pos < end) {
      a += (static_cast<std::uint32_t>(input[pos]) << 8) | input[pos + 1];
      b += a; pos += 2;
    }
    a = (a & 65535) + (a >> 16); b = (b & 65535) + (b >> 16);
  }
  if (pos < size) {
    a += static_cast<std::uint32_t>(input[pos]) << 8; b += a;
    a = (a & 65535) + (a >> 16); b = (b & 65535) + (b >> 16);
  }
  a = (a & 65535) + (a >> 16); b = (b & 65535) + (b >> 16);
  return (b << 16) | a;
}
} // namespace

QueryResult<std::shared_ptr<const Hdf5ChunkDecoder>> Hdf5ChunkDecoder::compile(
  const Hdf5DatasetLayout& d, Hdf5ChunkDecoderConfig config) {
  using Result = QueryResult<std::shared_ptr<const Hdf5ChunkDecoder>>;
  try {
    if (!config.max_decoded_bytes || !config.max_zlib_scratch_bytes)
      fail(QueryError::InvalidQuery, "HDF5 decoder budgets must be positive");
    if (!d.descriptor.supported || d.storage != Hdf5StorageLayout::Chunked ||
        d.chunk_shape.empty() || d.chunk_shape.size() > 4 ||
        d.chunk_shape.size() != d.descriptor.shape.size() ||
        (d.descriptor.value_type != LogicalTypeId::Float32 && d.descriptor.value_type != LogicalTypeId::Float64 && d.descriptor.value_type != LogicalTypeId::Int32) ||
        d.element_bytes != (d.descriptor.value_type == LogicalTypeId::Int32 ? 2 : d.descriptor.value_type == LogicalTypeId::Float32 ? 4 : 8) ||
        (d.byte_order != Hdf5ByteOrder::Little && d.byte_order != Hdf5ByteOrder::Big))
      fail(QueryError::UnsupportedCapability, "unsupported HDF5 chunk representation");
    auto decoder = std::shared_ptr<Hdf5ChunkDecoder>(new Hdf5ChunkDecoder);
    decoder->config_ = config;
    decoder->element_bytes_ = static_cast<std::size_t>(d.element_bytes);
    decoder->decoded_bytes_ = d.element_bytes;
    for (auto n : d.chunk_shape) {
      if (!n || n > config.max_decoded_bytes / decoder->decoded_bytes_)
        fail(QueryError::ResourceExhausted, "HDF5 chunk exceeds decoded byte budget");
      decoder->decoded_bytes_ *= n;
    }
    if (decoder->decoded_bytes_ > static_cast<std::uint64_t>(std::numeric_limits<std::ptrdiff_t>::max()))
      fail(QueryError::ResourceExhausted, "HDF5 chunk exceeds addressable capacity");
    // Public GWOSC files use Fletcher32 before DEFLATE. This exact second
    // profile is validated independently; arbitrary permutations remain rejected.
    const bool checksum_first = d.filters.size() == 2 &&
      d.filters[0].id == 3 && d.filters[1].id == 1;
    int prior = -1;
    for (const auto& filter : d.filters) {
      const int order = filter.id == 2 ? 0 : filter.id == 1 ? 1 : filter.id == 3 ? 2 : -1;
      if (order < 0 || (!checksum_first && order <= prior) || (filter.flags & ~1U))
        fail(QueryError::UnsupportedCapability, "unsupported HDF5 filter order or duplicate filter");
      if ((filter.id == 2 && filter.parameters != std::vector<std::uint32_t>{static_cast<std::uint32_t>(d.element_bytes)}) ||
          (filter.id == 1 && (filter.parameters.size() != 1 || filter.parameters[0] > 9)) ||
          (filter.id == 3 && !filter.parameters.empty()))
        fail(QueryError::UnsupportedCapability, "unsupported HDF5 filter parameters");
      decoder->filters_.push_back(filter.id); prior = order;
    }
    const std::uint16_t one = 1;
    const bool little = *reinterpret_cast<const std::uint8_t*>(&one) == 1;
    decoder->swap_endian_ = little != (d.byte_order == Hdf5ByteOrder::Little);
    return Result::success(std::move(decoder));
  } catch (const Failure& e) { return Result::failure(e.status); }
    catch (const std::bad_alloc&) { return Result::failure(error(QueryError::ResourceExhausted, "HDF5 decoder allocation failed")); }
    catch (...) { return Result::failure(error(QueryError::Internal, "HDF5 decoder compilation failed")); }
}
std::uint64_t Hdf5ChunkDecoder::inflated_bytes(std::uint32_t mask) const {
  const bool checksum_first = filters_.size() == 2 && filters_[0] == 3 &&
    filters_[1] == 1 && !(mask & 1U);
  const auto bytes = add(decoded_bytes_, checksum_first ? 4 : 0);
  if (bytes > static_cast<std::uint64_t>(std::numeric_limits<std::ptrdiff_t>::max()))
    fail(QueryError::ResourceExhausted, "HDF5 decoded intermediate exceeds addressable capacity");
  return bytes;
}
QueryResult<std::uint64_t> Hdf5ChunkDecoder::working_set_bytes(
  std::uint64_t encoded, std::uint32_t mask) const {
  using Result = QueryResult<std::uint64_t>;
  try {
    if (mask >> filters_.size()) fail(QueryError::CorruptPayload, "HDF5 chunk filter mask has undeclared bits");
    bool inflate = false, shuffle = false;
    for (std::size_t i = 0; i < filters_.size(); ++i) {
      if (mask & (1U << i)) continue;
      inflate |= filters_[i] == 1; shuffle |= filters_[i] == 2;
    }
    std::uint64_t peak = encoded;
    if (inflate) peak = add(add(add(encoded, inflated_bytes(mask)), config_.max_zlib_scratch_bytes), 1);
    if (shuffle) peak = std::max(peak, add(inflate ? decoded_bytes_ : encoded, decoded_bytes_));
    return Result::success(peak);
  } catch (const Failure& e) { return Result::failure(e.status); }
}
QueryResult<BinaryValue> Hdf5ChunkDecoder::decode(BinaryValue encoded,
  std::uint32_t mask, const std::function<bool()>& cancelled, FragmentDecodeTimings* timings) const {
  using Result = QueryResult<BinaryValue>;
  try {
    const auto peak = working_set_bytes(encoded.capacity(), mask);
    if (!peak.ok()) return Result::failure(peak.status());
    check_cancel(cancelled);
    for (std::size_t i = filters_.size(); i-- > 0;) {
      if (mask & (1U << i)) continue;
      if (filters_[i] == 3) {
        StageTimer timer(timings ? &timings->checksum_nanoseconds : nullptr);
        if (encoded.size() < 4) fail(QueryError::CorruptPayload, "truncated HDF5 Fletcher32 checksum");
        const auto n = encoded.size() - 4;
        const auto stored = static_cast<std::uint32_t>(encoded[n]) |
          (static_cast<std::uint32_t>(encoded[n + 1]) << 8) |
          (static_cast<std::uint32_t>(encoded[n + 2]) << 16) |
          (static_cast<std::uint32_t>(encoded[n + 3]) << 24);
        if (stored != checksum(encoded, n, cancelled))
          fail(QueryError::CorruptPayload, "HDF5 Fletcher32 checksum mismatch");
        encoded.resize(n);
      } else if (filters_[i] == 1) {
        StageTimer timer(timings ? &timings->inflate_nanoseconds : nullptr);
        // Assignment releases the old encoded buffer before unshuffle.
        encoded = inflate_exact(encoded, static_cast<std::size_t>(inflated_bytes(mask)),
          config_.max_zlib_scratch_bytes, cancelled);
      } else {
        StageTimer timer(timings ? &timings->shuffle_nanoseconds : nullptr);
        if (encoded.size() != decoded_bytes_)
          fail(QueryError::CorruptPayload, "HDF5 shuffle length differs from full chunk");
        BinaryValue output(static_cast<std::size_t>(decoded_bytes_));
        if (output.capacity() != decoded_bytes_)
          fail(QueryError::ResourceExhausted, "HDF5 decoder allocation exceeded reserved capacity");
        const auto count = output.size() / element_bytes_;
        for (std::size_t byte = 0; byte < element_bytes_; ++byte) {
          for (std::size_t start = 0; start < count; start += block) {
            check_cancel(cancelled);
            const auto end = start + std::min(block, count - start);
            for (auto j = start; j < end; ++j)
              output[j * element_bytes_ + byte] = encoded[byte * count + j];
          }
        }
        encoded = std::move(output);
      }
    }
    if (encoded.size() != decoded_bytes_)
      fail(QueryError::CorruptPayload, "HDF5 decoded length differs from full chunk");
    if (swap_endian_) {
      StageTimer timer(timings ? &timings->conversion_nanoseconds : nullptr);
      for (std::size_t start = 0; start < encoded.size(); start += block) {
        check_cancel(cancelled);
        const auto end = start + std::min(block, encoded.size() - start);
        for (auto p = start; p < end; p += element_bytes_)
          std::reverse(encoded.data() + p, encoded.data() + p + element_bytes_);
      }
    }
    check_cancel(cancelled);
    return Result::success(std::move(encoded));
  } catch (const Failure& e) { return Result::failure(e.status); }
    catch (const std::bad_alloc&) { return Result::failure(error(QueryError::ResourceExhausted, "HDF5 decoder allocation failed")); }
    catch (...) { return Result::failure(error(QueryError::Internal, "HDF5 chunk decoder callback failed")); }
}
} // namespace iotdbfs
