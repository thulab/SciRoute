#include "iotdbfs/formats/arrow/ArrowFragmentProvider.h"

#include <openssl/evp.h>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>

namespace iotdbfs {
namespace {
QueryStatus error(QueryError code, const char* message) {
  return QueryStatus::failure(code, QueryPhase::Execution, message);
}
std::uint64_t bitmap_bytes(std::uint64_t n) { return n / 8 + (n % 8 != 0); }
using FragmentResult = QueryResult<std::shared_ptr<const ArrowDecodedFragment>>;
using BatchResult = QueryResult<std::vector<ArrowFragmentBatchItem>>;
using LoadResult = QueryResult<std::shared_ptr<const DecodedFragment>>;
class BatchPayloadReadTimer {
public:
  BatchPayloadReadTimer(const FragmentLoadContext& context,
      std::uint64_t bytes, std::uint64_t ranges)
    : context_(context), bytes_(bytes), ranges_(ranges),
      start_(std::chrono::steady_clock::now()) { context_.begin_payload_read(); }
  ~BatchPayloadReadTimer() {
    context_.record_payload_read(bytes_, ranges_, static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - start_).count()));
  }
private:
  const FragmentLoadContext& context_;
  std::uint64_t bytes_, ranges_;
  std::chrono::steady_clock::time_point start_;
};
void number(std::string& key, std::uint64_t n) {
  for (unsigned i = 0; i < 8; ++i) key.push_back(static_cast<char>(n >> (i * 8)));
}
void string(std::string& key, const std::string& value) {
  number(key, value.size()); key += value;
}
QueryResult<std::string> hex_identity(const std::string& key) {
  unsigned char digest[EVP_MAX_MD_SIZE]; unsigned length = 0;
  if (EVP_Digest(key.data(), key.size(), digest, &length,
        EVP_sha256(), nullptr) != 1 || length != 32)
    return QueryResult<std::string>::failure(error(QueryError::Internal,
      "Arrow fragment identity hashing failed"));
  constexpr char hex[] = "0123456789abcdef";
  std::string result;
  result.reserve(64);
  for (unsigned i = 0; i < length; ++i) {
    result.push_back(hex[digest[i] >> 4]);
    result.push_back(hex[digest[i] & 15]);
  }
  return QueryResult<std::string>::success(std::move(result));
}
struct LayoutFragment final : DecodedFragment {
  ArrowLayoutDocument document;
  std::uint64_t resident_bytes() const override {
    std::uint64_t n = sizeof(document);
    n += document.fields.capacity() * sizeof(ArrowNativeField);
    for (const auto& f : document.fields)
      n += f.name.capacity() + f.native_type.capacity() + f.unsupported_reason.capacity();
    n += document.messages.capacity() * sizeof(ArrowMessageLayout);
    for (const auto& m : document.messages) {
      n += m.metadata.capacity() + m.nodes.capacity() * sizeof(ArrowFieldNode);
      for (const auto& f : m.nodes)
        n += f.path.capacity() * sizeof(std::uint32_t) + f.buffers.capacity() * sizeof(ArrowBufferSpan);
    }
    n += document.batches.capacity() * sizeof(ArrowBatchLayout);
    for (const auto& b : document.batches) {
      n += b.dictionary_epochs.size() * (sizeof(std::pair<const std::int64_t, std::uint64_t>) + 4 * sizeof(void*));
      n += b.arrays.capacity() * sizeof(ArrowArrayIndex);
      for (const auto& a : b.arrays) {
        n += a.levels.capacity() * sizeof(ArrowListLevelIndex);
        for (const auto& l : a.levels) n += l.offsets.capacity() * sizeof(std::uint64_t) + l.validity.capacity();
      }
    }
    return n;
  }
};
} // namespace

struct ArrowFragmentProvider::Impl {
  struct BatchPlan {
    std::string key, key_hex;
    std::uint64_t begin{0}, count{0}, leaf_count{0}, null_count{0};
    std::uint64_t first_bitmap_byte{0}, mask_size{0};
    ArrowBufferSpan values, bitmap;
    ArrowBufferCompression codec{ArrowBufferCompression::None};
    bool has_bitmap{false};
    FragmentMemory memory;
  };
  std::shared_ptr<const QuerySource> source;
  std::shared_ptr<const BinaryValue> bytes;
  std::shared_ptr<QueryRuntime> runtime;
  std::shared_ptr<const LayoutFragment> layout;
  QueryExecutionOptions options;
  ArrowDecodeConfig config;
  CancellationToken cancellation;
  bool owns_cancellation{true};
  ArrowDecodeMetrics metrics;
  QueryStatus status;
  std::string identity;
  std::string last_fragment_key;

  Impl(std::shared_ptr<const QuerySource> owned, std::shared_ptr<const BinaryValue> serialized,
      std::shared_ptr<QueryRuntime> service, QueryExecutionOptions request,
      ArrowDecodeConfig decode, std::optional<CancellationToken> token)
    : source(std::move(owned)), bytes(std::move(serialized)), runtime(std::move(service)),
      options(request), config(decode),
      cancellation(token ? *token : CancellationToken{}),
      owns_cancellation(!token.has_value()) {
    if (!source || !bytes || !runtime) {
      status = error(QueryError::InvalidQuery, "Arrow provider requires owned source, layout and runtime"); return;
    }
    if (!config.tile_elements || !config.max_buffer_bytes) {
      status = error(QueryError::InvalidQuery, "Arrow decoder limits must be positive"); return;
    }
    const auto& m = source->manifest();
    if (bytes->size() != m.layout_size || bytes->size() > ArrowLayoutLimits{}.max_layout_bytes) {
      status = error(QueryError::StalePlan, "Arrow serialized layout size changed"); return;
    }
    // SHA-256 includes schema, offsets, null maps, dictionary epochs and codecs.
    unsigned char digest[EVP_MAX_MD_SIZE]; unsigned length = 0;
    if (EVP_Digest(bytes->data(), bytes->size(), digest, &length, EVP_sha256(), nullptr) != 1 || length != 32) {
      status = error(QueryError::Internal, "Arrow layout identity hashing failed"); return;
    }
    identity = "arrow-decoder/1";
    number(identity, source->storage_namespace());
    string(identity, m.key.path); number(identity, static_cast<std::uint64_t>(m.key.version));
    number(identity, m.revision);
    number(identity, m.payload_size); number(identity, m.layout_size);
    identity.append(reinterpret_cast<const char*>(digest), length);
    // Include options affecting whether/how a physical buffer may be decoded.
    number(identity, config.max_buffer_bytes);
  }

  QueryStatus ensure_layout() {
    if (layout) return QueryStatus::success();
    auto serialized = bytes; const auto size = source->manifest().payload_size;
    // The binary codec stores counts explicitly and bounds every allocation.
    // 16x wire size + 64 KiB conservatively covers its vector/node expansion,
    // including parse temporaries. Planning artifacts are separately bounded.
    const auto reservation = bytes->size() * 16ULL + 65536;
    auto result = runtime->cache().get(identity + "layout", {reservation, bytes->size() * 2ULL + 65536},
      [serialized, size](const FragmentLoadContext& context) -> LoadResult {
        if (context.cancelled()) return LoadResult::failure(error(QueryError::Cancelled, "Arrow layout decoding cancelled"));
        auto parsed = detail::reconstruct_validated_arrow_layout(*serialized, size);
        if (!parsed.ok()) return LoadResult::failure(parsed.status());
        auto fragment = std::make_shared<LayoutFragment>();
        fragment->document = std::move(parsed.value());
        return LoadResult::success(std::move(fragment));
      }, cancellation);
    if (!result.ok()) return result.status();
    layout = result.value().as<LayoutFragment>();
    if (!layout) return error(QueryError::Internal, "Arrow layout cache type mismatch");
    return QueryStatus::success();
  }

  BatchResult get_batch(const std::vector<ArrowFragmentDemand>& demands) {
    if (cancellation.cancelled()) return BatchResult::failure(error(QueryError::Cancelled, "Arrow query cancelled"));
    if (!status.ok()) return BatchResult::failure(status);
    if (demands.empty()) return BatchResult::success({});
    auto ready = ensure_layout();
    if (!ready.ok()) return BatchResult::failure(ready);
    const auto& doc = layout->document;
    // Small independently compressed training blocks are batched like tiles.
    // Oversized physical buffers retain the scalar loader's existing bounds.
    auto scalar_batch = [&]() -> BatchResult {
      std::vector<ArrowFragmentBatchItem> result;
      result.reserve(demands.size());
      for (const auto& item : demands) {
        auto fragment = get(item.batch, item.field, item.leaf_position, item.demand_elements);
        if (!fragment.ok()) return BatchResult::failure(fragment.status());
        result.push_back({std::move(fragment.value()), last_fragment_key});
      }
      return BatchResult::success(std::move(result));
    };
    for (const auto& d : demands)
      if (d.batch >= doc.batches.size())
        return BatchResult::failure(error(QueryError::InvalidQuery, "Arrow record batch index out of range"));
    if (demands.size() > runtime->config().max_read_ranges_per_load ||
        demands.size() > 64)
      return BatchResult::failure(error(QueryError::ResourceExhausted,
        "Arrow training fragment batch exceeds tile count limit"));

    std::vector<BatchPlan> plans;
    std::vector<std::size_t> positions;
    std::vector<FragmentBatchItem> cache_items;
    plans.reserve(demands.size());
    positions.reserve(demands.size());
    cache_items.reserve(demands.size());
    auto logical_ranges = metrics.logical_ranges;
    auto logical_bytes = metrics.logical_bytes;
    for (const auto& d : demands) {
      const auto& batch = doc.batches[d.batch];
      const auto found = std::find_if(batch.arrays.begin(), batch.arrays.end(),
        [&d](const auto& array) { return array.field_index == d.field; });
      if (found == batch.arrays.end())
        return BatchResult::failure(error(QueryError::UnknownField, "Arrow field has no supported leaf array"));
      const auto& array = *found;
      if (d.leaf_position >= array.leaf_count || !d.demand_elements)
        return BatchResult::failure(error(QueryError::InvalidQuery, "Arrow leaf position or demand out of range"));
      const auto codec = doc.messages[batch.message_index].compression;
      const bool compressed = codec != ArrowBufferCompression::None;
      const auto begin = compressed ? 0 : d.leaf_position - d.leaf_position % config.tile_elements;
      const auto count = compressed ? array.leaf_count : std::min(config.tile_elements, array.leaf_count - begin);
      const auto mask_size = array.leaf_null_count ? bitmap_bytes(count) : 0;
      if (count > std::numeric_limits<std::uint64_t>::max() / 4 ||
          count > std::vector<float>().max_size() || count * 4 > config.max_buffer_bytes ||
          mask_size > config.max_buffer_bytes)
        return BatchResult::failure(error(QueryError::ResourceExhausted, "Arrow fragment exceeds decoded byte limit"));
      const auto consumed = std::min(d.demand_elements, begin + count - d.leaf_position);
      const auto logical = consumed * 4 + (array.leaf_null_count ? bitmap_bytes(consumed) : 0);
      if (logical_ranges >= options.max_ranges ||
          logical > options.max_payload_bytes - logical_bytes)
        return BatchResult::failure(error(QueryError::ResourceExhausted, "Arrow logical request budget exhausted"));
      ++logical_ranges;
      logical_bytes += logical;

      BatchPlan plan;
      plan.codec = codec;
      plan.begin = begin;
      plan.count = count;
      plan.leaf_count = array.leaf_count;
      plan.null_count = array.leaf_null_count;
      plan.mask_size = mask_size;
      plan.values = array.leaf_values;
      if (!compressed) {
        if (begin > plan.values.length / 4 || count * 4 > plan.values.length - begin * 4)
          return BatchResult::failure(error(QueryError::CorruptPayload, "Arrow float tile escapes buffer"));
        plan.values.offset += begin * 4;
        plan.values.length = count * 4;
      }
      plan.has_bitmap = array.leaf_null_count != 0;
      if (plan.has_bitmap) {
        plan.bitmap = array.leaf_validity;
        plan.first_bitmap_byte = compressed ? 0 : begin / 8;
        if (!compressed) {
          const auto needed = bitmap_bytes(begin + count) - plan.first_bitmap_byte;
          if (plan.first_bitmap_byte > plan.bitmap.length ||
              needed > plan.bitmap.length - plan.first_bitmap_byte)
            return BatchResult::failure(error(QueryError::CorruptPayload, "Arrow bitmap tile escapes buffer"));
          plan.bitmap.offset += plan.first_bitmap_byte;
          plan.bitmap.length = needed;
        }
      }
      if (plan.values.length > config.max_buffer_bytes || plan.bitmap.length > config.max_buffer_bytes ||
          plan.bitmap.length > std::numeric_limits<std::uint64_t>::max() - plan.values.length)
        return BatchResult::failure(error(QueryError::ResourceExhausted, "Arrow physical buffer exceeds byte limit"));
      plan.memory = {count * 4 + mask_size, plan.values.length + plan.bitmap.length};
      plan.key = identity + "leaf";
      number(plan.key, d.batch); number(plan.key, d.field);
      number(plan.key, begin); number(plan.key, count);
      number(plan.key, compressed ? 0 : config.tile_elements);
      auto key_hex = hex_identity(plan.key);
      if (!key_hex.ok()) return BatchResult::failure(key_hex.status());
      plan.key_hex = std::move(key_hex.value());
      auto duplicate = std::find_if(plans.begin(), plans.end(),
        [&plan](const BatchPlan& prior) { return prior.key == plan.key; });
      if (duplicate != plans.end()) {
        positions.push_back(static_cast<std::size_t>(duplicate - plans.begin()));
      } else {
        positions.push_back(plans.size());
        cache_items.push_back({plan.key, plan.memory});
        plans.push_back(std::move(plan));
      }
    }

    std::uint64_t shared_temporary = 0;
    for (const auto& plan : plans) {
      const auto bytes = plan.values.length + plan.bitmap.length;
      if (bytes > std::numeric_limits<std::uint64_t>::max() - shared_temporary)
        return BatchResult::failure(error(QueryError::ResourceExhausted, "Arrow batch byte count overflow"));
      shared_temporary += bytes;
    }
    if (shared_temporary > kMaxPayloadBytesPerBatch) {
      const bool compressed = std::any_of(plans.begin(), plans.end(), [](const auto& plan) {
        return plan.codec != ArrowBufferCompression::None;
      });
      if (compressed) return scalar_batch();
      return BatchResult::failure(error(QueryError::ResourceExhausted,
        "Arrow training fragment batch exceeds transport byte limit"));
    }
    // Fetched ranges and per-plan copied slices are charged separately. Decode
    // is sequential: at most one codec output (including permitted trailing
    // bytes) coexists with those buffers and the final resident fragments.
    if (std::any_of(plans.begin(), plans.end(), [](const auto& plan) {
          return plan.codec != ArrowBufferCompression::None;
        })) {
      if (config.max_buffer_bytes > std::numeric_limits<std::uint64_t>::max() - shared_temporary)
        return BatchResult::failure(error(QueryError::ResourceExhausted, "Arrow batch temporary bytes overflow"));
      shared_temporary += config.max_buffer_bytes;
    }

    metrics.logical_ranges = logical_ranges;
    metrics.logical_bytes = logical_bytes;

    auto owned = source;
    const auto limits = config;
    auto loaded = runtime->cache().get_batch(cache_items, shared_temporary,
      [owned, limits, plans](const std::vector<std::size_t>& misses,
          const FragmentLoadContext& context)
          -> QueryResult<std::vector<std::shared_ptr<const DecodedFragment>>> {
        using Result = QueryResult<std::vector<std::shared_ptr<const DecodedFragment>>>;
        if (context.cancelled()) return Result::failure(error(QueryError::Cancelled, "Arrow decoding cancelled"));
        std::vector<ArrowBufferSpan> spans;
        spans.reserve(misses.size() * 2);
        for (const auto index : misses) {
          spans.push_back(plans[index].values);
          if (plans[index].has_bitmap && plans[index].bitmap.length)
            spans.push_back(plans[index].bitmap);
        }
        std::sort(spans.begin(), spans.end(), [](const auto& a, const auto& b) {
          return a.offset < b.offset || (a.offset == b.offset && a.length < b.length);
        });
        std::vector<ArrowBufferSpan> merged;
        for (const auto& span : spans) {
          if (!span.length) continue;
          if (!merged.empty() && span.offset <= merged.back().offset + merged.back().length) {
            const auto end = std::max(merged.back().offset + merged.back().length,
              span.offset + span.length);
            merged.back().length = end - merged.back().offset;
          } else merged.push_back(span);
        }
        std::vector<BinaryValue> fetched(merged.size());
        for (std::size_t start = 0; start < merged.size();) {
          StructuredBlobPayloadRanges ranges;
          std::uint64_t total = 0;
          while (start + ranges.size() < merged.size() &&
              ranges.size() < kMaxPayloadRangesPerBatch) {
            const auto& span = merged[start + ranges.size()];
            if (span.length > kMaxPayloadBytesPerBatch)
              return Result::failure(error(QueryError::ResourceExhausted,
                "Arrow contiguous range exceeds transport byte limit"));
            if (span.length > kMaxPayloadBytesPerBatch - total) break;
            ranges.push_back({span.offset, span.length});
            total += span.length;
          }
          if (ranges.empty()) return Result::failure(error(QueryError::Internal, "empty Arrow range wave"));
          auto recorded = context.record_io(total, ranges.size());
          if (!recorded.ok()) return Result::failure(recorded);
          auto response = [&] {
            BatchPayloadReadTimer timer(context, total, ranges.size());
            return owned->read_payload_ranges(ranges);
          }();
          if (!response.ok()) return Result::failure(response.status());
          if (response.value().size() != ranges.size())
            return Result::failure(error(QueryError::ReadFailure, "short Arrow multi-range response"));
          for (std::size_t i = 0; i < ranges.size(); ++i) {
            if (response.value()[i].size() != ranges[i].length)
              return Result::failure(error(QueryError::ReadFailure, "short Arrow payload range"));
            fetched[start + i] = std::move(response.value()[i]);
          }
          start += ranges.size();
          if (context.cancelled()) return Result::failure(error(QueryError::Cancelled, "Arrow decoding cancelled"));
        }
        auto slice = [&merged, &fetched](ArrowBufferSpan span) -> QueryResult<BinaryValue> {
          for (std::size_t i = 0; i < merged.size(); ++i) {
            const auto& container = merged[i];
            if (span.offset < container.offset) continue;
            const auto relative = span.offset - container.offset;
            if (relative <= container.length &&
                span.length <= container.length - relative) {
              const auto begin = static_cast<std::size_t>(relative);
              return QueryResult<BinaryValue>::success({
                fetched[i].begin() + begin, fetched[i].begin() + begin + span.length});
            }
          }
          return QueryResult<BinaryValue>::failure(error(QueryError::Internal,
            "Arrow batch response omits planned buffer"));
        };
        ArrowRangeReader decoder(*owned, {}, limits, context.cancellation());
        std::vector<std::shared_ptr<const DecodedFragment>> result;
        result.reserve(misses.size());
        for (const auto index : misses) {
          struct TileAccounting {
            const FragmentLoadContext& context;
            ArrowRangeReader& decoder;
            std::uint64_t bytes, decompressions;
            ~TileAccounting() {
              const auto& current = decoder.metrics();
              context.record_decode(current.decoded_bytes - bytes,
                current.decompressions - decompressions);
            }
          } accounting{context, decoder, decoder.metrics().decoded_bytes,
              decoder.metrics().decompressions};
          const auto& plan = plans[index];
          auto raw_values = slice(plan.values);
          if (!raw_values.ok()) return Result::failure(raw_values.status());
          auto values = decoder.decode_buffer(std::move(raw_values.value()),
            plan.codec, plan.count * 4);
          if (!values.ok()) return Result::failure(values.status());
          auto fragment = std::make_shared<ArrowDecodedFragment>();
          fragment->begin = plan.begin;
          fragment->end = plan.begin + plan.count;
          fragment->values.resize(static_cast<std::size_t>(plan.count));
          for (std::uint64_t i = 0; i < plan.count; ++i) {
            if (i % 4096 == 0 && context.cancelled())
              return Result::failure(error(QueryError::Cancelled, "Arrow decoding cancelled"));
            const auto* p = values.value().data() + i * 4;
            const std::uint32_t bits = std::uint32_t(p[0]) | (std::uint32_t(p[1]) << 8) |
              (std::uint32_t(p[2]) << 16) | (std::uint32_t(p[3]) << 24);
            std::memcpy(&fragment->values[static_cast<std::size_t>(i)], &bits, sizeof(bits));
          }
          BinaryValue().swap(values.value());
          if (plan.has_bitmap) {
            auto raw_bitmap = slice(plan.bitmap);
            if (!raw_bitmap.ok()) return Result::failure(raw_bitmap.status());
            auto bitmap = decoder.decode_buffer(std::move(raw_bitmap.value()),
              plan.codec, plan.codec == ArrowBufferCompression::None ? plan.bitmap.length : bitmap_bytes(plan.leaf_count));
            if (!bitmap.ok()) return Result::failure(bitmap.status());
            fragment->validity.assign(static_cast<std::size_t>(plan.mask_size), 0);
            std::uint64_t nulls = 0;
            for (std::uint64_t i = 0; i < plan.count; ++i) {
              if (i % 4096 == 0 && context.cancelled())
                return Result::failure(error(QueryError::Cancelled, "Arrow decoding cancelled"));
              const auto source_bit = plan.begin + i - plan.first_bitmap_byte * 8;
              const bool valid = (bitmap.value()[source_bit / 8] & (1u << (source_bit % 8))) != 0;
              if (valid) fragment->validity[i / 8] |= std::uint8_t(1u << (i % 8));
              else ++nulls;
            }
            if (plan.begin == 0 && plan.count == plan.leaf_count && nulls != plan.null_count)
              return Result::failure(error(QueryError::CorruptPayload, "Arrow leaf bitmap contradicts null count"));
          }
          context.record_copy(fragment->resident_bytes());
          result.push_back(std::move(fragment));
        }
        if (context.cancelled()) return Result::failure(error(QueryError::Cancelled, "Arrow decoding cancelled"));
        return Result::success(std::move(result));
      }, cancellation);
    if (!loaded.ok()) return BatchResult::failure(loaded.status());
    std::vector<ArrowFragmentBatchItem> result;
    result.reserve(demands.size());
    for (const auto index : positions) {
      auto fragment = loaded.value()[index].as<ArrowDecodedFragment>();
      if (!fragment) return BatchResult::failure(error(QueryError::Internal, "Arrow fragment cache type mismatch"));
      result.push_back({std::move(fragment), plans[index].key_hex});
    }
    last_fragment_key = result.back().key_hex;
    return BatchResult::success(std::move(result));
  }

  FragmentResult get(std::uint64_t batch_index, std::uint32_t field, std::uint64_t position,
      std::uint64_t demand) {
    if (cancellation.cancelled()) return FragmentResult::failure(error(QueryError::Cancelled, "Arrow query cancelled"));
    if (!status.ok()) return FragmentResult::failure(status);
    auto ready = ensure_layout();
    if (!ready.ok()) return FragmentResult::failure(ready);
    const auto& doc = layout->document;
    if (batch_index >= doc.batches.size())
      return FragmentResult::failure(error(QueryError::InvalidQuery, "Arrow record batch index out of range"));
    const auto& batch = doc.batches[batch_index];
    const auto found = std::find_if(batch.arrays.begin(), batch.arrays.end(),
      [field](const auto& array) { return array.field_index == field; });
    if (found == batch.arrays.end())
      return FragmentResult::failure(error(QueryError::UnknownField, "Arrow field has no supported leaf array"));
    const auto& array = *found;
    if (position >= array.leaf_count || !demand)
      return FragmentResult::failure(error(QueryError::InvalidQuery, "Arrow leaf position or demand out of range"));
    const auto codec = doc.messages[batch.message_index].compression;
    const bool compressed = codec != ArrowBufferCompression::None;
    const auto begin = compressed ? 0 : position - position % config.tile_elements;
    const auto count = compressed ? array.leaf_count : std::min(config.tile_elements, array.leaf_count - begin);
    const auto mask_size = array.leaf_null_count ? bitmap_bytes(count) : 0;
    if (count > std::numeric_limits<std::uint64_t>::max() / 4 ||
        count > std::vector<float>().max_size() || count * 4 > config.max_buffer_bytes || mask_size > config.max_buffer_bytes)
      return FragmentResult::failure(error(QueryError::ResourceExhausted, "Arrow fragment exceeds decoded byte limit"));
    const auto consumed = std::min(demand, begin + count - position);
    const auto logical = consumed * 4 + (array.leaf_null_count ? bitmap_bytes(consumed) : 0);
    if (metrics.logical_ranges >= options.max_ranges || logical > options.max_payload_bytes - metrics.logical_bytes)
      return FragmentResult::failure(error(QueryError::ResourceExhausted, "Arrow logical request budget exhausted"));
    ++metrics.logical_ranges; metrics.logical_bytes += logical;
    std::string key = identity + "leaf";
    number(key, batch_index); number(key, field); number(key, begin); number(key, count);
    // Whole compressed buffers have the same key across tile-size preferences.
    number(key, compressed ? 0 : config.tile_elements);
    auto key_hex = hex_identity(key);
    if (!key_hex.ok()) return FragmentResult::failure(key_hex.status());
    const auto resident = count * 4 + mask_size;
    // While decoding a compressed buffer input and output coexist. They may
    // include native trailing bytes, hence reserve the codec cap before reads.
    const auto temporary = compressed ? config.max_buffer_bytes * 2 :
      std::max(count * 4, bitmap_bytes(begin + count) - begin / 8);
    if (compressed && config.max_buffer_bytes > std::numeric_limits<std::uint64_t>::max() / 2)
      return FragmentResult::failure(error(QueryError::ResourceExhausted, "Arrow temporary reservation overflow"));
    const auto values_span_original = array.leaf_values;
    const auto validity_span_original = array.leaf_validity;
    const auto leaf_count = array.leaf_count, null_count = array.leaf_null_count;
    auto owned = source; auto limits = config; const auto service = runtime->config();
    auto loaded = runtime->cache().get(key, {resident, temporary},
      [owned, limits, service, codec, compressed, begin, count, mask_size,
       values_span_original, validity_span_original, leaf_count, null_count]
      (const FragmentLoadContext& context) -> LoadResult {
        QueryExecutionOptions physical;
        physical.max_ranges = service.max_read_ranges_per_load;
        physical.max_payload_bytes = service.max_read_bytes_per_load;
        ArrowRangeReader reader(*owned, physical, limits, context.cancellation(), &context);
        struct Accounting {
          const FragmentLoadContext& context; ArrowRangeReader& reader;
          ~Accounting() {
            const auto& m = reader.metrics();
            context.record_decode(m.decoded_bytes, m.decompressions);
          }
        } accounting{context, reader};
    ArrowBufferSpan values_span = values_span_original;
    if (!compressed) {
      // The validated layout bounds the full physical buffer; subtraction
      // checks keep partial-span arithmetic safe as well.
      if (begin > values_span.length / 4 || count * 4 > values_span.length - begin * 4)
        return LoadResult::failure(error(QueryError::CorruptPayload, "Arrow float tile escapes buffer"));
      values_span.offset += begin * 4;
      values_span.length = count * 4;
    }
    auto bytes = reader.decode(values_span, codec, count * 4);
    if (!bytes.ok()) return LoadResult::failure(bytes.status());
    auto fragment = std::make_shared<ArrowDecodedFragment>();
    fragment->begin = begin;
    fragment->end = begin + count;
    fragment->values.resize(static_cast<std::size_t>(count));
    static_assert(sizeof(float) == 4 && std::numeric_limits<float>::is_iec559,
      "Arrow float32 requires IEEE-754 float");
    for (std::uint64_t i = 0; i < count; ++i) {
      if (i % 4096 == 0 && reader.cancelled())
        return LoadResult::failure(error(QueryError::Cancelled, "Arrow decoding cancelled"));
      const auto* p = bytes.value().data() + i * 4;
      const std::uint32_t bits = std::uint32_t(p[0]) | (std::uint32_t(p[1]) << 8) |
        (std::uint32_t(p[2]) << 16) | (std::uint32_t(p[3]) << 24);
      std::memcpy(&fragment->values[static_cast<std::size_t>(i)], &bits, sizeof(bits));
    }
    // Release the raw float bytes before reading/decompressing a bitmap.
    BinaryValue().swap(bytes.value());
    if (null_count) {
      auto mask_span = validity_span_original;
      const auto first_byte = compressed ? 0 : begin / 8;
      const auto needed = compressed ? bitmap_bytes(leaf_count) : bitmap_bytes(begin + count) - first_byte;
      if (!compressed) {
        if (first_byte > mask_span.length || needed > mask_span.length - first_byte)
          return LoadResult::failure(error(QueryError::CorruptPayload, "Arrow bitmap tile escapes buffer"));
        mask_span.offset += first_byte;
        mask_span.length = needed;
      }
      auto mask = reader.decode(mask_span, codec, needed);
      if (!mask.ok()) return LoadResult::failure(mask.status());
      fragment->validity.assign(static_cast<std::size_t>(mask_size), 0);
      std::uint64_t nulls = 0;
      for (std::uint64_t i = 0; i < count; ++i) {
        if (i % 4096 == 0 && reader.cancelled())
          return LoadResult::failure(error(QueryError::Cancelled, "Arrow decoding cancelled"));
        const auto source_bit = begin + i - first_byte * 8;
        const bool valid = (mask.value()[source_bit / 8] & (1u << (source_bit % 8))) != 0;
        if (valid) fragment->validity[i / 8] |= std::uint8_t(1u << (i % 8));
        else ++nulls;
      }
      if ((compressed || (begin == 0 && count == leaf_count)) && nulls != null_count)
        return LoadResult::failure(error(QueryError::CorruptPayload, "Arrow leaf bitmap contradicts null count"));
    }
    if (reader.cancelled())
      return LoadResult::failure(error(QueryError::Cancelled, "Arrow decoding cancelled"));

        context.record_copy(fragment->resident_bytes());
        return LoadResult::success(std::move(fragment));
      }, cancellation);
    if (!loaded.ok()) return FragmentResult::failure(loaded.status());
    auto fragment = loaded.value().as<ArrowDecodedFragment>();
    if (!fragment) return FragmentResult::failure(error(QueryError::Internal, "Arrow fragment cache type mismatch"));
    last_fragment_key = std::move(key_hex.value());
    return FragmentResult::success(std::move(fragment));
  }
};

ArrowFragmentProvider::ArrowFragmentProvider(std::shared_ptr<const QuerySource> source,
    std::shared_ptr<const BinaryValue> layout, std::shared_ptr<QueryRuntime> runtime,
    QueryExecutionOptions options, ArrowDecodeConfig config,
    std::optional<CancellationToken> cancellation)
  : impl_(std::make_unique<Impl>(std::move(source), std::move(layout),
      std::move(runtime), options, config, std::move(cancellation))) {}
ArrowFragmentProvider::~ArrowFragmentProvider() {
  if (impl_->owns_cancellation) impl_->cancellation.cancel();
}
FragmentResult ArrowFragmentProvider::get(std::uint64_t batch, std::uint32_t field,
    std::uint64_t position, std::uint64_t demand) {
  try { return impl_->get(batch, field, position, demand); }
  catch (const std::bad_alloc&) {
    return FragmentResult::failure(error(QueryError::ResourceExhausted, "Arrow fragment allocation failed"));
  }
}
BatchResult ArrowFragmentProvider::get_batch(const std::vector<ArrowFragmentDemand>& demands) {
  try { return impl_->get_batch(demands); }
  catch (const std::bad_alloc&) {
    return BatchResult::failure(error(QueryError::ResourceExhausted, "Arrow fragment batch allocation failed"));
  }
}
void ArrowFragmentProvider::cancel() { impl_->cancellation.cancel(); }
void ArrowFragmentProvider::account_output(std::uint64_t bytes) {
  impl_->metrics.output_bytes += bytes;
  impl_->runtime->cache().record_copy(bytes);
}
const ArrowDecodeMetrics& ArrowFragmentProvider::metrics() const { return impl_->metrics; }
const std::string& ArrowFragmentProvider::last_fragment_key() const {
  return impl_->last_fragment_key;
}

} // namespace iotdbfs
