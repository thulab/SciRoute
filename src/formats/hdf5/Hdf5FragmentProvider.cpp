#include "iotdbfs/formats/hdf5/Hdf5FragmentProvider.h"
#include "iotdbfs/formats/hdf5/Hdf5ChunkDecoder.h"
#include "iotdbfs/query/core/QueryProfile.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <chrono>
#include <ctime>
#include "iotdbfs/formats/hdf5/Hdf5RangeReader.h"
#include <map>
#include <set>
#include <stdexcept>
#include <unordered_map>

namespace iotdbfs {
namespace {
QueryStatus failure(QueryError code, const char* message) {
  return QueryStatus::failure(code, QueryPhase::Execution, message);
}
bool multiply(std::uint64_t a, std::uint64_t b, std::uint64_t& out) {
  if (a && b > std::numeric_limits<std::uint64_t>::max() / a) return false;
  out = a * b; return true;
}
bool product(const std::vector<std::uint64_t>& dimensions, std::uint64_t& out) {
  out = 1;
  for (auto n : dimensions) if (!multiply(out, n, out)) return false;
  return true;
}
bool little_endian() {
  const std::uint16_t one = 1;
  return *reinterpret_cast<const unsigned char*>(&one) == 1;
}
} // namespace

namespace {
bool deterministic_fill(const Hdf5DatasetLayout& d) {
  return d.fill_status != Hdf5FillStatus::Undefined && d.fill_time != 1 &&
    d.fill_value.size() == d.element_bytes;
}
std::uint32_t effective_mask(const Hdf5DatasetLayout& d, const Hdf5ChunkRecord& c) {
  if (d.chunk_options & 2U)
    for (std::size_t axis = 0; axis < c.origin.size(); ++axis)
      if (d.descriptor.shape[axis] - c.origin[axis] < d.chunk_shape[axis])
        return (1U << d.filters.size()) - 1;
  return c.filter_mask;
}
}
static QueryResult<std::uint64_t> selection_working_bytes_impl(const Hdf5DatasetLayout& d,
    const Hdf5QueryBinding& b, Hdf5QueryKernelConfig config, PayloadReadCapabilities transport,
    std::shared_ptr<const Hdf5ChunkDecoder>* decoder_out) {
  using R = QueryResult<std::uint64_t>;
  query_profile_count("hdf5.selection.validation_calls");
  auto fail = [](QueryError c, const char* m) { return R::failure(failure(c, m)); };
  if ((config.read_parallelism != 1 && config.read_parallelism != 2 && config.read_parallelism != 4) ||
      !config.max_batch_fragments || config.max_batch_fragments > 64 || !config.max_batch_encoded_bytes)
    return fail(QueryError::InvalidQuery, "invalid HDF5 read parallelism or batch limits");
  if (!config.contiguous_tile_elements || !config.max_buffer_bytes ||
      !config.max_decoded_chunk_bytes || !config.max_encoded_chunk_bytes)
    return fail(QueryError::InvalidQuery, "HDF5 tile and buffer limits must be positive");
  if (config.max_buffer_bytes > static_cast<std::uint64_t>(std::numeric_limits<std::ptrdiff_t>::max()))
    return fail(QueryError::ResourceExhausted, "HDF5 buffer limit exceeds addressable storage");
  if (!d.descriptor.supported ||
      (d.storage != Hdf5StorageLayout::Contiguous && d.storage != Hdf5StorageLayout::Chunked))
    return fail(QueryError::UnsupportedCapability, "HDF5 requires supported contiguous or chunked datasets");
  if (d.descriptor.shape != b.descriptor().shape ||
      d.descriptor.value_type != b.descriptor().value_type ||
      d.descriptor.shape.empty() || d.descriptor.shape.size() > 4 ||
      (d.descriptor.value_type != LogicalTypeId::Float32 && d.descriptor.value_type != LogicalTypeId::Float64 && d.descriptor.value_type != LogicalTypeId::Int32) ||
      d.element_bytes != (d.descriptor.value_type == LogicalTypeId::Int32 ? 2U : d.descriptor.value_type == LogicalTypeId::Float32 ? 4U : 8U) ||
      (d.byte_order != Hdf5ByteOrder::Little && d.byte_order != Hdf5ByteOrder::Big))
    return fail(QueryError::CorruptLayout, "HDF5 dataset does not match the bound numeric shape");
  if (!transport.max_bytes || !transport.max_ranges)
    return fail(QueryError::UnsupportedCapability, "invalid HDF5 source transport limits");
  const auto wire_limit=std::min(kMaxPayloadBytesPerBatch,transport.max_bytes);
  auto transport_peak=[&](std::uint64_t bytes)->R {
    if (bytes<=wire_limit) return R::success(bytes);
    return hdf5_range_read_temporary_bytes({{0,bytes}}, {config.read_parallelism,wire_limit,kMaxPayloadRangesPerBatch});
  };
  std::uint64_t total = 0, full_bytes = 0, peak = 0;
  if (!product(d.descriptor.shape, total) || !multiply(total, d.element_bytes, full_bytes))
    return fail(QueryError::CorruptLayout, "HDF5 dataset byte size overflows");
  if (d.storage == Hdf5StorageLayout::Contiguous) {
    if (!d.filters.empty() || d.chunk_options)
      return fail(QueryError::CorruptLayout, "HDF5 contiguous dataset declares chunk filters/options");
    if (d.contiguous_offset) {
      if (d.contiguous_size != full_bytes)
        return fail(QueryError::CorruptLayout, "HDF5 contiguous allocation does not match its shape");
    } else if (b.row_count() && (d.allocation_status != 0 || !deterministic_fill(d)))
      return fail(QueryError::UnsupportedCapability, "HDF5 unallocated contiguous data has no deterministic fill");
    peak = std::min(total, config.contiguous_tile_elements) * d.element_bytes;
    if (d.contiguous_offset && peak) {
      auto wire=transport_peak(peak); if (!wire.ok()) return wire;
      peak=std::max(peak,wire.value());
    }
  } else {
    if (!d.index_complete) return fail(QueryError::LayoutUnavailable, "HDF5 chunk index is incomplete");
    if (d.chunk_options & ~2U) return fail(QueryError::UnsupportedCapability, "HDF5 chunk options are unsupported");
    query_profile_count("hdf5.selection.decoder_compiles");
    auto compiled = Hdf5ChunkDecoder::compile(d, {config.max_decoded_chunk_bytes, config.max_zlib_scratch_bytes});
    if (!compiled.ok()) return R::failure(compiled.status());
    if (decoder_out) *decoder_out = compiled.value();
    const auto decoded = compiled.value()->decoded_bytes();
    std::uint64_t expected = b.row_count() ? 1 : 0;
    if (expected) for (std::size_t axis = 0; axis < d.chunk_shape.size(); ++axis) {
      const auto first = b.origin()[axis] / d.chunk_shape[axis];
      const auto last = (b.origin()[axis] + b.shape()[axis] - 1) / d.chunk_shape[axis];
      if (!multiply(expected, last - first + 1, expected))
        return fail(QueryError::ResourceExhausted, "HDF5 selected chunk count overflows");
    }
    std::set<std::vector<std::uint64_t>> seen;
    for (const auto& chunk : d.chunks) {
      if (chunk.origin.size() != d.chunk_shape.size() ||
          (chunk.filter_mask >> d.filters.size()))
        return fail(QueryError::CorruptLayout, "HDF5 chunk has invalid origin or mask");
      bool intersects = b.row_count() != 0;
      for (std::size_t axis = 0; axis < chunk.origin.size(); ++axis) {
        if (chunk.origin[axis] >= d.descriptor.shape[axis] || chunk.origin[axis] % d.chunk_shape[axis])
          return fail(QueryError::CorruptLayout, "HDF5 chunk origin is outside the native grid");
        if (b.row_count()) {
          const auto first = b.origin()[axis] / d.chunk_shape[axis];
          const auto last = (b.origin()[axis] + b.shape()[axis] - 1) / d.chunk_shape[axis];
          const auto ordinal = chunk.origin[axis] / d.chunk_shape[axis];
          intersects &= ordinal >= first && ordinal <= last;
        }
      }
      if (!seen.insert(chunk.origin).second || !intersects)
        return fail(QueryError::CorruptLayout, "HDF5 selected chunk index repeats or includes an unrelated chunk");
      if (chunk.encoded_size > config.max_encoded_chunk_bytes)
        return fail(QueryError::ResourceExhausted, "HDF5 encoded chunk exceeds configured limit");
      auto working = compiled.value()->working_set_bytes(chunk.encoded_size, effective_mask(d, chunk));
      if (!working.ok()) return R::failure(working.status());
      auto wire=transport_peak(chunk.encoded_size); if (!wire.ok()) return wire;
      peak = std::max({peak, working.value(), wire.value()});
    }
    if (seen.size() < expected) {
      if (!deterministic_fill(d)) return fail(QueryError::UnsupportedCapability, "HDF5 missing chunk has no deterministic fill");
      peak = std::max(peak, decoded);
    }
  }
  if (!b.row_count()) peak = 0;
  if (peak > config.max_buffer_bytes ||
      (b.row_count() && b.projected_row_bytes() > config.max_buffer_bytes - peak))
    return fail(QueryError::ResourceExhausted, "HDF5 buffer cannot hold codec working memory and one output row");
  return R::success(peak);
}
QueryResult<std::uint64_t> hdf5_selection_working_bytes(const Hdf5DatasetLayout& d,
    const Hdf5QueryBinding& b, Hdf5QueryKernelConfig config, PayloadReadCapabilities transport) {
  return selection_working_bytes_impl(d,b,config,transport,nullptr);
}
std::array<std::uint64_t, 8> hdf5_selection_config_signature(
    const Hdf5QueryKernelConfig& config) {
  return {config.contiguous_tile_elements, config.max_buffer_bytes,
    config.max_decoded_chunk_bytes, config.max_encoded_chunk_bytes,
    config.max_zlib_scratch_bytes, config.read_parallelism,
    config.max_batch_fragments, config.max_batch_encoded_bytes};
}
QueryResult<Hdf5PreparedSelection> prepare_hdf5_selection(const Hdf5DatasetLayout& d,
    const Hdf5QueryBinding& b, Hdf5QueryKernelConfig config, PayloadReadCapabilities transport) {
  Hdf5PreparedSelection prepared;
  auto checked=selection_working_bytes_impl(d,b,config,transport,&prepared.decoder);
  if (!checked.ok()) return QueryResult<Hdf5PreparedSelection>::failure(checked.status());
  prepared.working_bytes=checked.value();
  prepared.config_signature=hdf5_selection_config_signature(config);
  return QueryResult<Hdf5PreparedSelection>::success(std::move(prepared));
}
QueryStatus validate_hdf5_selection(const Hdf5DatasetLayout& d,
    const Hdf5QueryBinding& b, Hdf5QueryKernelConfig config, PayloadReadCapabilities transport) {
  auto checked = hdf5_selection_working_bytes(d, b, config, transport);
  return checked.ok() ? QueryStatus::success() : checked.status();
}

namespace {
std::optional<std::uint64_t> thread_cpu_time() {
#ifdef CLOCK_THREAD_CPUTIME_ID
  timespec value{};
  if (clock_gettime(CLOCK_THREAD_CPUTIME_ID,&value)==0 && value.tv_sec>=0)
    return std::uint64_t(value.tv_sec)*1000000000ULL+value.tv_nsec;
#endif
  return std::nullopt;
}
struct SharedBytes final : DecodedFragment {
  BinaryValue bytes;
  std::uint64_t resident_bytes() const override { return bytes.capacity(); }
};
struct ProducerMetrics {
  std::atomic<std::uint64_t> bytes{0}, ranges{0}, decoded{0}, inflated{0}, fills{0}, decodes{0};
};
// Instrumentation has independent lifetime; it never carries request budgets,
// cancellation or a provider pointer into a shared producer.
class ObservedSource final : public QuerySource {
public:
  ObservedSource(std::shared_ptr<const QuerySource> source, std::shared_ptr<ProducerMetrics> m)
    : source_(std::move(source)), metrics_(std::move(m)) {}
  std::uint64_t storage_namespace() const override { return source_->storage_namespace(); }
  const StructuredBlobManifest& manifest() const override { return source_->manifest(); }
  QueryResult<std::optional<std::string>> verified_layout_identity() const override {
    return source_->verified_layout_identity();
  }
  PayloadReadCapabilities payload_read_capabilities() const override { return source_->payload_read_capabilities(); }
  QueryResult<BinaryValue> read_payload(std::uint64_t o, std::uint64_t n) const override {
    metrics_->bytes += n; ++metrics_->ranges;
    try { return source_->read_payload(o, n); }
    catch (const std::bad_alloc&) { return QueryResult<BinaryValue>::failure(failure(QueryError::ResourceExhausted,"HDF5 payload allocation failed")); }
    catch (...) { return QueryResult<BinaryValue>::failure(failure(QueryError::ReadFailure,"HDF5 payload callback threw")); }
  }
  QueryResult<StructuredBlobPayloadBatch> read_payload_ranges(const StructuredBlobPayloadRanges& ranges) const override {
    for (const auto& r : ranges) metrics_->bytes += r.length;
    metrics_->ranges += ranges.size();
    try { return source_->read_payload_ranges(ranges); }
    catch (const std::bad_alloc&) { return QueryResult<StructuredBlobPayloadBatch>::failure(failure(QueryError::ResourceExhausted,"HDF5 batch allocation failed")); }
    catch (...) { return QueryResult<StructuredBlobPayloadBatch>::failure(failure(QueryError::ReadFailure,"HDF5 batch callback threw")); }
  }
  QueryResult<BinaryValue> read_layout(std::uint64_t o, std::uint64_t n) const override { return source_->read_layout(o,n); }
private:
  std::shared_ptr<const QuerySource> source_;
  std::shared_ptr<ProducerMetrics> metrics_;
};
struct Demand {
  std::uint64_t key{0}, offset{0}, length{0}, decoded{0}, resident{0}, working{0};
  std::uint32_t mask{0};
  bool fill{false};
};
std::string fragment_key(const std::string& identity, std::uint64_t key) {
  auto value = identity;
  for (unsigned i = 0; i < 8; ++i) value.push_back(static_cast<char>(key >> (8*i)));
  return value;
}
std::string fill_fragment_key(const std::string& identity) {
  // Data keys have an eight-byte suffix; the one-byte fill suffix cannot
  // collide with a physical chunk or contiguous tile key.
  return identity + '\x01';
}
}
struct Hdf5FragmentProvider::Impl {
  std::shared_ptr<const QuerySource> source;
  Hdf5DatasetLayout dataset;
  QueryExecutionOptions options;
  Hdf5QueryKernelConfig config;
  CancellationToken cancelled;
  Hdf5FragmentMetrics metrics;
  QueryStatus sticky;
  std::shared_ptr<const Hdf5ChunkDecoder> decoder;
  std::shared_ptr<QueryRuntime> runtime;
  std::string identity;
  std::shared_ptr<ProducerMetrics> producers = std::make_shared<ProducerMetrics>();
  std::unordered_map<std::uint64_t, std::size_t> chunk_indices;
  std::map<std::uint64_t, std::shared_ptr<const SharedBytes>> leases;
  std::shared_ptr<const SharedBytes> fill_lease;
  std::uint64_t lease_bytes{0};
  std::set<std::uint64_t> charged;
  std::uint64_t elements{0}, logical_bytes{0}, logical_ranges{0};
  std::uint64_t pending_copy_bytes{0};
  std::vector<std::uint64_t> slab_origin, slab_shape, walk, run_coordinate;
  bool walk_done{true};
  bool chunk_major{false};
  std::optional<std::uint64_t> last_walk_key;
  std::optional<Demand> pending;

  void clear_leases() { leases.clear(); fill_lease.reset(); lease_bytes = 0; }
  void flush_copy_bytes() {
    if (pending_copy_bytes) {
      runtime->cache().record_copy(pending_copy_bytes);
      pending_copy_bytes = 0;
    }
  }
  void erase_lease(std::map<std::uint64_t, std::shared_ptr<const SharedBytes>>::iterator& it) {
    lease_bytes -= it->second->bytes.capacity();
    it = leases.erase(it);
  }
  void retain(std::uint64_t key, std::shared_ptr<const SharedBytes> bytes) {
    auto [it, inserted] = leases.emplace(key, std::move(bytes));
    if (inserted) lease_bytes += it->second->bytes.capacity();
  }
  void retain_fill(std::shared_ptr<const SharedBytes> bytes) {
    if (!fill_lease) {
      lease_bytes += bytes->bytes.capacity();
      fill_lease = std::move(bytes);
    }
  }
  void trim_leases(std::uint64_t allowance) {
    while (lease_bytes > allowance && !leases.empty()) {
      auto it = leases.begin(); erase_lease(it);
    }
    if (lease_bytes > allowance && fill_lease) {
      lease_bytes -= fill_lease->bytes.capacity();
      fill_lease.reset();
    }
  }
  bool slab_intersects(std::uint64_t key) const {
    if (dataset.storage != Hdf5StorageLayout::Chunked || slab_shape.empty()) return false;
    for (std::size_t axis = slab_shape.size(); axis-- > 0;) {
      const auto full = dataset.descriptor.shape[axis], chunk = dataset.chunk_shape[axis];
      const auto grid = (full - 1) / chunk + 1;
      const auto begin = (key % grid) * chunk;
      key /= grid;
      const auto end = begin + std::min(chunk, full - begin);
      if (begin >= slab_origin[axis] + slab_shape[axis] || end <= slab_origin[axis])
        return false;
    }
    return key == 0;
  }
  void reserve_leases(std::uint64_t output_bytes) {
    if (dataset.storage != Hdf5StorageLayout::Chunked) { clear_leases(); return; }
    for (auto it = leases.begin(); it != leases.end();) {
      if (!slab_intersects(it->first)) erase_lease(it);
      else ++it;
    }
    const auto allowance = config.max_buffer_bytes - output_bytes;
    trim_leases(allowance);
  }

  bool is_cancelled() const { return cancelled.cancelled(); }
  std::uint64_t linear(const std::vector<std::uint64_t>& c) const {
    std::uint64_t n=0;
    for (std::size_t a=0; a<c.size(); ++a) n=n*dataset.descriptor.shape[a]+c[a];
    return n;
  }
  Demand demand(const std::vector<std::uint64_t>& c, std::uint64_t& local, std::uint64_t& available) const {
    Demand d;
    local=0;
    if (dataset.storage == Hdf5StorageLayout::Contiguous) {
      const auto n=linear(c); d.key=n/config.contiguous_tile_elements;
      const auto begin=d.key*config.contiguous_tile_elements;
      const auto count=std::min(config.contiguous_tile_elements,elements-begin);
      local=n-begin; available=count-local;
      d.fill=!dataset.contiguous_offset;
      if (!d.fill) d.offset=*dataset.contiguous_offset+begin*dataset.element_bytes;
      d.length=d.decoded=count*dataset.element_bytes;
    } else {
      for (std::size_t a=0;a<c.size();++a) {
        const auto ordinal=c[a]/dataset.chunk_shape[a];
        local=local*dataset.chunk_shape[a]+c[a]%dataset.chunk_shape[a];
        const auto grid=dataset.descriptor.shape[a]/dataset.chunk_shape[a]+(dataset.descriptor.shape[a]%dataset.chunk_shape[a]!=0);
        d.key=d.key*grid+ordinal;
      }
      auto found=chunk_indices.find(d.key); d.fill=found==chunk_indices.end();
      d.decoded=decoder->decoded_bytes();
      if (d.fill) d.length=d.decoded;
      else {
        const auto& chunk=dataset.chunks[found->second];
        d.offset=chunk.offset; d.length=chunk.encoded_size; d.mask=effective_mask(dataset,chunk);
      }
      available=dataset.chunk_shape.back()-c.back()%dataset.chunk_shape.back();
    }
    d.resident=std::max(d.decoded,d.length);
    d.working=d.resident;
    if (!d.fill && decoder) {
      // Fletcher-before-DEFLATE retains four bytes of checksum capacity.
      if (dataset.filters.size()==2 && dataset.filters[0].id==3 && dataset.filters[1].id==1 && !(d.mask&1U))
        d.resident=std::max(d.resident,d.decoded+4);
      auto work=decoder->working_set_bytes(d.length,d.mask);
      if (work.ok()) d.working=std::max(work.value(),d.resident);
    }
    return d;
  }
  std::optional<Demand> next_demand() {
    if (pending) { auto d=std::move(pending); pending.reset(); return d; }
    while (!walk_done) {
      std::uint64_t local,available;
      auto d=demand(walk,local,available);
      if (dataset.storage==Hdf5StorageLayout::Chunked) {
        for (std::size_t a=walk.size();a-->0;) {
          const auto end=slab_origin[a]+slab_shape[a];
          const auto step=dataset.chunk_shape[a]-walk[a]%dataset.chunk_shape[a];
          if (step<end-walk[a]) { walk[a]+=step; break; }
          walk[a]=slab_origin[a]; if (!a) walk_done=true;
        }
      } else {
        const auto end=slab_origin.back()+slab_shape.back();
        const auto take=std::min(available,end-walk.back());
        walk.back()+=take;
        if (walk.back()==end) {
          walk.back()=slab_origin.back();
          if (walk.size()==1) walk_done=true;
          else for (std::size_t a=walk.size()-1;a-->0;) {
            if (++walk[a]<slab_origin[a]+slab_shape[a]) break;
            walk[a]=slab_origin[a]; if (!a) walk_done=true;
          }
        }
      }
      // Contiguous tiles can intersect several selected rows.
      if (last_walk_key && *last_walk_key==d.key) continue;
      last_walk_key=d.key; return d;
    }
    return std::nullopt;
  }
  Hdf5RangeReadConfig read_config() const {
    return {config.read_parallelism, std::min(kMaxPayloadBytesPerBatch,source->payload_read_capabilities().max_bytes), kMaxPayloadRangesPerBatch};
  }
  bool direct(const std::vector<Demand>& plans) const {
    return plans.size()==1 && (plans[0].fill || plans[0].length<=std::min(kMaxPayloadBytesPerBatch,source->payload_read_capabilities().max_bytes));
  }
  QueryResult<FragmentMemory> memory(const std::vector<Demand>& plans) const {
    using R=QueryResult<FragmentMemory>;
    FragmentMemory m;
    for (const auto& d:plans) {
      if (d.resident>std::numeric_limits<std::uint64_t>::max()-m.resident_bytes)
        return R::failure(failure(QueryError::ResourceExhausted,"HDF5 batch reservation overflows"));
      m.resident_bytes+=d.resident;
    }
    if (plans.size()==1) {
      auto peak=plans[0].working;
      if (!plans[0].fill && !direct(plans)) {
        auto wire=hdf5_range_read_temporary_bytes({{plans[0].offset,plans[0].length}},read_config());
        if (!wire.ok()) return R::failure(wire.status());
        peak=std::max(peak,wire.value());
      }
      // The encoded output becomes the resident buffer for raw/in-place
      // codecs. Transport responses are gone before decoding starts.
      m.temporary_bytes=peak>m.resident_bytes?peak-m.resident_bytes:0;
    }
    else {
      StructuredBlobPayloadRanges spans;
      std::uint64_t extra=0;
      for (const auto& d:plans) if (!d.fill) {
        spans.push_back({d.offset,d.length});
        // Resident reservations cover final buffers, transport owns encoded
        // buffers; only codec scratch/other intermediates remain extra.
        if (d.working>d.resident && d.working-d.resident>d.length)
          extra=std::max(extra,d.working-d.resident-d.length);
      }
      if (!spans.empty()) {
        auto temp=hdf5_range_read_temporary_bytes(spans,read_config());
        if (!temp.ok()) return R::failure(temp.status());
        if (temp.value()>std::numeric_limits<std::uint64_t>::max()-extra)
          return R::failure(failure(QueryError::ResourceExhausted,"HDF5 codec reservation overflows"));
        m.temporary_bytes=temp.value()+extra;
      }
    }
    return R::success(m);
  }
  QueryStatus load(std::vector<Demand> plans) {
    auto reservation=memory(plans);
    if (!reservation.ok()) return reservation.status();
    // Request demand is independent of cache residency and producer ownership.
    for (const auto& d:plans) if (!d.fill && !charged.count(d.key)) {
      if (logical_ranges>=options.max_ranges || d.length>options.max_payload_bytes-logical_bytes)
        return failure(QueryError::ResourceExhausted,"HDF5 logical fragment demand budget exhausted");
      ++logical_ranges; logical_bytes+=d.length; charged.insert(d.key);
    }
    std::vector<FragmentBatchItem> items;
    for (const auto& d:plans) {
      const auto key=d.fill && dataset.storage==Hdf5StorageLayout::Chunked ?
        fill_fragment_key(identity) : fragment_key(identity,d.key);
      items.push_back({key,{d.resident,0}});
    }
    auto observed=std::make_shared<ObservedSource>(source,producers);
    auto codec=decoder;
    const auto width=dataset.element_bytes;
    const bool swap=(dataset.byte_order==Hdf5ByteOrder::Little)!=little_endian();
    auto fill_value=dataset.fill_value;
    if (!little_endian()) std::reverse(fill_value.begin(),fill_value.end());
    const auto filters=dataset.filters;
    const auto rc=read_config();
    auto counters=producers;
    auto claimed=std::make_shared<std::atomic<std::size_t>>(0);
    const bool scalar=direct(plans);
    std::function<void(const std::string&, std::uint64_t)> decode_observer;
    std::string observer_identity;
    try {
      if (config.decoded_fragment_observer) {
        observer_identity = identity;
        decode_observer = config.decoded_fragment_observer;
      }
    } catch (...) { decode_observer = {}; }
    auto loader=[observed,codec,width,swap,fill_value,filters,plans,rc,counters,scalar,claimed,decode_observer=std::move(decode_observer),observer_identity=std::move(observer_identity)]
      (const std::vector<std::size_t>& misses,const FragmentLoadContext& context)
        -> QueryResult<std::vector<std::shared_ptr<const DecodedFragment>>> {
      using R=QueryResult<std::vector<std::shared_ptr<const DecodedFragment>>>;
      *claimed=misses.size();
      auto stopped=[&] { return context.cancelled(); };
      if (stopped()) return R::failure(failure(QueryError::Cancelled,"HDF5 producer cancelled"));
      std::vector<BinaryValue> encoded;
      if (scalar && !plans[misses[0]].fill) {
        const auto& d=plans[misses[0]];
        auto budget=context.record_io(d.length);
        if (!budget.ok()) return R::failure(budget);
        struct Timer {
          const FragmentLoadContext& c; std::uint64_t n;
          std::chrono::steady_clock::time_point start=std::chrono::steady_clock::now();
          Timer(const FragmentLoadContext& c,std::uint64_t n):c(c),n(n) { c.begin_payload_read(); }
          ~Timer() { c.record_payload_read(n,1,std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now()-start).count()); }
        } timer(context,d.length);
        auto read=observed->read_payload(d.offset,d.length);
        if (!read.ok()) return R::failure(read.status());
        if (read.value().size()!=d.length)
          return R::failure(failure(QueryError::ReadFailure,"short HDF5 payload read"));
        if (read.value().capacity()>d.length)
          return R::failure(failure(QueryError::ResourceExhausted,"HDF5 payload exceeded reserved capacity"));
        encoded.push_back(std::move(read.value()));
      } else if (!scalar) {
        StructuredBlobPayloadRanges spans;
        for (auto i:misses) if (!plans[i].fill) spans.push_back({plans[i].offset,plans[i].length});
        if (!spans.empty()) {
          auto read=read_hdf5_payload_ranges(observed,spans,context,rc);
          if (!read.ok()) return R::failure(read.status());
          encoded=std::move(read.value());
        }
      }
      std::vector<std::shared_ptr<const DecodedFragment>> result;
      std::size_t next_encoded=0;
      for (auto i:misses) {
        if (stopped()) return R::failure(failure(QueryError::Cancelled,"HDF5 producer cancelled"));
        const auto& d=plans[i]; auto fragment=std::make_shared<SharedBytes>();
        if (d.fill) {
          fragment->bytes.resize(d.decoded);
          std::memcpy(fragment->bytes.data(),fill_value.data(),width);
          for (std::size_t filled=width;filled<fragment->bytes.size();) {
            if (stopped()) return R::failure(failure(QueryError::Cancelled,"HDF5 fill cancelled"));
            const auto copy=std::min(filled,fragment->bytes.size()-filled);
            std::memcpy(fragment->bytes.data()+filled,fragment->bytes.data(),copy);
            filled+=copy;
          }
          counters->fills+=d.decoded;
        } else {
          auto bytes=std::move(encoded[next_encoded++]);
          const auto wall_start=std::chrono::steady_clock::now();
          const auto cpu_start=thread_cpu_time();
          FragmentDecodeTimings timings;
          struct DecodeObservation {
            const FragmentLoadContext& context; FragmentDecodeTimings& timings;
            ~DecodeObservation() { context.record_decode_stages(timings); }
          } observation{context,timings};
          if (codec) {
            auto decoded=codec->decode(std::move(bytes),d.mask,stopped,
              context.detailed_profile_enabled() ? &timings : nullptr);
            if (!decoded.ok()) return R::failure(decoded.status());
            bytes=std::move(decoded.value());
          } else if (swap) {
            struct ConversionTimer {
              std::uint64_t* value;
              std::chrono::steady_clock::time_point start;
              ~ConversionTimer() { if (value) *value += static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now()-start).count()); }
            } timer{context.detailed_profile_enabled() ? &timings.conversion_nanoseconds : nullptr,
              context.detailed_profile_enabled() ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{}};
            for (std::size_t p=0;p<bytes.size();p+=width) {
              if ((p&65535U)==0 && stopped()) return R::failure(failure(QueryError::Cancelled,"HDF5 byte swap cancelled"));
              std::reverse(bytes.begin()+p,bytes.begin()+p+width);
            }
          }
          const auto cpu_end=thread_cpu_time();
          const auto wall_ns=std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now()-wall_start).count();
          const bool cpu_valid=cpu_start && cpu_end && *cpu_end>=*cpu_start;
          std::uint64_t inflations=0;
          for (std::size_t f=0;f<filters.size();++f) if (filters[f].id==1 && !(d.mask&(1U<<f))) {
            ++inflations; counters->inflated+=bytes.size();
            if (f==1 && filters[0].id==3 && !(d.mask&1U)) counters->inflated+=4;
          }
          counters->decoded+=bytes.size(); ++counters->decodes;
          context.record_decode(bytes.size(),inflations,wall_ns,
            cpu_valid?*cpu_end-*cpu_start:0,cpu_valid?1:0);
          if (decode_observer) try { decode_observer(fragment_key(observer_identity,d.key),bytes.size()); } catch (...) {}
          fragment->bytes=std::move(bytes);
        }
        if (fragment->resident_bytes()>d.resident)
          return R::failure(failure(QueryError::ResourceExhausted,"HDF5 decoded capacity exceeds reservation"));
        result.push_back(std::move(fragment));
      }
      if (stopped()) return R::failure(failure(QueryError::Cancelled,"HDF5 producer cancelled"));
      return R::success(std::move(result));
    };
    if (plans.size()==1) {
      auto loaded=runtime->cache().get(items[0].key,reservation.value(),
        [loader](const FragmentLoadContext& ctx)->QueryResult<std::shared_ptr<const DecodedFragment>> {
          auto r=loader({0},ctx);
          if (!r.ok()) return QueryResult<std::shared_ptr<const DecodedFragment>>::failure(r.status());
          return QueryResult<std::shared_ptr<const DecodedFragment>>::success(std::move(r.value()[0]));
        },cancelled);
      if (!loaded.ok()) return loaded.status();
      if (plans[0].fill && dataset.storage==Hdf5StorageLayout::Chunked)
        retain_fill(loaded.value().as<SharedBytes>());
      else retain(plans[0].key,loaded.value().as<SharedBytes>());
    } else {
      auto loaded=runtime->cache().get_batch(items,reservation.value().temporary_bytes,std::move(loader),cancelled);
      if (!loaded.ok()) return loaded.status();
      for (std::size_t i=0;i<plans.size();++i) retain(plans[i].key,loaded.value()[i].as<SharedBytes>());
    }
    metrics.cache_hits+=plans.size()-claimed->load();
    return QueryStatus::success();
  }
  QueryResult<const BinaryValue*> get(const Demand& requested,std::uint64_t output_bytes) {
    using R=QueryResult<const BinaryValue*>;
    const bool shared_fill=requested.fill && dataset.storage==Hdf5StorageLayout::Chunked;
    if (shared_fill && fill_lease) { ++metrics.cache_hits; return R::success(&fill_lease->bytes); }
    auto found=leases.find(requested.key);
    if (!shared_fill && found!=leases.end()) { ++metrics.cache_hits; return R::success(&found->second->bytes); }
    if (is_cancelled()) return R::failure(failure(QueryError::Cancelled,"HDF5 query cancelled"));
    std::vector<Demand> plans{requested};
    auto upcoming=next_demand();
    // Chunk-major assembly may consume retained chunks without calling get().
    // Advance the prefetch walker past those already assembled keys so the
    // first new chunk can still start a merged read wave.
    if (chunk_major)
      while (upcoming && upcoming->key < requested.key) upcoming=next_demand();
    const bool sequential=!shared_fill && upcoming &&
      !(upcoming->fill && dataset.storage==Hdf5StorageLayout::Chunked) &&
      upcoming->key==requested.key;
    if (!sequential) pending=std::move(upcoming);
    const auto cap=std::min({config.max_batch_fragments,runtime->config().max_entries,runtime->config().max_inflight});
    std::uint64_t encoded=requested.fill?0:requested.length;
    if (sequential) while (plans.size()<cap) {
      auto next=next_demand(); if (!next) break;
      if (next->fill && dataset.storage==Hdf5StorageLayout::Chunked) { pending=next; break; }
      if (!next->fill && (encoded>config.max_batch_encoded_bytes || next->length>config.max_batch_encoded_bytes-encoded)) { pending=next; break; }
      plans.push_back(*next);
      auto m=memory(plans);
      const auto allowance=std::min(config.max_buffer_bytes-output_bytes,runtime->config().memory_budget_bytes);
      if (!m.ok() || m.value().resident_bytes>allowance || m.value().temporary_bytes>allowance-m.value().resident_bytes) {
        plans.pop_back(); pending=next; break;
      }
      if (!next->fill) encoded+=next->length;
    }
    auto m=memory(plans);
    const auto allowance=std::min(config.max_buffer_bytes-output_bytes,runtime->config().memory_budget_bytes);
    if (!m.ok()) return R::failure(m.status());
    if (m.value().resident_bytes>allowance || m.value().temporary_bytes>allowance-m.value().resident_bytes)
      return R::failure(failure(QueryError::ResourceExhausted,"HDF5 fragment group exceeds buffer/runtime budget"));
    const auto retained_limit = allowance - m.value().resident_bytes - m.value().temporary_bytes;
    trim_leases(retained_limit);
    auto result=load(std::move(plans));
    if (!result.ok()) return R::failure(result);
    if (shared_fill) {
      if (!fill_lease) return R::failure(failure(QueryError::Internal,"HDF5 cache returned wrong fill fragment"));
      return R::success(&fill_lease->bytes);
    }
    found=leases.find(requested.key);
    if (found==leases.end() || !found->second) return R::failure(failure(QueryError::Internal,"HDF5 cache returned wrong fragment type"));
    return R::success(&found->second->bytes);
  }
  QueryStatus copy(const std::vector<std::uint64_t>& coordinate,
      std::uint64_t count,void* destination,std::uint64_t output_bytes) {
    if (!sticky.ok()) return sticky;
    if (is_cancelled()) return failure(QueryError::Cancelled,"HDF5 query cancelled");
    if (coordinate.size()!=dataset.descriptor.shape.size() || (!destination && count))
      return failure(QueryError::InvalidDomainSlice,"invalid HDF5 copy coordinates");
    for (std::size_t a=0;a<coordinate.size();++a) if (coordinate[a]>=dataset.descriptor.shape[a])
      return failure(QueryError::InvalidDomainSlice,"HDF5 coordinate outside dataset");
    if (output_bytes>config.max_buffer_bytes || count>output_bytes/dataset.element_bytes)
      return failure(QueryError::ResourceExhausted,"HDF5 copy exceeds output reservation");
    if (count>dataset.descriptor.shape.back()-coordinate.back())
      return failure(QueryError::InvalidDomainSlice,"HDF5 copy crosses innermost row");
    run_coordinate.assign(coordinate.begin(),coordinate.end());
    auto& c=run_coordinate;
    auto* output=static_cast<std::uint8_t*>(destination);
    while (count) {
      std::uint64_t local,available; auto d=demand(c,local,available);
      auto fragment=get(d,output_bytes); if (!fragment.ok()) return fragment.status();
      const auto take=std::min(count,available); const auto n=take*dataset.element_bytes;
      if (local>fragment.value()->size()/dataset.element_bytes || n>fragment.value()->size()-local*dataset.element_bytes)
        return failure(QueryError::CorruptPayload,"HDF5 decoded fragment shorter than shape");
      std::memcpy(output,fragment.value()->data()+local*dataset.element_bytes,n);
      metrics.useful_bytes+=n; pending_copy_bytes+=n;
      output+=n; count-=take; c.back()+=take;
    }
    return QueryStatus::success();
  }
  QueryStatus copy_chunked_slab(void* destination, std::uint64_t value_bytes,
      std::uint64_t logical_width, std::uint64_t output_bytes) {
    if (!sticky.ok()) return sticky;
    if (is_cancelled()) return failure(QueryError::Cancelled,"HDF5 query cancelled");
    if (dataset.storage != Hdf5StorageLayout::Chunked || slab_shape.empty() ||
        !destination || logical_width < dataset.element_bytes ||
        output_bytes > config.max_buffer_bytes)
      return failure(QueryError::InvalidDomainSlice,"invalid HDF5 chunked slab output");
    std::uint64_t rows, needed;
    if (!product(slab_shape,rows) || !multiply(rows,logical_width,needed) ||
        needed > value_bytes)
      return failure(QueryError::ResourceExhausted,"HDF5 chunked slab output exceeds reservation");
    chunk_major = true;
    const auto rank = slab_shape.size();
    std::vector<std::uint64_t> first(rank), last(rank), index(rank), chunk_origin(rank), begin(rank), end(rank);
    for (std::size_t axis=0; axis<rank; ++axis) {
      first[axis] = slab_origin[axis] / dataset.chunk_shape[axis];
      last[axis] = (slab_origin[axis] + slab_shape[axis] - 1) / dataset.chunk_shape[axis];
    }
    index = first;
    auto* output = static_cast<std::uint8_t*>(destination);
    bool done = false;
    while (!done) {
      if (is_cancelled()) return failure(QueryError::Cancelled,"HDF5 query cancelled during chunk assembly");
      for (std::size_t axis=0; axis<rank; ++axis) {
        chunk_origin[axis] = index[axis] * dataset.chunk_shape[axis];
        begin[axis] = std::max(slab_origin[axis],chunk_origin[axis]);
        end[axis] = std::min(slab_origin[axis]+slab_shape[axis],
          chunk_origin[axis] + std::min(dataset.chunk_shape[axis],
            dataset.descriptor.shape[axis]-chunk_origin[axis]));
      }
      std::uint64_t local, available;
      auto demand_for_chunk = demand(chunk_origin,local,available);
      auto fragment = get(demand_for_chunk,output_bytes);
      if (!fragment.ok()) return fragment.status();
      auto coordinate = begin;
      bool copied = false;
      while (!copied) {
        if (is_cancelled()) return failure(QueryError::Cancelled,"HDF5 query cancelled during chunk assembly");
        std::uint64_t local_index=0, row_index=0;
        for (std::size_t axis=0; axis+1<rank; ++axis) {
          local_index = local_index*dataset.chunk_shape[axis] +
            coordinate[axis]-chunk_origin[axis];
          row_index = row_index*slab_shape[axis] + coordinate[axis]-slab_origin[axis];
        }
        local_index = local_index*dataset.chunk_shape.back() + begin.back()-chunk_origin.back();
        const auto count = end.back()-begin.back();
        const auto bytes = count*dataset.element_bytes;
        const auto source_offset = local_index*dataset.element_bytes;
        const auto target_offset = row_index*slab_shape.back()*logical_width +
          (begin.back()-slab_origin.back())*dataset.element_bytes;
        if (source_offset > fragment.value()->size() ||
            bytes > fragment.value()->size()-source_offset ||
            target_offset > value_bytes || bytes > value_bytes-target_offset)
          return failure(QueryError::CorruptPayload,"HDF5 chunk intersection exceeds decoded or output buffer");
        std::memcpy(output+target_offset,fragment.value()->data()+source_offset,bytes);
        metrics.useful_bytes+=bytes; pending_copy_bytes+=bytes;
        if (rank==1) copied=true;
        else for (std::size_t axis=rank-1; axis-- > 0;) {
          if (++coordinate[axis] < end[axis]) break;
          coordinate[axis]=begin[axis];
          if (axis==0) copied=true;
        }
      }
      for (std::size_t axis=rank; axis-- > 0;) {
        if (++index[axis] <= last[axis]) break;
        index[axis]=first[axis];
        if (axis==0) done=true;
      }
    }
    return QueryStatus::success();
  }
};

Hdf5FragmentProvider::Hdf5FragmentProvider(std::unique_ptr<Impl> impl):impl_(std::move(impl)) {}
Hdf5FragmentProvider::~Hdf5FragmentProvider() { impl_->flush_copy_bytes(); }
QueryResult<std::unique_ptr<Hdf5FragmentProvider>> Hdf5FragmentProvider::create(
    std::shared_ptr<const QuerySource> source,Hdf5DatasetLayout dataset,
    const Hdf5QueryBinding& binding,QueryExecutionOptions options,
    Hdf5QueryKernelConfig config,CancellationToken cancelled,
    std::shared_ptr<QueryRuntime> runtime,std::string identity,
    Hdf5PreparedSelection prepared) {
  using R=QueryResult<std::unique_ptr<Hdf5FragmentProvider>>;
  if (!source || !runtime || identity.empty()) return R::failure(failure(QueryError::InvalidQuery,"HDF5 provider needs source/runtime/identity"));
  if (dataset.storage==Hdf5StorageLayout::Chunked && !prepared.decoder)
    return R::failure(failure(QueryError::Internal,"HDF5 planned chunk decoder is absent"));
  if (prepared.working_bytes>config.max_buffer_bytes ||
      (binding.row_count() && binding.projected_row_bytes()>config.max_buffer_bytes-prepared.working_bytes))
    return R::failure(failure(QueryError::ResourceExhausted,"HDF5 prepared selection exceeds buffer budget"));
  const auto size=source->manifest().payload_size;
  if (dataset.contiguous_offset && (*dataset.contiguous_offset>size || dataset.contiguous_size>size-*dataset.contiguous_offset))
    return R::failure(failure(QueryError::CorruptLayout,"HDF5 allocation exceeds payload"));
  for (const auto& c:dataset.chunks) if (c.offset>size || c.encoded_size>size-c.offset)
    return R::failure(failure(QueryError::CorruptLayout,"HDF5 chunk exceeds payload"));
  try {
    auto impl=std::make_unique<Impl>(); impl->source=std::move(source); impl->dataset=std::move(dataset);
    impl->options=options; impl->config=config; impl->cancelled=std::move(cancelled);
    impl->runtime=std::move(runtime); impl->identity=std::move(identity);
    product(impl->dataset.descriptor.shape,impl->elements);
    impl->run_coordinate.reserve(impl->dataset.descriptor.shape.size());
    impl->decoder=std::move(prepared.decoder);
    impl->chunk_indices.reserve(impl->dataset.chunks.size());
    for (std::size_t i=0;i<impl->dataset.chunks.size();++i) {
      std::uint64_t key=0;
      for (std::size_t axis=0;axis<impl->dataset.chunk_shape.size();++axis) {
        const auto grid=(impl->dataset.descriptor.shape[axis]-1)/impl->dataset.chunk_shape[axis]+1;
        key=key*grid+impl->dataset.chunks[i].origin[axis]/impl->dataset.chunk_shape[axis];
      }
      impl->chunk_indices.emplace(key,i);
    }
    return R::success(std::unique_ptr<Hdf5FragmentProvider>(new Hdf5FragmentProvider(std::move(impl))));
  } catch (const std::bad_alloc&) { return R::failure(failure(QueryError::ResourceExhausted,"HDF5 provider allocation failed")); }
}
QueryStatus Hdf5FragmentProvider::reserve_output(std::uint64_t bytes) {
  if (!impl_->sticky.ok()) return impl_->sticky;
  if (impl_->is_cancelled()) return failure(QueryError::Cancelled,"HDF5 query cancelled");
  if (bytes>impl_->config.max_buffer_bytes) return failure(QueryError::ResourceExhausted,"HDF5 output exceeds buffer budget");
  impl_->reserve_leases(bytes); return QueryStatus::success();
}
void Hdf5FragmentProvider::begin_slab(const std::vector<std::uint64_t>& origin,const std::vector<std::uint64_t>& shape) {
  impl_->flush_copy_bytes();
  impl_->slab_origin=origin; impl_->slab_shape=shape; impl_->walk=origin;
  impl_->walk_done=std::find(shape.begin(),shape.end(),0)!=shape.end();
  impl_->chunk_major=false;
  impl_->pending.reset(); impl_->last_walk_key.reset();
}
void Hdf5FragmentProvider::finish_slab() { impl_->flush_copy_bytes(); }
QueryStatus Hdf5FragmentProvider::copy_run(const std::vector<std::uint64_t>& c,std::uint64_t count,void* destination,std::uint64_t bytes) {
  try { impl_->sticky=impl_->copy(c,count,destination,bytes); }
  catch (const std::bad_alloc&) { impl_->sticky=failure(QueryError::ResourceExhausted,"HDF5 fragment allocation failed"); }
  catch (...) { impl_->sticky=failure(QueryError::ReadFailure,"HDF5 fragment callback failed"); }
  if (!impl_->sticky.ok()) clear(); return impl_->sticky;
}
QueryStatus Hdf5FragmentProvider::copy_chunked_slab(void* destination,
    std::uint64_t value_bytes, std::uint64_t logical_element_bytes,
    std::uint64_t output_bytes) {
  try {
    impl_->sticky=impl_->copy_chunked_slab(destination,value_bytes,
      logical_element_bytes,output_bytes);
  } catch (const std::bad_alloc&) {
    impl_->sticky=failure(QueryError::ResourceExhausted,"HDF5 chunked slab allocation failed");
  } catch (...) {
    impl_->sticky=failure(QueryError::ReadFailure,"HDF5 chunked slab callback failed");
  }
  if (!impl_->sticky.ok()) clear();
  return impl_->sticky;
}
const Hdf5FragmentMetrics& Hdf5FragmentProvider::metrics() const {
  impl_->flush_copy_bytes();
  impl_->metrics.payload_bytes=impl_->producers->bytes.load(); impl_->metrics.payload_ranges=impl_->producers->ranges.load();
  impl_->metrics.decoded_bytes=impl_->producers->decoded.load(); impl_->metrics.inflated_bytes=impl_->producers->inflated.load();
  impl_->metrics.fill_bytes=impl_->producers->fills.load(); impl_->metrics.decode_count=impl_->producers->decodes.load();
  return impl_->metrics;
}
void Hdf5FragmentProvider::clear() { impl_->flush_copy_bytes(); impl_->clear_leases(); }
} // namespace iotdbfs
