#include "iotdbfs/formats/hdf5/Hdf5RangeReader.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <future>
#include <limits>
#include <new>

namespace iotdbfs {
namespace {
QueryStatus failure(QueryError error, const char* message) {
  return QueryStatus::failure(error, QueryPhase::Execution, message);
}
QueryStatus cancelled() {
  return failure(QueryError::Cancelled, "HDF5 range producer was cancelled");
}
bool valid_config(const Hdf5RangeReadConfig& c) {
  return (c.max_parallel_reads == 1 || c.max_parallel_reads == 2 ||
    c.max_parallel_reads == 4) && c.max_bytes_per_call != 0 &&
    c.max_bytes_per_call <= kMaxPayloadBytesPerBatch &&
    c.max_ranges_per_call != 0 &&
    c.max_ranges_per_call <= kMaxPayloadRangesPerBatch &&
    c.max_transport_ranges != 0 && c.max_transport_ranges <= 65536;
}
struct Call { StructuredBlobPayloadRanges ranges; std::uint64_t bytes{0}; };
QueryResult<StructuredBlobPayloadBatch> read_call(const QuerySource& source,
    const Call& call, bool multi, const FragmentLoadContext& context) {
  if (context.cancelled())
    return QueryResult<StructuredBlobPayloadBatch>::failure(cancelled());
  auto reserved = context.record_io(call.bytes, call.ranges.size());
  if (!reserved.ok())
    return QueryResult<StructuredBlobPayloadBatch>::failure(reserved);
  const auto start = std::chrono::steady_clock::now();
  context.begin_payload_read();
  // Every begun call is observed on success, failure, and exceptions.
  struct Timer {
    const FragmentLoadContext& context; const Call& call;
    std::chrono::steady_clock::time_point start;
    ~Timer() {
      context.record_payload_read(call.bytes, call.ranges.size(),
        std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - start).count());
    }
  } timer{context, call, start};
  try {
    if (multi && call.ranges.size() > 1)
      return source.read_payload_ranges(call.ranges);
    auto result = source.read_payload(call.ranges.front().offset,
      call.ranges.front().length);
    if (!result.ok())
      return QueryResult<StructuredBlobPayloadBatch>::failure(result.status());
    StructuredBlobPayloadBatch batch;
    batch.push_back(std::move(result.value()));
    return QueryResult<StructuredBlobPayloadBatch>::success(std::move(batch));
  } catch (const std::bad_alloc&) {
    return QueryResult<StructuredBlobPayloadBatch>::failure(failure(
      QueryError::ResourceExhausted, "HDF5 range read allocation failed"));
  } catch (...) {
    return QueryResult<StructuredBlobPayloadBatch>::failure(failure(
      QueryError::ReadFailure, "HDF5 range source threw an exception"));
  }
}
} // namespace

QueryResult<std::uint64_t> hdf5_range_read_temporary_bytes(
    const StructuredBlobPayloadRanges& spans, Hdf5RangeReadConfig config) {
  if (!valid_config(config))
    return QueryResult<std::uint64_t>::failure(failure(QueryError::InvalidQuery,
      "invalid HDF5 range reader limits"));
  if (spans.size() > config.max_transport_ranges)
    return QueryResult<std::uint64_t>::failure(failure(QueryError::ResourceExhausted,
      "HDF5 input span count exceeds the transport planning bound"));
  std::uint64_t total = 0;
  for (const auto& span : spans) {
    if (!span.length || span.offset > UINT64_MAX - span.length ||
        span.length > SIZE_MAX || span.length > UINT64_MAX - total)
      return QueryResult<std::uint64_t>::failure(failure(QueryError::InvalidQuery,
        "HDF5 range span is empty or overflows"));
    total += span.length;
  }
  const auto wave = std::min(total,
    config.max_parallel_reads * config.max_bytes_per_call);
  if (wave > UINT64_MAX - total)
    return QueryResult<std::uint64_t>::failure(failure(QueryError::ResourceExhausted,
      "HDF5 range buffer reservation overflows"));
  return QueryResult<std::uint64_t>::success(total + wave);
}

QueryResult<std::vector<BinaryValue>> read_hdf5_payload_ranges(
    const std::shared_ptr<const QuerySource>& source,
    const StructuredBlobPayloadRanges& spans, const FragmentLoadContext& context,
    Hdf5RangeReadConfig config) {
  using Result = QueryResult<std::vector<BinaryValue>>;
  auto memory = hdf5_range_read_temporary_bytes(spans, config);
  if (!memory.ok()) return Result::failure(memory.status());
  if (!source) return Result::failure(failure(QueryError::InvalidQuery,
    "HDF5 range reader requires a source"));
  if (context.cancelled()) return Result::failure(cancelled());
  try {
    const auto caps = source->payload_read_capabilities();
    if (!caps.max_bytes || !caps.max_ranges)
      return Result::failure(failure(QueryError::UnsupportedCapability,
        "HDF5 source advertises invalid transport bounds"));
    const auto bytes_limit = std::min(config.max_bytes_per_call, caps.max_bytes);
    const auto ranges_limit = caps.multi_range
      ? std::min(config.max_ranges_per_call, caps.max_ranges) : 1;
    for (const auto& span : spans)
      if (span.offset > source->manifest().payload_size ||
          span.length > source->manifest().payload_size - span.offset)
        return Result::failure(failure(QueryError::InvalidQuery,
          "HDF5 span exceeds the fixed source version"));
    StructuredBlobPayloadRanges sorted = spans, merged;
    std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
      return a.offset < b.offset;
    });
    for (const auto& span : sorted) {
      if (!merged.empty() && span.offset <= merged.back().offset + merged.back().length) {
        auto& last = merged.back();
        last.length = std::max(last.offset + last.length,
          span.offset + span.length) - last.offset;
      } else merged.push_back(span);
    }
    std::vector<Call> calls;
    std::size_t transport_ranges = 0;
    for (const auto& span : merged) {
      auto offset = span.offset, remaining = span.length;
      while (remaining) {
        if (transport_ranges == config.max_transport_ranges)
          return Result::failure(failure(QueryError::ResourceExhausted,
            "HDF5 split ranges exceed the transport planning bound"));
        ++transport_ranges;
        if (calls.empty() || calls.back().ranges.size() == ranges_limit ||
            calls.back().bytes == bytes_limit)
          calls.emplace_back();
        auto& call = calls.back();
        const auto n = std::min(remaining, bytes_limit - call.bytes);
        call.ranges.push_back({offset, n}); call.bytes += n;
        offset += n; remaining -= n;
      }
    }
    std::vector<BinaryValue> output;
    output.reserve(spans.size());
    for (const auto& span : spans) {
      output.emplace_back(static_cast<std::size_t>(span.length));
      if (output.back().capacity() != span.length)
        return Result::failure(failure(QueryError::ResourceExhausted,
          "HDF5 span allocation exceeds reserved capacity"));
    }
    for (std::size_t begin = 0; begin < calls.size(); begin += config.max_parallel_reads) {
      if (context.cancelled()) return Result::failure(cancelled());
      const auto end = std::min(calls.size(), begin + config.max_parallel_reads);
      QueryStatus status = QueryStatus::success();
      const auto consume = [&](std::size_t i,
          QueryResult<StructuredBlobPayloadBatch> result) {
        if (!result.ok()) { if (status.ok()) status = result.status(); return; }
        if (context.cancelled()) { if (status.ok()) status = cancelled(); return; }
        if (!status.ok()) return;
        auto& buffers = result.value();
        const auto& call = calls[i];
        if (buffers.size() != call.ranges.size()) {
          status = failure(QueryError::ReadFailure,
            "HDF5 multi-range response has the wrong buffer count"); return;
        }
        for (std::size_t j = 0; j < buffers.size(); ++j) {
          const auto& bytes = buffers[j]; const auto& range = call.ranges[j];
          if (bytes.size() != range.length) {
            status = failure(QueryError::ReadFailure,
              "HDF5 payload range response has the wrong length"); break;
          }
          if (bytes.capacity() != range.length) {
            status = failure(QueryError::ResourceExhausted,
              "HDF5 payload response exceeds reserved capacity"); break;
          }
          for (std::size_t k = 0; k < spans.size(); ++k) {
            const auto first = std::max(range.offset, spans[k].offset);
            const auto last = std::min(range.offset + range.length,
              spans[k].offset + spans[k].length);
            if (first >= last) continue;
            std::memcpy(output[k].data() + first - spans[k].offset,
              bytes.data() + first - range.offset, last - first);
            context.record_copy(last - first);
          }
        }
      };
      if (end == begin + 1) {
        consume(begin, read_call(*source, calls[begin], caps.multi_range, context));
      } else {
        std::vector<std::future<QueryResult<StructuredBlobPayloadBatch>>> pending;
        pending.reserve(end - begin);
        for (auto i = begin; i < end; ++i)
          pending.push_back(std::async(std::launch::async, [&, i] {
            return read_call(*source, calls[i], caps.multi_range, context);
          }));
        for (auto i = begin; i < end; ++i)
          consume(i, pending[i - begin].get());
      }
      if (!status.ok()) return Result::failure(status);
    }
    if (context.cancelled()) return Result::failure(cancelled());
    return Result::success(std::move(output));
  } catch (const std::bad_alloc&) {
    return Result::failure(failure(QueryError::ResourceExhausted,
      "HDF5 range reader allocation failed"));
  } catch (...) {
    // Futures from launched std::async calls drain on stack unwinding.
    return Result::failure(failure(QueryError::Internal,
      "HDF5 range reader could not complete its wave"));
  }
}
} // namespace iotdbfs
