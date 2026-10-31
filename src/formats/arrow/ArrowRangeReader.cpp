#include "iotdbfs/formats/arrow/ArrowRangeReader.h"

#include <arrow/util/compression.h>
#include <chrono>
#include <limits>
#include <new>

namespace iotdbfs {
namespace {
class PayloadReadTimer {
public:
  PayloadReadTimer(const FragmentLoadContext* service, std::uint64_t bytes)
    : service_(service), bytes_(bytes), start_(std::chrono::steady_clock::now()) {
    if (service_) service_->begin_payload_read();
  }
  ~PayloadReadTimer() {
    if (service_) service_->record_payload_read(bytes_, 1, static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - start_).count()));
  }
private:
  const FragmentLoadContext* service_;
  std::uint64_t bytes_;
  std::chrono::steady_clock::time_point start_;
};
QueryStatus error(QueryError code, const char* message) {
  return QueryStatus::failure(code, QueryPhase::Execution, message);
}
QueryStatus cancelled_status() { return error(QueryError::Cancelled, "Arrow decoding cancelled"); }
std::uint64_t little_u64(const std::uint8_t* data) {
  std::uint64_t result = 0;
  for (unsigned i = 0; i < 8; ++i) result |= std::uint64_t(data[i]) << (8 * i);
  return result;
}
} // namespace

ArrowRangeReader::ArrowRangeReader(const QuerySource& source,
  QueryExecutionOptions options, ArrowDecodeConfig config, CancellationToken cancellation,
  const FragmentLoadContext* service)
  : source_(source), options_(options), config_(config),
    cancellation_(std::move(cancellation)), service_(service) {}

void ArrowRangeReader::cancel() { cancellation_.cancel(); }
bool ArrowRangeReader::cancelled() const { return cancellation_.cancelled(); }

QueryResult<BinaryValue> ArrowRangeReader::read(ArrowBufferSpan span) {
  using Result = QueryResult<BinaryValue>;
  if (cancelled()) return Result::failure(cancelled_status());
  const auto size = source_.manifest().payload_size;
  if (span.offset > size || span.length > size - span.offset)
    return Result::failure(error(QueryError::CorruptPayload, "Arrow buffer escapes source"));
  if (span.length > config_.max_buffer_bytes || span.length > BinaryValue().max_size())
    return Result::failure(error(QueryError::ResourceExhausted, "Arrow physical buffer exceeds byte limit"));
  if (!span.length) return Result::success({});
  if (reserved_ranges_ >= options_.max_ranges ||
      span.length > options_.max_payload_bytes - reserved_bytes_)
    return Result::failure(error(QueryError::ResourceExhausted, "Arrow physical read budget exhausted"));
  if (service_) {
    auto status = service_->record_io(span.length);
    if (!status.ok()) return Result::failure(status);
  }
  ++reserved_ranges_;
  reserved_bytes_ += span.length;
  try {
    auto bytes = [&] {
      PayloadReadTimer timer(service_, span.length);
      return source_.read_payload(span.offset, span.length);
    }();
    if (!bytes.ok()) return Result::failure(bytes.status());
    ++metrics_.read_ranges;
    metrics_.payload_bytes += bytes.value().size();
    if (cancelled()) return Result::failure(cancelled_status());
    if (bytes.value().size() != span.length)
      return Result::failure(error(QueryError::ReadFailure, "short Arrow payload read"));
    return Result::success(std::move(bytes.value()));
  } catch (const std::bad_alloc&) {
    return Result::failure(error(QueryError::ResourceExhausted, "Arrow payload allocation failed"));
  }
}

QueryResult<BinaryValue> ArrowRangeReader::decode(ArrowBufferSpan span,
  ArrowBufferCompression compression, std::uint64_t minimum_decoded_bytes) {
  using Result = QueryResult<BinaryValue>;
  if (cancelled()) return Result::failure(cancelled_status());
  if (minimum_decoded_bytes > config_.max_buffer_bytes ||
      minimum_decoded_bytes > BinaryValue().max_size())
    return Result::failure(error(QueryError::ResourceExhausted, "Arrow decoded buffer exceeds byte limit"));
  if (compression != ArrowBufferCompression::None &&
      compression != ArrowBufferCompression::Lz4 && compression != ArrowBufferCompression::Zstd)
    return Result::failure(error(QueryError::CorruptPayload, "invalid Arrow compression codec"));
  auto bytes = read(span);
  if (!bytes.ok()) return bytes;
  return decode_buffer(std::move(bytes.value()), compression, minimum_decoded_bytes);
}

QueryResult<BinaryValue> ArrowRangeReader::decode_buffer(BinaryValue data,
  ArrowBufferCompression compression, std::uint64_t minimum_decoded_bytes) {
  using Result = QueryResult<BinaryValue>;
  if (cancelled()) return Result::failure(cancelled_status());
  if (minimum_decoded_bytes > config_.max_buffer_bytes ||
      minimum_decoded_bytes > BinaryValue().max_size())
    return Result::failure(error(QueryError::ResourceExhausted, "Arrow decoded buffer exceeds byte limit"));
  if (compression != ArrowBufferCompression::None &&
      compression != ArrowBufferCompression::Lz4 && compression != ArrowBufferCompression::Zstd)
    return Result::failure(error(QueryError::CorruptPayload, "invalid Arrow compression codec"));
  if (data.size() > config_.max_buffer_bytes)
    return Result::failure(error(QueryError::ResourceExhausted, "Arrow physical buffer exceeds byte limit"));
  if (compression == ArrowBufferCompression::None || data.empty()) {
    if (data.size() < minimum_decoded_bytes)
      return Result::failure(error(QueryError::CorruptPayload, "truncated Arrow buffer"));
    metrics_.decoded_bytes += data.size();
    return Result::success(std::move(data));
  }
  if (data.size() < 8)
    return Result::failure(error(QueryError::CorruptPayload, "truncated Arrow compression prefix"));
  const auto declared = little_u64(data.data());
  if (declared == std::numeric_limits<std::uint64_t>::max()) {
    if (data.size() - 8 < minimum_decoded_bytes)
      return Result::failure(error(QueryError::CorruptPayload, "truncated Arrow uncompressed IPC buffer"));
    data.erase(data.begin(), data.begin() + 8);
    metrics_.decoded_bytes += data.size();
    return Result::success(std::move(data));
  }
  if (declared > std::uint64_t(std::numeric_limits<std::int64_t>::max()) ||
      declared < minimum_decoded_bytes)
    return Result::failure(error(QueryError::CorruptPayload, "invalid Arrow decoded buffer length"));
  if (declared > config_.max_buffer_bytes || declared > BinaryValue().max_size())
    return Result::failure(error(QueryError::ResourceExhausted, "Arrow decompressed buffer exceeds byte limit"));
  try {
    if (cancelled()) return Result::failure(cancelled_status());
    auto codec = arrow::util::Codec::Create(compression == ArrowBufferCompression::Lz4
      ? arrow::Compression::LZ4_FRAME : arrow::Compression::ZSTD);
    if (!codec.ok())
      return Result::failure(error(QueryError::UnsupportedCapability, "Arrow buffer codec unavailable"));
    BinaryValue output(static_cast<std::size_t>(declared));
    auto decoded = (*codec)->Decompress(static_cast<std::int64_t>(data.size() - 8),
      data.data() + 8, static_cast<std::int64_t>(declared), output.data());
    if (!decoded.ok() || *decoded != static_cast<std::int64_t>(declared))
      return Result::failure(error(QueryError::CorruptPayload, "invalid compressed Arrow buffer"));
    ++metrics_.decompressions;
    metrics_.decoded_bytes += declared;
    if (cancelled()) return Result::failure(cancelled_status());
    return Result::success(std::move(output));
  } catch (const std::bad_alloc&) {
    return Result::failure(error(QueryError::ResourceExhausted, "Arrow decompression allocation failed"));
  }
}

} // namespace iotdbfs
