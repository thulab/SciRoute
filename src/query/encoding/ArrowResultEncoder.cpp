#include "iotdbfs/query/encoding/ArrowResultEncoder.h"

#include "iotdbfs/query/core/QueryPlanning.h"
#include "iotdbfs/query/core/QueryProfile.h"

#include <arrow/api.h>
#include <arrow/io/interfaces.h>
#include <arrow/ipc/api.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace iotdbfs {
namespace {

constexpr const char* kProtocol = "iotdbfs-query-batch-ipc/1";

QueryStatus encoding_error(QueryError code, std::string message) {
  return QueryStatus::failure(code, QueryPhase::Encoding, std::move(message));
}

QueryStatus arrow_error(const arrow::Status& status, bool exceeded = false) {
  return encoding_error(exceeded ? QueryError::ResourceExhausted : QueryError::Internal,
    "Arrow IPC encoding failed: " + status.ToString());
}

class CappedOutputStream final : public arrow::io::OutputStream {
public:
  explicit CappedOutputStream(std::uint64_t limit) : limit_(limit) {}

  arrow::Status Close() override { closed_ = true; return arrow::Status::OK(); }
  bool closed() const override { return closed_; }
  arrow::Result<std::int64_t> Tell() const override {
    return static_cast<std::int64_t>(bytes_.size());
  }
  arrow::Status Write(const void* data, std::int64_t size) override {
    if (closed_) return arrow::Status::IOError("Arrow IPC output is closed");
    if (size < 0) return arrow::Status::Invalid("negative Arrow IPC write size");
    if (size == 0) return arrow::Status::OK();
    if (static_cast<std::uint64_t>(size) > limit_ - bytes_.size()) {
      exceeded_ = true;
      return arrow::Status::CapacityError("Arrow IPC byte limit exceeded");
    }
    const auto* begin = static_cast<const std::uint8_t*>(data);
    bytes_.insert(bytes_.end(), begin, begin + size);
    return arrow::Status::OK();
  }
  bool exceeded() const { return exceeded_; }
  BinaryValue take_bytes() { return std::move(bytes_); }

private:
  std::uint64_t limit_;
  BinaryValue bytes_;
  bool closed_{false};
  bool exceeded_{false};
};

std::string type_name(LogicalTypeId type) {
  switch (type) {
  case LogicalTypeId::Boolean: return "boolean";
  case LogicalTypeId::Int32: return "int32";
  case LogicalTypeId::Int64: return "int64";
  case LogicalTypeId::UInt32: return "uint32";
  case LogicalTypeId::UInt64: return "uint64";
  case LogicalTypeId::Float32: return "float32";
  case LogicalTypeId::Float64: return "float64";
  case LogicalTypeId::Utf8: return "utf8";
  case LogicalTypeId::Binary: return "binary";
  case LogicalTypeId::TimestampNs: return "timestamp_ns";
  }
  return "unknown";
}

std::string role_name(FieldRole role) {
  switch (role) {
  case FieldRole::Attribute: return "attribute";
  case FieldRole::Dimension: return "dimension";
  case FieldRole::Measure: return "measure";
  case FieldRole::SourceIdentity: return "source_identity";
  }
  return "unknown";
}

std::string domain_name(DomainKind kind) {
  switch (kind) {
  case DomainKind::Rows: return "rows";
  case DomainKind::DenseGrid: return "dense_grid";
  case DomainKind::TimeSeries: return "time_series";
  }
  return "unknown";
}

std::shared_ptr<arrow::DataType> arrow_type(const LogicalType& type) {
  switch (type.id) {
  case LogicalTypeId::Boolean: return arrow::boolean();
  case LogicalTypeId::Int32: return arrow::int32();
  case LogicalTypeId::Int64: return arrow::int64();
  case LogicalTypeId::UInt32: return arrow::uint32();
  case LogicalTypeId::UInt64: return arrow::uint64();
  case LogicalTypeId::Float32: return arrow::float32();
  case LogicalTypeId::Float64: return arrow::float64();
  case LogicalTypeId::Utf8: return arrow::utf8();
  case LogicalTypeId::Binary: return arrow::binary();
  case LogicalTypeId::TimestampNs:
    return type.timezone ? arrow::timestamp(arrow::TimeUnit::NANO, *type.timezone)
                         : arrow::timestamp(arrow::TimeUnit::NANO);
  }
  return {};
}

std::shared_ptr<arrow::Schema> make_schema(const LogicalSchema& schema,
    const QueryResultMetadata& descriptors) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(schema.fields().size());
  for (const auto& field : schema.fields()) {
    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    metadata->Append("iotdbfs.logical_field_id", field.field_id);
    metadata->Append("iotdbfs.display_name", field.display_name);
    metadata->Append("iotdbfs.role", role_name(field.role));
    if (field.unit) metadata->Append("iotdbfs.unit", *field.unit);
    if (field.type.timezone)
      metadata->Append("iotdbfs.timezone", *field.type.timezone);
    fields.push_back(arrow::field(field.field_id, arrow_type(field.type),
      field.type.nullable, std::move(metadata)));
  }
  auto metadata = std::make_shared<arrow::KeyValueMetadata>();
  metadata->Append("iotdbfs.protocol", kProtocol);
  metadata->Append("iotdbfs.result_kind", "query");
  for (const auto& entry : descriptors) metadata->Append(entry.first, entry.second);
  return arrow::schema(std::move(fields), std::move(metadata));
}

std::shared_ptr<arrow::KeyValueMetadata> make_batch_metadata(
    const QueryBatch& batch) {
  auto metadata = std::make_shared<arrow::KeyValueMetadata>();
  metadata->Append("iotdbfs.domain.present", batch.domain() ? "1" : "0");
  if (batch.domain()) {
    const auto& domain = *batch.domain();
    metadata->Append("iotdbfs.domain.kind", domain_name(domain.kind));
    metadata->Append("iotdbfs.domain.dimensions",
      std::to_string(domain.dimensions.size()));
    metadata->Append("iotdbfs.domain.origin.present",
      domain.origin.empty() ? "0" : "1");
    for (std::size_t i = 0; i < domain.dimensions.size(); ++i) {
      const auto prefix = "iotdbfs.domain.dimension." + std::to_string(i) + ".";
      const auto& dimension = domain.dimensions[i];
      metadata->Append(prefix + "field_id", dimension.field.field_id);
      metadata->Append(prefix + "type", type_name(dimension.type.id));
      metadata->Append(prefix + "nullable", dimension.type.nullable ? "1" : "0");
      if (dimension.type.timezone)
        metadata->Append(prefix + "timezone", *dimension.type.timezone);
      if (dimension.unit) metadata->Append(prefix + "unit", *dimension.unit);
      metadata->Append(prefix + "shape", std::to_string(domain.shape[i]));
      if (!domain.origin.empty())
        metadata->Append(prefix + "origin", std::to_string(domain.origin[i]));
    }
  }
  metadata->Append("iotdbfs.sources", std::to_string(batch.sources().size()));
  for (std::size_t i = 0; i < batch.sources().size(); ++i) {
    const auto prefix = "iotdbfs.source." + std::to_string(i) + ".";
    const auto& source = batch.sources()[i];
    metadata->Append(prefix + "path", source.path);
    metadata->Append(prefix + "version", std::to_string(source.version));
    metadata->Append(prefix + "format_id", source.format_id);
  }
  return metadata;
}

bool same_schema(const LogicalSchema& left, const LogicalSchema& right) {
  if (left.fields().size() != right.fields().size()) return false;
  for (std::size_t i = 0; i < left.fields().size(); ++i) {
    const auto& a = left.fields()[i];
    const auto& b = right.fields()[i];
    if (a.field_id != b.field_id || a.display_name != b.display_name ||
        a.type != b.type || a.role != b.role || a.unit != b.unit)
      return false;
  }
  return true;
}

bool is_valid(const LogicalArray& column, std::size_t index) {
  return column.validity.empty() ||
    (column.validity[index / 8] & (1u << (index % 8))) != 0;
}

template<typename Builder, typename Value>
arrow::Status append_numeric(arrow::ArrayBuilder* base,
    const LogicalArray& column, const std::vector<Value>& values) {
  auto* builder = static_cast<Builder*>(base);
  for (std::size_t i = 0; i < values.size(); ++i) {
    arrow::Status status = is_valid(column, i)
      ? builder->Append(values[i]) : builder->AppendNull();
    if (!status.ok()) return status;
  }
  return arrow::Status::OK();
}

arrow::Status append_column(arrow::ArrayBuilder* builder,
    const LogicalArray& column) {
  switch (column.type.id) {
  case LogicalTypeId::Boolean:
    return append_numeric<arrow::BooleanBuilder>(builder, column,
      std::get<std::vector<bool>>(column.values));
  case LogicalTypeId::Int32:
    return append_numeric<arrow::Int32Builder>(builder, column,
      std::get<std::vector<std::int32_t>>(column.values));
  case LogicalTypeId::Int64:
    return append_numeric<arrow::Int64Builder>(builder, column,
      std::get<std::vector<std::int64_t>>(column.values));
  case LogicalTypeId::UInt32:
    return append_numeric<arrow::UInt32Builder>(builder, column,
      std::get<std::vector<std::uint32_t>>(column.values));
  case LogicalTypeId::UInt64:
    return append_numeric<arrow::UInt64Builder>(builder, column,
      std::get<std::vector<std::uint64_t>>(column.values));
  case LogicalTypeId::Float32:
    return append_numeric<arrow::FloatBuilder>(builder, column,
      std::get<std::vector<float>>(column.values));
  case LogicalTypeId::Float64:
    return append_numeric<arrow::DoubleBuilder>(builder, column,
      std::get<std::vector<double>>(column.values));
  case LogicalTypeId::Utf8: {
    auto* typed = static_cast<arrow::StringBuilder*>(builder);
    const auto& values = std::get<std::vector<std::string>>(column.values);
    for (std::size_t i = 0; i < values.size(); ++i) {
      if (values[i].size() > static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max()))
        return arrow::Status::CapacityError("UTF-8 value exceeds Arrow offset limit");
      auto status = is_valid(column, i) ? typed->Append(values[i]) : typed->AppendNull();
      if (!status.ok()) return status;
    }
    return arrow::Status::OK();
  }
  case LogicalTypeId::Binary: {
    auto* typed = static_cast<arrow::BinaryBuilder*>(builder);
    const auto& values = std::get<std::vector<BinaryValue>>(column.values);
    for (std::size_t i = 0; i < values.size(); ++i) {
      if (values[i].size() > static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max()))
        return arrow::Status::CapacityError("binary value exceeds Arrow offset limit");
      auto status = is_valid(column, i)
        ? typed->Append(values[i].data(), static_cast<std::int32_t>(values[i].size()))
        : typed->AppendNull();
      if (!status.ok()) return status;
    }
    return arrow::Status::OK();
  }
  case LogicalTypeId::TimestampNs: {
    auto* typed = static_cast<arrow::TimestampBuilder*>(builder);
    const auto& values = std::get<std::vector<TimestampNs>>(column.values);
    for (std::size_t i = 0; i < values.size(); ++i) {
      auto status = is_valid(column, i)
        ? typed->Append(values[i].value) : typed->AppendNull();
      if (!status.ok()) return status;
    }
    return arrow::Status::OK();
  }
  }
  return arrow::Status::NotImplemented("unknown logical type");
}

QueryResult<std::shared_ptr<arrow::RecordBatch>> make_record_batch(
    const QueryBatch& batch, const std::shared_ptr<arrow::Schema>& schema) {
  if (batch.row_count() > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
    return QueryResult<std::shared_ptr<arrow::RecordBatch>>::failure(
      encoding_error(QueryError::ResourceExhausted,
        "query batch exceeds Arrow row-count limit"));
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(batch.columns().size());
  for (std::size_t i = 0; i < batch.columns().size(); ++i) {
    const auto& column = batch.columns()[i];
    auto made = arrow::MakeBuilder(schema->field(i)->type());
    if (!made.ok())
      return QueryResult<std::shared_ptr<arrow::RecordBatch>>::failure(
        arrow_error(made.status()));
    auto builder = std::move(made).ValueOrDie();
    auto status = append_column(builder.get(), column);
    if (!status.ok())
      return QueryResult<std::shared_ptr<arrow::RecordBatch>>::failure(
        arrow_error(status, status.IsCapacityError()));
    std::shared_ptr<arrow::Array> array;
    status = builder->Finish(&array);
    if (!status.ok())
      return QueryResult<std::shared_ptr<arrow::RecordBatch>>::failure(
        arrow_error(status, status.IsCapacityError()));
    arrays.push_back(std::move(array));
  }
  return QueryResult<std::shared_ptr<arrow::RecordBatch>>::success(
    arrow::RecordBatch::Make(schema, static_cast<std::int64_t>(batch.row_count()),
      std::move(arrays)));
}

class StreamEncoder {
public:
  static QueryResult<StreamEncoder> create(const LogicalSchema& schema,
      std::uint64_t limit, const QueryResultMetadata& metadata) {
    QueryProfileTimer timer("typed_ipc_encode");
    QueryProfileTimer schema_timer("typed_ipc_schema");
    if (limit == 0 || limit > static_cast<std::uint64_t>(
        std::numeric_limits<std::int64_t>::max()) ||
        limit > std::numeric_limits<std::size_t>::max())
      return QueryResult<StreamEncoder>::failure(encoding_error(
        QueryError::InvalidQuery, "invalid Arrow IPC byte limit"));
    auto valid = ArrowResultEncoder::validate_metadata(metadata, limit);
    if (!valid.ok()) return QueryResult<StreamEncoder>::failure(valid);
    auto sink = std::make_shared<CappedOutputStream>(limit);
    auto native_schema = make_schema(schema, metadata);
    auto writer = arrow::ipc::MakeStreamWriter(sink, native_schema);
    if (!writer.ok())
      return QueryResult<StreamEncoder>::failure(
        arrow_error(writer.status(), sink->exceeded()));
    return QueryResult<StreamEncoder>::success(StreamEncoder(schema,
      std::move(native_schema), std::move(sink), std::move(writer).ValueOrDie()));
  }

  QueryStatus append(const QueryBatch& batch) {
    QueryProfileTimer timer("typed_ipc_encode");
    if (!same_schema(schema_, batch.schema()))
      return encoding_error(QueryError::SchemaMismatch,
        "query batch schema differs from IPC stream schema");
    auto native = [&] {
      QueryProfileTimer timer("typed_ipc_array_build");
      return make_record_batch(batch, native_schema_);
    }();
    if (!native.ok()) return native.status();
    auto status = [&] {
      QueryProfileTimer timer("typed_ipc_write");
      return writer_->WriteRecordBatch(*native.value(), make_batch_metadata(batch));
    }();
    if (status.ok()) {
      query_profile_count("typed_ipc_batches");
      query_profile_count("typed_ipc_rows", batch.row_count());
    }
    return status.ok() ? QueryStatus::success() :
      arrow_error(status, sink_->exceeded());
  }

  QueryResult<BinaryValue> finish() {
    QueryProfileTimer timer("typed_ipc_encode");
    QueryProfileTimer finish_timer("typed_ipc_finish");
    auto status = writer_->Close();
    if (!status.ok()) return QueryResult<BinaryValue>::failure(
      arrow_error(status, sink_->exceeded()));
    status = sink_->Close();
    if (!status.ok()) return QueryResult<BinaryValue>::failure(
      arrow_error(status, sink_->exceeded()));
    auto bytes = sink_->take_bytes();
    query_profile_count("typed_ipc_bytes", bytes.size());
    return QueryResult<BinaryValue>::success(std::move(bytes));
  }

private:
  StreamEncoder(const LogicalSchema& schema,
      std::shared_ptr<arrow::Schema> native_schema,
      std::shared_ptr<CappedOutputStream> sink,
      std::shared_ptr<arrow::ipc::RecordBatchWriter> writer)
    : schema_(schema), native_schema_(std::move(native_schema)),
      sink_(std::move(sink)), writer_(std::move(writer)) {}

  LogicalSchema schema_;
  std::shared_ptr<arrow::Schema> native_schema_;
  std::shared_ptr<CappedOutputStream> sink_;
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer_;
};

} // namespace

QueryStatus ArrowResultEncoder::validate_metadata(const QueryResultMetadata& metadata,
    std::uint64_t max_ipc_bytes) {
  if (metadata.size() > max_metadata_entries)
    return encoding_error(QueryError::ResourceExhausted, "query result metadata entry limit exceeded");
  std::uint64_t remaining = std::min(max_metadata_bytes, max_ipc_bytes);
  std::set<std::string> keys;
  for (const auto& entry : metadata) {
    if (entry.first.size() > max_metadata_key_bytes)
      return encoding_error(QueryError::ResourceExhausted, "query result metadata key limit exceeded");
    if (entry.first.empty() || entry.first == "iotdbfs.protocol" || entry.first == "iotdbfs.result_kind" ||
        entry.first.find('\0') != std::string::npos || !keys.insert(entry.first).second)
      return encoding_error(QueryError::InvalidQuery, "duplicate, empty or reserved query result metadata key");
    if (entry.first.size() > remaining)
      return encoding_error(QueryError::ResourceExhausted, "query result metadata byte limit exceeded");
    remaining -= entry.first.size();
    if (entry.second.size() > remaining)
      return encoding_error(QueryError::ResourceExhausted, "query result metadata byte limit exceeded");
    remaining -= entry.second.size();
  }
  return QueryStatus::success();
}

QueryResult<BinaryValue> ArrowResultEncoder::encode_query_batch(
    const QueryBatch& batch, std::uint64_t max_ipc_bytes,
    const QueryResultMetadata& metadata) {
  auto encoder = StreamEncoder::create(batch.schema(), max_ipc_bytes, metadata);
  if (!encoder.ok()) return QueryResult<BinaryValue>::failure(encoder.status());
  auto status = encoder.value().append(batch);
  if (!status.ok()) return QueryResult<BinaryValue>::failure(std::move(status));
  return encoder.value().finish();
}

QueryResult<BinaryValue> ArrowResultEncoder::encode_query_batches(
    const LogicalSchema& schema, const std::vector<QueryBatch>& batches,
    std::uint64_t max_ipc_bytes, const QueryResultMetadata& metadata) {
  auto encoder = StreamEncoder::create(schema, max_ipc_bytes, metadata);
  if (!encoder.ok()) return QueryResult<BinaryValue>::failure(encoder.status());
  for (const auto& batch : batches) {
    auto status = encoder.value().append(batch);
    if (!status.ok()) return QueryResult<BinaryValue>::failure(std::move(status));
  }
  return encoder.value().finish();
}

QueryResult<BinaryValue> ArrowResultEncoder::encode_query_cursor(
    const LogicalSchema& schema, QueryBatchCursor& cursor,
    std::uint64_t max_ipc_bytes, const QueryResultMetadata& metadata) {
  if (!same_schema(schema, cursor.schema())) {
    cursor.cancel();
    return QueryResult<BinaryValue>::failure(encoding_error(
      QueryError::SchemaMismatch, "cursor schema differs from IPC stream schema"));
  }
  auto encoder = StreamEncoder::create(schema, max_ipc_bytes, metadata);
  if (!encoder.ok()) {
    cursor.cancel();
    return QueryResult<BinaryValue>::failure(encoder.status());
  }
  for (;;) {
    auto next = [&] {
      QueryProfileTimer timer("typed_cursor_execute");
      query_profile_count("typed_cursor_next_calls");
      return cursor.next();
    }();
    if (!next.ok()) {
      cursor.cancel();
      return QueryResult<BinaryValue>::failure(next.status());
    }
    if (!next.value()) break;
    query_profile_count("typed_cursor_batches");
    query_profile_count("typed_cursor_rows", next.value()->row_count());
    auto status = encoder.value().append(*next.value());
    if (!status.ok()) {
      cursor.cancel();
      return QueryResult<BinaryValue>::failure(std::move(status));
    }
  }
  auto completed = encoder.value().finish();
  if (!completed.ok()) cursor.cancel();
  return completed;
}

} // namespace iotdbfs
