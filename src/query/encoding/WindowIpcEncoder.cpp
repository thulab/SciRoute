#include "iotdbfs/query/encoding/WindowIpcEncoder.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <algorithm>
#include <iomanip>
#include <limits>
#include <locale>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace iotdbfs {
namespace {
using Metadata = std::pair<std::vector<std::string>, std::vector<std::string>>;

QueryStatus failed(QueryError code, const std::string& message) {
  return QueryStatus::failure(code, QueryPhase::Encoding, message);
}

QueryStatus arrow_failure(const arrow::Status& status) {
  return failed(QueryError::Internal, "Arrow IPC encoding failed: " +
    status.ToString());
}

void json_string(std::string& output, const std::string& value) {
  constexpr char hex[] = "0123456789abcdef";
  output.push_back('"');
  for (unsigned char ch : value) {
    switch (ch) {
      case '"': output += "\\\""; break;
      case '\\': output += "\\\\"; break;
      case '\b': output += "\\b"; break;
      case '\f': output += "\\f"; break;
      case '\n': output += "\\n"; break;
      case '\r': output += "\\r"; break;
      case '\t': output += "\\t"; break;
      default:
        if (ch < 0x20) {
          output += "\\u00";
          output.push_back(hex[ch >> 4]);
          output.push_back(hex[ch & 15]);
        } else output.push_back(static_cast<char>(ch));
    }
  }
  output.push_back('"');
}

void json_interval(std::string& output, StepInterval interval) {
  output += "[" + std::to_string(interval.begin) + "," +
    std::to_string(interval.end) + "]";
}

void json_channels(std::string& output,
    const std::vector<WindowChannelSpan>& channels) {
  output.push_back('[');
  for (std::size_t i = 0; i < channels.size(); ++i) {
    if (i) output.push_back(',');
    const auto& channel = channels[i];
    output += "{\"field_id\":"; json_string(output, channel.channel.field_id);
    output += ",\"ordinal\":" + std::to_string(channel.channel.ordinal);
    output += ",\"label\":"; json_string(output, channel.channel.label);
    output += ",\"available_steps\":";
    if (channel.available_steps) json_interval(output, *channel.available_steps);
    else output += "null";
    output += ",\"available_ranges\":[";
    for (std::size_t j = 0; j < channel.available_ranges.size(); ++j) {
      if (j) output.push_back(',');
      json_interval(output, channel.available_ranges[j]);
    }
    output += "]}";
  }
  output.push_back(']');
}

void json_spans(std::string& output, const std::vector<WindowSourceSpan>& spans) {
  output.push_back('[');
  for (std::size_t i = 0; i < spans.size(); ++i) {
    if (i) output.push_back(',');
    const auto& span = spans[i];
    output += "{\"role\":";
    json_string(output, span.role == WindowRole::Input ? "input" : "target");
    output += ",\"path\":"; json_string(output, span.source.path);
    output += ",\"version\":" + std::to_string(span.source.version);
    output += ",\"format_id\":"; json_string(output, span.source.format_id);
    output += ",\"field_id\":"; json_string(output, span.field_id);
    output += ",\"record_batch_index\":";
    if (span.record_batch_index) output += std::to_string(*span.record_batch_index);
    else output += "null";
    output += ",\"row_index\":";
    if (span.row_index) output += std::to_string(*span.row_index);
    else output += "null";
    output += ",\"channel_ordinal\":" + std::to_string(span.channel_ordinal);
    output += ",\"steps\":"; json_interval(output, span.steps);
    output += ",\"fragment_key\":"; json_string(output, span.fragment_key);
    output.push_back('}');
  }
  output.push_back(']');
}

std::string descriptor_json(const WindowDescriptor& descriptor) {
  std::string output;
  output.reserve(256 + descriptor.source_spans.size() * 160);
  output += "{\"sample_id\":"; json_string(output, descriptor.sample_id);
  output += ",\"view_id\":"; json_string(output, descriptor.view_id);
  output += ",\"series_id\":"; json_string(output, descriptor.series_id);
  output += ",\"anchor\":" + std::to_string(descriptor.anchor);
  output += ",\"epoch\":" + std::to_string(descriptor.epoch);
  output += ",\"seed\":" + std::to_string(descriptor.seed);
  output += ",\"shard_index\":" + std::to_string(descriptor.shard_index);
  output += ",\"input\":"; json_interval(output, descriptor.input);
  output += ",\"target\":"; json_interval(output, descriptor.target);
  output += ",\"timezone\":"; json_string(output, descriptor.timezone);
  output += ",\"input_channels\":";
  json_channels(output, descriptor.input_channels);
  output += ",\"target_channels\":";
  json_channels(output, descriptor.target_channels);
  output += ",\"source_spans\":";
  json_spans(output, descriptor.source_spans);
  output.push_back('}');
  return output;
}

void metadata_add(Metadata& metadata, std::string key, std::string value) {
  metadata.first.push_back(std::move(key));
  metadata.second.push_back(std::move(value));
}

void channel_metadata(Metadata& metadata, const char* role,
    const std::vector<TrainingChannelRef>& channels) {
  metadata_add(metadata, std::string("iotdbfs.") + role + ".count",
    std::to_string(channels.size()));
  for (std::size_t i = 0; i < channels.size(); ++i) {
    const std::string prefix = std::string("iotdbfs.") + role + "." +
      std::to_string(i) + ".";
    metadata_add(metadata, prefix + "field_id", channels[i].field_id);
    metadata_add(metadata, prefix + "ordinal", std::to_string(channels[i].ordinal));
    metadata_add(metadata, prefix + "label", channels[i].label);
  }
}

std::string decimal(double value) {
  std::ostringstream output;
  output.imbue(std::locale::classic());
  output << std::setprecision(std::numeric_limits<double>::max_digits10)
    << value;
  return output.str();
}

void transform_metadata(Metadata& metadata, const TrainingViewSpec& view) {
  const auto& transform = view.transform;
  metadata_add(metadata, "iotdbfs.transform.kind",
    transform.kind == TrainingTransformKind::Standardize ? "standardize" :
      "identity");
  metadata_add(metadata, "iotdbfs.transform.artifact_id", transform.artifact_id);
  metadata_add(metadata, "iotdbfs.transform.statistics.count",
    std::to_string(transform.statistics.size()));
  std::map<std::pair<std::string, std::uint64_t>, std::pair<double, double>> ordered;
  for (const auto& statistic : transform.statistics)
    ordered[{statistic.field_id, statistic.ordinal}] =
      {statistic.mean, statistic.stddev};
  std::size_t index = 0;
  for (const auto& entry : ordered) {
    const auto prefix = "iotdbfs.transform.statistics." +
      std::to_string(index++) + ".";
    metadata_add(metadata, prefix + "field_id", entry.first.first);
    metadata_add(metadata, prefix + "ordinal", std::to_string(entry.first.second));
    metadata_add(metadata, prefix + "mean", decimal(entry.second.first));
    metadata_add(metadata, prefix + "stddev", decimal(entry.second.second));
  }
}

std::shared_ptr<arrow::Schema> make_schema(const TrainingViewSpec& view,
    const WindowBatch& batch) {
  std::vector<std::shared_ptr<arrow::Field>> fields = {
    arrow::field("sample_id", arrow::utf8(), true),
    arrow::field("sample_index", arrow::uint32(), false),
    arrow::field("role", arrow::utf8(), false),
    arrow::field("step", arrow::uint64(), false),
    arrow::field("time_ns", arrow::int64(), true),
    arrow::field("descriptor_json", arrow::large_utf8(), true),
  };
  for (std::size_t i = 0; i < view.input_channels.size(); ++i)
    fields.push_back(arrow::field("input." + std::to_string(i),
      arrow::float32(), true));
  for (std::size_t i = 0; i < view.target_channels.size(); ++i)
    fields.push_back(arrow::field("target." + std::to_string(i),
      arrow::float32(), true));
  Metadata metadata;
  metadata_add(metadata, "iotdbfs.protocol", "iotdbfs-window-flat/3");
  metadata_add(metadata, "iotdbfs.sample_id_location", "first_row_per_sample");
  metadata_add(metadata, "iotdbfs.view_id", view.view_id);
  metadata_add(metadata, "iotdbfs.view_identity", training_view_identity(view));
  metadata_add(metadata, "iotdbfs.snapshot_fingerprint", view.snapshot_fingerprint);
  metadata_add(metadata, "iotdbfs.input_length", std::to_string(view.input_length));
  metadata_add(metadata, "iotdbfs.gap", std::to_string(view.gap));
  metadata_add(metadata, "iotdbfs.target_length", std::to_string(view.target_length));
  metadata_add(metadata, "iotdbfs.stride", std::to_string(view.stride));
  metadata_add(metadata, "iotdbfs.sample_count",
    std::to_string(batch.samples().size()));
  channel_metadata(metadata, "input", view.input_channels);
  channel_metadata(metadata, "target", view.target_channels);
  transform_metadata(metadata, view);
  if (batch.continuation()) {
    const auto& next = *batch.continuation();
    metadata_add(metadata, "iotdbfs.continuation.selection_identity",
      next.selection_identity);
    metadata_add(metadata, "iotdbfs.continuation.series_index",
      std::to_string(next.series_index));
    metadata_add(metadata, "iotdbfs.continuation.next_anchor",
      std::to_string(next.next_anchor));
    metadata_add(metadata, "iotdbfs.continuation.eligible_count",
      std::to_string(next.eligible_count));
  }
  return arrow::schema(std::move(fields), arrow::key_value_metadata(
    std::move(metadata.first), std::move(metadata.second)));
}

class RowBuilders {
public:
  RowBuilders(std::size_t inputs, std::size_t targets)
    : inputs_(inputs), targets_(targets) {
    for (auto& builder : inputs_) builder = std::make_unique<arrow::FloatBuilder>();
    for (auto& builder : targets_) builder = std::make_unique<arrow::FloatBuilder>();
  }

  arrow::Status append(const WindowSample& sample, std::uint32_t sample_index,
      WindowRole role, std::uint64_t offset, bool first) {
    const auto& descriptor = sample.descriptor;
    if (first) ARROW_RETURN_NOT_OK(sample_id_.Append(descriptor.sample_id));
    else ARROW_RETURN_NOT_OK(sample_id_.AppendNull());
    ARROW_RETURN_NOT_OK(sample_index_.Append(sample_index));
    ARROW_RETURN_NOT_OK(role_.Append(role == WindowRole::Input ? "input" : "target"));
    const auto step = (role == WindowRole::Input ? descriptor.input.begin :
      descriptor.target.begin) + offset;
    ARROW_RETURN_NOT_OK(step_.Append(step));
    const auto& times = role == WindowRole::Input ? descriptor.input_times_ns :
      descriptor.target_times_ns;
    if (times.empty()) ARROW_RETURN_NOT_OK(time_ns_.AppendNull());
    else ARROW_RETURN_NOT_OK(time_ns_.Append(times[offset]));
    if (first) ARROW_RETURN_NOT_OK(descriptor_.Append(descriptor_json(descriptor)));
    else ARROW_RETURN_NOT_OK(descriptor_.AppendNull());
    for (std::size_t i = 0; i < inputs_.size(); ++i) {
      if (role != WindowRole::Input ||
          !sample.input_validity[offset * inputs_.size() + i])
        ARROW_RETURN_NOT_OK(inputs_[i]->AppendNull());
      else ARROW_RETURN_NOT_OK(inputs_[i]->Append(
        sample.input_values[offset * inputs_.size() + i]));
    }
    for (std::size_t i = 0; i < targets_.size(); ++i) {
      if (role != WindowRole::Target ||
          !sample.target_validity[offset * targets_.size() + i])
        ARROW_RETURN_NOT_OK(targets_[i]->AppendNull());
      else ARROW_RETURN_NOT_OK(targets_[i]->Append(
        sample.target_values[offset * targets_.size() + i]));
    }
    ++rows_;
    return arrow::Status::OK();
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> finish(
      const std::shared_ptr<arrow::Schema>& schema) {
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(6 + inputs_.size() + targets_.size());
    ARROW_ASSIGN_OR_RAISE(auto sample_id, sample_id_.Finish());
    arrays.push_back(std::move(sample_id));
    ARROW_ASSIGN_OR_RAISE(auto sample_index, sample_index_.Finish());
    arrays.push_back(std::move(sample_index));
    ARROW_ASSIGN_OR_RAISE(auto role, role_.Finish());
    arrays.push_back(std::move(role));
    ARROW_ASSIGN_OR_RAISE(auto step, step_.Finish());
    arrays.push_back(std::move(step));
    ARROW_ASSIGN_OR_RAISE(auto time_ns, time_ns_.Finish());
    arrays.push_back(std::move(time_ns));
    ARROW_ASSIGN_OR_RAISE(auto descriptor, descriptor_.Finish());
    arrays.push_back(std::move(descriptor));
    for (auto& builder : inputs_) {
      ARROW_ASSIGN_OR_RAISE(auto value, builder->Finish());
      arrays.push_back(std::move(value));
    }
    for (auto& builder : targets_) {
      ARROW_ASSIGN_OR_RAISE(auto value, builder->Finish());
      arrays.push_back(std::move(value));
    }
    auto batch = arrow::RecordBatch::Make(schema, rows_, std::move(arrays));
    rows_ = 0;
    return batch;
  }

  std::int64_t rows() const { return rows_; }

private:
  arrow::StringBuilder sample_id_;
  arrow::UInt32Builder sample_index_;
  arrow::StringBuilder role_;
  arrow::UInt64Builder step_;
  arrow::Int64Builder time_ns_;
  arrow::LargeStringBuilder descriptor_;
  std::vector<std::unique_ptr<arrow::FloatBuilder>> inputs_, targets_;
  std::int64_t rows_{0};
};

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
      return arrow::Status::CapacityError("window IPC byte limit exceeded");
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

QueryStatus write_failure(const arrow::Status& status,
    const CappedOutputStream& sink) {
  return sink.exceeded() ? failed(QueryError::ResourceExhausted,
    "window IPC stream exceeds encoded-byte limit") : arrow_failure(status);
}
} // namespace

QueryResult<BinaryValue> encode_window_batch_ipc(const TrainingViewSpec& view,
    const WindowBatch& batch, std::uint64_t max_ipc_bytes) {
  using Result = QueryResult<BinaryValue>;
  if (!max_ipc_bytes || batch.view_id() != view.view_id ||
      !validate_training_transform(view).ok() ||
      batch.samples().size() > std::numeric_limits<std::uint32_t>::max())
    return Result::failure(failed(QueryError::InvalidQuery,
      "invalid window IPC view, batch, or byte limit"));
  const auto inputs = view.input_channels.size();
  const auto targets = view.target_channels.size();
  if (!view.input_length || inputs == 0 ||
      (view.target_length == 0) != (targets == 0) ||
      view.input_length > std::numeric_limits<std::size_t>::max() / inputs ||
      (targets && view.target_length >
        std::numeric_limits<std::size_t>::max() / targets))
    return Result::failure(failed(QueryError::InvalidQuery,
      "invalid window IPC shape"));
  const auto input_size = static_cast<std::size_t>(view.input_length * inputs);
  const auto target_size = static_cast<std::size_t>(view.target_length * targets);
  const auto identity = training_view_identity(view);
  for (const auto& sample : batch.samples()) {
    const auto& d = sample.descriptor;
    if (d.view_id != view.view_id ||
        d.sample_id != training_sample_id(view.snapshot_fingerprint,
          identity, d.series_id, d.anchor) ||
        d.input.end < d.input.begin ||
        d.input.end - d.input.begin != view.input_length ||
        d.target.end < d.target.begin ||
        d.target.end - d.target.begin != view.target_length ||
        sample.input_values.size() != input_size ||
        sample.input_validity.size() != input_size ||
        sample.target_values.size() != target_size ||
        sample.target_validity.size() != target_size ||
        (!d.input_times_ns.empty() &&
          d.input_times_ns.size() != view.input_length) ||
        (!d.target_times_ns.empty() &&
          d.target_times_ns.size() != view.target_length))
      return Result::failure(failed(QueryError::InvalidQuery,
        "window IPC sample does not match the view"));
  }
  auto schema = make_schema(view, batch);
  auto sink = std::make_shared<CappedOutputStream>(max_ipc_bytes);
  auto writer_result = arrow::ipc::MakeStreamWriter(sink, schema);
  if (!writer_result.ok()) return Result::failure(
    write_failure(writer_result.status(), *sink));
  auto writer = writer_result.ValueOrDie();
  RowBuilders builders(inputs, targets);
  const std::uint64_t group_rows = 1024;
  for (std::size_t sample_index = 0; sample_index < batch.samples().size(); ++sample_index) {
    const auto& sample = batch.samples()[sample_index];
    bool first = true;
    for (const auto role : {WindowRole::Input, WindowRole::Target}) {
      const auto length = role == WindowRole::Input ? view.input_length :
        view.target_length;
      for (std::uint64_t offset = 0; offset < length; ++offset) {
        auto status = builders.append(sample,
          static_cast<std::uint32_t>(sample_index), role, offset, first);
        if (!status.ok()) return Result::failure(arrow_failure(status));
        first = false;
        if (builders.rows() == group_rows) {
          auto record = builders.finish(schema);
          if (!record.ok()) return Result::failure(arrow_failure(record.status()));
          status = writer->WriteRecordBatch(*record.ValueOrDie());
          if (!status.ok()) return Result::failure(write_failure(status, *sink));
        }
      }
    }
  }
  if (builders.rows()) {
    auto record = builders.finish(schema);
    if (!record.ok()) return Result::failure(arrow_failure(record.status()));
    auto status = writer->WriteRecordBatch(*record.ValueOrDie());
    if (!status.ok()) return Result::failure(write_failure(status, *sink));
  }
  auto status = writer->Close();
  if (!status.ok()) return Result::failure(write_failure(status, *sink));
  return Result::success(sink->take_bytes());
}

} // namespace iotdbfs
