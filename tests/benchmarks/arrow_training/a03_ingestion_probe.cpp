// A03 ingestion oracle: the adapter only reads List indexes; the independent
// Arrow reader decodes full batches afterwards to verify the physical index.
// Build against iotdbfs_core and Arrow 21.0.0. No IoTDB server is required.
#include "iotdbfs/formats/arrow/ArrowFormatAdapter.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <algorithm>
#include <array>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>

namespace {
using namespace iotdbfs;

void require(bool ok, const std::string& what) {
  if (!ok) throw std::runtime_error(what);
}
void checked(const arrow::Status& status) {
  require(status.ok(), status.ToString());
}
template <typename T> T checked(arrow::Result<T> result) {
  require(result.ok(), result.status().ToString());
  return std::move(result).ValueOrDie();
}
std::string json_string(const std::string& text) {
  static constexpr char hex[] = "0123456789abcdef";
  std::string result = "\"";
  for (unsigned char c : text) {
    if (c == '"' || c == '\\') { result += '\\'; result += c; }
    else if (c < 32) { result += "\\u00"; result += hex[c >> 4]; result += hex[c & 15]; }
    else result += c;
  }
  return result + '"';
}

void compare_level(const ArrowListLevelIndex& index,
    const std::shared_ptr<arrow::Array>& array) {
  require(index.offsets.size() == static_cast<std::uint64_t>(array->length()) + 1,
    "List offset count differs from Arrow oracle");
  for (std::int64_t i = 0; i <= array->length(); ++i) {
    const auto offset = array->type_id() == arrow::Type::LIST
      ? std::static_pointer_cast<arrow::ListArray>(array)->value_offset(i)
      : std::static_pointer_cast<arrow::LargeListArray>(array)->value_offset(i);
    require(index.offsets[i] == static_cast<std::uint64_t>(offset),
      "List offset differs from Arrow oracle");
    if (i < array->length()) {
      const bool valid = index.validity.empty() ||
        (index.validity.at(i / 8) & (std::uint8_t(1) << (i % 8)));
      require(valid == array->IsValid(i), "List validity differs from Arrow oracle");
    }
  }
}

void run(const std::string& path) {
  std::ifstream sequential(path, std::ios::binary | std::ios::ate);
  require(static_cast<bool>(sequential), "cannot open source");
  const auto end = sequential.tellg();
  require(end >= 0, "cannot determine source size");
  const auto size = static_cast<std::uint64_t>(end);
  sequential.seekg(0);
  std::ifstream random(path, std::ios::binary);
  std::vector<ArrowBufferSpan> reads;
  FormatSource source{size, [&](std::uint64_t offset, std::uint64_t length)
      -> std::optional<BinaryValue> {
    if (offset > size || length > size - offset ||
        length > static_cast<std::uint64_t>(std::numeric_limits<std::streamsize>::max()))
      return std::nullopt;
    reads.push_back({offset, length});
    BinaryValue bytes(length);
    random.clear(); random.seekg(static_cast<std::streamoff>(offset));
    if (!random) return std::nullopt;
    random.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(length));
    if (static_cast<std::uint64_t>(random.gcount()) != length) return std::nullopt;
    return bytes;
  }};
  ArrowFormatAdapter adapter;
  require(adapter.matches(source), "source is not a recognized IPC Schema stream");
  auto session = adapter.begin_ingestion(source);
  require(static_cast<bool>(session), "missing ingestion session");
  std::array<std::uint8_t, 11> chunk{};
  std::uint64_t consumed = 0;
  while (consumed < size) {
    const auto count = std::min<std::uint64_t>(chunk.size(), size - consumed);
    sequential.read(reinterpret_cast<char*>(chunk.data()), count);
    require(static_cast<std::uint64_t>(sequential.gcount()) == count, "source changed while streaming");
    auto status = session->consume_chunk(consumed, chunk.data(), count);
    require(status.ok(), status.error());
    consumed += count;
  }
  auto artifacts = session->finish();
  require(artifacts.ok(), artifacts.error());
  auto decoded = parse_arrow_layout(artifacts.value().layout, size);
  require(decoded.ok(), decoded.status().message());
  const auto& layout = decoded.value();
  std::size_t supported_arrays = 0, compressed_batches = 0;
  // Only direct stable-source rereads count here. Sequential ingestion input
  // already contains all bytes and is intentionally copied once by the core.
  for (const auto& batch : layout.batches) {
    const auto compression = layout.messages.at(batch.message_index).compression;
    if (compression != ArrowBufferCompression::None) ++compressed_batches;
    for (const auto& index : batch.arrays) {
      ++supported_arrays;
      for (const auto& read : reads) {
        const auto& values = index.leaf_values;
        const bool disjoint = read.offset + read.length <= values.offset ||
          read.offset >= values.offset + values.length;
        const bool prefix_only = compression != ArrowBufferCompression::None &&
          read.offset == values.offset && read.length <= 8;
        require(disjoint || prefix_only, "adapter reread float value bytes beyond compression prefix");
      }
    }
  }

  // Oracle is opened only after ingestion has finished and read tracing passed.
  auto input = checked(arrow::io::ReadableFile::Open(path));
  auto reader = checked(arrow::ipc::RecordBatchStreamReader::Open(input));
  require(static_cast<std::size_t>(reader->schema()->num_fields()) == layout.fields.size(),
    "schema field count differs from Arrow oracle");
  for (std::size_t i = 0; i < layout.fields.size(); ++i) {
    const auto field = reader->schema()->field(i);
    require(field->name() == layout.fields[i].name &&
      field->type()->ToString() == layout.fields[i].native_type, "native schema differs from Arrow oracle");
  }
  std::uint64_t rows = 0;
  std::size_t batch_number = 0;
  for (;;) {
    std::shared_ptr<arrow::RecordBatch> batch;
    checked(reader->ReadNext(&batch));
    if (!batch) break;
    checked(batch->ValidateFull());
    require(batch_number < layout.batches.size(), "adapter omitted a record batch");
    const auto& indexed = layout.batches[batch_number++];
    require(indexed.row_begin == rows && indexed.row_count == static_cast<std::uint64_t>(batch->num_rows()),
      "batch row extent differs from Arrow oracle");
    rows += batch->num_rows();
    std::size_t expected_arrays = 0;
    for (std::size_t field = 0; field < layout.fields.size(); ++field) {
      if (!layout.fields[field].list_depth) continue;
      ++expected_arrays;
      auto found = std::find_if(indexed.arrays.begin(), indexed.arrays.end(),
        [field](const ArrowArrayIndex& index) { return index.field_index == field; });
      require(found != indexed.arrays.end(), "missing supported field index");
      require(found->levels.size() == layout.fields[field].list_depth, "wrong List depth");
      auto array = batch->column(field);
      for (const auto& level : found->levels) {
        require(array->type_id() == arrow::Type::LIST || array->type_id() == arrow::Type::LARGE_LIST,
          "oracle field is not List or LargeList");
        compare_level(level, array);
        array = array->type_id() == arrow::Type::LIST
          ? std::static_pointer_cast<arrow::ListArray>(array)->values()
          : std::static_pointer_cast<arrow::LargeListArray>(array)->values();
      }
      require(array->type_id() == arrow::Type::FLOAT, "oracle leaf type is not float32");
      require(found->leaf_count == static_cast<std::uint64_t>(array->length()), "leaf count differs");
      require(found->leaf_null_count == static_cast<std::uint64_t>(array->null_count()), "leaf null count differs");
    }
    require(indexed.arrays.size() == expected_arrays, "unexpected supported array index");
  }
  require(batch_number == layout.batches.size() && rows == layout.total_rows, "stream totals differ");
  std::uint64_t read_bytes = 0;
  for (const auto& read : reads) read_bytes += read.length;
  std::cout << "{\"path\":" << json_string(path) << ",\"ok\":true,\"source_bytes\":" << size
    << ",\"layout_bytes\":" << artifacts.value().layout.size()
    << ",\"batches\":" << batch_number << ",\"rows\":" << rows
    << ",\"fields\":" << layout.fields.size() << ",\"supported_arrays\":" << supported_arrays
    << ",\"compressed_batches\":" << compressed_batches
    << ",\"source_reread_calls\":" << reads.size() << ",\"source_reread_bytes\":" << read_bytes
    << ",\"index_read_bytes\":" << layout.index_read_bytes
    << ",\"index_decoded_bytes\":" << layout.index_decoded_bytes
    << ",\"float_value_reread_bytes\":0,\"chunk_bytes\":11}\n";
}
} // namespace

int main(int argc, char** argv) {
  if (argc < 2) { std::cerr << "usage: a03_ingestion_probe STREAM [STREAM ...]\n"; return 2; }
  bool failed = false;
  for (int i = 1; i < argc; ++i) {
    try { run(argv[i]); }
    catch (const std::exception& error) {
      std::cout << "{\"path\":" << json_string(argv[i]) << ",\"ok\":false,\"error\":"
        << json_string(error.what()) << "}\n";
      failed = true;
    }
  }
  return failed ? 1 : 0;
}
