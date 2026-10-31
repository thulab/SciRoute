// A05 public-data oracle. Compare shared overlapping windows with native Arrow.
// No training annotations, IoTDB instance, or private datasets are required.
#include "iotdbfs/formats/arrow/ArrowFormatAdapter.h"
#include "iotdbfs/formats/arrow/ArrowFormatQueryAdapter.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <fstream>
#include <future>
#include <mutex>
#include <iomanip>
#include <iostream>
#include <limits>
#include <stdexcept>

namespace {
using namespace iotdbfs;
void require(bool ok, const std::string& why) { if (!ok) throw std::runtime_error(why); }
void checked(const arrow::Status& status) { require(status.ok(), status.ToString()); }
template<class T> T checked(arrow::Result<T> result) {
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
class FileSource final : public QuerySource {
public:
  explicit FileSource(std::string filename) : path(std::move(filename)), input(path, std::ios::binary | std::ios::ate) {
    require(bool(input), "cannot open input");
    const auto end = input.tellg(); require(end >= 0, "cannot determine input length");
    manifest_.key = {path, 1}; manifest_.payload_size = static_cast<std::uint64_t>(end);
    manifest_.format_id = "arrow"; manifest_.published = true; manifest_.phase = StructuredBlobPhase::Published;
    FormatSource ingestion{manifest_.payload_size, [&](std::uint64_t off, std::uint64_t count)
        -> std::optional<BinaryValue> {
      auto bytes = read_payload(off, count);
      if (!bytes.ok()) return std::nullopt;
      return std::move(bytes.value());
    }};
    ArrowFormatAdapter adapter;
    auto session = adapter.begin_ingestion(ingestion); require(bool(session), "cannot create ingestion");
    std::ifstream sequential(path, std::ios::binary);
    std::array<std::uint8_t, 65536> chunk{};
    for (std::uint64_t off = 0; off < manifest_.payload_size;) {
      const auto count = std::min<std::uint64_t>(chunk.size(), manifest_.payload_size - off);
      sequential.read(reinterpret_cast<char*>(chunk.data()), count);
      require(static_cast<std::uint64_t>(sequential.gcount()) == count, "short ingestion read");
      auto status = session->consume_chunk(off, chunk.data(), count); require(status.ok(), status.error());
      off += count;
    }
    auto result = session->finish(); require(result.ok(), result.error());
    layout = std::move(result.value().layout); manifest_.layout_size = layout.size();
    reads.clear();
  }
  const StructuredBlobManifest& manifest() const override { return manifest_; }
  QueryResult<BinaryValue> read_payload(std::uint64_t off, std::uint64_t count) const override {
    if (off > manifest_.payload_size || count > manifest_.payload_size - off)
      return failure("out of bounds input read");
    std::lock_guard<std::mutex> lock(read_mutex);
    input.clear(); input.seekg(static_cast<std::streamoff>(off));
    BinaryValue bytes(count);
    input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(count));
    if (static_cast<std::uint64_t>(input.gcount()) != count) return failure("short input read");
    reads.push_back({off, count}); return QueryResult<BinaryValue>::success(std::move(bytes));
  }
  QueryResult<BinaryValue> read_layout(std::uint64_t off, std::uint64_t count) const override {
    if (off > layout.size() || count > layout.size() - off) return failure("out of bounds layout read");
    return QueryResult<BinaryValue>::success({layout.begin() + off, layout.begin() + off + count});
  }
  static QueryResult<BinaryValue> failure(const char* why) {
    return QueryResult<BinaryValue>::failure(QueryStatus::failure(QueryError::ReadFailure, QueryPhase::Storage, why));
  }
  std::string path;
  mutable std::ifstream input;
  mutable std::mutex read_mutex;
  StructuredBlobManifest manifest_;
  BinaryValue layout;
  mutable std::vector<ArrowBufferSpan> reads;
};
struct NativeSpan { std::shared_ptr<arrow::Array> values; std::int64_t begin, length; bool valid; };
NativeSpan native_span(const std::shared_ptr<arrow::Array>& array, std::int64_t index) {
  require(array->type_id() == arrow::Type::LIST || array->type_id() == arrow::Type::LARGE_LIST,
    "native target is not List or LargeList");
  if (array->type_id() == arrow::Type::LIST) {
    auto list = std::static_pointer_cast<arrow::ListArray>(array);
    return {list->values(), list->value_offset(index), list->value_length(index), list->IsValid(index)};
  }
  auto list = std::static_pointer_cast<arrow::LargeListArray>(array);
  return {list->values(), list->value_offset(index), list->value_length(index), list->IsValid(index)};
}
bool valid(const LogicalArray& a, std::size_t row) {
  return a.validity.empty() || (a.validity.at(row / 8) & (1u << (row % 8)));
}
void run(const std::string& path) {
  auto owner = std::make_shared<FileSource>(path);
  auto& source = *owner;
  auto parsed = parse_arrow_layout(source.layout, source.manifest_.payload_size);
  require(parsed.ok(), parsed.status().message());
  const auto& doc = parsed.value();
  auto target = std::find_if(doc.fields.begin(), doc.fields.end(), [](const ArrowNativeField& f) {
    return f.name == "target" && f.list_depth;
  });
  if (target == doc.fields.end()) target = std::find_if(doc.fields.begin(), doc.fields.end(),
    [](const ArrowNativeField& f) { return f.list_depth != 0; });
  require(target != doc.fields.end(), "source has no Float32 List field");
  const auto field = static_cast<std::uint32_t>(target - doc.fields.begin());
  const auto& batch = doc.batches.at(0);
  auto index = std::find_if(batch.arrays.begin(), batch.arrays.end(), [field](const ArrowArrayIndex& a) {
    return a.field_index == field;
  });
  require(index != batch.arrays.end() && batch.row_count, "first batch lacks target row");
  std::vector<ArrayValuePath> children;
  if (target->list_depth == 1) children = {{{}}};
  else {
    const auto count = index->levels[0].offsets.at(1) - index->levels[0].offsets.at(0);
    require(count > 0, "first target row has no children");
    children = count >= 3 ? std::vector<ArrayValuePath>{{{2}}, {{0}}} : std::vector<ArrayValuePath>{{{0}}};
  }
  QueryFragment q;
  q.physical_array = PhysicalArraySubject{target->name, 0, 0, children};
  q.projection = {{"index"}};
  for (const auto& child : children)
    q.projection.push_back({child.child_indices.empty() ? "value" : "value." + std::to_string(child.child_indices[0])});
  q.slices = {{{"index"}, DomainBound{LogicalValue::uint64(3), BoundKind::Inclusive},
    DomainBound{LogicalValue::uint64(17), BoundKind::Exclusive}}};
  ArrowDecodeConfig config; config.tile_elements = 4096;
  auto runtime = std::make_shared<QueryRuntime>();
  ArrowFormatQueryAdapter adapter(config, runtime);
  auto plan = adapter.prepare(QueryPlanningContext(source), q); require(plan.ok(), plan.status().message());
  auto binding = arrow_physical_binding(plan.value().implementation()); require(binding.ok(), binding.status().message());
  QueryExecutionOptions options; options.target_batch_rows = 5;
  auto cursor = adapter.execute(QueryExecutionContext(owner), plan.value().implementation(), options);
  require(cursor.ok(), cursor.status().message());
  std::vector<QueryBatch> batches;
  for (;;) {
    auto next = cursor.value()->next(); require(next.ok(), next.status().message());
    if (!next.value()) break;
    batches.push_back(std::move(*next.value()));
  }
  auto metrics = arrow_decode_metrics(*cursor.value()); require(metrics.ok(), metrics.status().message());
  const auto initial = runtime->stats();

  struct ViewOutput {
    std::vector<QueryBatch> batches;
    ArrowDecodeMetrics metrics;
    std::uint64_t length;
  };
  auto run_view = [&](ArrowFormatQueryAdapter& view_adapter, bool subset) {
    auto fragment = q;
    if (subset) {
      fragment.physical_array->values = {children.back()};
      fragment.projection = {{"index"}, q.projection.back()};
      fragment.slices = {{{"index"}, DomainBound{LogicalValue::uint64(7), BoundKind::Inclusive},
        DomainBound{LogicalValue::uint64(14), BoundKind::Exclusive}}};
    }
    auto view_plan = view_adapter.prepare(QueryPlanningContext(source), fragment);
    require(view_plan.ok(), view_plan.status().message());
    auto view_binding = arrow_physical_binding(view_plan.value().implementation());
    require(view_binding.ok(), view_binding.status().message());
    auto view_cursor = view_adapter.execute(QueryExecutionContext(owner),
      view_plan.value().implementation(), options);
    require(view_cursor.ok(), view_cursor.status().message());
    ViewOutput result; result.length = view_binding.value().length;
    for (;;) {
      auto next = view_cursor.value()->next(); require(next.ok(), next.status().message());
      if (!next.value()) break;
      result.batches.push_back(std::move(*next.value()));
    }
    auto demand = arrow_decode_metrics(*view_cursor.value());
    require(demand.ok(), demand.status().message()); result.metrics = demand.value();
    return result;
  };
  const auto overlap = run_view(adapter, true);
  const auto warm = runtime->stats();
  require(warm.decode_count == initial.decode_count && warm.read_bytes == initial.read_bytes &&
    warm.decompressions == initial.decompressions && warm.producer_count == initial.producer_count,
    "overlapping view did not reuse decoded fragments");

  // Start both views from an empty runtime. Their union is exactly the first
  // query's canonical fragments, independent of worker scheduling.
  auto concurrent_runtime = std::make_shared<QueryRuntime>();
  ArrowFormatQueryAdapter concurrent_adapter(config, concurrent_runtime);
  std::promise<void> start; auto gate = start.get_future().share();
  auto first_future = std::async(std::launch::async, [&] {
    gate.wait(); return run_view(concurrent_adapter, false);
  });
  auto overlap_future = std::async(std::launch::async, [&] {
    gate.wait(); return run_view(concurrent_adapter, true);
  });
  start.set_value();
  const auto concurrent_first = first_future.get();
  const auto concurrent_overlap = overlap_future.get();
  const auto concurrent = concurrent_runtime->stats();
  require(concurrent.decode_count == initial.decode_count && concurrent.read_bytes == initial.read_bytes &&
    concurrent.decompressions == initial.decompressions && concurrent.producer_count == initial.producer_count,
    "concurrent views produced duplicate physical fragments");

  // Independent Arrow decoder opened after our queries complete.
  auto native_input = checked(arrow::io::ReadableFile::Open(path));
  auto native_reader = checked(arrow::ipc::RecordBatchStreamReader::Open(native_input));
  std::shared_ptr<arrow::RecordBatch> native_batch;
  checked(native_reader->ReadNext(&native_batch)); require(bool(native_batch), "native batch absent");
  checked(native_batch->ValidateFull());
  auto row = native_span(native_batch->column(field), 0);
  std::vector<NativeSpan> leaves;
  for (const auto& child : children) {
    auto leaf = row;
    if (!child.child_indices.empty() && leaf.valid)
      leaf = native_span(row.values, row.begin + child.child_indices[0]);
    leaves.push_back(leaf);
  }
  auto compare_native = [&](const std::vector<QueryBatch>& result, bool subset, std::uint64_t length) {
  const std::uint64_t begin = subset ? 7 : 3, end = subset ? 14 : 17;
  std::uint64_t count = 0;
  for (const auto& b : result) {
    require(bool(b.domain()) && b.domain()->origin.at(0) == begin + count,
      "incorrect positional domain");
    const auto& positions = std::get<std::vector<std::uint64_t>>(b.columns()[0].values);
    for (std::size_t r = 0; r < b.row_count(); ++r) {
      require(positions[r] == begin + count, "position differs from native requested window"); ++count;
      for (std::size_t c = 0; c < (subset ? 1 : leaves.size()); ++c) {
        const auto& ref = subset ? leaves.back() : leaves[c]; const auto& actual = b.columns()[c + 1];
        auto array = std::dynamic_pointer_cast<arrow::FloatArray>(ref.values);
        const bool expected_valid = ref.valid && array && array->IsValid(ref.begin + positions[r]);
        require(valid(actual, r) == expected_valid, "leaf validity differs from native Arrow");
        if (expected_valid) {
          const auto expected = array->Value(ref.begin + positions[r]);
          const auto value = std::get<std::vector<float>>(actual.values)[r];
          require((std::isnan(expected) && std::isnan(value)) || expected == value,
            "Float32 differs from native Arrow");
        }
      }
    }
  }
  const auto expected_rows = length <= begin ? 0 : std::min<std::uint64_t>(end, length) - begin;
  require(count == expected_rows, "returned window length differs");
  return count;
  };
  const auto count = compare_native(batches, false, binding.value().length);
  const auto overlap_count = compare_native(overlap.batches, true, overlap.length);
  compare_native(concurrent_first.batches, false, concurrent_first.length);
  compare_native(concurrent_overlap.batches, true, concurrent_overlap.length);
  std::uint64_t counted_reads = 0;
  for (const auto& r : source.reads) counted_reads += r.length;
  require(counted_reads == warm.read_bytes + concurrent.read_bytes, "service read metric differs from trace");
  require(source.reads.size() == warm.read_ranges + concurrent.read_ranges, "service range metric differs from trace");
  auto write_stats = [](const QueryRuntimeStats& s) {
    std::cout << "{\"read_bytes\":" << s.read_bytes << ",\"read_ranges\":" << s.read_ranges
      << ",\"decode_count\":" << s.decode_count << ",\"decoded_bytes\":" << s.decoded_bytes
      << ",\"decompressions\":" << s.decompressions << ",\"hits\":" << s.hits << ",\"misses\":" << s.misses
      << ",\"waits\":" << s.waits << ",\"producer_count\":" << s.producer_count
      << ",\"copy_bytes\":" << s.copy_bytes << ",\"cache_owned_bytes\":" << s.cache_owned_bytes
      << ",\"pinned_bytes\":" << s.pinned_bytes << ",\"live_bytes\":" << s.live_bytes
      << ",\"peak_bytes\":" << s.peak_bytes << '}';
  };
  std::cout << "{\"path\":" << json_string(path) << ",\"ok\":true,\"oracle\":\"native-arrow\",\"source_bytes\":"
    << source.manifest_.payload_size << ",\"field\":" << json_string(target->name)
    << ",\"window_rows\":" << count << ",\"overlap_rows\":" << overlap_count << ",\"columns\":" << q.projection.size()
    << ",\"sequential_reuse\":true,\"concurrent_single_producer\":true"
    << ",\"first_logical_bytes\":" << metrics.value().logical_bytes
    << ",\"overlap_logical_bytes\":" << overlap.metrics.logical_bytes
    << ",\"first_logical_ranges\":" << metrics.value().logical_ranges
    << ",\"overlap_logical_ranges\":" << overlap.metrics.logical_ranges
    << ",\"first_output_bytes\":" << metrics.value().output_bytes
    << ",\"overlap_output_bytes\":" << overlap.metrics.output_bytes
    << ",\"service_after_first\":";
  write_stats(initial);
  std::cout << ",\"service_after_overlap\":"; write_stats(warm);
  std::cout << ",\"service_concurrent_cold\":"; write_stats(concurrent);
  std::cout << ",\"rows\":[";
  bool comma = false;
  for (const auto& b : batches) for (std::size_t r = 0; r < b.row_count(); ++r) {
    if (comma) std::cout << ','; comma = true;
    std::cout << '[' << std::get<std::vector<std::uint64_t>>(b.columns()[0].values)[r];
    for (std::size_t c = 1; c < b.columns().size(); ++c) {
      const auto& a = b.columns()[c]; std::cout << ',';
      if (!valid(a, r)) std::cout << "null";
      else {
        const auto v = std::get<std::vector<float>>(a.values)[r];
        if (std::isnan(v)) std::cout << "\"NaN\"";
        else if (std::isinf(v)) std::cout << (v > 0 ? "\"Infinity\"" : "\"-Infinity\"");
        else std::cout << std::setprecision(std::numeric_limits<float>::max_digits10) << v;
      }
    }
    std::cout << ']';
  }
  std::cout << "]}\n";
}
} // namespace
int main(int argc, char** argv) {
  if (argc < 2) { std::cerr << "usage: arrow_query_probe STREAM [STREAM ...]\n"; return 2; }
  try { for (int i = 1; i < argc; ++i) run(argv[i]); }
  catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
  return 0;
}
