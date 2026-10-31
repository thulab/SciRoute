// A00 native Arrow API smoke: decode one selected nested-series window.
// This intentionally uses Arrow's batch reader as the full-batch comparison
// path; a00_range_spike.py measures selective physical reads separately.

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>

namespace {

arrow::Status Run(const std::string& path, int batch_index, int row, int channel,
                  int start, int end) {
  ARROW_ASSIGN_OR_RAISE(auto input, arrow::io::ReadableFile::Open(path));
  ARROW_ASSIGN_OR_RAISE(auto stream, arrow::ipc::RecordBatchStreamReader::Open(input));

  std::shared_ptr<arrow::RecordBatch> batch;
  for (int index = 0; index <= batch_index; ++index) {
    ARROW_RETURN_NOT_OK(stream->ReadNext(&batch));
    if (!batch) {
      return arrow::Status::Invalid("Batch index exceeds stream");
    }
  }
  if (row < 0 || row >= batch->num_rows()) {
    return arrow::Status::Invalid("Row index out of bounds");
  }
  const int field_index = batch->schema()->GetFieldIndex("target");
  if (field_index < 0 || batch->column(field_index)->type_id() != arrow::Type::LIST) {
    return arrow::Status::Invalid("Expected target: list<list<float32>>");
  }
  auto outer = std::static_pointer_cast<arrow::ListArray>(batch->column(field_index));
  if (outer->IsNull(row) || outer->values()->type_id() != arrow::Type::LIST) {
    return arrow::Status::Invalid("Null row or non-list channel axis");
  }
  if (channel < 0 || channel >= outer->value_length(row)) {
    return arrow::Status::Invalid("Channel index out of bounds");
  }
  auto inner = std::static_pointer_cast<arrow::ListArray>(outer->values());
  const int64_t inner_row = outer->value_offset(row) + channel;
  if (inner->IsNull(inner_row) || inner->values()->type_id() != arrow::Type::FLOAT) {
    return arrow::Status::Invalid("Null channel or non-float values");
  }
  if (start < 0 || end < start || end > inner->value_length(inner_row)) {
    return arrow::Status::Invalid("Step interval out of bounds");
  }
  auto floats = std::static_pointer_cast<arrow::FloatArray>(inner->values());
  const int64_t value_start = inner->value_offset(inner_row) + start;
  std::cout << "{\"values\":[";
  for (int step = 0; step < end - start; ++step) {
    if (step) {
      std::cout << ',';
    }
    const int64_t index = value_start + step;
    if (floats->IsNull(index)) {
      std::cout << "null";
    } else {
      std::cout << std::setprecision(9) << floats->Value(index);
    }
  }
  std::cout << "],\"batch_rows\":" << batch->num_rows() << "}\n";
  return arrow::Status::OK();
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 7) {
    std::cerr << "usage: a00_cpp_stream_spike STREAM BATCH ROW CHANNEL START END\n";
    return 2;
  }
  const auto status = Run(argv[1], std::atoi(argv[2]), std::atoi(argv[3]),
                          std::atoi(argv[4]), std::atoi(argv[5]), std::atoi(argv[6]));
  if (!status.ok()) {
    std::cerr << status.ToString() << '\n';
    return 1;
  }
  return 0;
}
