#pragma once

#include "iotdbfs/formats/arrow/ArrowQueryModel.h"

#include <map>
#include <memory>

namespace iotdbfs {

struct ArrowLayoutLimits {
  std::uint64_t max_metadata_bytes{8 * 1024 * 1024};
  std::uint64_t max_layout_bytes{64 * 1024 * 1024};
  std::uint64_t max_index_bytes{32 * 1024 * 1024};
  std::uint64_t max_index_read_bytes{128 * 1024 * 1024};
  // Applies to index buffers actually read/decompressed, not passive values spans.
  std::uint64_t max_buffer_bytes{64 * 1024 * 1024};
  std::uint64_t max_messages{65536};
  std::uint64_t max_nodes{262144};
  std::uint32_t max_depth{64};
};

struct ArrowBufferSpan {
  std::uint64_t offset{0}; // Absolute position in the unchanged IPC stream.
  std::uint64_t length{0}; // Compressed bytes including IPC prefix, if present.
};

struct ArrowFieldNode {
  std::vector<std::uint32_t> path; // Native schema child ordinals.
  std::uint64_t length{0};
  std::uint64_t null_count{0};
  std::vector<ArrowBufferSpan> buffers;
};

enum class ArrowMessageKind : std::uint8_t { Schema = 1, Dictionary = 2, RecordBatch = 3 };
enum class ArrowBufferCompression : std::uint8_t { None = 0, Lz4 = 1, Zstd = 2 };

struct ArrowMessageLayout {
  ArrowMessageKind kind{ArrowMessageKind::Schema};
  std::uint64_t offset{0};
  std::uint64_t body_offset{0};
  std::uint64_t body_length{0};
  BinaryValue metadata; // Verified native FlatBuffer, retained for future decoding.
  ArrowBufferCompression compression{ArrowBufferCompression::None};
  std::uint64_t rows{0};
  std::vector<ArrowFieldNode> nodes;
  std::optional<std::int64_t> dictionary_id;
  bool dictionary_delta{false};
  // For a delta, previous message of this id. Replacement starts a new chain.
  std::optional<std::uint64_t> previous_dictionary;
};

struct ArrowNativeField {
  std::string name;
  std::string native_type;
  std::uint32_t list_depth{0}; // 1/2 for supported List[LargeList]/Float32 shapes.
  std::string unsupported_reason; // Other native fields are retained, not coerced.
};

struct ArrowListLevelIndex {
  std::vector<std::uint64_t> offsets;
  BinaryValue validity; // Empty means all valid; otherwise Arrow LSB bitmap.
};

struct ArrowArrayIndex {
  std::uint32_t field_index{0};
  std::vector<ArrowListLevelIndex> levels;
  std::uint64_t leaf_count{0};
  std::uint64_t leaf_null_count{0};
  ArrowBufferSpan leaf_values;
  ArrowBufferSpan leaf_validity;
};

struct ArrowBatchLayout {
  std::uint64_t message_index{0};
  std::uint64_t row_begin{0}; // Top-level rows, never interpreted as a time axis.
  std::uint64_t row_count{0};
  std::map<std::int64_t, std::uint64_t> dictionary_epochs;
  std::vector<ArrowArrayIndex> arrays;
};

struct ArrowLayoutDocument {
  std::uint64_t source_size{0};
  std::uint64_t total_rows{0};
  bool explicit_eos{false};
  std::uint64_t index_read_bytes{0};
  std::uint64_t index_decoded_bytes{0};
  std::vector<ArrowNativeField> fields;
  std::vector<ArrowMessageLayout> messages;
  std::vector<ArrowBatchLayout> batches;
};

// Binary, bounded artifact with the ASCII header IOTDBFS-ARROW-LAYOUT/1\n.
// Container is always modern IPC stream; file/footer and legacy framing are
// unsupported. Neither codec reads or writes source payload bytes.
QueryStatus validate_arrow_layout(const ArrowLayoutDocument& layout,
  const ArrowLayoutLimits& limits = {});
QueryResult<BinaryValue> serialize_arrow_layout(const ArrowLayoutDocument& layout,
  const ArrowLayoutLimits& limits = {});
QueryResult<ArrowLayoutDocument> parse_arrow_layout(const BinaryValue& bytes,
  std::uint64_t source_size, const ArrowLayoutLimits& limits = {});

namespace detail {
// Internal reconstruction of an immutable wire image that has ALREADY passed
// parse_arrow_layout with these limits and this source size. This rechecks the
// wire framing, bounded collection lengths and source size, but deliberately
// does not decode/validate native Arrow schema metadata again. Never use this
// entry point to establish trust in storage or caller-provided layout bytes.
QueryResult<ArrowLayoutDocument> reconstruct_validated_arrow_layout(
  const BinaryValue& bytes, std::uint64_t source_size,
  const ArrowLayoutLimits& limits = {});
// Internal offset view over a privately owned, immutable document that has
// ALREADY passed parse_arrow_layout/validate_arrow_layout. Checks coordinates
// but deliberately avoids validating every other batch again. The caller must
// retain no mutable alias; public/caller-provided documents use the checked
// make_arrow_list_index factory below.
QueryResult<std::shared_ptr<const ArrayListIndex>> make_validated_arrow_list_index(
  std::shared_ptr<const ArrowLayoutDocument> layout,
  std::uint64_t batch_index, std::uint32_t field_index);
} // namespace detail
QueryResult<std::shared_ptr<const ArrayListIndex>> make_arrow_list_index(
  std::shared_ptr<const ArrowLayoutDocument> layout,
  std::uint64_t batch_index, std::uint32_t field_index,
  const ArrowLayoutLimits& limits = {});

} // namespace iotdbfs
