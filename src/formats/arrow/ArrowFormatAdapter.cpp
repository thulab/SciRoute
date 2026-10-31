#include "iotdbfs/formats/arrow/ArrowFormatAdapter.h"
#include "iotdbfs/MetadataSerialization.h"

#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/compression.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <set>
#include <stdexcept>

namespace iotdbfs {
namespace {

[[noreturn]] void corrupt(const std::string& message) {
  throw std::runtime_error("CorruptPayload: " + message);
}
[[noreturn]] void exhausted(const std::string& message) {
  throw std::runtime_error("ResourceExhausted: " + message);
}
[[noreturn]] void unsupported(const std::string& message) {
  throw std::runtime_error("UnsupportedCapability: " + message);
}

std::uint64_t le(const std::uint8_t* p, std::size_t n) {
  std::uint64_t value = 0;
  for (std::size_t i = 0; i < n; ++i) value |= std::uint64_t(p[i]) << (8 * i);
  return value;
}
std::int64_t signed_le(const std::uint8_t* p, std::size_t n) {
  auto bits = le(p, n);
  if (n < 8 && (bits & (std::uint64_t(1) << (n * 8 - 1))))
    bits |= ~((std::uint64_t(1) << (n * 8)) - 1);
  std::int64_t result;
  std::memcpy(&result, &bits, sizeof(result));
  return result;
}
std::uint64_t nonnegative(std::int64_t value, const char* what) {
  if (value < 0) corrupt(std::string("negative ") + what);
  return static_cast<std::uint64_t>(value);
}
void charge(std::uint64_t& used, std::uint64_t size, std::uint64_t limit,
    const char* what) {
  if (used > limit || size > limit - used) exhausted(what);
  used += size;
}

// Arrow verifies the entire FlatBuffer before these field accesses. Bounds
// are also checked here; no private generated Arrow/FlatBuffers headers are
// required. Field numbers are the stable Arrow 21 format/Message.fbs schema.
class Flat {
public:
  Flat(const BinaryValue& bytes, std::uint64_t at) : bytes_(bytes), at_(at) {
    need(at_, 4);
    const auto delta = signed_le(bytes_.data() + at_, 4);
    if (delta >= 0) {
      if (std::uint64_t(delta) > at_) corrupt("invalid vtable offset");
      table_ = at_ - std::uint64_t(delta);
    } else {
      const auto distance = std::uint64_t(-delta);
      if (distance > bytes_.size() - at_) corrupt("invalid shared vtable");
      table_ = at_ + distance;
    }
    need(table_, 4);
    table_size_ = le(bytes_.data() + table_, 2);
    need(table_, table_size_);
  }
  static Flat root(const BinaryValue& bytes) {
    if (bytes.size() < 4) corrupt("missing FlatBuffer root");
    return Flat(bytes, le(bytes.data(), 4));
  }
  std::optional<std::uint64_t> field(std::uint32_t number) const {
    const auto slot = std::uint64_t(4) + 2 * std::uint64_t(number);
    if (slot + 2 > table_size_) return std::nullopt;
    const auto relative = le(bytes_.data() + table_ + slot, 2);
    if (!relative) return std::nullopt;
    if (relative > bytes_.size() - at_) corrupt("invalid FlatBuffer field");
    return at_ + relative;
  }
  std::int64_t scalar(std::uint32_t number, std::size_t width,
      std::int64_t fallback = 0) const {
    auto pos = field(number);
    if (!pos) return fallback;
    need(*pos, width);
    return signed_le(bytes_.data() + *pos, width);
  }
  Flat child(std::uint32_t number) const {
    auto pos = field(number);
    if (!pos) corrupt("missing FlatBuffer child");
    need(*pos, 4);
    const auto relative = le(bytes_.data() + *pos, 4);
    if (relative > bytes_.size() - *pos) corrupt("invalid FlatBuffer child");
    return Flat(bytes_, *pos + relative);
  }
  std::pair<std::uint64_t, std::uint64_t> vector(std::uint32_t number,
      std::uint64_t stride) const {
    auto pos = field(number);
    if (!pos) return {0, 0};
    need(*pos, 4);
    const auto relative = le(bytes_.data() + *pos, 4);
    if (relative > bytes_.size() - *pos) corrupt("invalid vector offset");
    const auto start = *pos + relative;
    need(start, 4);
    const auto count = le(bytes_.data() + start, 4);
    if (stride && count > (bytes_.size() - start - 4) / stride)
      corrupt("truncated FlatBuffer vector");
    return {start + 4, count};
  }
private:
  void need(std::uint64_t pos, std::uint64_t n) const {
    if (pos > bytes_.size() || n > bytes_.size() - pos)
      corrupt("FlatBuffer access outside metadata");
  }
  const BinaryValue& bytes_;
  std::uint64_t at_, table_, table_size_;
};

std::unique_ptr<arrow::ipc::Message> open_message(const BinaryValue& metadata) {
  auto buffer = std::make_shared<arrow::Buffer>(metadata.data(), metadata.size());
  auto opened = arrow::ipc::Message::Open(buffer, nullptr);
  if (!opened.ok()) corrupt(opened.status().ToString());
  auto result = std::move(opened).ValueOrDie();
  if (!result->Verify()) corrupt("invalid IPC FlatBuffer");
  return result;
}

bool is_list(const std::shared_ptr<arrow::DataType>& type) {
  return type->id() == arrow::Type::LIST || type->id() == arrow::Type::LARGE_LIST;
}

class ArrowIngestionSession final : public FormatIngestionSession {
public:
  ArrowIngestionSession(FormatSource source, ArrowLayoutLimits limits)
    : source_(std::move(source)), limits_(limits) {
    layout_.source_size = source_.size;
  }

  FormatStatus consume_chunk(std::uint64_t offset, const std::uint8_t* data,
      std::size_t length) override {
    if (!error_.empty()) return FormatStatus::failure(error_);
    try {
      if (finished_) corrupt("session already finished");
      if (offset != consumed_ || consumed_ > source_.size ||
          length > source_.size - consumed_ || (length && !data))
        corrupt("nonsequential or out-of-range stream chunk");
      std::size_t at = 0;
      while (at < length) {
        if (ended_) corrupt("trailing bytes after IPC EOS");
        if (body_remaining_) {
          const auto n = std::min<std::uint64_t>(body_remaining_, length - at);
          body_remaining_ -= n;
          at += n;
          continue;
        }
        if (!metadata_length_) {
          if (!prefix_size_) message_offset_ = consumed_ + at;
          const auto n = std::min<std::size_t>(8 - prefix_size_, length - at);
          std::copy_n(data + at, n, prefix_.data() + prefix_size_);
          prefix_size_ += n;
          at += n;
          if (prefix_size_ != 8) continue;
          if (le(prefix_.data(), 4) != 0xffffffffu)
            unsupported("only modern IPC stream framing is supported");
          const auto size = signed_le(prefix_.data() + 4, 4);
          if (size == 0) {
            if (!schema_) corrupt("IPC EOS before Schema");
            ended_ = layout_.explicit_eos = true;
            prefix_size_ = 0;
            continue;
          }
          if (size < 0 || size % 8 != 0) corrupt("invalid IPC metadata length");
          if (std::uint64_t(size) > limits_.max_metadata_bytes)
            exhausted("IPC metadata exceeds limit");
          if (message_offset_ > source_.size || source_.size - message_offset_ < 8 ||
              std::uint64_t(size) > source_.size - message_offset_ - 8)
            corrupt("truncated IPC metadata");
          if (layout_.messages.size() >= limits_.max_messages)
            exhausted("IPC message count exceeds limit");
          charge(layout_memory_, size + 256, limits_.max_layout_bytes,
            "IPC layout metadata exceeds limit");
          metadata_length_ = size;
          metadata_.clear();
          metadata_.reserve(metadata_length_);
        }
        const auto n = std::min<std::size_t>(metadata_length_ - metadata_.size(),
          length - at);
        metadata_.insert(metadata_.end(), data + at, data + at + n);
        at += n;
        if (metadata_.size() == metadata_length_) {
          add_message();
          metadata_length_ = 0;
          prefix_size_ = 0;
        }
      }
      consumed_ += length;
      return FormatStatus::success();
    } catch (const std::exception& e) {
      error_ = e.what();
      return FormatStatus::failure(error_);
    }
  }

  FormatResult<FormatIngestionArtifacts> finish() override {
    if (!error_.empty()) return FormatResult<FormatIngestionArtifacts>::failure(error_);
    try {
      if (finished_) corrupt("session already finished");
      finished_ = true;
      if (!schema_ || consumed_ != source_.size || prefix_size_ ||
          metadata_length_ || body_remaining_)
        corrupt("truncated IPC stream");
      for (auto& batch : layout_.batches) build_indexes(batch);
      auto encoded = serialize_arrow_layout(layout_, limits_);
      if (!encoded.ok()) throw std::runtime_error(encoded.status().message());
      FormatIngestionArtifacts artifacts;
      artifacts.layout = std::move(encoded.value());
      artifacts.metadata = {
        {"arrow.container", MetadataValue("stream")},
        {"arrow.layout_version", MetadataValue(std::int64_t(1))},
        {"arrow.record_batches", MetadataValue(std::int64_t(layout_.batches.size()))},
        {"arrow.rows", MetadataValue(std::int64_t(layout_.total_rows))},
        {"arrow.fields", MetadataValue(std::int64_t(layout_.fields.size()))},
      };
      return FormatResult<FormatIngestionArtifacts>::success(std::move(artifacts));
    } catch (const std::exception& e) {
      error_ = e.what();
      return FormatResult<FormatIngestionArtifacts>::failure(error_);
    }
  }

private:
  void inspect_type(const std::shared_ptr<arrow::DataType>& type,
      std::vector<int> path) {
    if (path.size() > limits_.max_depth) exhausted("schema depth exceeds limit");
    charge(schema_nodes_, 1, limits_.max_nodes, "schema node count exceeds limit");
    if (type->id() == arrow::Type::DICTIONARY) {
      auto id = dictionaries_.fields().GetFieldId(path);
      if (!id.ok()) corrupt(id.status().ToString());
      dictionary_ids_.insert(*id);
      return;
    }
    for (int i = 0; i < type->num_fields(); ++i) {
      auto child = path;
      child.push_back(i);
      inspect_type(type->field(i)->type(), std::move(child));
    }
  }

  void add_message() {
    auto message = open_message(metadata_);
    if (message->metadata_version() != arrow::ipc::MetadataVersion::V5)
      unsupported("only IPC metadata V5 is supported");
    ArrowMessageLayout record;
    record.offset = message_offset_;
    record.body_offset = message_offset_ + 8 + metadata_.size();
    record.body_length = nonnegative(message->body_length(), "IPC body length");
    if (record.body_length % 8 != 0 || record.body_offset > source_.size ||
        record.body_length > source_.size - record.body_offset)
      corrupt("invalid or truncated IPC body span");
    record.metadata = std::move(metadata_);
    // Message holds a view of this allocation; moving the vector preserves it.
    if (!schema_) {
      if (message->type() != arrow::ipc::MessageType::SCHEMA || record.body_length)
        corrupt("IPC stream must start with a body-free Schema message");
      auto schema = arrow::ipc::ReadSchema(*message, &dictionaries_);
      if (!schema.ok()) corrupt(schema.status().ToString());
      schema_ = *schema;
      if (schema_->endianness() != arrow::Endianness::Little)
        unsupported("big-endian IPC schema");
      record.kind = ArrowMessageKind::Schema;
      std::set<std::string> names;
      for (int i = 0; i < schema_->num_fields(); ++i) {
        const auto& field = schema_->field(i);
        inspect_type(field->type(), {i});
        ArrowNativeField native;
        native.name = field->name();
        native.native_type = field->type()->ToString();
        auto type = field->type();
        std::uint32_t depth = 0;
        while (is_list(type)) {
          ++depth;
          type = type->field(0)->type();
        }
        if ((depth == 1 || depth == 2) && type->id() == arrow::Type::FLOAT)
          native.list_depth = depth;
        else
          native.unsupported_reason = "physical array queries require one or two List levels with float32 leaves";
        if (!names.insert(native.name).second || native.name.empty())
          unsupported("physical field names must be nonempty and unique");
        charge(layout_memory_, 128 + native.name.size() + native.native_type.size() +
          native.unsupported_reason.size(), limits_.max_layout_bytes,
          "native schema exceeds layout limit");
        layout_.fields.push_back(std::move(native));
      }
    } else {
      auto header = Flat::root(record.metadata).child(2);
      if (message->type() == arrow::ipc::MessageType::RECORD_BATCH) {
        record.kind = ArrowMessageKind::RecordBatch;
        parse_batch(header, schema_->fields(), record);
        for (auto id : dictionary_ids_) {
          if (!epochs_.count(id)) corrupt("record batch uses an undefined dictionary");
        }
        ArrowBatchLayout batch;
        batch.message_index = layout_.messages.size();
        batch.row_begin = layout_.total_rows;
        batch.row_count = record.rows;
        batch.dictionary_epochs = epochs_;
        charge(layout_memory_, 96 + 64 * epochs_.size(), limits_.max_layout_bytes,
          "batch dictionary epochs exceed layout limit");
        charge(layout_.total_rows, record.rows,
          std::numeric_limits<std::int64_t>::max(), "top-level row count exceeds int64");
        layout_.batches.push_back(std::move(batch));
      } else if (message->type() == arrow::ipc::MessageType::DICTIONARY_BATCH) {
        record.kind = ArrowMessageKind::Dictionary;
        const auto id = header.scalar(0, 8);
        auto type = dictionaries_.GetDictionaryType(id);
        if (!type.ok()) corrupt("dictionary id is absent from schema");
        record.dictionary_id = id;
        record.dictionary_delta = header.scalar(2, 1) != 0;
        if (record.dictionary_delta) {
          auto previous = epochs_.find(id);
          if (previous == epochs_.end()) corrupt("dictionary delta without base");
          record.previous_dictionary = previous->second;
        }
        parse_batch(header.child(1), {arrow::field("dictionary", *type)}, record);
        epochs_[id] = layout_.messages.size();
      } else {
        unsupported("unexpected IPC message or concatenated stream Schema");
      }
    }
    body_remaining_ = record.body_length;
    layout_.messages.push_back(std::move(record));
  }

  void parse_batch(const Flat& header, const arrow::FieldVector& fields,
      ArrowMessageLayout& record) {
    record.rows = nonnegative(header.scalar(0, 8), "record batch rows");
    if (header.field(3)) {
      auto compression = header.child(3);
      if (compression.scalar(1, 1) != 0) unsupported("IPC compression method");
      switch (compression.scalar(0, 1)) {
      case 0: record.compression = ArrowBufferCompression::Lz4; break;
      case 1: record.compression = ArrowBufferCompression::Zstd; break;
      default: unsupported("IPC compression codec");
      }
    }
    const auto nodes = header.vector(1, 16);
    const auto buffers = header.vector(2, 16);
    const auto variadic = header.vector(4, 8);
    charge(node_count_, nodes.second, limits_.max_nodes, "record node count exceeds limit");
    charge(layout_memory_, nodes.second * 96 + buffers.second * 16,
      limits_.max_layout_bytes, "record nodes/buffers exceed layout limit");
    std::uint64_t node = 0, buffer = 0, extra = 0, previous_end = record.body_offset;
    std::function<void(std::shared_ptr<arrow::DataType>, std::vector<std::uint32_t>)> visit;
    visit = [&](std::shared_ptr<arrow::DataType> type, std::vector<std::uint32_t> path) {
      if (path.size() > limits_.max_depth) exhausted("record nesting exceeds limit");
      if (node >= nodes.second) corrupt("missing IPC field node");
      ArrowFieldNode item;
      item.path = path;
      const auto pos = nodes.first + 16 * node++;
      item.length = nonnegative(signed_le(record.metadata.data() + pos, 8), "field length");
      item.null_count = nonnegative(signed_le(record.metadata.data() + pos + 8, 8), "null count");
      if (item.null_count > item.length || (path.size() == 1 && item.length != record.rows))
        corrupt("inconsistent IPC field length/null count");
      if (type->id() == arrow::Type::EXTENSION)
        type = std::static_pointer_cast<arrow::ExtensionType>(type)->storage_type();
      const auto native = type->layout();
      std::uint64_t count = 0;
      for (const auto& spec : native.buffers)
        if (spec.kind != arrow::DataTypeLayout::ALWAYS_NULL) ++count;
      if (native.variadic_spec) {
        if (extra >= variadic.second) corrupt("missing variadic buffer count");
        auto n = nonnegative(signed_le(record.metadata.data() + variadic.first +
          8 * extra++, 8), "variadic buffer count");
        if (n > buffers.second) corrupt("excessive variadic buffer count");
        count += n;
      }
      if (count > buffers.second - buffer) corrupt("missing IPC buffers");
      for (std::uint64_t i = 0; i < count; ++i) {
        const auto bp = buffers.first + 16 * buffer++;
        auto offset = nonnegative(signed_le(record.metadata.data() + bp, 8), "buffer offset");
        auto length = nonnegative(signed_le(record.metadata.data() + bp + 8, 8), "buffer length");
        if (offset % 8 != 0 || offset > record.body_length || length > record.body_length - offset)
          corrupt("IPC buffer outside body");
        offset += record.body_offset;
        if (offset < previous_end) corrupt("overlapping or unordered IPC buffers");
        previous_end = offset + length;
        item.buffers.push_back({offset, length});
      }
      charge(layout_memory_, path.size() * 4, limits_.max_layout_bytes,
        "node paths exceed layout limit");
      record.nodes.push_back(std::move(item));
      for (int i = 0; i < type->num_fields(); ++i) {
        auto child = path;
        child.push_back(i);
        visit(type->field(i)->type(), std::move(child));
      }
    };
    for (std::uint32_t i = 0; i < fields.size(); ++i) visit(fields[i]->type(), {i});
    if (node != nodes.second || buffer != buffers.second || extra != variadic.second)
      corrupt("IPC node/buffer count does not match native schema");
  }

  BinaryValue read(const ArrowBufferSpan& span) {
    if (span.offset > source_.size || span.length > source_.size - span.offset)
      corrupt("index read outside source");
    if (span.length > limits_.max_buffer_bytes) exhausted("index buffer exceeds limit");
    charge(layout_.index_read_bytes, span.length, limits_.max_index_read_bytes,
      "index read budget exhausted");
    if (!span.length) return {};
    auto bytes = source_.read ? source_.read(span.offset, span.length) : std::nullopt;
    if (!bytes || bytes->size() != span.length)
      throw std::runtime_error("ReadFailure: short or failed Arrow index read");
    return std::move(*bytes);
  }

  BinaryValue decode_index(const ArrowBufferSpan& span, ArrowBufferCompression compression) {
    auto data = read(span);
    if (compression == ArrowBufferCompression::None || data.empty()) {
      charge(layout_.index_decoded_bytes, data.size(), limits_.max_index_read_bytes,
        "decoded index byte budget exhausted");
      return data;
    }
    if (data.size() < 8) corrupt("truncated compressed buffer prefix");
    const auto size = signed_le(data.data(), 8);
    if (size == -1) {
      data.erase(data.begin(), data.begin() + 8);
      charge(layout_.index_decoded_bytes, data.size(), limits_.max_index_read_bytes,
        "decoded index byte budget exhausted");
      return data;
    }
    if (size < 0) corrupt("invalid compressed buffer length");
    if (std::uint64_t(size) > limits_.max_buffer_bytes)
      exhausted("decompressed index buffer exceeds limit");
    charge(layout_.index_decoded_bytes, size, limits_.max_index_read_bytes,
      "decoded index byte budget exhausted");
    BinaryValue output(size);
    auto codec = arrow::util::Codec::Create(compression == ArrowBufferCompression::Lz4
      ? arrow::Compression::LZ4_FRAME : arrow::Compression::ZSTD);
    if (!codec.ok()) unsupported(codec.status().ToString());
    auto decoded = (*codec)->Decompress(data.size() - 8, data.data() + 8,
      output.size(), output.data());
    if (!decoded.ok() || *decoded != size) corrupt("invalid compressed index buffer");
    return output;
  }

  BinaryValue validity(const ArrowFieldNode& node, ArrowBufferCompression compression) {
    if (node.buffers.empty()) corrupt("missing validity buffer descriptor");
    auto bits = decode_index(node.buffers[0], compression);
    const auto bytes = node.length / 8 + (node.length % 8 != 0);
    if (bits.empty() && node.null_count == 0) return {};
    if (bits.size() < bytes) corrupt("truncated validity bitmap");
    std::uint64_t nulls = 0;
    for (std::uint64_t i = 0; i < node.length; ++i)
      nulls += !(bits[i / 8] & (std::uint8_t(1) << (i % 8)));
    if (nulls != node.null_count) corrupt("validity bitmap contradicts null count");
    bits.resize(bytes);
    return bits;
  }

  void build_indexes(ArrowBatchLayout& batch) {
    const auto& message = layout_.messages[batch.message_index];
    std::vector<std::size_t> roots(layout_.fields.size(), message.nodes.size());
    for (std::size_t i = 0; i < message.nodes.size(); ++i)
      if (message.nodes[i].path.size() == 1)
        roots.at(message.nodes[i].path[0]) = i;
    for (std::uint32_t fi = 0; fi < layout_.fields.size(); ++fi) {
      auto depth = layout_.fields[fi].list_depth;
      if (!depth) continue;
      if (roots[fi] >= message.nodes.size() || message.nodes.size() - roots[fi] <= depth)
        corrupt("missing nested array nodes");
      auto first = message.nodes.begin() + roots[fi];
      ArrowArrayIndex index;
      index.field_index = fi;
      auto type = schema_->field(fi)->type();
      for (std::uint32_t level = 0; level < depth; ++level) {
        const auto& node = first[level];
        const auto& child = first[level + 1];
        if (node.buffers.size() != 2) corrupt("invalid List buffer layout");
        const auto width = type->id() == arrow::Type::LARGE_LIST ? 8u : 4u;
        if (node.length >= limits_.max_index_bytes / 8)
          exhausted("List index exceeds memory limit");
        charge(index_memory_, (node.length + 1) * 8 + 64,
          limits_.max_index_bytes, "List indexes exceed memory limit");
        charge(layout_memory_, (node.length + 1) * 8 + 64,
          limits_.max_layout_bytes, "List indexes exceed layout limit");
        auto offsets = decode_index(node.buffers[1], message.compression);
        if ((node.length + 1) > offsets.size() / width)
          corrupt("truncated List offsets");
        ArrowListLevelIndex list;
        list.offsets.reserve(node.length + 1);
        std::uint64_t previous = 0;
        for (std::uint64_t i = 0; i <= node.length; ++i) {
          const auto offset = nonnegative(signed_le(offsets.data() + i * width, width), "List offset");
          if (offset < previous || offset > child.length) corrupt("List offset outside child array");
          list.offsets.push_back(offset);
          previous = offset;
        }
        list.validity = validity(node, message.compression);
        charge(index_memory_, list.validity.size(), limits_.max_index_bytes,
          "List validity exceeds memory limit");
        charge(layout_memory_, list.validity.size(), limits_.max_layout_bytes,
          "List validity exceeds layout limit");
        index.levels.push_back(std::move(list));
        type = type->field(0)->type();
      }
      const auto& leaf = first[depth];
      if (leaf.buffers.size() != 2) corrupt("invalid float32 buffer layout");
      index.leaf_count = leaf.length;
      index.leaf_null_count = leaf.null_count;
      index.leaf_validity = leaf.buffers[0];
      index.leaf_values = leaf.buffers[1];
      // Validate leaf bitmap, but retain its native location for window reads.
      validity(leaf, message.compression);
      if (leaf.length > std::numeric_limits<std::uint64_t>::max() / 4)
        corrupt("float32 byte length overflow");
      auto available = index.leaf_values.length;
      if (message.compression != ArrowBufferCompression::None && available) {
        if (available < 8) corrupt("truncated float32 compression prefix");
        auto prefix = read({index.leaf_values.offset, 8});
        const auto size = signed_le(prefix.data(), 8);
        if (size < -1) corrupt("invalid float32 compression prefix");
        available = size == -1 ? available - 8 : std::uint64_t(size);
      }
      if (available < leaf.length * 4) corrupt("truncated float32 value buffer");
      batch.arrays.push_back(std::move(index));
    }
  }

  FormatSource source_;
  ArrowLayoutLimits limits_;
  ArrowLayoutDocument layout_;
  std::shared_ptr<arrow::Schema> schema_;
  arrow::ipc::DictionaryMemo dictionaries_;
  std::set<std::int64_t> dictionary_ids_;
  std::map<std::int64_t, std::uint64_t> epochs_;
  std::array<std::uint8_t, 8> prefix_{};
  std::size_t prefix_size_{0};
  BinaryValue metadata_;
  std::uint64_t metadata_length_{0}, message_offset_{0}, body_remaining_{0};
  std::uint64_t consumed_{0}, layout_memory_{0}, index_memory_{0}, schema_nodes_{0}, node_count_{0};
  bool ended_{false}, finished_{false};
  std::string error_;
};

} // namespace

ArrowFormatAdapter::ArrowFormatAdapter(ArrowLayoutLimits limits) : limits_(limits) {}
const std::string& ArrowFormatAdapter::id() const {
  static const std::string value = "arrow";
  return value;
}
const std::vector<std::string>& ArrowFormatAdapter::extensions() const {
  static const std::vector<std::string> value{"arrow", "arrows", "ipc"};
  return value;
}
bool ArrowFormatAdapter::matches(const FormatSource& source) const {
  if (source.size < 8 || !source.read) return false;
  auto prefix = source.read(0, 8);
  if (!prefix || prefix->size() != 8) return false;
  const bool modern = le(prefix->data(), 4) == 0xffffffffu;
  const auto frame = modern ? 8u : 4u;
  const auto size = signed_le(prefix->data() + (modern ? 4 : 0), 4);
  if (size <= 0 || std::uint64_t(size) > limits_.max_metadata_bytes ||
      std::uint64_t(size) > source.size - frame) return false;
  auto metadata = source.read(frame, size);
  if (!metadata || metadata->size() != std::uint64_t(size)) return false;
  try {
    auto message = open_message(*metadata);
    if (message->type() != arrow::ipc::MessageType::SCHEMA || message->body_length() != 0)
      return false;
    arrow::ipc::DictionaryMemo memo;
    return arrow::ipc::ReadSchema(*message, &memo).ok();
  } catch (const std::exception&) {
    return false;
  }
}
std::unique_ptr<FormatIngestionSession> ArrowFormatAdapter::begin_ingestion(
    const FormatSource& source) const {
  return std::make_unique<ArrowIngestionSession>(source, limits_);
}
FormatStatus ArrowFormatAdapter::validate_layout_artifact(
    const std::vector<std::uint8_t>& layout, std::uint64_t payload_size) const {
  // Establish trust through the public parser, including native IPC schema,
  // dictionary and FlatBuffer checks. Reconstruction is only for trusted bytes.
  auto parsed = parse_arrow_layout(layout, payload_size, limits_);
  return parsed.ok() ? FormatStatus::success()
                     : FormatStatus::failure(parsed.status().message());
}
FormatXattrList ArrowFormatAdapter::list_xattrs(long long, const std::string&,
    const Inode& inode) const {
  if (inode.format_id != id()) return {};
  auto metadata = deserialize_metadata(inode.metadata);
  if (!metadata.ok()) return {};
  FormatXattrList attrs;
  for (const char* name : {"arrow.container", "arrow.layout_version",
      "arrow.record_batches", "arrow.rows", "arrow.fields"}) {
    auto found = metadata.value().find(name);
    if (found != metadata.value().end())
      attrs.emplace_back(std::string("user.") + name, metadata_value_text(found->second));
  }
  return attrs;
}
std::optional<std::string> ArrowFormatAdapter::get_xattr(long long row_time,
    const std::string& path, const Inode& inode, const std::string& name) const {
  for (const auto& attr : list_xattrs(row_time, path, inode))
    if (attr.first == name) return attr.second;
  return std::nullopt;
}

} // namespace iotdbfs
