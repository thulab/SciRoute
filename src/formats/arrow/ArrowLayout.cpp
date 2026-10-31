#include "iotdbfs/formats/arrow/ArrowLayout.h"

#include <arrow/api.h>
#include <arrow/ipc/api.h>

#include <algorithm>
#include <cstring>
#include <functional>
#include <limits>
#include <new>
#include <set>
#include <utility>

namespace iotdbfs {
namespace {
constexpr char kHeader[] = "IOTDBFS-ARROW-LAYOUT/1\n";
constexpr char kPrefix[] = "IOTDBFS-ARROW-LAYOUT/";
constexpr std::uint8_t kStream = 1;
struct LayoutFailure { QueryStatus status; };
[[noreturn]] void fail(QueryError code, const char* message) {
  throw LayoutFailure{QueryStatus::failure(code, QueryPhase::Planning, message)};
}
void require(bool condition, const char* message) {
  if (!condition) fail(QueryError::CorruptLayout, message);
}
void budget(bool condition, const char* message) {
  if (!condition) fail(QueryError::ResourceExhausted, message);
}
void add_budget(std::uint64_t& current, std::uint64_t amount,
    std::uint64_t maximum, const char* message) {
  budget(current <= maximum && amount <= maximum - current, message);
  current += amount;
}
bool contained(ArrowBufferSpan span, std::uint64_t begin, std::uint64_t size) {
  return span.offset >= begin && span.offset - begin <= size &&
    span.length <= size - (span.offset - begin);
}
std::uint64_t bitmap_bytes(std::uint64_t count) {
  return count / 8 + (count % 8 != 0);
}

[[noreturn]] void corrupt(const char* message) { fail(QueryError::CorruptLayout, message); }
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


class Encoder {
public:
  Encoder(std::uint64_t limit, BinaryValue* output) : limit_(limit), output_(output) {}
  void raw(const std::uint8_t* bytes, std::uint64_t size) {
    add_budget(size_, size, limit_, "Arrow layout exceeds serialized layout budget");
    if (output_ && size) output_->insert(output_->end(), bytes, bytes + size);
  }
  void u8(std::uint8_t value) { raw(&value, 1); }
  void u32(std::uint32_t value) {
    for (unsigned i = 0; i < 4; ++i) u8(static_cast<std::uint8_t>(value >> (i * 8)));
  }
  void u64(std::uint64_t value) {
    for (unsigned i = 0; i < 8; ++i) u8(static_cast<std::uint8_t>(value >> (i * 8)));
  }
  void bytes(const BinaryValue& value) { u64(value.size()); raw(value.data(), value.size()); }
  void string(const std::string& value) {
    u64(value.size()); raw(reinterpret_cast<const std::uint8_t*>(value.data()), value.size());
  }
  void span(ArrowBufferSpan value) { u64(value.offset); u64(value.length); }
  void optional(const std::optional<std::uint64_t>& value) {
    u8(value.has_value()); if (value) u64(*value);
  }
private:
  std::uint64_t limit_, size_{0};
  BinaryValue* output_;
};

void encode(Encoder& out, const ArrowLayoutDocument& doc) {
  out.raw(reinterpret_cast<const std::uint8_t*>(kHeader), sizeof(kHeader) - 1);
  out.u8(kStream);
  out.u64(doc.source_size); out.u64(doc.total_rows); out.u8(doc.explicit_eos);
  out.u64(doc.index_read_bytes); out.u64(doc.index_decoded_bytes);
  out.u64(doc.fields.size());
  for (const auto& field : doc.fields) {
    out.string(field.name); out.string(field.native_type); out.u32(field.list_depth);
    out.string(field.unsupported_reason);
  }
  out.u64(doc.messages.size());
  for (const auto& msg : doc.messages) {
    out.u8(static_cast<std::uint8_t>(msg.kind)); out.u64(msg.offset);
    out.u64(msg.body_offset); out.u64(msg.body_length); out.bytes(msg.metadata);
    out.u8(static_cast<std::uint8_t>(msg.compression)); out.u64(msg.rows);
    out.u64(msg.nodes.size());
    for (const auto& node : msg.nodes) {
      out.u64(node.path.size()); for (auto part : node.path) out.u32(part);
      out.u64(node.length); out.u64(node.null_count); out.u64(node.buffers.size());
      for (auto span : node.buffers) out.span(span);
    }
    out.u8(msg.dictionary_id.has_value());
    if (msg.dictionary_id) out.u64(static_cast<std::uint64_t>(*msg.dictionary_id));
    out.u8(msg.dictionary_delta); out.optional(msg.previous_dictionary);
  }
  out.u64(doc.batches.size());
  for (const auto& batch : doc.batches) {
    out.u64(batch.message_index); out.u64(batch.row_begin); out.u64(batch.row_count);
    out.u64(batch.dictionary_epochs.size());
    for (auto epoch : batch.dictionary_epochs) {
      out.u64(static_cast<std::uint64_t>(epoch.first)); out.u64(epoch.second);
    }
    out.u64(batch.arrays.size());
    for (const auto& array : batch.arrays) {
      out.u32(array.field_index); out.u64(array.levels.size());
      for (const auto& level : array.levels) {
        out.u64(level.offsets.size()); for (auto offset : level.offsets) out.u64(offset);
        out.bytes(level.validity);
      }
      out.u64(array.leaf_count); out.u64(array.leaf_null_count);
      out.span(array.leaf_values); out.span(array.leaf_validity);
    }
  }
}

class Decoder {
public:
  explicit Decoder(const BinaryValue& bytes) : bytes_(bytes) {}
  std::uint64_t remaining() const { return bytes_.size() - position_; }
  void skip(std::uint64_t size) {
    require(size <= remaining(), "truncated Arrow layout"); position_ += size;
  }
  std::uint8_t u8() { require(remaining() >= 1, "truncated Arrow layout"); return bytes_[position_++]; }
  bool boolean() { auto n = u8(); require(n <= 1, "invalid Arrow layout boolean"); return n != 0; }
  std::uint32_t u32() {
    std::uint32_t value = 0; for (unsigned i = 0; i < 4; ++i) value |= std::uint32_t(u8()) << (i * 8); return value;
  }
  std::uint64_t u64() {
    std::uint64_t value = 0; for (unsigned i = 0; i < 8; ++i) value |= std::uint64_t(u8()) << (i * 8); return value;
  }
  std::int64_t i64() {
    const auto raw = u64(); std::int64_t value; std::memcpy(&value, &raw, sizeof(value)); return value;
  }
  std::uint64_t count(std::uint64_t limit, std::uint64_t minimum_bytes) {
    const auto n = u64();
    budget(n <= limit, "Arrow layout collection exceeds budget");
    require(n <= remaining() / minimum_bytes, "truncated Arrow layout collection");
    return n;
  }
  BinaryValue bytes(std::uint64_t limit) {
    const auto n = count(limit, 1);
    BinaryValue result(bytes_.begin() + position_, bytes_.begin() + position_ + n);
    position_ += n; return result;
  }
  std::string string(std::uint64_t limit) {
    const auto n = count(limit, 1);
    std::string result(reinterpret_cast<const char*>(bytes_.data() + position_), n);
    position_ += n; return result;
  }
  ArrowBufferSpan span() { auto offset = u64(); return {offset, u64()}; }
  std::optional<std::uint64_t> optional() { if (boolean()) return u64(); return {}; }
private:
  const BinaryValue& bytes_;
  std::size_t position_{0};
};

// This decoder only copies explicitly counted wire collections. Every reserve
// is preceded by count(limit, minimum_serialized_stride); it never recursively
// expands native schema metadata. Scalars/offsets/spans have 1:1 wire storage;
// structural vector elements have the strides asserted below. Three strings
// per native field include generous fixed SSO/capacity rounding slack. On the
// supported libc++/libstdc++ ABIs these bounds leave ample room within a 16x
// wire-size resident reservation; codec reconstruction needs no second schema
// tree or native Arrow temporary allocations.
static_assert(sizeof(ArrowNativeField) + 3 * 32 <= 8 * 28,
  "revisit Arrow layout reconstruction reservation for this ABI");
static_assert(sizeof(ArrowMessageLayout) <= 8 * 53 &&
  sizeof(ArrowFieldNode) <= 8 * 32 && sizeof(ArrowBatchLayout) <= 8 * 40 &&
  sizeof(ArrowArrayIndex) <= 8 * 60 && sizeof(ArrowListLevelIndex) <= 8 * 16,
  "revisit Arrow layout reconstruction reservation for this ABI");

ArrowLayoutDocument decode(Decoder& in, const ArrowLayoutLimits& limits) {
  in.skip(sizeof(kHeader) - 1);
  require(in.u8() == kStream, "Arrow layout container is not an IPC stream");
  ArrowLayoutDocument doc;
  doc.source_size = in.u64(); doc.total_rows = in.u64(); doc.explicit_eos = in.boolean();
  doc.index_read_bytes = in.u64(); doc.index_decoded_bytes = in.u64();
  auto n = in.count(limits.max_nodes, 28); doc.fields.reserve(n);
  for (std::uint64_t i = 0; i < n; ++i) {
    ArrowNativeField field; field.name = in.string(limits.max_layout_bytes);
    field.native_type = in.string(limits.max_layout_bytes); field.list_depth = in.u32();
    field.unsupported_reason = in.string(limits.max_layout_bytes); doc.fields.push_back(std::move(field));
  }
  n = in.count(limits.max_messages, 53); doc.messages.reserve(n);
  std::uint64_t nodes = 0, index_bytes = 0;
  for (std::uint64_t i = 0; i < n; ++i) {
    ArrowMessageLayout msg; msg.kind = static_cast<ArrowMessageKind>(in.u8());
    msg.offset = in.u64(); msg.body_offset = in.u64(); msg.body_length = in.u64();
    msg.metadata = in.bytes(limits.max_metadata_bytes);
    msg.compression = static_cast<ArrowBufferCompression>(in.u8()); msg.rows = in.u64();
    auto count = in.count(limits.max_nodes, 32);
    add_budget(nodes, count, limits.max_nodes, "Arrow layout node budget exceeded");
    msg.nodes.reserve(count);
    for (std::uint64_t j = 0; j < count; ++j) {
      ArrowFieldNode node; const auto depth = in.count(limits.max_depth, 4);
      node.path.reserve(depth); for (std::uint64_t k = 0; k < depth; ++k) node.path.push_back(in.u32());
      node.length = in.u64(); node.null_count = in.u64();
      const auto buffers = in.count(limits.max_nodes, 16); node.buffers.reserve(buffers);
      for (std::uint64_t k = 0; k < buffers; ++k) node.buffers.push_back(in.span());
      msg.nodes.push_back(std::move(node));
    }
    if (in.boolean()) msg.dictionary_id = in.i64();
    msg.dictionary_delta = in.boolean(); msg.previous_dictionary = in.optional();
    doc.messages.push_back(std::move(msg));
  }
  n = in.count(limits.max_messages, 40); doc.batches.reserve(n);
  for (std::uint64_t i = 0; i < n; ++i) {
    ArrowBatchLayout batch; batch.message_index = in.u64();
    batch.row_begin = in.u64(); batch.row_count = in.u64();
    auto count = in.count(limits.max_nodes, 16);
    for (std::uint64_t j = 0; j < count; ++j) {
      auto id = in.i64(); auto epoch = in.u64();
      require(batch.dictionary_epochs.emplace(id, epoch).second, "duplicate Arrow dictionary epoch");
    }
    count = in.count(limits.max_nodes, 60); batch.arrays.reserve(count);
    for (std::uint64_t j = 0; j < count; ++j) {
      ArrowArrayIndex array; array.field_index = in.u32();
      auto levels = in.count(2, 16); array.levels.reserve(levels);
      for (std::uint64_t k = 0; k < levels; ++k) {
        ArrowListLevelIndex level; auto offsets = in.count(limits.max_index_bytes / 8, 8);
        add_budget(index_bytes, offsets * 8, limits.max_index_bytes, "Arrow offset index exceeds budget");
        level.offsets.reserve(offsets);
        for (std::uint64_t p = 0; p < offsets; ++p) level.offsets.push_back(in.u64());
        level.validity = in.bytes(limits.max_index_bytes - index_bytes);
        index_bytes += level.validity.size(); array.levels.push_back(std::move(level));
      }
      array.leaf_count = in.u64(); array.leaf_null_count = in.u64();
      array.leaf_values = in.span(); array.leaf_validity = in.span();
      batch.arrays.push_back(std::move(array));
    }
    doc.batches.push_back(std::move(batch));
  }
  require(in.remaining() == 0, "trailing bytes in Arrow layout");
  return doc;
}

void validate_native_batch(const ArrowMessageLayout& msg,
    const arrow::FieldVector& fields, const ArrowLayoutLimits& limits) {
  auto message_header = Flat::root(msg.metadata).child(2);
  if (msg.kind == ArrowMessageKind::Dictionary) {
    require(msg.dictionary_id == message_header.scalar(0, 8) &&
      msg.dictionary_delta == (message_header.scalar(2, 1) != 0),
      "Arrow dictionary identity differs from native metadata");
  }
  const auto header = msg.kind == ArrowMessageKind::Dictionary ? message_header.child(1) : message_header;
  require(header.scalar(0, 8) >= 0 && static_cast<std::uint64_t>(header.scalar(0, 8)) == msg.rows,
    "Arrow row count differs from native metadata");
  auto compression = ArrowBufferCompression::None;
  if (header.field(3)) {
    auto native = header.child(3);
    require(native.scalar(1, 1) == 0 && (native.scalar(0, 1) == 0 || native.scalar(0, 1) == 1),
      "unsupported native Arrow buffer compression");
    compression = native.scalar(0, 1) == 0 ? ArrowBufferCompression::Lz4 : ArrowBufferCompression::Zstd;
  }
  require(compression == msg.compression, "Arrow compression differs from native metadata");
  const auto nodes = header.vector(1, 16), buffers = header.vector(2, 16), variadic = header.vector(4, 8);
  require(nodes.second == msg.nodes.size(), "Arrow field node count differs from native metadata");
  std::uint64_t node_index = 0, buffer_index = 0, extra = 0;
  std::function<void(std::shared_ptr<arrow::DataType>, std::vector<std::uint32_t>)> visit;
  visit = [&](std::shared_ptr<arrow::DataType> type, std::vector<std::uint32_t> path) {
    budget(path.size() <= limits.max_depth, "Arrow schema nesting exceeds budget");
    require(node_index < msg.nodes.size(), "Arrow schema has an unindexed field node");
    const auto& node = msg.nodes[node_index];
    auto pos = nodes.first + 16 * node_index++;
    require(node.path == path && signed_le(msg.metadata.data() + pos, 8) >= 0 &&
      node.length == static_cast<std::uint64_t>(signed_le(msg.metadata.data() + pos, 8)) &&
      signed_le(msg.metadata.data() + pos + 8, 8) >= 0 &&
      node.null_count == static_cast<std::uint64_t>(signed_le(msg.metadata.data() + pos + 8, 8)),
      "Arrow field node differs from native schema/metadata");
    if (type->id() == arrow::Type::EXTENSION)
      type = std::static_pointer_cast<arrow::ExtensionType>(type)->storage_type();
    const auto layout = type->layout();
    std::uint64_t count = 0;
    for (const auto& spec : layout.buffers)
      if (spec.kind != arrow::DataTypeLayout::ALWAYS_NULL) ++count;
    if (layout.variadic_spec) {
      require(extra < variadic.second, "Arrow variadic buffer count is missing");
      auto n = signed_le(msg.metadata.data() + variadic.first + 8 * extra++, 8);
      require(n >= 0 && static_cast<std::uint64_t>(n) <= buffers.second,
        "Arrow variadic buffer count is invalid");
      count += static_cast<std::uint64_t>(n);
    }
    require(count == node.buffers.size() && buffer_index <= buffers.second &&
      count <= buffers.second - buffer_index, "Arrow node buffer count differs from native metadata");
    for (const auto& span : node.buffers) {
      const auto bp = buffers.first + 16 * buffer_index++;
      const auto offset = signed_le(msg.metadata.data() + bp, 8);
      const auto length = signed_le(msg.metadata.data() + bp + 8, 8);
      require(offset >= 0 && length >= 0 && static_cast<std::uint64_t>(offset) <= msg.body_length &&
        span.offset == msg.body_offset + static_cast<std::uint64_t>(offset) &&
        span.length == static_cast<std::uint64_t>(length),
        "Arrow buffer span differs from native metadata");
    }
    for (int i = 0; i < type->num_fields(); ++i) {
      auto child = path; child.push_back(i); visit(type->field(i)->type(), std::move(child));
    }
  };
  for (std::uint32_t i = 0; i < fields.size(); ++i) visit(fields[i]->type(), {i});
  require(node_index == nodes.second && buffer_index == buffers.second && extra == variadic.second,
    "Arrow native schema has unconsumed node/buffer metadata");
}

void validate(const ArrowLayoutDocument& doc, const ArrowLayoutLimits& limits) {
  budget(doc.fields.size() <= limits.max_nodes && doc.messages.size() <= limits.max_messages &&
    doc.batches.size() <= limits.max_messages, "Arrow layout collection exceeds budget");
  budget(doc.index_read_bytes <= limits.max_index_read_bytes &&
    doc.index_decoded_bytes <= limits.max_index_read_bytes, "Arrow ingestion index budget exceeded");
  require(!doc.messages.empty(), "Arrow stream layout has no schema message");
  for (const auto& field : doc.fields) {
    require(!field.native_type.empty(), "Arrow native field type is empty");
    require(field.list_depth <= 2, "Arrow supported field exceeds two List levels");
    require((field.list_depth == 0) == !field.unsupported_reason.empty(),
      "Arrow field support descriptor is inconsistent");
  }
  std::uint64_t next = 0, node_count = 0, record_count = 0;
  std::map<std::int64_t, std::uint64_t> dictionaries;
  arrow::ipc::DictionaryMemo dictionary_types;
  std::shared_ptr<arrow::Schema> schema;
  std::set<std::int64_t> required_dictionary_ids;
  for (std::uint64_t i = 0; i < doc.messages.size(); ++i) {
    const auto& msg = doc.messages[i];
    require(msg.kind == ArrowMessageKind::Schema || msg.kind == ArrowMessageKind::Dictionary ||
      msg.kind == ArrowMessageKind::RecordBatch, "unknown Arrow message kind");
    require((i == 0) == (msg.kind == ArrowMessageKind::Schema), "Arrow schema must be the first and only schema message");
    require(msg.offset == next && msg.offset % 8 == 0, "Arrow message chain is not contiguous and aligned");
    require(!msg.metadata.empty() && msg.metadata.size() % 8 == 0, "Arrow metadata is empty or unaligned");
    budget(msg.metadata.size() <= limits.max_metadata_bytes, "Arrow metadata exceeds budget");
    require(msg.offset <= doc.source_size && doc.source_size - msg.offset >= 8 &&
      msg.metadata.size() <= doc.source_size - msg.offset - 8 &&
      msg.body_offset == msg.offset + 8 + msg.metadata.size(), "Arrow message metadata span is invalid");
    require(contained({msg.body_offset, msg.body_length}, 0, doc.source_size) &&
      msg.body_length % 8 == 0, "Arrow message body span is invalid");
    next = msg.body_offset + msg.body_length;
    require(msg.rows <= static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()),
      "Arrow row count exceeds native signed range");
    require(msg.compression == ArrowBufferCompression::None ||
      msg.compression == ArrowBufferCompression::Lz4 || msg.compression == ArrowBufferCompression::Zstd,
      "unknown Arrow buffer compression");
    auto native = arrow::ipc::Message::Open(std::make_shared<arrow::Buffer>(
      msg.metadata.data(), static_cast<std::int64_t>(msg.metadata.size())), nullptr);
    require(native.ok() && (*native)->Verify(), "invalid native Arrow message metadata");
    const auto expected_type = msg.kind == ArrowMessageKind::Schema ? arrow::ipc::MessageType::SCHEMA :
      (msg.kind == ArrowMessageKind::Dictionary ? arrow::ipc::MessageType::DICTIONARY_BATCH : arrow::ipc::MessageType::RECORD_BATCH);
    require((*native)->type() == expected_type && (*native)->body_length() >= 0 &&
      static_cast<std::uint64_t>((*native)->body_length()) == msg.body_length,
      "Arrow native metadata does not match indexed message");
    if (i == 0) require(msg.body_length == 0 && msg.rows == 0 && msg.nodes.empty() &&
      msg.compression == ArrowBufferCompression::None, "Arrow schema carries record data");
    require((*native)->metadata_version() == arrow::ipc::MetadataVersion::V5,
      "Arrow layout requires IPC metadata V5");
    if (i == 0) {
      auto decoded = arrow::ipc::ReadSchema(**native, &dictionary_types);
      require(decoded.ok(), "Arrow native schema cannot be decoded");
      schema = *decoded;
      require(schema->endianness() == arrow::Endianness::Little &&
        static_cast<std::size_t>(schema->num_fields()) == doc.fields.size(),
        "Arrow schema endianness or field count differs from layout");
      std::uint64_t schema_nodes = 0;
      std::function<void(const std::shared_ptr<arrow::DataType>&, std::vector<int>)> inspect;
      inspect = [&](const std::shared_ptr<arrow::DataType>& type, std::vector<int> path) {
        budget(path.size() <= limits.max_depth, "Arrow native schema depth exceeds budget");
        add_budget(schema_nodes, 1, limits.max_nodes, "Arrow native schema node count exceeds budget");
        if (type->id() == arrow::Type::DICTIONARY) {
          auto id = dictionary_types.fields().GetFieldId(path);
          require(id.ok(), "Arrow native dictionary field has no ID");
          required_dictionary_ids.insert(*id);
          return;
        }
        for (int child = 0; child < type->num_fields(); ++child) {
          auto nested = path; nested.push_back(child); inspect(type->field(child)->type(), std::move(nested));
        }
      };
      for (std::size_t f = 0; f < doc.fields.size(); ++f) {
        inspect(schema->field(f)->type(), {static_cast<int>(f)});
        const auto& native_field = schema->field(f);
        const auto& field = doc.fields[f];
        require(native_field->name() == field.name && native_field->type()->ToString() == field.native_type,
          "Arrow native field descriptor differs from schema");
        auto type = native_field->type(); std::uint32_t depth = 0;
        while (type->id() == arrow::Type::LIST || type->id() == arrow::Type::LARGE_LIST) {
          ++depth; type = type->field(0)->type();
        }
        const auto supported_depth = (depth == 1 || depth == 2) && type->id() == arrow::Type::FLOAT ? depth : 0;
        require(field.list_depth == supported_depth, "Arrow physical List shape differs from schema");
      }
    } else if (msg.kind == ArrowMessageKind::RecordBatch) {
      validate_native_batch(msg, schema->fields(), limits);
    } else {
      require(msg.dictionary_id.has_value(), "Arrow dictionary has no identity");
      auto type = dictionary_types.GetDictionaryType(*msg.dictionary_id);
      require(type.ok(), "Arrow dictionary identity is absent from schema");
      validate_native_batch(msg, {arrow::field("dictionary", *type)}, limits);
    }

    if (msg.kind == ArrowMessageKind::Dictionary) {
      require(msg.dictionary_id.has_value(), "Arrow dictionary has no identity");
      if (msg.dictionary_delta) {
        auto found = dictionaries.find(*msg.dictionary_id);
        require(found != dictionaries.end() && msg.previous_dictionary == found->second,
          "Arrow dictionary delta has no matching previous epoch");
      } else require(!msg.previous_dictionary, "Arrow dictionary replacement retains a previous epoch");
      dictionaries[*msg.dictionary_id] = i;
    } else require(!msg.dictionary_id && !msg.dictionary_delta && !msg.previous_dictionary,
      "non-dictionary Arrow message has dictionary identity");
    add_budget(node_count, msg.nodes.size(), limits.max_nodes, "Arrow layout node budget exceeded");
    std::set<std::vector<std::uint32_t>> paths;
    std::uint64_t roots = 0, previous_buffer_end = msg.body_offset;
    std::vector<std::uint32_t> previous_path;
    for (const auto& node : msg.nodes) {
      budget(node.path.size() <= limits.max_depth, "Arrow field nesting exceeds budget");
      require(!node.path.empty() && node.null_count <= node.length &&
        node.length <= static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()),
        "Arrow field node path or null count is invalid");
      require(previous_path.empty() || previous_path < node.path, "Arrow field nodes are not in schema preorder");
      previous_path = node.path;
      if (node.path.size() > 1) {
        auto parent = node.path; parent.pop_back();
        require(paths.count(parent) != 0, "Arrow field node has no parent");
      } else {
        if (msg.kind == ArrowMessageKind::RecordBatch)
          require(node.path[0] == roots && node.length == msg.rows,
            "Arrow root field length or ordinal does not match batch");
        ++roots;
      }
      require(paths.insert(node.path).second, "duplicate Arrow field node path");
      budget(node.buffers.size() <= limits.max_nodes, "Arrow field buffer count exceeds budget");
      for (auto span : node.buffers) {
        require(contained(span, msg.body_offset, msg.body_length), "Arrow field buffer escapes message body");
        require(span.offset % 8 == 0 && span.offset >= previous_buffer_end,
          "Arrow field buffers overlap or are unaligned");
        previous_buffer_end = span.offset + span.length;
      }
    }
    if (msg.kind == ArrowMessageKind::RecordBatch) {
      require(roots == doc.fields.size(), "Arrow batch does not index every root field"); ++record_count;
    }
  }
  require(next <= doc.source_size && doc.source_size - next == (doc.explicit_eos ? 8u : 0u),
    "Arrow stream end does not match source size");
  require(record_count == doc.batches.size(), "Arrow batch/message counts differ");
  std::uint64_t rows = 0, last_message = 0, index_bytes = 0, epoch_cursor = 0;
  std::map<std::int64_t, std::uint64_t> visible_epochs;
  for (const auto& batch : doc.batches) {
    require(batch.message_index > last_message && batch.message_index < doc.messages.size(),
      "Arrow batch message reference is invalid");
    last_message = batch.message_index;
    const auto& msg = doc.messages[batch.message_index];
    require(msg.kind == ArrowMessageKind::RecordBatch && batch.row_count == msg.rows &&
      batch.row_begin == rows && batch.row_count <= std::numeric_limits<std::uint64_t>::max() - rows,
      "Arrow batch row range is invalid");
    rows += batch.row_count;
    while (epoch_cursor < batch.message_index) {
      const auto& prior = doc.messages[epoch_cursor];
      if (prior.kind == ArrowMessageKind::Dictionary)
        visible_epochs[*prior.dictionary_id] = epoch_cursor;
      ++epoch_cursor;
    }
    require(batch.dictionary_epochs == visible_epochs, "Arrow batch dictionary epochs differ from visible dictionaries");
    for (auto id : required_dictionary_ids)
      require(visible_epochs.count(id) != 0, "Arrow batch uses an undefined native dictionary");
    for (auto epoch : batch.dictionary_epochs) {
      require(epoch.second < batch.message_index, "Arrow dictionary epoch is not earlier than its batch");
      const auto& dependency = doc.messages[epoch.second];
      require(dependency.kind == ArrowMessageKind::Dictionary && dependency.dictionary_id == epoch.first,
        "Arrow dictionary epoch references a different dictionary");
      const auto latest = visible_epochs.find(epoch.first);
      require(latest != visible_epochs.end() && latest->second == epoch.second,
        "Arrow batch uses a superseded dictionary epoch");
    }
    std::vector<std::size_t> root_positions(doc.fields.size(), msg.nodes.size());
    for (std::size_t i = 0; i < msg.nodes.size(); ++i)
      if (msg.nodes[i].path.size() == 1) root_positions[msg.nodes[i].path[0]] = i;
    std::set<std::uint32_t> fields;
    for (const auto& array : batch.arrays) {
      require(array.field_index < doc.fields.size() && fields.insert(array.field_index).second,
        "Arrow array index field is missing or duplicated");
      const auto depth = doc.fields[array.field_index].list_depth;
      require(depth != 0 && array.levels.size() == depth && array.leaf_null_count <= array.leaf_count,
        "Arrow array shape or leaf null count is invalid");
      require(contained(array.leaf_values, msg.body_offset, msg.body_length) &&
        contained(array.leaf_validity, msg.body_offset, msg.body_length), "Arrow leaf buffers escape message body");
      const auto root = root_positions[array.field_index];
      require(root < msg.nodes.size() && depth < msg.nodes.size() - root,
        "Arrow supported List chain is incomplete");
      std::vector<std::uint32_t> path{array.field_index};
      for (std::uint32_t level = 0; level < depth; ++level) {
        const auto& index = array.levels[level];
        require(!index.offsets.empty(), "Arrow List has no terminal offset");
        const auto count = index.offsets.size() - 1;
        if (level == 0) require(count == batch.row_count, "Arrow outer List count differs from batch rows");
        const auto* found = &msg.nodes[root + level];
        require(found->path == path && found->length == count && found->buffers.size() == 2,
          "Arrow List index does not match native node");
        require(index.validity.empty() ? found->null_count == 0 : index.validity.size() == bitmap_bytes(count),
          "Arrow List validity size is invalid");
        std::uint64_t nulls = 0;
        if (!index.validity.empty()) for (std::uint64_t i = 0; i < count; ++i)
          nulls += (index.validity[i / 8] & (1u << (i % 8))) == 0;
        require(nulls == found->null_count, "Arrow List validity does not match native null count");
        const auto child_count = level + 1 == depth ? array.leaf_count :
          (array.levels[level + 1].offsets.empty() ? 0 : array.levels[level + 1].offsets.size() - 1);
        require(std::is_sorted(index.offsets.begin(), index.offsets.end()) && index.offsets.back() <= child_count,
          "Arrow List offsets are reversed or exceed child level");
        budget(index.offsets.size() <= limits.max_index_bytes / 8, "Arrow offsets exceed index budget");
        add_budget(index_bytes, index.offsets.size() * 8, limits.max_index_bytes, "Arrow offsets exceed index budget");
        add_budget(index_bytes, index.validity.size(), limits.max_index_bytes, "Arrow validity exceeds index budget");
        path.push_back(0);
      }
      const auto* leaf = &msg.nodes[root + depth];
      require(leaf->path == path && leaf->length == array.leaf_count && leaf->null_count == array.leaf_null_count &&
        leaf->buffers.size() == 2, "Arrow leaf index does not match native node");
      require(leaf->buffers[0].offset == array.leaf_validity.offset && leaf->buffers[0].length == array.leaf_validity.length &&
        leaf->buffers[1].offset == array.leaf_values.offset && leaf->buffers[1].length == array.leaf_values.length,
        "Arrow leaf index references different native buffers");
      if (msg.compression == ArrowBufferCompression::None) {
        require(array.leaf_count <= std::numeric_limits<std::uint64_t>::max() / 4 &&
          array.leaf_values.length >= array.leaf_count * 4,
          "Arrow Float32 value buffer is too short");
        require(array.leaf_null_count == 0 || array.leaf_validity.length >= bitmap_bytes(array.leaf_count),
          "Arrow leaf validity buffer is too short");
      }
    }
    for (std::uint32_t i = 0; i < doc.fields.size(); ++i)
      require(doc.fields[i].list_depth == 0 || fields.count(i) != 0,
        "Arrow supported field has no physical List index");
  }
  require(rows == doc.total_rows, "Arrow total rows differs from batch rows");
  // Count the actual binary encoding as well as individual index/metadata budgets.
  Encoder counter(limits.max_layout_bytes, nullptr); encode(counter, doc);
}

class StoredArrowListIndex final : public ArrayListIndex {
public:
  StoredArrowListIndex(std::shared_ptr<const ArrowLayoutDocument> layout, const ArrowArrayIndex* array)
    : layout_(std::move(layout)), array_(array) {}
  std::uint32_t list_depth() const override { return array_->levels.size(); }
  std::uint64_t list_count(std::uint32_t level) const override {
    return level < array_->levels.size() ? array_->levels[level].offsets.size() - 1 : 0;
  }
  std::uint64_t leaf_count() const override { return array_->leaf_count; }
  QueryResult<ArrayListSpan> list_span(std::uint32_t level, std::uint64_t index) const override {
    if (level >= array_->levels.size() || index >= list_count(level))
      return QueryResult<ArrayListSpan>::failure(QueryStatus::failure(QueryError::InvalidQuery,
        QueryPhase::Planning, "Arrow List coordinate is out of range"));
    const auto& stored = array_->levels[level];
    return QueryResult<ArrayListSpan>::success({stored.offsets[index], stored.offsets[index + 1],
      stored.validity.empty() || (stored.validity[index / 8] & (1u << (index % 8))) != 0});
  }
private:
  std::shared_ptr<const ArrowLayoutDocument> layout_;
  const ArrowArrayIndex* array_;
};

QueryStatus allocation_failure() {
  return QueryStatus::failure(QueryError::ResourceExhausted, QueryPhase::Planning,
    "Arrow layout allocation failed");
}
} // namespace

QueryStatus validate_arrow_layout(const ArrowLayoutDocument& layout, const ArrowLayoutLimits& limits) {
  try { validate(layout, limits); return QueryStatus::success(); }
  catch (const LayoutFailure& error) { return error.status; }
  catch (const std::bad_alloc&) { return allocation_failure(); }
}
QueryResult<BinaryValue> serialize_arrow_layout(const ArrowLayoutDocument& layout, const ArrowLayoutLimits& limits) {
  try {
    validate(layout, limits); BinaryValue bytes; Encoder out(limits.max_layout_bytes, &bytes); encode(out, layout);
    return QueryResult<BinaryValue>::success(std::move(bytes));
  } catch (const LayoutFailure& error) { return QueryResult<BinaryValue>::failure(error.status); }
  catch (const std::bad_alloc&) { return QueryResult<BinaryValue>::failure(allocation_failure()); }
}
QueryResult<ArrowLayoutDocument> parse_arrow_layout(const BinaryValue& bytes,
    std::uint64_t source_size, const ArrowLayoutLimits& limits) {
  auto parsed = detail::reconstruct_validated_arrow_layout(bytes, source_size, limits);
  if (!parsed.ok()) return parsed;
  auto status = validate_arrow_layout(parsed.value(), limits);
  if (!status.ok()) return QueryResult<ArrowLayoutDocument>::failure(status);
  return parsed;
}

QueryResult<ArrowLayoutDocument> detail::reconstruct_validated_arrow_layout(
    const BinaryValue& bytes, std::uint64_t source_size,
    const ArrowLayoutLimits& limits) {
  try {
    budget(bytes.size() <= limits.max_layout_bytes, "Arrow serialized layout exceeds budget");
    if (bytes.size() < sizeof(kHeader) - 1 ||
        !std::equal(kHeader, kHeader + sizeof(kHeader) - 1, bytes.begin())) {
      if (bytes.size() >= sizeof(kPrefix) - 1 &&
          std::equal(kPrefix, kPrefix + sizeof(kPrefix) - 1, bytes.begin())) {
        auto pos = sizeof(kPrefix) - 1;
        const auto first = pos;
        while (pos < bytes.size() && bytes[pos] >= '0' && bytes[pos] <= '9') ++pos;
        if (pos > first && pos < bytes.size() && bytes[pos] == '\n')
          fail(QueryError::UnsupportedLayoutVersion, "unsupported Arrow layout version");
      }
      fail(QueryError::CorruptLayout, "invalid Arrow layout header");
    }
    Decoder in(bytes); auto doc = decode(in, limits);
    if (doc.source_size != source_size) fail(QueryError::StalePlan, "Arrow layout source size does not match bound source");
    return QueryResult<ArrowLayoutDocument>::success(std::move(doc));
  } catch (const LayoutFailure& error) { return QueryResult<ArrowLayoutDocument>::failure(error.status); }
  catch (const std::bad_alloc&) { return QueryResult<ArrowLayoutDocument>::failure(allocation_failure()); }
}
QueryResult<std::shared_ptr<const ArrayListIndex>> make_arrow_list_index(
    std::shared_ptr<const ArrowLayoutDocument> layout, std::uint64_t batch_index,
    std::uint32_t field_index, const ArrowLayoutLimits& limits) {
  using Result = QueryResult<std::shared_ptr<const ArrayListIndex>>;
  if (!layout) return Result::failure(QueryStatus::failure(QueryError::CorruptLayout,
    QueryPhase::Planning, "Arrow layout is null"));
  auto status = validate_arrow_layout(*layout, limits);
  if (!status.ok()) return Result::failure(status);
  return detail::make_validated_arrow_list_index(std::move(layout), batch_index, field_index);
}

QueryResult<std::shared_ptr<const ArrayListIndex>> detail::make_validated_arrow_list_index(
    std::shared_ptr<const ArrowLayoutDocument> layout, std::uint64_t batch_index,
    std::uint32_t field_index) {
  using Result = QueryResult<std::shared_ptr<const ArrayListIndex>>;
  if (!layout) return Result::failure(QueryStatus::failure(QueryError::CorruptLayout,
    QueryPhase::Planning, "Arrow layout is null"));
  if (batch_index >= layout->batches.size() || field_index >= layout->fields.size())
    return Result::failure(QueryStatus::failure(QueryError::InvalidQuery, QueryPhase::Planning,
      "Arrow batch or native field ordinal is out of range"));
  const auto& arrays = layout->batches[batch_index].arrays;
  auto found = std::find_if(arrays.begin(), arrays.end(), [&](const ArrowArrayIndex& array) { return array.field_index == field_index; });
  if (found == arrays.end()) return Result::failure(QueryStatus::failure(QueryError::UnsupportedCapability,
    QueryPhase::Planning, "Arrow native field has no supported List/Float32 shape"));
  try {
    return Result::success(std::make_shared<StoredArrowListIndex>(std::move(layout), &*found));
  } catch (const std::bad_alloc&) { return Result::failure(allocation_failure()); }
}
} // namespace iotdbfs
