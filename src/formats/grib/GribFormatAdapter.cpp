#include "iotdbfs/formats/grib/GribFormatAdapter.h"
#include "iotdbfs/formats/grib/GribLayout.h"
#include "iotdbfs/formats/grib/gribUtils.h"
#include "iotdbfs/MetadataSerialization.h"
#include <algorithm>
#include <charconv>
#include <limits>
#include <map>
#include <stdexcept>
#include <string_view>
#include <variant>
#include <cstdint>
#include <utility>

namespace iotdbfs {
namespace {

// Layout v1 is a flat JSON object with string and signed integer values.
// The legacy label scanner deliberately accepts partial tokens; proofs must
// validate the entire record before that artifact can be shared.
class LayoutRecordParser {
public:
  using Value = std::variant<std::string, std::int64_t>;
  explicit LayoutRecordParser(std::string_view text) : text_(text) {}
  std::map<std::string, Value> parse() {
    std::map<std::string, Value> result;
    expect('{');
    if (!take('}')) {
      do {
        auto key = string();
        expect(':');
        whitespace();
        Value value;
        if (position_ < text_.size() && text_[position_] == '"')
          value = string();
        else value = integer();
        if (!result.emplace(std::move(key), std::move(value)).second)
          fail("duplicate label");
      } while (take(','));
      expect('}');
    }
    whitespace();
    if (position_ != text_.size()) fail("trailing record content");
    return result;
  }
private:
  [[noreturn]] void fail(const char* reason) const {
    throw std::runtime_error(std::string("GRIB layout has malformed record: ") + reason);
  }
  void whitespace() {
    while (position_ < text_.size() && (text_[position_] == ' ' ||
        text_[position_] == '\r' || text_[position_] == '\t')) ++position_;
  }
  bool take(char value) {
    whitespace();
    if (position_ == text_.size() || text_[position_] != value) return false;
    ++position_;
    return true;
  }
  void expect(char value) { if (!take(value)) fail("unexpected token"); }
  std::string string() {
    whitespace();
    const auto begin = position_;
    expect('"');
    bool escaped = false;
    while (position_ < text_.size()) {
      const auto c = static_cast<unsigned char>(text_[position_++]);
      if (!escaped && c == '"') {
        // Reuse the project's strict UTF-8, JSON escape and surrogate parser.
        const auto token = std::string(text_.substr(begin, position_ - begin));
        auto decoded = deserialize_metadata(
          "{\"x\":{\"type\":\"string\",\"value\":" + token + "}}",
          token.size() + 64);
        if (!decoded.ok()) fail("invalid string");
        return std::get<std::string>(decoded.value().at("x").value());
      }
      if (!escaped && c == '\\') escaped = true;
      else escaped = false;
    }
    fail("unterminated string");
  }
  std::int64_t integer() {
    whitespace();
    const auto begin = position_;
    if (position_ < text_.size() && text_[position_] == '-') ++position_;
    const auto digits = position_;
    while (position_ < text_.size() && text_[position_] >= '0' &&
        text_[position_] <= '9') ++position_;
    if (digits == position_ || (position_ - digits > 1 && text_[digits] == '0'))
      fail("invalid integer");
    std::int64_t value = 0;
    const auto parsed = std::from_chars(text_.data() + begin,
      text_.data() + position_, value);
    if (parsed.ec != std::errc{} || parsed.ptr != text_.data() + position_)
      fail("integer overflow");
    return value;
  }
  std::string_view text_;
  std::size_t position_{0};
};

FormatViewType to_format_view(GribVirtualType type) {
  switch (type) {
    case GribVirtualType::Binary: return FormatViewType::Range;
    case GribVirtualType::Data: return FormatViewType::Materialized;
    case GribVirtualType::None: return FormatViewType::None;
  }
  return FormatViewType::None;
}

class GribFormatQuery final : public FormatQuery {
public:
  explicit GribFormatQuery(GribQuerySpec value)
    : FormatQuery(to_format_view(value.type)), spec(std::move(value)) {}

  GribQuerySpec spec;
};

const GribFormatQuery* as_grib_query(const FormatQuery& query) {
  return dynamic_cast<const GribFormatQuery*>(&query);
}

class GribIngestionSession final : public FormatIngestionSession {
public:
  explicit GribIngestionSession(const GribWriteStreamer::Config& config)
    : streamer_(config) {}

  FormatStatus consume_chunk(std::uint64_t, const std::uint8_t* data,
    std::size_t length) override {
    streamer_.feed_chunk(data, length);
    streamer_.process_complete_messages();
    if (streamer_.failed()) {
      return FormatStatus::failure(streamer_.error().empty() ?
        "GRIB ingestion failed" : streamer_.error());
    }
    return FormatStatus::success();
  }

  FormatResult<FormatIngestionArtifacts> finish() override {
    streamer_.finalize();
    if (streamer_.failed()) {
      return FormatResult<FormatIngestionArtifacts>::failure(
        streamer_.error().empty() ? "GRIB ingestion failed" :
        streamer_.error());
    }
    if (!streamer_.is_grib()) {
      return FormatResult<FormatIngestionArtifacts>::failure(
        "GRIB adapter received unrecognized content");
    }
    if (!streamer_.summary()) {
      return FormatResult<FormatIngestionArtifacts>::failure(
        "GRIB metadata could not be derived");
    }
    FormatIngestionArtifacts artifacts;
    artifacts.metadata = *streamer_.summary();
    const auto& layout = streamer_.layout();
    artifacts.layout.assign(layout.begin(), layout.end());
    return FormatResult<FormatIngestionArtifacts>::success(
      std::move(artifacts));
  }

private:
  GribWriteStreamer streamer_;
};

} // namespace

static FormatResult<std::vector<grib_utils::MsgMatch>> parse_grib_layout_records(
    const std::string& records, std::uint64_t payload_size) {
  std::vector<grib_utils::MsgMatch> result;
  try {
    for (std::size_t start = 0; start < records.size();) {
      const auto end = records.find('\n', start);
      const auto line = std::string_view(records).substr(start,
        end == std::string::npos ? records.size() - start : end - start);
      start = end == std::string::npos ? records.size() : end + 1;
      if (line.empty()) continue;
      const auto labels = LayoutRecordParser(line).parse();
      auto label = [&](const char* name, const char* compact)
          -> const LayoutRecordParser::Value* {
        const auto full = labels.find(name);
        const auto abbreviated = labels.find(compact);
        if (full != labels.end() && abbreviated != labels.end() &&
            full != abbreviated) throw std::runtime_error("GRIB layout repeats aliased label");
        if (full != labels.end()) return &full->second;
        return abbreviated == labels.end() ? nullptr : &abbreviated->second;
      };
      auto integer = [&](const char* name, const char* compact, bool required)
          -> std::optional<std::int64_t> {
        const auto* value = label(name, compact);
        if (!value) {
          if (required) throw std::runtime_error(std::string("GRIB layout omits required label ") + name);
          return std::nullopt;
        }
        const auto* number = std::get_if<std::int64_t>(value);
        if (!number) throw std::runtime_error(std::string("GRIB layout has invalid integer label ") + name);
        return *number;
      };
      grib_utils::MsgMatch match;
      const auto offset = *integer("offset", "of", true);
      const auto length = *integer("totalLength", "ln", true);
      if (offset < 0 || length <= 0 || static_cast<std::uint64_t>(offset) > payload_size ||
          static_cast<std::uint64_t>(length) > payload_size - static_cast<std::uint64_t>(offset))
        throw std::runtime_error("GRIB layout contains an invalid message range");
      match.info.off = static_cast<std::uint64_t>(offset);
      match.info.len = static_cast<std::uint64_t>(length);
      match.info.ok = true;
      auto integer_label = [&](const char* name, const char* compact, bool required = false) {
        auto value = integer(name, compact, required);
        if (value) match.labels[name] = std::to_string(*value);
        return value;
      };
      const auto edition = *integer_label("edition", "ed", true);
      if (edition < std::numeric_limits<std::int32_t>::min() ||
          edition > std::numeric_limits<std::int32_t>::max())
        throw std::runtime_error("GRIB layout has invalid integer label edition");
      integer_label("level", "lv"); integer_label("step", "st");
      integer_label("paramId", "pi"); integer_label("validityDate", "vd");
      integer_label("validityTime", "vt");
      match.info.ni = integer("ni", "ni", false).value_or(0);
      match.info.nj = integer("nj", "nj", false).value_or(0);
      match.info.numberOfDataPoints = integer("numberOfDataPoints", "np", false).value_or(0);
      for (const auto& names : {std::pair{"shortName", "sn"}, {"typeOfLevel", "tl"},
          {"stepRange", "sr"}, {"unit", "u"}, {"globalDomain", "gd"}}) {
        const auto* value = label(names.first, names.second);
        if (!value) continue;
        const auto* text = std::get_if<std::string>(value);
        if (!text) throw std::runtime_error(std::string("GRIB layout has invalid string label ") + names.first);
        match.labels[names.first] = *text;
      }
      // A proof also certifies the existing v1 reader interpretation. Reject
      // syntactically valid spellings that its scanner cannot represent (for
      // example escaped label names or a space before a label's colon).
      const auto reader = grib_utils::find_all_msg_matches(std::string(line), {});
      if (reader.size() != 1 || reader.front().info.off != match.info.off ||
          reader.front().info.len != match.info.len || reader.front().labels != match.labels ||
          reader.front().info.ni != match.info.ni || reader.front().info.nj != match.info.nj ||
          reader.front().info.numberOfDataPoints != match.info.numberOfDataPoints)
        throw std::runtime_error("GRIB layout record differs from the v1 reader interpretation");
      result.push_back(std::move(match));
    }
  } catch (const std::exception& error) {
    return FormatResult<std::vector<grib_utils::MsgMatch>>::failure(error.what());
  }
  return FormatResult<std::vector<grib_utils::MsgMatch>>::success(std::move(result));
}

FormatStatus GribFormatAdapter::validate_layout_artifact(
    const std::vector<std::uint8_t>& layout, std::uint64_t payload_size) const {
  auto document = parse_grib_layout(layout);
  if (!document.ok()) return FormatStatus::failure(document.status().message());
  auto parsed = parse_grib_layout_records(document.value().records, payload_size);
  if (!parsed.ok()) return FormatStatus::failure(parsed.error());
  return FormatStatus::success();
}

GribFormatAdapter::GribFormatAdapter(Config config)
  : config_(std::move(config)), query_service_(true, config_.queries) {}

const std::string& GribFormatAdapter::id() const {
  static const std::string value = "grib";
  return value;
}

const std::vector<std::string>& GribFormatAdapter::extensions() const {
  static const std::vector<std::string> value = {
    "grib", "grib2", "grb", "grb2"
  };
  return value;
}

bool GribFormatAdapter::matches(const FormatSource& source) const {
  if (source.size == 0 || !source.read)
    return false;

  auto detector_config = config_.ingestion;
  detector_config.metadata_config.reset();
  GribWriteStreamer detector(detector_config);

  std::uint64_t offset = 0;
  std::size_t chunk_size = detector_config.chunk_size;
  if (chunk_size == 0)
    chunk_size = 1024 * 1024;
  while (offset < source.size && !detector.type_decided()) {
    const auto want = std::min<std::uint64_t>(chunk_size,
      source.size - offset);
    auto bytes = source.read(offset, want);
    if (!bytes || bytes->size() != static_cast<std::size_t>(want))
      return false;
    detector.feed_chunk(bytes->data(), bytes->size());
    detector.process_complete_messages();
    offset += want;
  }
  if (!detector.type_decided())
    detector.finalize();
  return !detector.failed() && detector.is_grib();
}

std::unique_ptr<FormatIngestionSession>
GribFormatAdapter::begin_ingestion(const FormatSource&) const {
  return std::make_unique<GribIngestionSession>(config_.ingestion);
}

FormatQueryPtr GribFormatAdapter::parse_query(
  const std::string& query) const {
  auto parsed = query_service_.parse_query_spec(query);
  if (!parsed.ok || parsed.type == GribVirtualType::None)
    return {};
  return std::make_shared<GribFormatQuery>(std::move(parsed));
}

bool GribFormatAdapter::can_handle_virtual_query(
  const std::string& path) const {
  return query_service_.can_handle_virtual_query(path);
}

FormatVirtualAttr GribFormatAdapter::get_virtual_attr(long long row_time,
  const std::string& path, const FormatQuery& query,
  const std::string& raw_query) const {
  const auto* grib_query = as_grib_query(query);
  if (!grib_query)
    return {};
  const auto attr = query_service_.get_virtual_attr(row_time, path,
    grib_query->spec, raw_query);
  return { attr.ok, attr.size };
}

FormatOpenResult GribFormatAdapter::open_virtual(long long row_time,
  const std::string& path, const FormatQuery& query,
  const std::string& raw_query) const {
  const auto* grib_query = as_grib_query(query);
  if (!grib_query)
    return {};
  auto opened = query_service_.open_virtual(row_time, path,
    grib_query->spec, raw_query);
  FormatOpenResult result;
  result.ok = opened.ok;
  result.type = to_format_view(opened.type);
  result.range.offset = opened.binary.offset;
  result.range.length = opened.binary.length;
  result.data = std::move(opened.data.bytes);
  return result;
}

std::optional<std::string> GribFormatAdapter::get_xattr(long long row_time,
  const std::string& path, const Inode& inode,
  const std::string& name) const {
  if (!query_service_.enabled_for_path(path))
    return std::nullopt;
  auto attrs = query_service_.build_inode_xattrs(inode);
  if ((name == "user.grib.index.keys" ||
      name == "user.grib.index.values") && config_.queries.read_layout_text) {
    auto layout = config_.queries.read_layout_text(row_time, path);
    if (layout && !layout->empty())
      query_service_.append_index_xattrs(*layout, attrs);
  }
  const auto found = std::find_if(attrs.begin(), attrs.end(),
    [&](const auto& item) { return item.first == name; });
  if (found == attrs.end())
    return std::nullopt;
  return found->second;
}

FormatXattrList GribFormatAdapter::list_xattrs(long long row_time,
  const std::string& path, const Inode& inode) const {
  if (!query_service_.enabled_for_path(path))
    return {};
  auto attrs = query_service_.build_inode_xattrs(inode);
  if (config_.queries.read_layout_text) {
    auto layout = config_.queries.read_layout_text(row_time, path);
    if (layout && !layout->empty())
      query_service_.append_index_xattr_names(attrs);
  }
  return attrs;
}

} // namespace iotdbfs
