#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "iotdbfs/query/training/arrow/DatasetSnapshot.h"
#include "iotdbfs/query/core/QueryProfile.h"

#include <openssl/sha.h>
#include <openssl/rand.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <limits>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>

namespace iotdbfs {
namespace {

constexpr char kCoverageSidecarProtocol[] = "iotdbfs-arrow-coverage/4";

QueryStatus invalid(const std::string& message) {
  return QueryStatus::failure(QueryError::InvalidQuery, QueryPhase::Validation,
    message);
}
QueryStatus corrupt(const std::string& message) {
  return QueryStatus::failure(QueryError::CorruptLayout, QueryPhase::Planning,
    message);
}
QueryStatus read_error(const std::string& message) {
  return QueryStatus::failure(QueryError::ReadFailure, QueryPhase::Storage,
    message);
}
QueryStatus missing(const StructuredBlobVersionKey& key) {
  QueryErrorDetail detail;
  detail.source = QuerySourceReference{key.path, key.version, "arrow"};
  return QueryStatus::failure(QueryError::SourceNotFound, QueryPhase::Catalog,
    "snapshot's exact Arrow File Version was not found", std::move(detail));
}

QueryStatus validate_row_time(const SnapshotRowBinding& row) {
  const auto& time = row.time;
  const auto length = row.steps.end - row.steps.begin;
  if (time.kind == TrainingTimeKind::Step) {
    if (time.start_ns || time.cadence_ns || !time.timezone.empty() ||
        !time.timestamps_ns.empty())
      return invalid("step-only row cannot carry absolute time");
  } else if (time.kind == TrainingTimeKind::RegularNanoseconds) {
    if (time.cadence_ns <= 0 || time.timezone.empty() || !time.timestamps_ns.empty() ||
        static_cast<__int128>(time.start_ns) +
          static_cast<__int128>(length - 1) * time.cadence_ns >
          std::numeric_limits<std::int64_t>::max())
      return invalid("invalid or overflowing regular row time");
  } else if (time.kind == TrainingTimeKind::ExplicitNanoseconds) {
    if (time.start_ns || time.cadence_ns || time.timezone.empty() ||
        time.timestamps_ns.size() != length ||
        !std::is_sorted(time.timestamps_ns.begin(), time.timestamps_ns.end()) ||
        std::adjacent_find(time.timestamps_ns.begin(), time.timestamps_ns.end()) !=
          time.timestamps_ns.end())
      return invalid("explicit row timestamps must match length and increase strictly");
  } else return invalid("unknown row time mode");
  return QueryStatus::success();
}

class Bytes {
public:
  void u64(std::uint64_t v) {
    for (unsigned i = 0; i < 8; ++i) data_.push_back(static_cast<char>(v >> (i * 8)));
  }
  void str(const std::string& s) { u64(s.size()); data_.append(s); }
  void opt(const std::optional<std::string>& s) {
    u64(s.has_value()); if (s) str(*s);
  }
  const std::string& data() const { return data_; }
private:
  std::string data_;
};

class Reader {
public:
  explicit Reader(const std::string& data) : data_(data) {}
  std::uint64_t u64() {
    if (data_.size() - pos_ < 8) throw std::runtime_error("truncated sidecar");
    std::uint64_t v = 0;
    for (unsigned i = 0; i < 8; ++i)
      v |= std::uint64_t(static_cast<unsigned char>(data_[pos_++])) << (i * 8);
    return v;
  }
  std::string str() {
    auto n = u64();
    if (n > data_.size() - pos_) throw std::runtime_error("bad sidecar string");
    auto value = data_.substr(pos_, static_cast<std::size_t>(n));
    pos_ += static_cast<std::size_t>(n);
    return value;
  }
  bool done() const { return pos_ == data_.size(); }
private:
  const std::string& data_;
  std::size_t pos_{0};
};

std::string sha256(const std::string& bytes) {
  unsigned char hash[SHA256_DIGEST_LENGTH];
  SHA256(reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size(), hash);
  constexpr char digits[] = "0123456789abcdef";
  std::string result;
  result.reserve(2 * SHA256_DIGEST_LENGTH);
  for (auto byte : hash) {
    result.push_back(digits[byte >> 4]);
    result.push_back(digits[byte & 15]);
  }
  return result;
}

std::optional<std::string> process_snapshot_identity() {
  // Backend namespaces are unique only within a process. A fresh process
  // must not accept a sidecar written by another process's namespace 1.
  static const auto identity = []() -> std::optional<std::string> {
    std::string nonce(16, '\0');
    if (RAND_bytes(reinterpret_cast<unsigned char*>(&nonce[0]),
          static_cast<int>(nonce.size())) != 1) return std::nullopt;
    return sha256(nonce);
  }();
  if (!identity) return std::nullopt;
  return sha256(*identity + std::to_string(::getpid()));
}

// Small strict JSON reader for HF state.json. It recognizes only the two
// relevant top-level keys and skips arbitrary other valid JSON values.
class StateReader {
public:
  explicit StateReader(const std::string& s) : s_(s) {}
  std::vector<std::string> files(bool& has_indices) {
    ws(); take('{');
    std::vector<std::string> names;
    bool saw_data = false, saw_indices = false;
    while (peek() != '}') {
      auto key = string(); ws(); take(':');
      if (key == "_data_files") {
        if (saw_data) throw std::runtime_error("duplicate _data_files");
        saw_data = true; names = file_array();
      } else if (key == "_indices_data_files") {
        if (saw_indices) throw std::runtime_error("duplicate _indices_data_files");
        saw_indices = true;
        if (peek() == 'n') literal("null");
        else has_indices = !file_array().empty();
      } else skip();
      if (peek() == ',') {
        ++pos_;
        if (peek() == '}') throw std::runtime_error("trailing comma in HF state");
        continue;
      }
      break;
    }
    take('}'); ws();
    if (pos_ != s_.size() || !saw_data || names.empty())
      throw std::runtime_error("invalid HF state or empty _data_files");
    return names;
  }
private:
  void ws() {
    while (pos_ < s_.size() && (s_[pos_] == ' ' || s_[pos_] == '\t' ||
           s_[pos_] == '\n' || s_[pos_] == '\r')) ++pos_;
  }
  char peek() { ws(); return pos_ < s_.size() ? s_[pos_] : '\0'; }
  void take(char c) { if (peek() != c) throw std::runtime_error("invalid HF state JSON"); ++pos_; }
  void literal(const char* value) {
    const auto n = std::strlen(value);
    if (s_.compare(pos_, n, value) != 0)
      throw std::runtime_error("invalid HF state literal");
    pos_ += n;
  }
  std::uint32_t hex_quad() {
    if (s_.size() - pos_ < 4) throw std::runtime_error("truncated HF state Unicode escape");
    std::uint32_t value = 0;
    for (unsigned i = 0; i < 4; ++i) {
      const char c = s_[pos_++];
      value <<= 4;
      if (c >= '0' && c <= '9') value |= c - '0';
      else if (c >= 'a' && c <= 'f') value |= c - 'a' + 10;
      else if (c >= 'A' && c <= 'F') value |= c - 'A' + 10;
      else throw std::runtime_error("invalid HF state Unicode escape");
    }
    return value;
  }
  static void utf8(std::string& out, std::uint32_t codepoint) {
    if (codepoint < 0x80) out.push_back(static_cast<char>(codepoint));
    else if (codepoint < 0x800) {
      out.push_back(static_cast<char>(0xc0 | (codepoint >> 6)));
      out.push_back(static_cast<char>(0x80 | (codepoint & 0x3f)));
    } else if (codepoint < 0x10000) {
      out.push_back(static_cast<char>(0xe0 | (codepoint >> 12)));
      out.push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3f)));
      out.push_back(static_cast<char>(0x80 | (codepoint & 0x3f)));
    } else {
      out.push_back(static_cast<char>(0xf0 | (codepoint >> 18)));
      out.push_back(static_cast<char>(0x80 | ((codepoint >> 12) & 0x3f)));
      out.push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3f)));
      out.push_back(static_cast<char>(0x80 | (codepoint & 0x3f)));
    }
  }
  std::string string() {
    take('"'); std::string result;
    while (pos_ < s_.size()) {
      char c = s_[pos_++];
      if (c == '"') return result;
      if (static_cast<unsigned char>(c) < 32)
        throw std::runtime_error("unescaped control character in HF state string");
      if (c == '\\') {
        if (pos_ == s_.size()) throw std::runtime_error("truncated HF state escape");
        c = s_[pos_++];
        if (c == '"' || c == '\\' || c == '/') result.push_back(c);
        else if (c == 'b') result.push_back('\b');
        else if (c == 'f') result.push_back('\f');
        else if (c == 'n') result.push_back('\n');
        else if (c == 'r') result.push_back('\r');
        else if (c == 't') result.push_back('\t');
        else if (c == 'u') {
          auto codepoint = hex_quad();
          if (codepoint >= 0xd800 && codepoint <= 0xdbff) {
            if (s_.size() - pos_ < 6 || s_[pos_] != '\\' || s_[pos_ + 1] != 'u')
              throw std::runtime_error("unpaired HF state high surrogate");
            pos_ += 2;
            const auto low = hex_quad();
            if (low < 0xdc00 || low > 0xdfff)
              throw std::runtime_error("unpaired HF state high surrogate");
            codepoint = 0x10000 + ((codepoint - 0xd800) << 10) + (low - 0xdc00);
          } else if (codepoint >= 0xdc00 && codepoint <= 0xdfff)
            throw std::runtime_error("unpaired HF state low surrogate");
          utf8(result, codepoint);
        } else throw std::runtime_error("invalid HF state string escape");
      } else result.push_back(c);
    }
    throw std::runtime_error("invalid HF state string");
  }
  void number() {
    if (peek() == '-') ++pos_;
    if (pos_ == s_.size()) throw std::runtime_error("invalid HF state number");
    if (s_[pos_] == '0') ++pos_;
    else {
      if (s_[pos_] < '1' || s_[pos_] > '9')
        throw std::runtime_error("invalid HF state number");
      while (pos_ < s_.size() && s_[pos_] >= '0' && s_[pos_] <= '9') ++pos_;
    }
    if (pos_ < s_.size() && s_[pos_] == '.') {
      ++pos_;
      if (pos_ == s_.size() || s_[pos_] < '0' || s_[pos_] > '9')
        throw std::runtime_error("invalid HF state number");
      while (pos_ < s_.size() && s_[pos_] >= '0' && s_[pos_] <= '9') ++pos_;
    }
    if (pos_ < s_.size() && (s_[pos_] == 'e' || s_[pos_] == 'E')) {
      ++pos_;
      if (pos_ < s_.size() && (s_[pos_] == '+' || s_[pos_] == '-')) ++pos_;
      if (pos_ == s_.size() || s_[pos_] < '0' || s_[pos_] > '9')
        throw std::runtime_error("invalid HF state number");
      while (pos_ < s_.size() && s_[pos_] >= '0' && s_[pos_] <= '9') ++pos_;
    }
  }
  void skip(unsigned depth = 0) {
    if (depth >= 64) throw std::runtime_error("HF state nesting limit exceeded");
    char c = peek();
    if (c == '"') { string(); return; }
    if (c == '{') {
      ++pos_;
      while (peek() != '}') {
        string(); take(':'); skip(depth + 1);
        if (peek() == ',') {
          ++pos_;
          if (peek() == '}') throw std::runtime_error("trailing comma in HF state object");
          continue;
        }
        break;
      }
      take('}'); return;
    }
    if (c == '[') {
      ++pos_;
      while (peek() != ']') {
        skip(depth + 1);
        if (peek() == ',') {
          ++pos_;
          if (peek() == ']') throw std::runtime_error("trailing comma in HF state array");
          continue;
        }
        break;
      }
      take(']'); return;
    }
    if (c == 'n') { literal("null"); return; }
    if (c == 't') { literal("true"); return; }
    if (c == 'f') { literal("false"); return; }
    if (c == '-' || (c >= '0' && c <= '9')) { number(); return; }
    throw std::runtime_error("invalid HF state JSON value");
  }
  std::vector<std::string> file_array() {
    take('['); std::vector<std::string> result;
    while (peek() != ']') {
      take('{'); std::optional<std::string> name;
      while (peek() != '}') {
        auto key = string(); take(':');
        if (key == "filename") { if (name) throw std::runtime_error("duplicate filename"); name = string(); }
        else skip();
        if (peek() == ',') {
          ++pos_;
          if (peek() == '}') throw std::runtime_error("trailing comma in HF state file");
          continue;
        }
        break;
      }
      take('}');
      if (!name || name->empty()) throw std::runtime_error("HF data file lacks filename");
      result.push_back(*name);
      if (peek() == ',') {
        ++pos_;
        if (peek() == ']') throw std::runtime_error("trailing comma in HF state file array");
        continue;
      }
      break;
    }
    take(']'); return result;
  }
  const std::string& s_;
  std::size_t pos_{0};
};

QueryResult<std::string> compute_snapshot_fingerprint(
    const DatasetSnapshot& snapshot) {
  QueryProfileTimer timer("snapshot_fingerprint");
  using Result = QueryResult<std::string>;
  if (snapshot.dataset_id.empty() || snapshot.storage_namespace == 0 ||
      snapshot.shards.empty() ||
      snapshot.manifests.size() != snapshot.shards.size() ||
      snapshot.rows.empty())
    return Result::failure(invalid("snapshot has incomplete fingerprint inputs"));
  const bool extended_layout = std::any_of(snapshot.rows.begin(), snapshot.rows.end(),
    [](const SnapshotRowBinding& row) { return row.value_layout != SnapshotValueLayout::ChannelMajorLists; });
  // Preserve identities of semantically unchanged channel-major declarations.
  // New physical interpretations use a versioned content domain and hash layout.
  Bytes out; out.str(extended_layout ? "iotdbfs-dataset-snapshot-content/2" :
    "iotdbfs-dataset-snapshot-content/1");
  out.str(snapshot.dataset_id);
  out.u64(snapshot.shards.size());
  for (std::size_t i = 0; i < snapshot.shards.size(); ++i) {
    const auto& shard = snapshot.shards[i];
    const auto& m = snapshot.manifests[i];
    out.str(shard.filename); out.str(shard.key.path);
    out.u64(static_cast<std::uint64_t>(shard.key.version));
    out.str(m.key.path); out.u64(static_cast<std::uint64_t>(m.key.version));
    out.str(m.format_id.value_or("")); out.u64(m.revision);
    out.u64(m.payload_size); out.u64(m.layout_size);
    out.u64(static_cast<std::uint64_t>(m.phase)); out.u64(m.published);
    auto metadata = serialize_metadata(m.metadata, 65536);
    if (!metadata.ok())
      return Result::failure(invalid("snapshot manifest metadata is invalid: " +
        metadata.error()));
    out.str(metadata.value());
  }
  out.u64(snapshot.rows.size());
  for (const auto& row : snapshot.rows) {
    if (row.channel_table && !row.channels.empty())
      return Result::failure(invalid("row channels and channel_table are mutually exclusive"));
    out.u64(row.logical_row); out.u64(row.shard_index);
    out.u64(row.batch_index); out.u64(row.row_index);
    out.str(row.series_id); out.str(row.entity_id);
    out.u64(row.steps.begin); out.u64(row.steps.end);
    out.u64(static_cast<std::uint64_t>(row.time.kind));
    out.u64(static_cast<std::uint64_t>(row.time.start_ns));
    out.u64(static_cast<std::uint64_t>(row.time.cadence_ns));
    out.str(row.time.timezone);
    out.u64(row.time.timestamps_ns.size());
    for (auto t : row.time.timestamps_ns) out.u64(static_cast<std::uint64_t>(t));
    out.str(row.target_field);
    if (extended_layout) out.u64(static_cast<std::uint64_t>(row.value_layout));
    out.u64(row.declared_n_channels.has_value());
    if (row.declared_n_channels) out.u64(*row.declared_n_channels);
    out.u64(row.declared_length.has_value());
    if (row.declared_length) out.u64(*row.declared_length);
    out.u64(row.channel_bindings().size());
    for (const auto& channel : row.channel_bindings()) {
      out.str(channel.field_id); out.u64(channel.original_ordinal);
      out.u64(channel.physical_ordinal.has_value());
      if (channel.physical_ordinal) out.u64(*channel.physical_ordinal);
      out.str(channel.label); out.opt(channel.unit); out.u64(channel.length);
    }
  }
  return Result::success(sha256(out.data()));
}

QueryStatus validate_snapshot_fingerprint(const DatasetSnapshot& snapshot) {
  auto actual = compute_snapshot_fingerprint(snapshot);
  if (!actual.ok()) return actual.status();
  if (snapshot.fingerprint != actual.value())
    return QueryStatus::failure(QueryError::StalePlan,
      QueryPhase::Validation,
      "dataset snapshot content no longer matches its fingerprint");
  auto process_identity = process_snapshot_identity();
  if (!process_identity)
    return QueryStatus::failure(QueryError::Internal, QueryPhase::Validation,
      "cannot generate process snapshot identity");
  if (snapshot.process_identity != *process_identity)
    return QueryStatus::failure(QueryError::StalePlan, QueryPhase::Validation,
      "dataset snapshot belongs to a different process instance");
  return QueryStatus::success();
}

std::string sidecar_binding(const DatasetSnapshot& snapshot) {
  Bytes out; out.str("iotdbfs-arrow-coverage-binding/1");
  out.str(snapshot.fingerprint); out.str(snapshot.process_identity);
  out.u64(snapshot.storage_namespace);
  return sha256(out.data());
}

std::string sidecar_encode(const DatasetCoverageIndex& index,
    const std::string& binding) {
  Bytes out; out.str(kCoverageSidecarProtocol);
  out.str(binding); out.u64(index.spans().size());
  for (const auto& span : index.spans()) {
    out.str(span.series_id); out.str(span.field_id);
    out.u64(span.original_ordinal); out.u64(span.logical_row);
    out.u64(span.shard_index);
    out.u64(span.batch_index); out.u64(span.row_index);
    out.u64(span.physical_ordinal.has_value());
    if (span.physical_ordinal) out.u64(*span.physical_ordinal);
    out.u64(span.steps.begin); out.u64(span.steps.end);
    out.u64(span.source_step_begin);
    out.u64(span.null_series); out.u64(span.null_channel);
    out.u64(span.missing_channel);
  }
  auto payload = out.data();
  return payload + sha256(payload);
}

QueryResult<std::vector<DatasetCoverageSpan>> sidecar_decode(
    const std::string& bytes, const std::string& expected) {
  using Result = QueryResult<std::vector<DatasetCoverageSpan>>;
  if (bytes.size() < 64 || sha256(bytes.substr(0, bytes.size() - 64)) != bytes.substr(bytes.size() - 64))
    return Result::failure(corrupt("coverage sidecar checksum mismatch"));
  try {
    auto payload = bytes.substr(0, bytes.size() - 64); Reader in(payload);
    if (in.str() != kCoverageSidecarProtocol || in.str() != expected)
      return Result::failure(corrupt("coverage sidecar protocol or snapshot differs"));
    auto count = in.u64();
    if (count > 1000000) return Result::failure(corrupt("coverage sidecar span limit exceeded"));
    std::vector<DatasetCoverageSpan> spans; spans.reserve(static_cast<std::size_t>(count));
    for (std::uint64_t i = 0; i < count; ++i) {
      DatasetCoverageSpan s;
      s.series_id = in.str(); s.field_id = in.str();
      s.original_ordinal = in.u64(); s.logical_row = in.u64();
      s.shard_index = in.u64();
      s.batch_index = in.u64(); s.row_index = in.u64();
      if (in.u64()) s.physical_ordinal = in.u64();
      s.steps = {in.u64(), in.u64()}; s.source_step_begin = in.u64();
      s.null_series = in.u64(); s.null_channel = in.u64();
      s.missing_channel = in.u64();
      if (s.steps.begin >= s.steps.end || s.source_step_begin != s.steps.begin ||
          s.series_id.empty() || s.field_id.empty())
        return Result::failure(corrupt("coverage sidecar contains invalid span"));
      spans.push_back(std::move(s));
    }
    if (!in.done()) return Result::failure(corrupt("coverage sidecar has trailing data"));
    return Result::success(std::move(spans));
  } catch (const std::exception& e) { return Result::failure(corrupt(e.what())); }
}

bool valid_cached_spans(const std::vector<DatasetCoverageSpan>& spans,
    const DatasetSnapshot& snapshot) {
  std::size_t expected_spans = 0;
  for (const auto& row : snapshot.rows) {
    if (row.channel_bindings().size() > spans.size() - expected_spans) return false;
    expected_spans += row.channel_bindings().size();
  }
  if (expected_spans != spans.size()) return false;
  using DeclaredChannel = std::tuple<std::uint64_t, std::string, std::uint64_t>;
  std::set<DeclaredChannel> seen;
  for (std::size_t i = 0; i < spans.size(); ++i) {
    const auto& span = spans[i];
    if (span.logical_row >= snapshot.rows.size()) return false;
    const auto& row = snapshot.rows[span.logical_row];
    if (span.series_id != row.series_id || span.shard_index != row.shard_index ||
        span.batch_index != row.batch_index || span.row_index != row.row_index ||
        span.steps.begin != row.steps.begin || span.steps.end != row.steps.end ||
        span.source_step_begin != row.steps.begin) return false;
    auto channel = std::find_if(row.channel_bindings().begin(), row.channel_bindings().end(),
      [&](const SnapshotChannelBinding& c) {
        return c.field_id == span.field_id &&
          c.original_ordinal == span.original_ordinal &&
          c.physical_ordinal == span.physical_ordinal;
      });
    if (channel == row.channel_bindings().end()) return false;
    if (!seen.emplace(span.logical_row, span.field_id,
          span.original_ordinal).second) return false;
    if (i && std::tie(spans[i - 1].series_id, spans[i - 1].field_id,
          spans[i - 1].original_ordinal, spans[i - 1].steps.begin) >
        std::tie(span.series_id, span.field_id, span.original_ordinal,
          span.steps.begin)) return false;
    if (i && spans[i - 1].series_id == span.series_id &&
        spans[i - 1].field_id == span.field_id &&
        spans[i - 1].original_ordinal == span.original_ordinal &&
        spans[i - 1].steps.end > span.steps.begin) return false;
  }
  for (const auto& row : snapshot.rows)
    for (const auto& channel : row.channel_bindings())
      if (!seen.count({row.logical_row, channel.field_id,
            channel.original_ordinal})) return false;
  return true;
}

QueryStatus validate_segment_channel_declarations(
    const std::vector<SnapshotRowBinding>& rows) {
  QueryProfileTimer timer("snapshot_channel_declarations");
  using ChannelKey = std::pair<std::string, std::uint64_t>;
  using SegmentKey = std::pair<std::uint64_t, std::uint64_t>;
  std::map<std::string, std::set<ChannelKey>> series_channels;
  std::map<std::string, std::map<SegmentKey, std::set<ChannelKey>>>
    segment_channels;
  for (const auto& row : rows) {
    const SegmentKey segment{row.steps.begin, row.steps.end};
    auto& declared = segment_channels[row.series_id][segment];
    for (const auto& channel : row.channel_bindings()) {
      const ChannelKey key{channel.field_id, channel.original_ordinal};
      series_channels[row.series_id].insert(key);
      declared.insert(key);
    }
  }
  for (const auto& series : segment_channels) {
    const auto& expected = series_channels.at(series.first);
    for (const auto& segment : series.second) {
      if (segment.second == expected) continue;
      return invalid("series step segment omits a channel declaration; "
        "declare an absent channel without physical_ordinal");
    }
  }
  return QueryStatus::success();
}

QueryStatus publish_sidecar(const std::string& path, const std::string& bytes) {
  if (bytes.size() > 256ULL * 1024 * 1024)
    return QueryStatus::failure(QueryError::ResourceExhausted,
      QueryPhase::Planning, "coverage sidecar exceeds size limit");
  // Rename is atomic on the same filesystem. mkstemp makes concurrent
  // preparers independent, including processes using the same destination.
  std::string pattern = path + ".tmp.XXXXXX";
  std::vector<char> name(pattern.begin(), pattern.end()); name.push_back('\0');
  int fd = ::mkstemp(name.data());
  if (fd < 0) return read_error("cannot create coverage sidecar temporary file");
  std::size_t written = 0;
  while (written < bytes.size()) {
    auto n = ::write(fd, bytes.data() + written, bytes.size() - written);
    if (n <= 0) { ::close(fd); std::remove(name.data()); return read_error("cannot write coverage sidecar"); }
    written += static_cast<std::size_t>(n);
  }
  if (::fchmod(fd, 0644) != 0) {
    ::close(fd); std::remove(name.data());
    return read_error("cannot set coverage sidecar read permissions");
  }
  int sync_result = ::fsync(fd);
  int close_result = ::close(fd);
  if (sync_result != 0 || close_result != 0) {
    std::remove(name.data()); return read_error("cannot sync coverage sidecar");
  }
  if (std::rename(name.data(), path.c_str()) != 0) {
    std::remove(name.data()); return read_error("cannot atomically publish coverage sidecar");
  }
  return QueryStatus::success();
}

bool valid_bit(const BinaryValue& bitmap, std::uint64_t index) {
  return bitmap.empty() || (bitmap[index / 8] & (1u << (index % 8))) != 0;
}

} // namespace

QueryResult<DatasetSnapshot> prepare_dataset_snapshot(
    const DatasetSnapshotRequest& request,
    const PublishedVersionCatalog& catalog,
    std::shared_ptr<const StructuredBlobStorage> storage,
    std::size_t max_sources, const std::optional<QueryPrincipal>& principal) try {
  using Result = QueryResult<DatasetSnapshot>;
  if (!storage || request.dataset_id.empty() || request.shards.empty() ||
      request.shards.size() > max_sources || request.rows.empty() ||
      request.rows.size() > 1000000)
    return Result::failure(invalid("snapshot requires storage, dataset, shards and logical rows"));
  if (request.hf_state_json) {
    if (request.hf_state_json->size() > 8ULL * 1024 * 1024)
      return Result::failure(invalid("HF state.json exceeds size limit"));
    try {
      bool indices = false;
      auto names = StateReader(*request.hf_state_json).files(indices);
      if (names.size() != request.shards.size())
        return Result::failure(invalid("HF shard count differs from snapshot"));
      for (std::size_t i = 0; i < names.size(); ++i)
        if (names[i] != request.shards[i].filename)
          return Result::failure(invalid("HF state shard order differs from snapshot"));
      if (indices)
        return Result::failure(invalid("HF _indices_data_files require a verified logical-to-physical mapping, which is not supported"));
    } catch (const std::exception& e) {
      return Result::failure(invalid(std::string("invalid HF state.json: ") + e.what()));
    }
  }
  QueryMetadataTrace scan_fees;
  auto selected = [&] {
    ScopedQueryMetadataTrace trace("");
    QueryProfileTimer timer("snapshot_catalog_scan");
    auto value = catalog.scan(request.selector, max_sources);
    scan_fees = trace.release();
    return value;
  }();
  if (!selected.ok()) return Result::failure(selected.status());
  std::unique_ptr<ScopedQueryMetadataBudget> metadata_scope;
  std::shared_ptr<QueryExecutionLease> execution_lease;
  std::unique_ptr<ScopedQueryExecutionLease> execution_scope;
  if (selected.value().read_context) {
    metadata_scope = std::make_unique<ScopedQueryMetadataBudget>();
    replay_query_metadata_trace(scan_fees);
    if (!principal) return Result::failure(QueryStatus::failure(
      QueryError::PermissionDenied, QueryPhase::Catalog, "owner snapshot requires a caller principal"));
    auto admitted = selected.value().read_context->admit(*principal);
    if (!admitted.ok()) return Result::failure(admitted.status());
    execution_lease = admitted.value()->execution_lease();
    if (execution_lease) execution_scope = std::make_unique<ScopedQueryExecutionLease>(execution_lease);
  }
  if (selected.value().residual_file_predicate)
    return Result::failure(invalid("snapshot selector has an unevaluated file predicate"));
  if (selected.value().versions().size() != request.shards.size())
    return Result::failure(invalid("snapshot shards must equal the selected version set"));
  std::map<StructuredBlobVersionKey, StructuredBlobManifest> catalog_map;
  for (const auto& m : selected.value().versions()) catalog_map.emplace(m.key, m);
  std::set<StructuredBlobVersionKey> keys;
  std::set<std::string> filenames;
  DatasetSnapshot snapshot; snapshot.dataset_id = request.dataset_id;
  snapshot.read_context = selected.value().read_context;
  snapshot.shards = request.shards; snapshot.rows = request.rows;
  for (const auto& shard : request.shards) {
    if (shard.filename.empty() || !filenames.insert(shard.filename).second ||
        shard.key.path.empty() || shard.key.version < 0 ||
        !keys.insert(shard.key).second || !catalog_map.count(shard.key))
      return Result::failure(invalid("snapshot shard has no unique selected exact version"));
    auto opened = [&] {
      QueryProfileTimer timer("snapshot_source_open");
      if (snapshot.read_context) {
        auto frozen = snapshot.read_context->manifest(shard.key);
        if (!frozen.ok()) return QueryResult<std::shared_ptr<const QuerySource>>::failure(frozen.status());
        return open_catalog_query_source(storage, frozen.value(), storage->storage_namespace());
      }
      return open_structured_blob_query_source(storage, shard.key);
    }();
    if (!opened.ok()) return Result::failure(opened.status());
    if (snapshot.storage_namespace == 0)
      snapshot.storage_namespace = opened.value()->storage_namespace();
    else if (snapshot.storage_namespace != opened.value()->storage_namespace())
      return Result::failure(invalid("snapshot shards belong to different storage namespaces"));
    const auto& m = opened.value()->manifest();
    if (m.format_id != "arrow" || m.revision != catalog_map.at(shard.key).revision ||
        m.payload_size != catalog_map.at(shard.key).payload_size ||
        m.layout_size != catalog_map.at(shard.key).layout_size)
      return Result::failure(corrupt("snapshot catalog and source manifest differ"));
    snapshot.manifests.push_back(m);
  }
  std::set<std::uint64_t> logical;
  for (const auto& row : request.rows) {
    if (!logical.insert(row.logical_row).second || row.shard_index >= request.shards.size() ||
        row.series_id.empty() || row.target_field.empty() || row.steps.begin >= row.steps.end ||
        row.channel_bindings().empty() ||
        (row.value_layout != SnapshotValueLayout::ChannelMajorLists &&
         row.value_layout != SnapshotValueLayout::StepMajorFlat))
      return Result::failure(invalid("invalid or duplicate snapshot logical row"));
    auto time_status = validate_row_time(row);
    if (!time_status.ok()) return Result::failure(time_status);
  }
  // Caller order is the logical dataset order, including any explicit indices
  // mapping. Numeric logical IDs must be contiguous to detect omitted rows.
  for (std::size_t i = 0; i < request.rows.size(); ++i)
    if (request.rows[i].logical_row != i)
      return Result::failure(invalid("snapshot rows must be in contiguous logical order"));
  auto process_identity = process_snapshot_identity();
  if (!process_identity)
    return Result::failure(QueryStatus::failure(QueryError::Internal,
      QueryPhase::Validation, "cannot generate process snapshot identity"));
  snapshot.process_identity = *process_identity;
  auto computed = compute_snapshot_fingerprint(snapshot);
  if (!computed.ok()) return Result::failure(computed.status());
  snapshot.fingerprint = std::move(computed.value());
  snapshot = deep_copy_dataset_snapshot(snapshot);
  if (metadata_scope && !metadata_scope->status(QueryPhase::Encoding).ok())
    return Result::failure(metadata_scope->status(QueryPhase::Encoding));
  return Result::success(std::move(snapshot));
} catch (const QueryMetadataBudgetExceeded& error) {
  return QueryResult<DatasetSnapshot>::failure(QueryStatus::failure(
    QueryError::ResourceExhausted, QueryPhase::Catalog, error.what()));
}

QueryStatus validate_dataset_snapshot_runtime(const DatasetSnapshot& snapshot,
    std::shared_ptr<const StructuredBlobStorage> storage) {
  if (!storage) return invalid("snapshot storage is required");
  auto identity = validate_snapshot_fingerprint(snapshot);
  if (!identity.ok()) return identity;
  if (storage->storage_namespace() != snapshot.storage_namespace)
    return QueryStatus::failure(QueryError::StalePlan, QueryPhase::Validation,
      "dataset snapshot belongs to a different storage namespace");
  return QueryStatus::success();
}

QueryStatus verify_dataset_snapshot_sources(const DatasetSnapshot& snapshot,
    std::shared_ptr<const StructuredBlobStorage> storage) {
  // Parent stage includes snapshot_fingerprint and all exact-version opens.
  QueryProfileTimer timer("snapshot_verify_sources");
  auto identity = validate_dataset_snapshot_runtime(snapshot, storage);
  if (!identity.ok()) return identity;
  for (std::size_t i = 0; i < snapshot.shards.size(); ++i) {
    auto opened = snapshot.read_context
      ? open_catalog_query_source(storage, snapshot.manifests[i], snapshot.storage_namespace)
      : open_structured_blob_query_source(storage, snapshot.shards[i].key);
    if (!opened.ok()) return opened.status();
    if (opened.value()->storage_namespace() != snapshot.storage_namespace)
      return QueryStatus::failure(QueryError::StalePlan, QueryPhase::Validation,
        "dataset snapshot belongs to a different storage namespace");
    const auto& m = opened.value()->manifest();
    if (m.revision != snapshot.manifests[i].revision ||
        m.payload_size != snapshot.manifests[i].payload_size ||
        m.layout_size != snapshot.manifests[i].layout_size)
      return corrupt("snapshot exact version manifest changed");
  }
  return QueryStatus::success();
}

DatasetSnapshot deep_copy_dataset_snapshot(const DatasetSnapshot& snapshot) {
  DatasetSnapshot result = snapshot;
  std::map<const void*, std::shared_ptr<const std::vector<SnapshotChannelBinding>>> copies;
  for (auto& row : result.rows) if (row.channel_table) {
    auto found = copies.find(row.channel_table.get());
    if (found == copies.end())
      found = copies.emplace(row.channel_table.get(),
        std::make_shared<const std::vector<SnapshotChannelBinding>>(*row.channel_table)).first;
    row.channel_table = found->second;
  }
  return result;
}

std::uint64_t snapshot_channel_metadata_bytes(const DatasetSnapshot& snapshot) noexcept {
  std::uint64_t bytes = 0;
  const auto maximum = std::numeric_limits<std::uint64_t>::max();
  auto add = [&](std::uint64_t n) { bytes = n > maximum - bytes ? maximum : bytes + n; };
  auto charge = [&](const std::vector<SnapshotChannelBinding>& channels) {
    add(channels.capacity() > maximum / sizeof(SnapshotChannelBinding) ? maximum :
      channels.capacity() * sizeof(SnapshotChannelBinding));
    if (channels.capacity()) add(64);
    for (const auto& c : channels) {
      add(c.field_id.capacity()); add(65); add(c.label.capacity()); add(65);
      if (c.unit) { add(c.unit->capacity()); add(65); }
    }
  };
  for (std::size_t i = 0; i < snapshot.rows.size(); ++i) {
    const auto& row = snapshot.rows[i];
    // Even a cleared owned vector can retain its old allocation.
    charge(row.channels);
    if (!row.channel_table) continue;
    bool first = true;
    for (std::size_t j = 0; j < i; ++j)
      if (snapshot.rows[j].channel_table == row.channel_table) { first = false; break; }
    if (first) { add(128 + sizeof(*row.channel_table)); charge(*row.channel_table); }
  }
  return bytes;
}

DatasetCoverageIndex::DatasetCoverageIndex(const DatasetCoverageIndex& other) {
  // Diagnostic expansion is a const operation, so preparing another module
  // from the same index must synchronize with it as well.
  std::lock_guard<std::mutex> lock(*other.spans_mutex_);
  fingerprint_ = other.fingerprint_;
  process_identity_ = other.process_identity_;
  storage_namespace_ = other.storage_namespace_;
  spans_ = other.spans_;
  compact_rows_ = other.compact_rows_;
  ranges_ = other.ranges_;
  compact_ranges_ = other.compact_ranges_;
}

DatasetCoverageIndex& DatasetCoverageIndex::operator=(const DatasetCoverageIndex& other) {
  if (this == &other) return *this;
  // Copy first for the strong exception guarantee, then replace the fields
  // while holding this instance's distinct diagnostic mutex.
  DatasetCoverageIndex copy(other);
  if (!spans_mutex_) spans_mutex_ = std::make_shared<std::mutex>();
  std::lock_guard<std::mutex> lock(*spans_mutex_);
  fingerprint_.swap(copy.fingerprint_);
  process_identity_.swap(copy.process_identity_);
  std::swap(storage_namespace_, copy.storage_namespace_);
  spans_.swap(copy.spans_);
  compact_rows_.swap(copy.compact_rows_);
  ranges_.swap(copy.ranges_);
  compact_ranges_.swap(copy.compact_ranges_);
  return *this;
}

DatasetCoverageSpan DatasetCoverageIndex::expand(CompactRef ref) const {
  const auto& row = compact_rows_[ref.row];
  const auto& channel = (*row.channels)[ref.channel];
  auto span = row.common;
  span.field_id = channel.field_id;
  span.original_ordinal = channel.original_ordinal;
  span.physical_ordinal = channel.physical_ordinal;
  span.null_channel = row.null_channels[ref.channel];
  span.missing_channel = !channel.physical_ordinal;
  return span;
}

const std::vector<DatasetCoverageSpan>& DatasetCoverageIndex::spans() const {
  std::lock_guard<std::mutex> lock(*spans_mutex_);
  if (spans_.empty() && !compact_rows_.empty()) {
    std::vector<DatasetCoverageSpan> expanded;
    for (const auto& group : compact_ranges_)
      for (auto ref : group.second) expanded.push_back(expand(ref));
    spans_ = std::move(expanded);
  }
  return spans_;
}

void DatasetCoverageIndex::compact(const DatasetSnapshot& snapshot) {
  if (std::none_of(snapshot.rows.begin(), snapshot.rows.end(),
      [](const SnapshotRowBinding& row) { return bool(row.channel_table); })) return;
  // Retain physical/time coordinates once per row and compact integer references
  // per channel. Strings and channel declarations are shared, never per span.
  compact_rows_.resize(snapshot.rows.size());
  std::map<const void*, std::shared_ptr<const std::vector<SnapshotChannelBinding>>> copies;
  std::vector<std::map<std::pair<std::string, std::uint64_t>, std::uint32_t>> positions(snapshot.rows.size());
  for (std::size_t i = 0; i < snapshot.rows.size(); ++i) {
    const auto& source = snapshot.rows[i];
    auto& row = compact_rows_[i];
    const void* identity = &source.channel_bindings();
    auto found = copies.find(identity);
    if (found == copies.end()) found = copies.emplace(identity,
      std::make_shared<const std::vector<SnapshotChannelBinding>>(source.channel_bindings())).first;
    row.channels = found->second;
    row.null_channels.resize(row.channels->size());
    for (std::size_t c = 0; c < row.channels->size(); ++c)
      positions[i].emplace(std::make_pair((*row.channels)[c].field_id,
        (*row.channels)[c].original_ordinal), c);
  }
  for (const auto& span : spans_) {
    auto& row = compact_rows_[span.logical_row];
    row.common = span;
    const auto c = positions[span.logical_row].at({span.field_id, span.original_ordinal});
    row.null_channels[c] = span.null_channel;
    compact_ranges_[{span.series_id, span.field_id, span.original_ordinal}].push_back(
      {static_cast<std::uint32_t>(span.logical_row), c});
  }
  for (auto& row : compact_rows_) {
    row.common.field_id.clear(); row.common.field_id.shrink_to_fit();
    row.common.physical_ordinal.reset();
  }
  std::vector<DatasetCoverageSpan>().swap(spans_);
  ranges_.clear();
}

QueryResult<std::vector<DatasetCoverageSpan>> DatasetCoverageIndex::lookup(
    const std::string& series_id, const std::string& field_id,
    std::uint64_t original_ordinal, StepInterval requested) const {
  using Result = QueryResult<std::vector<DatasetCoverageSpan>>;
  if (series_id.empty() || field_id.empty() || requested.begin > requested.end)
    return Result::failure(invalid("invalid coverage lookup"));
  std::vector<DatasetCoverageSpan> found;
  if (requested.begin == requested.end) return Result::success(std::move(found));
  auto overlaps = lookup_overlaps(series_id, field_id, original_ordinal, requested);
  if (!overlaps.ok()) return overlaps;
  auto cursor = requested.begin;
  for (auto& span : overlaps.value()) {
    if (span.steps.begin != cursor)
      return Result::failure(QueryStatus::failure(QueryError::InvalidDomainSlice,
        QueryPhase::Planning, "requested window crosses an uncovered step gap"));
    cursor = span.steps.end;
  }
  if (cursor != requested.end)
    return Result::failure(QueryStatus::failure(QueryError::InvalidDomainSlice,
      QueryPhase::Planning, "requested window exceeds declared coverage"));
  return overlaps;
}

QueryResult<std::vector<DatasetCoverageSpan>> DatasetCoverageIndex::lookup_overlaps(
    const std::string& series_id, const std::string& field_id,
    std::uint64_t original_ordinal, StepInterval requested) const {
  using Result = QueryResult<std::vector<DatasetCoverageSpan>>;
  if (series_id.empty() || field_id.empty() || requested.begin > requested.end)
    return Result::failure(invalid("invalid coverage lookup"));
  std::vector<DatasetCoverageSpan> found;
  if (requested.begin == requested.end) return Result::success(std::move(found));
  if (!compact_rows_.empty()) {
    auto group = compact_ranges_.find({series_id, field_id, original_ordinal});
    if (group == compact_ranges_.end()) return Result::success(std::move(found));
    const auto& refs = group->second;
    auto hit = std::lower_bound(refs.begin(), refs.end(), requested.begin,
      [&](CompactRef ref, std::uint64_t step) { return compact_rows_[ref.row].common.steps.end <= step; });
    for (; hit != refs.end() && compact_rows_[hit->row].common.steps.begin < requested.end; ++hit) {
      auto span = expand(*hit);
      span.steps = {std::max(span.steps.begin, requested.begin), std::min(span.steps.end, requested.end)};
      found.push_back(std::move(span));
    }
    return Result::success(std::move(found));
  }
  auto group = ranges_.find({series_id, field_id, original_ordinal});
  if (group == ranges_.end()) return Result::success(std::move(found));
  const auto first = spans_.begin() + group->second.first;
  const auto last = spans_.begin() + group->second.second;
  auto hit = std::lower_bound(first, last, requested.begin,
    [](const DatasetCoverageSpan& span, std::uint64_t step) {
      return span.steps.end <= step;
    });
  for (; hit != last && hit->steps.begin < requested.end; ++hit) {
    auto clipped = *hit;
    clipped.steps = {std::max(hit->steps.begin, requested.begin),
      std::min(hit->steps.end, requested.end)};
    found.push_back(std::move(clipped));
  }
  return Result::success(std::move(found));
}

std::uint64_t DatasetCoverageIndex::estimated_metadata_bytes() const noexcept {
  std::lock_guard<std::mutex> lock(*spans_mutex_);
  constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
  // Fixed allowance for allocator bookkeeping/alignment, and four pointers
  // for tree links/color. Strings are charged even when stored inline.
  constexpr std::uint64_t allocation = 64;
  constexpr std::uint64_t tree_node = allocation + 4 * sizeof(void*);
  std::uint64_t bytes = sizeof(*this);
  auto add = [&](std::uint64_t amount) {
    bytes = amount > maximum - bytes ? maximum : bytes + amount;
  };
  auto array = [&](std::uint64_t count, std::uint64_t width) {
    add(count && width > maximum / count ? maximum : count * width);
  };
  auto string = [&](const std::string& value) {
    add(value.capacity()); add(1); add(allocation);
  };
  add(sizeof(std::mutex) + 128);
  string(fingerprint_); string(process_identity_);
  array(spans_.capacity(), sizeof(DatasetCoverageSpan));
  if (spans_.capacity()) add(allocation);
  for (const auto& span : spans_) {
    string(span.series_id); string(span.field_id);
  }
  array(ranges_.size(), sizeof(decltype(ranges_)::value_type) + tree_node);
  for (const auto& range : ranges_) {
    string(std::get<0>(range.first)); string(std::get<1>(range.first));
  }
  array(compact_rows_.capacity(), sizeof(CompactRow));
  // Count each shared channel table once without allocating in this noexcept method.
  for (std::size_t i = 0; i < compact_rows_.size(); ++i) {
    const auto& row = compact_rows_[i];
    string(row.common.series_id); string(row.common.field_id);
    array(row.null_channels.capacity(), sizeof(unsigned char)); add(allocation);
    bool first = true;
    for (std::size_t j = 0; j < i; ++j)
      if (compact_rows_[j].channels == row.channels) { first = false; break; }
    if (first) {
      array(row.channels->capacity(), sizeof(SnapshotChannelBinding)); add(128);
      for (const auto& c : *row.channels) {
        string(c.field_id); string(c.label); if (c.unit) string(*c.unit);
      }
    }
  }
  array(compact_ranges_.size(), sizeof(decltype(compact_ranges_)::value_type) + tree_node);
  for (const auto& group : compact_ranges_) {
    string(std::get<0>(group.first)); string(std::get<1>(group.first));
    array(group.second.capacity(), sizeof(CompactRef)); add(allocation);
  }
  return bytes;
}

void DatasetCoverageIndex::rebuild_ranges() {
  QueryProfileTimer timer("coverage_ranges_build");
  ranges_.clear();
  for (std::size_t i = 0; i < spans_.size(); ++i) {
    const auto& span = spans_[i];
    LookupKey key{span.series_id, span.field_id, span.original_ordinal};
    auto found = ranges_.emplace(std::move(key), std::make_pair(i, i + 1));
    if (!found.second) found.first->second.second = i + 1;
  }
}

QueryResult<DatasetCoverageIndex> prepare_dataset_coverage_index(
    const DatasetSnapshot& snapshot,
    std::shared_ptr<const StructuredBlobStorage> storage,
    const std::optional<std::string>& sidecar_path) {
  using Result = QueryResult<DatasetCoverageIndex>;
  auto valid = verify_dataset_snapshot_sources(snapshot, storage);
  if (!valid.ok()) return Result::failure(valid);
  for (std::size_t i = 0; i < snapshot.rows.size(); ++i)
    if (snapshot.rows[i].logical_row != i ||
        snapshot.rows[i].shard_index >= snapshot.shards.size())
      return Result::failure(invalid("snapshot logical row mapping is invalid"));
  auto channel_declarations =
    validate_segment_channel_declarations(snapshot.rows);
  if (!channel_declarations.ok()) return Result::failure(channel_declarations);
  DatasetCoverageIndex index;
  index.fingerprint_ = snapshot.fingerprint;
  index.process_identity_ = snapshot.process_identity;
  index.storage_namespace_ = snapshot.storage_namespace;
  const auto binding = sidecar_binding(snapshot);
  if (sidecar_path) {
    std::ifstream stream(*sidecar_path, std::ios::binary);
    if (stream) {
      stream.seekg(0, std::ios::end);
      auto size = stream.tellg();
      if (size >= 0 && size <= 256LL * 1024 * 1024) {
        stream.seekg(0, std::ios::beg);
        std::string bytes((std::istreambuf_iterator<char>(stream)), std::istreambuf_iterator<char>());
        auto parsed = sidecar_decode(bytes, binding);
        if (parsed.ok() && valid_cached_spans(parsed.value(), snapshot)) {
          index.spans_ = std::move(parsed.value());
          index.rebuild_ranges();
          index.compact(snapshot);
          return Result::success(std::move(index));
        }
      }
    }
  }
  {
  // Inclusive parent stage: layout open/read/parse are nested children. One
  // timer covers all spans, avoiding diagnostic work for every channel.
  QueryProfileTimer timer("coverage_span_build");
  std::vector<std::shared_ptr<const ArrowLayoutDocument>> layouts(snapshot.shards.size());
  std::map<std::string, std::map<std::pair<std::string, std::uint64_t>,
      std::pair<std::string, std::optional<std::string>>>> identities;
  std::map<std::string, std::string> entities;
  std::map<std::string, std::vector<const SnapshotRowBinding*>> by_series;
  for (const auto& row : snapshot.rows) {
    if (row.channel_bindings().size() > 4096 ||
        index.spans_.size() > 1000000 - row.channel_bindings().size())
      return Result::failure(QueryStatus::failure(QueryError::ResourceExhausted,
        QueryPhase::Planning, "coverage index exceeds channel/span limit"));
    auto entity = entities.emplace(row.series_id, row.entity_id);
    if (!entity.second && entity.first->second != row.entity_id)
      return Result::failure(invalid("same series has inconsistent entity identity"));
    const auto& m = snapshot.manifests.at(row.shard_index);
    if (!layouts[row.shard_index]) {
      auto source = [&] {
        QueryProfileTimer timer("coverage_source_open");
        return snapshot.read_context
          ? open_catalog_query_source(storage, m, snapshot.storage_namespace)
          : open_structured_blob_query_source(storage, m.key);
      }();
      if (!source.ok()) return Result::failure(source.status());
      if (m.layout_size > ArrowLayoutLimits{}.max_layout_bytes)
        return Result::failure(corrupt("snapshot Arrow layout exceeds limit"));
      auto bytes = [&] {
        QueryProfileTimer timer("coverage_layout_read");
        return source.value()->read_layout(0, m.layout_size);
      }();
      if (!bytes.ok()) return Result::failure(
        bytes.status().code() == QueryError::StalePlan ? missing(m.key) : bytes.status());
      if (bytes.value().size() != m.layout_size)
        return Result::failure(read_error("short Arrow layout read"));
      auto parsed = [&] {
        QueryProfileTimer timer("coverage_layout_parse");
        return parse_arrow_layout(bytes.value(), m.payload_size);
      }();
      if (!parsed.ok()) return Result::failure(parsed.status());
      layouts[row.shard_index] = std::make_shared<const ArrowLayoutDocument>(std::move(parsed.value()));
    }
    const auto& layout = *layouts[row.shard_index];
    if (row.batch_index >= layout.batches.size() ||
        row.row_index >= layout.batches[row.batch_index].row_count)
      return Result::failure(invalid("snapshot physical batch/row is out of range"));
    auto field = std::find_if(layout.fields.begin(), layout.fields.end(),
      [&](const ArrowNativeField& f) { return f.name == row.target_field; });
    if (field == layout.fields.end() || field->unsupported_reason.size() ||
        (field->list_depth != 1 && field->list_depth != 2))
      return Result::failure(invalid("snapshot target is not supported List Float32"));
    const auto field_index = static_cast<std::uint32_t>(field - layout.fields.begin());
    const auto& arrays = layout.batches[row.batch_index].arrays;
    auto array = std::find_if(arrays.begin(), arrays.end(),
      [&](const ArrowArrayIndex& a) { return a.field_index == field_index; });
    if (array == arrays.end()) return Result::failure(corrupt("target has no Arrow array index"));
    const auto& outer = array->levels[0];
    const bool null_row = !valid_bit(outer.validity, row.row_index);
    const auto outer_begin = outer.offsets[row.row_index];
    const auto outer_end = outer.offsets[row.row_index + 1];
    const bool step_major = row.value_layout == SnapshotValueLayout::StepMajorFlat;
    if (step_major && (field->list_depth != 1 || !row.declared_n_channels ||
        !*row.declared_n_channels || !row.declared_length ||
        *row.declared_length != row.steps.end - row.steps.begin ||
        *row.declared_length > std::numeric_limits<std::uint64_t>::max() / *row.declared_n_channels))
      return Result::failure(invalid("step-major binding requires flat List and valid declared dimensions"));
    const auto count = step_major ? *row.declared_n_channels :
      (field->list_depth == 1 ? 1 : outer_end - outer_begin);
    if (step_major && !null_row && outer_end - outer_begin != *row.declared_length * count)
      return Result::failure(invalid("step-major flat length differs from declared dimensions"));
    if (!null_row && row.declared_n_channels && *row.declared_n_channels != count)
      return Result::failure(invalid("declared n_channels differs from Arrow O offsets"));
    std::set<std::uint64_t> originals, physicals;
    for (const auto& channel : row.channel_bindings()) {
      if ((step_major && channel.length != *row.declared_length) ||
          channel.field_id != row.target_field || channel.label.empty() ||
          !originals.insert(channel.original_ordinal).second ||
          (channel.physical_ordinal && !physicals.insert(*channel.physical_ordinal).second))
        return Result::failure(invalid("invalid or duplicate channel mapping"));
      auto key = std::make_pair(channel.field_id, channel.original_ordinal);
      auto id = std::make_pair(channel.label, channel.unit);
      auto& known = identities[row.series_id];
      if (known.count(key) && known[key] != id)
        return Result::failure(invalid("channel label or unit changed within a series"));
      known[key] = id;
      DatasetCoverageSpan span;
      span.series_id = row.series_id; span.field_id = channel.field_id;
      span.original_ordinal = channel.original_ordinal;
      span.logical_row = row.logical_row;
      span.shard_index = row.shard_index; span.batch_index = row.batch_index;
      span.row_index = row.row_index; span.physical_ordinal = channel.physical_ordinal;
      span.steps = row.steps; span.source_step_begin = row.steps.begin;
      span.null_series = null_row;
      span.missing_channel = !channel.physical_ordinal;
      if (!null_row && channel.physical_ordinal) {
        if (*channel.physical_ordinal >= count)
          return Result::failure(invalid("physical channel ordinal exceeds Arrow O offsets"));
        std::uint64_t begin, end;
        if (step_major) {
          begin = 0; end = *row.declared_length;
        } else if (field->list_depth == 1) {
          if (*channel.physical_ordinal != 0)
            return Result::failure(invalid("single-level List has only channel zero"));
          begin = outer_begin; end = outer_end;
        } else {
          const auto child = outer_begin + *channel.physical_ordinal;
          const auto& inner = array->levels[1];
          begin = inner.offsets[child]; end = inner.offsets[child + 1];
          span.null_channel = !valid_bit(inner.validity, child);
        }
        if (!span.null_channel &&
            (end - begin != channel.length || end - begin != row.steps.end - row.steps.begin))
          return Result::failure(invalid("channel length differs from Arrow I offsets or step coverage"));
        if (row.declared_length && !span.null_channel && *row.declared_length != end - begin)
          return Result::failure(invalid("declared length differs from Arrow offsets"));
      }
      index.spans_.push_back(std::move(span));
    }
    by_series[row.series_id].push_back(&row);
  }
  for (auto& entry : by_series) {
    auto& rows = entry.second;
    std::sort(rows.begin(), rows.end(), [](auto a, auto b) { return a->steps.begin < b->steps.begin; });
    for (std::size_t i = 1; i < rows.size(); ++i) {
      const auto& a = *rows[i - 1]; const auto& b = *rows[i];
      if (a.steps.end > b.steps.begin) {
        if (a.steps.begin != b.steps.begin || a.steps.end != b.steps.end ||
            a.time.kind != b.time.kind || a.time.start_ns != b.time.start_ns ||
            a.time.cadence_ns != b.time.cadence_ns ||
            a.time.timezone != b.time.timezone ||
            a.time.timestamps_ns != b.time.timestamps_ns)
          return Result::failure(invalid("same-series segments partially overlap or disagree on time"));
        continue; // Explicit variable partitions may cover identical steps.
      }
      if (a.steps.end != b.steps.begin) continue; // Explicit gap, never implicit continuation.
      if (a.time.kind != b.time.kind)
        return Result::failure(invalid("adjacent same-series segments mix time modes"));
      if (a.time.kind == TrainingTimeKind::RegularNanoseconds) {
        if (a.time.cadence_ns <= 0 || a.time.cadence_ns != b.time.cadence_ns ||
            a.time.timezone.empty() || a.time.timezone != b.time.timezone ||
            static_cast<__int128>(a.time.start_ns) +
              static_cast<__int128>(a.steps.end - a.steps.begin) * a.time.cadence_ns != b.time.start_ns)
          return Result::failure(invalid("adjacent segments are not time-contiguous"));
      } else if (a.time.kind != TrainingTimeKind::Step) {
        return Result::failure(invalid("cross-file irregular time requires a declared calendar rule"));
      }
    }
  }
  std::sort(index.spans_.begin(), index.spans_.end(), [](const auto& a, const auto& b) {
    return std::tie(a.series_id, a.field_id, a.original_ordinal, a.steps.begin,
             a.shard_index, a.batch_index, a.row_index) <
           std::tie(b.series_id, b.field_id, b.original_ordinal, b.steps.begin,
             b.shard_index, b.batch_index, b.row_index);
  });
  for (std::size_t i = 1; i < index.spans_.size(); ++i) {
    const auto& a = index.spans_[i - 1]; const auto& b = index.spans_[i];
    if (a.series_id == b.series_id && a.field_id == b.field_id &&
        a.original_ordinal == b.original_ordinal && a.steps.end > b.steps.begin)
      return Result::failure(invalid("same series/channel has overlapping coverage"));
  }
  }
  if (sidecar_path) {
    auto status = publish_sidecar(*sidecar_path, sidecar_encode(index, binding));
    (void)status; // The sidecar is a cache; the prepared in-memory index is authoritative.
  }
  index.rebuild_ranges();
  index.compact(snapshot);
  return Result::success(std::move(index));
}

QueryStatus validate_snapshot_view(const DatasetSnapshot& snapshot,
    const DatasetCoverageIndex& index, const TrainingViewSpec& view) {
  // Includes fingerprint and channel-declaration validation children.
  QueryProfileTimer timer("snapshot_view_validate");
  auto identity = validate_snapshot_fingerprint(snapshot);
  if (!identity.ok()) return identity;
  if (index.process_identity() != snapshot.process_identity ||
      index.storage_namespace() != snapshot.storage_namespace)
    return QueryStatus::failure(QueryError::StalePlan, QueryPhase::Validation,
      "coverage index belongs to a different runtime storage instance");
  if (snapshot.fingerprint.empty() || index.snapshot_fingerprint() != snapshot.fingerprint ||
      view.snapshot_fingerprint != snapshot.fingerprint || view.view_id.empty() ||
      view.input_channels.empty() || view.input_length == 0 || view.stride == 0)
    return invalid("training view does not bind this prepared snapshot/index");
  auto channel_declarations =
    validate_segment_channel_declarations(snapshot.rows);
  if (!channel_declarations.ok()) return channel_declarations;
  std::map<std::string,
    std::map<std::pair<std::string, std::uint64_t>, std::string>> series;
  for (const auto& row : snapshot.rows) {
    auto& channels = series[row.series_id];
    for (const auto& channel : row.channel_bindings())
      channels.emplace(std::make_pair(channel.field_id,
        channel.original_ordinal), channel.label);
  }
  auto check = [&](const std::vector<TrainingChannelRef>& requested) -> QueryStatus {
    std::set<std::pair<std::string, std::uint64_t>> seen;
    for (const auto& ref : requested) {
      if (ref.field_id.empty() || ref.label.empty() ||
          !seen.emplace(ref.field_id, ref.ordinal).second)
        return invalid("view has an invalid or duplicate channel identity");
      for (const auto& entry : series) {
        auto found = entry.second.find({ref.field_id, ref.ordinal});
        if (found != entry.second.end() && found->second != ref.label)
          return invalid("view channel label differs from snapshot binding");
        if (found == entry.second.end() &&
            view.missing.missing_channel == MissingTreatment::RejectWindow)
          return invalid("view selects a channel absent from a series");
      }
    }
    return QueryStatus::success();
  };
  auto status = check(view.input_channels);
  if (!status.ok()) return status;
  return check(view.target_channels);
}

} // namespace iotdbfs
