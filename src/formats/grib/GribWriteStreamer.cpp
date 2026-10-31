#include "iotdbfs/formats/grib/GribWriteStreamer.h"

#include "iotdbfs/formats/grib/GribLayout.h"
#include "iotdbfs/Logging.h"
#include "iotdbfs/formats/grib/gribUtils.h"

#include <algorithm>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

namespace iotdbfs {
namespace {

constexpr std::size_t kMaxSummaryMembers = 4096;

bool extract_message_values(codes_handle* h, int edition,
  MetadataMap& values, const std::shared_ptr<const MetadataConfig>& config,
  std::string& error) {
  if (!h)
    return false;
  auto canonical = grib_utils::extract_message_metadata(h, edition);
  if (!canonical.ok()) {
    error = canonical.error();
    return false;
  }
  if (config) {
    auto result = grib_utils::extract_configured_metadata(h, *config);
    if (!result.ok()) {
      error = result.error();
      return false;
    }
    values = std::move(result.value());
  }
  for (const auto& [key, value] : canonical.value())
    values.insert_or_assign(key, value);
  return true;
}

bool merge_scalar(MetadataMap& summary, const std::string& key,
    const MetadataValue& scalar, std::size_t& member_count,
    std::string& error) {
  MetadataValueType set_type;
  switch (scalar.type()) {
    case MetadataValueType::String: set_type = MetadataValueType::StringSet; break;
    case MetadataValueType::Int64: set_type = MetadataValueType::Int64Set; break;
    case MetadataValueType::Double: set_type = MetadataValueType::DoubleSet; break;
    case MetadataValueType::Bool: set_type = MetadataValueType::BoolSet; break;
    case MetadataValueType::StringSet:
    case MetadataValueType::Int64Set:
    case MetadataValueType::DoubleSet:
    case MetadataValueType::BoolSet:
      error = "GRIB message metadata must be scalar: " + key;
      return false;
  }
  auto found = summary.find(key);
  if (found == summary.end()) {
    switch (set_type) {
      case MetadataValueType::StringSet:
        found = summary.emplace(key, MetadataValue(MetadataStringSet{})).first;
        break;
      case MetadataValueType::Int64Set:
        found = summary.emplace(key, MetadataValue(MetadataInt64Set{})).first;
        break;
      case MetadataValueType::DoubleSet:
        found = summary.emplace(key, MetadataValue(MetadataDoubleSet{})).first;
        break;
      case MetadataValueType::BoolSet:
        found = summary.emplace(key, MetadataValue(MetadataBoolSet{})).first;
        break;
      default: break;
    }
  }
  if (found == summary.end() || found->second.type() != set_type) {
    error = "GRIB metadata type changed across messages: " + key;
    return false;
  }
  bool inserted = false;
  auto& storage = found->second.value();
  switch (set_type) {
    case MetadataValueType::StringSet:
      inserted = std::get<MetadataStringSet>(storage).insert(
        std::get<std::string>(scalar.value())).second;
      break;
    case MetadataValueType::Int64Set:
      inserted = std::get<MetadataInt64Set>(storage).insert(
        std::get<std::int64_t>(scalar.value())).second;
      break;
    case MetadataValueType::DoubleSet:
      inserted = std::get<MetadataDoubleSet>(storage).insert(
        std::get<double>(scalar.value())).second;
      break;
    case MetadataValueType::BoolSet:
      inserted = std::get<MetadataBoolSet>(storage).insert(
        std::get<bool>(scalar.value())).second;
      break;
    default: break;
  }
  if (inserted && ++member_count > kMaxSummaryMembers) {
    error = "GRIB file metadata summary exceeds distinct-value limit";
    return false;
  }
  return true;
}

void add_int64_bounds(MetadataMap& summary, const char* field) {
  const auto found = summary.find(field);
  if (found == summary.end() ||
      found->second.type() != MetadataValueType::Int64Set)
    return;
  const auto& values = std::get<MetadataInt64Set>(found->second.value());
  if (values.empty()) return;
  summary.insert_or_assign(std::string(field) + "Min",
    MetadataValue(*values.begin()));
  summary.insert_or_assign(std::string(field) + "Max",
    MetadataValue(*values.rbegin()));
}

codes_handle* parse_message(const std::uint8_t* data, std::size_t size,
  int edition) {
  // The scanner trusts advertised lengths. Reject incomplete framing before
  // handing it to ecCodes, which may return a partial handle or fail unsafely
  // for a length shorter than the indicator section.
  const std::size_t header_size = edition == 2 ? 16 : 8;
  if (size < header_size + 4 || std::memcmp(data + size - 4, "7777", 4) != 0)
    return nullptr;
  auto* handle = grib_utils::new_handle_from_message_guarded(data, size);
  if (!handle)
    handle = grib_utils::new_handle_from_message_copy_guarded(data, size);
  return handle;
}

} // namespace

GribWriteStreamer::GribWriteStreamer(const Config& cfg)
  : cfg_(cfg), window_(), base_offset_(0) {
  window_.reserve(cfg_.window_reserve_bytes);
}

void GribWriteStreamer::feed_chunk(const std::uint8_t* data, std::size_t n) {
  if (!n || !data || failed_)
    return;
  LOG_DEBUG("Feeding chunk of size " + std::to_string(n));
  compact_window_if_needed();
  window_.insert(window_.end(), data, data + n);
}

void GribWriteStreamer::process_complete_messages(bool final) {
  if (failed_)
    return;

  std::size_t available = window_size();
  if (!available)
    return;
  if (!should_attempt_process(available, final))
    return;

  std::size_t scan_start = std::min(scan_resume_offset_, available);
  stats_.scan_calls += 1;
  stats_.scan_bytes += (available - scan_start);

  auto scan = grib_utils::find_messages_incremental(window_data(), available,
    scan_start);
  scan_resume_offset_ = scan.next_offset;
  auto& msgs = scan.messages;
  stats_.messages_found += msgs.size();
  std::vector<codes_handle*> handles(msgs.size(), nullptr);

  if (!type_decided_) {
    bool looks = false;
    for (std::size_t i = 0; i < msgs.size(); ++i) {
      const auto& message = msgs[i];
      codes_handle* h = parse_message(window_data() + message.pos,
        message.len, message.edition);
      if (h) {
        handles[i] = h;
        looks = true;
        break;
      }
    }
    if (looks) {
      is_grib_ = true;
      type_decided_ = true;
    }
    else if (final || (available >= effective_process_threshold() &&
      !scan.incomplete_tail && scan.next_offset > 0)) {
      is_grib_ = false;
      type_decided_ = true;
    }
    if (!type_decided_) {
      schedule_next_process(available);
      return;
    }
  }

  if (!is_grib_) {
    for (auto* h : handles) {
      if (h)
        codes_handle_delete(h);
    }
    discard_raw_prefix(final);
    return;
  }

  // Emit bytes already scanned as well as complete messages. Keep only the
  // incomplete message or header overlap so raw tails do not grow forever.
  std::size_t flush_end = final ? available : scan.next_offset;
  std::vector<std::string> layout_records;
  layout_records.reserve(msgs.size());
  for (std::size_t i = 0; i < msgs.size(); ++i) {
    const auto& m = msgs[i];
    const std::uint8_t* msg_ptr = window_data() + m.pos;
    std::size_t msg_len = m.len;
    flush_end = std::max(flush_end, m.pos + m.len);
    codes_handle* h = handles[i];
    if (!h)
      h = parse_message(msg_ptr, msg_len, m.edition);
    if (!h)
      continue;

    std::string metadata_error;
    MetadataMap message_values;
    if (!extract_message_values(h, m.edition, message_values,
        cfg_.metadata_config, metadata_error)) {
      codes_handle_delete(h);
      handles[i] = nullptr;
      for (auto* pending : handles) {
        if (pending) codes_handle_delete(pending);
      }
      mark_failure(metadata_error.empty() ?
        "GRIB message metadata extraction failed" : metadata_error);
      return;
    }
    if (!summary_) summary_.emplace();
    for (const auto& [key, value] : message_values) {
      if (!merge_scalar(*summary_, key, value, summary_member_count_,
          metadata_error)) {
        codes_handle_delete(h);
        handles[i] = nullptr;
        for (auto* pending : handles) {
          if (pending) codes_handle_delete(pending);
        }
        mark_failure(metadata_error);
        return;
      }
    }

    std::uint64_t abs_off = base_offset_ + static_cast<std::uint64_t>(m.pos);
    layout_records.push_back(
      grib_utils::build_index_line(h, abs_off, msg_len, m.edition));
    codes_handle_delete(h);
    handles[i] = nullptr;
  }

  for (auto* h : handles) {
    if (h)
      codes_handle_delete(h);
  }

  if (flush_end > 0) {
    if (!layout_records.empty() && layout_.empty())
      layout_ = grib_layout_header();
    for (const auto& line : layout_records)
      layout_ += line;
    consume_window(flush_end);
    base_offset_ += static_cast<std::uint64_t>(flush_end);
  }
  else if (!final) {
    schedule_next_process(available);
  }
}

void GribWriteStreamer::finalize() {
  if (failed_)
    return;

  process_complete_messages(true);
  if (failed_)
    return;

  if (!type_decided_) {
    is_grib_ = false;
    type_decided_ = true;
  }
  if (!is_grib_) {
    discard_raw_prefix(true);
    return;
  }

  if (summary_) {
    add_int64_bounds(*summary_, "level");
    add_int64_bounds(*summary_, "step");
    const auto serialized = serialize_metadata(*summary_, 65536);
    if (!serialized.ok()) {
      mark_failure(serialized.error());
      return;
    }
  }

  std::size_t remaining = window_size();
  if (remaining) {
    base_offset_ += static_cast<std::uint64_t>(remaining);
    consume_window(remaining);
  }
}

void GribWriteStreamer::reset_state() {
  window_.clear();
  window_head_ = 0;
  base_offset_ = 0;
  next_process_threshold_ = 0;
  scan_resume_offset_ = 0;
  is_grib_ = false;
  type_decided_ = false;
  failed_ = false;
  error_.clear();
  layout_.clear();
  stats_ = GribWriteStreamerStats{};
  summary_.reset();
  summary_member_count_ = 0;
}

bool GribWriteStreamer::is_grib() const {
  return is_grib_;
}

bool GribWriteStreamer::type_decided() const {
  return type_decided_;
}

bool GribWriteStreamer::failed() const {
  return failed_;
}

const std::string& GribWriteStreamer::error() const {
  return error_;
}

const std::string& GribWriteStreamer::layout() const {
  return layout_;
}

const std::optional<MetadataMap>& GribWriteStreamer::summary() const {
  return summary_;
}

const GribWriteStreamerStats& GribWriteStreamer::stats() const {
  return stats_;
}

std::size_t GribWriteStreamer::window_size() const {
  return window_.size() - window_head_;
}

const std::uint8_t* GribWriteStreamer::window_data() const {
  return window_.data() + window_head_;
}

std::size_t GribWriteStreamer::effective_process_threshold() const {
  return std::max(cfg_.process_threshold_bytes, cfg_.detect_threshold_bytes);
}

void GribWriteStreamer::schedule_next_process(std::size_t available) {
  std::size_t retry = cfg_.process_retry_bytes;
  if (retry == 0)
    retry = std::max<std::size_t>(cfg_.chunk_size, 64 * 1024);
  next_process_threshold_ = available + retry;
}

bool GribWriteStreamer::should_attempt_process(std::size_t available,
  bool final) const {
  if (final)
    return available > 0;
  return available >=
    std::max(effective_process_threshold(), next_process_threshold_);
}

void GribWriteStreamer::consume_window(std::size_t n) {
  if (n == 0)
    return;
  window_head_ = std::min(window_head_ + n, window_.size());
  next_process_threshold_ = 0;
  if (scan_resume_offset_ > n)
    scan_resume_offset_ -= n;
  else
    scan_resume_offset_ = 0;
  if (window_head_ == window_.size()) {
    window_.clear();
    window_head_ = 0;
  }
  else if (window_head_ >= cfg_.chunk_size * 8 ||
    window_head_ >= window_.size() / 2) {
    compact_window_if_needed();
  }
}

void GribWriteStreamer::compact_window_if_needed() {
  if (window_head_ == 0)
    return;
  std::size_t tail = window_size();
  if (tail == 0) {
    window_.clear();
    window_head_ = 0;
    return;
  }
  if (window_head_ < cfg_.chunk_size && window_head_ < tail)
    return;
  std::memmove(window_.data(), window_.data() + window_head_, tail);
  window_.resize(tail);
  window_head_ = 0;
}

void GribWriteStreamer::discard_raw_prefix(bool final) {
  if (failed_)
    return;
  std::size_t available = window_size();
  if (!available)
    return;
  std::size_t chunk_size = cfg_.chunk_size == 0 ? available : cfg_.chunk_size;
  std::size_t flush_bytes =
    final ? available : (available / chunk_size) * chunk_size;
  if (!flush_bytes)
    return;
  consume_window(flush_bytes);
  base_offset_ += static_cast<std::uint64_t>(flush_bytes);
}

void GribWriteStreamer::mark_failure(const std::string& message) {
  failed_ = true;
  error_ = message;
}

} // namespace iotdbfs
