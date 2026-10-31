#pragma once

#include "iotdbfs/formats/grib/GribTypes.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace iotdbfs {

struct GribWriteStreamerStats {
  std::size_t scan_calls{ 0 };
  std::size_t scan_bytes{ 0 };
  std::size_t messages_found{ 0 };
};

class GribWriteStreamer {
public:
  struct Config {
    std::size_t chunk_size{ 4 * 1024 * 1024 };
    std::size_t process_threshold_bytes{ 4 * 1024 * 1024 };
    std::size_t process_retry_bytes{ 2 * 1024 * 1024 };
    std::size_t window_reserve_bytes{ 8 * 1024 * 1024 };
    std::size_t detect_threshold_bytes{ 2 * 1024 * 1024 };
    std::shared_ptr<const MetadataConfig> metadata_config;
  };

  explicit GribWriteStreamer(const Config& cfg);

  void feed_chunk(const std::uint8_t* data, std::size_t n);
  void process_complete_messages(bool final = false);
  void finalize();
  void reset_state();

  bool is_grib() const;
  bool type_decided() const;
  bool failed() const;
  const std::string& error() const;
  const std::string& layout() const;
  const std::optional<MetadataMap>& summary() const;
  const GribWriteStreamerStats& stats() const;

private:
  Config cfg_;
  std::vector<std::uint8_t> window_;
  std::size_t window_head_{ 0 };
  std::uint64_t base_offset_{ 0 };
  std::size_t next_process_threshold_{ 0 };
  std::size_t scan_resume_offset_{ 0 };
  bool is_grib_{ false };
  bool type_decided_{ false };
  bool failed_{ false };
  std::string error_;
  std::string layout_;
  GribWriteStreamerStats stats_;
  std::optional<MetadataMap> summary_;
  std::size_t summary_member_count_{ 0 };

  std::size_t window_size() const;
  const std::uint8_t* window_data() const;
  std::size_t effective_process_threshold() const;
  void schedule_next_process(std::size_t available);
  bool should_attempt_process(std::size_t available, bool final) const;
  void consume_window(std::size_t n);
  void compact_window_if_needed();
  void discard_raw_prefix(bool final);
  void mark_failure(const std::string& message);
};

} // namespace iotdbfs
