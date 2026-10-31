#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include <eccodes.h>

#include "iotdbfs/MetadataConfig.h"

namespace grib_utils {

struct MsgLoc {
  size_t pos;
  size_t len;
  int edition;
};

struct MsgScanResult {
  std::vector<MsgLoc> messages;
  size_t next_offset{ 0 };
  bool incomplete_tail{ false };
};

// Creates a new eccodes handle referencing the provided message memory.
codes_handle* new_handle_from_message_guarded(const uint8_t* msg, size_t size);
// Creates a new eccodes handle by copying the message into eccodes-managed memory.
codes_handle* new_handle_from_message_copy_guarded(const uint8_t* msg,
  size_t size);

// Scan raw bytes and return the offsets/length of GRIB messages.
std::vector<MsgLoc> find_messages(const uint8_t* data, size_t length);
MsgScanResult find_messages_incremental(const uint8_t* data, size_t length,
  size_t start_offset);

// Write a compact JSON line describing a GRIB message for indexing.
std::string build_index_line(codes_handle* h, uint64_t file_offset_of_msg,
  size_t total_len, int edition);

struct FileMeta {
  bool has{ false };
  std::string shortName;
  std::string typeOfProcessedData;
  long typeOfGeneratingProcess{ 0 };
  std::string typeOfLevel;
  long centre{ 0 };
  long generatingProcessIdentifier{ 0 };
  long discipline{ 0 };
  long parameterCategory{ 0 };
  long parameterNumber{ 0 };
  std::string unit;
  double latitudeOfFirstGridPoint{ 0 };
  double longitudeOfFirstGridPoint{ 0 };
  double latitudeOfLastGridPoint{ 0 };
  double longitudeOfLastGridPoint{ 0 };
  std::string globalDomain;
  double iDirectionIncrement{ 0 };
  double jDirectionIncrement{ 0 };
  long Ni{ 0 };
  long Nj{ 0 };
  long long numberOfDataPoints{ 0 };
};

// Extract high-level metadata from one GRIB message.
bool extract_file_meta(codes_handle* h, FileMeta& meta);

// Extract the canonical typed values contributed by one message. File-level
// ingestion aggregates these scalars into typed sets and numeric bounds.
iotdbfs::MetadataResult<iotdbfs::MetadataMap>
extract_message_metadata(codes_handle* h, int edition);

// Extract metadata fields declared by configuration from one GRIB message.
// Values are read as scalars from ecCodes; required fields that are missing or
// invalid cause a failure result while optional fields are skipped.
iotdbfs::MetadataResult<iotdbfs::MetadataMap>
extract_configured_metadata(codes_handle* h,
  const iotdbfs::MetadataConfig& config);

struct MsgInfo {
  uint64_t off{ 0 };
  uint64_t len{ 0 };
  bool ok{ false };
  long long ni{ 0 };
  long long nj{ 0 };
  long long numberOfDataPoints{ 0 };
};

struct MsgMatch {
  MsgInfo info;
  std::map<std::string, std::string> labels;
};

// Look up message length/offset by matching query key/value pairs. The lookup
// succeeds only when exactly one valid message record matches.
MsgInfo find_msg_info(const std::string& index_text,
  const std::map<std::string, std::string>& kv);

std::vector<MsgMatch> find_all_msg_matches(const std::string& index_text,
  const std::map<std::string, std::string>& kv, size_t limit = 0);

enum class DataDType { F32, F64 };
size_t dtype_bytes(DataDType t);
DataDType parse_dtype_from_kv(const std::map<std::string, std::string>& kv);

using DecodedValues = std::variant<std::vector<float>, std::vector<double>>;

using ReadObjectPartFunc =
  std::function<std::optional<std::vector<uint8_t>>(uint64_t off, uint64_t len)>;

// Query eccodes for the number of values contained in a message buffer.
std::optional<size_t>
get_values_count_from_message_bytes(const std::vector<uint8_t>& msg);

std::optional<size_t>
compute_values_count_for_msg(const MsgInfo& mi,
  const ReadObjectPartFunc& reader);

std::optional<size_t>
compute_values_count_for_msg(const MsgInfo& mi,
  const ReadObjectPartFunc& reader,
  const std::string& cache_token);

struct CropParams {
  bool enabled{ false };
  double lat0{ 0 };
  double lon0{ 0 };
  double lat1{ 0 };
  double lon1{ 0 };
  // The legacy string query uses equal start/end corners to mean "from this
  // point to the grid end". Typed Domain Slices set this false because an
  // equal closed interval selects one coordinate.
  bool extend_equal_corners_to_grid_end{ true };
};

// Remove crop-related keys from a query map before index matching.
void strip_crop_keys(std::map<std::string, std::string>& kv);
CropParams parse_crop_params_from_kv(const std::map<std::string,
  std::string>& kv);

std::optional<size_t>
compute_cropped_value_count(const MsgInfo& mi, const CropParams& cp,
  const ReadObjectPartFunc& reader);

std::optional<size_t>
compute_cropped_value_count(const MsgInfo& mi, const CropParams& cp,
  const ReadObjectPartFunc& reader,
  const std::string& cache_token);

std::optional<std::vector<uint8_t>> decode_and_maybe_crop_values(
  const MsgInfo& mi, DataDType dtype, const CropParams& cp,
  const ReadObjectPartFunc& reader);

std::optional<std::vector<uint8_t>> decode_and_maybe_crop_values(
  const MsgInfo& mi, DataDType dtype, const CropParams& cp,
  const ReadObjectPartFunc& reader,
  const std::string& cache_token);

// Decode an entire message into the requested dtype without cropping.
std::optional<std::vector<uint8_t>>
decode_values_as_dtype(const MsgInfo& mi, DataDType dtype,
  const ReadObjectPartFunc& reader);

std::optional<std::vector<uint8_t>>
decode_values_as_dtype(const MsgInfo& mi, DataDType dtype,
  const ReadObjectPartFunc& reader,
  const std::string& cache_token);

struct InterpParams {
  enum class Border { CLAMP, AS_NAN };
  bool enabled{ false };
  long outNi{ 0 };
  long outNj{ 0 };
  double dlon{ 0.0 };
  double dlat{ 0.0 };
  Border border{ Border::CLAMP };
};

InterpParams parse_interp_params_from_kv(const std::map<std::string,
  std::string>& kv);

struct GridDescription {
  std::uint64_t ni{ 0 };
  std::uint64_t nj{ 0 };
  std::uint64_t origin_i{ 0 };
  std::uint64_t origin_j{ 0 };
  std::vector<double> latitude_axis;
  std::vector<double> longitude_axis;
};

struct GridAxesDescription {
  std::uint64_t ni{ 0 };
  std::uint64_t nj{ 0 };
  double longitude_increment{ 0.0 };
  double latitude_increment{ 0.0 };
  std::vector<double> latitudes;
  std::vector<double> longitudes;
};

// Owns one message buffer and exactly one ecCodes handle for the full typed
// query lifecycle. Geometry inspection, crop planning, and value decoding use
// the same handle; instances are move-only and task-local.
class MessageDecoder {
public:
  static std::optional<MessageDecoder> open(const MsgInfo& mi,
    const ReadObjectPartFunc& reader, const std::string& cache_token = {});

  ~MessageDecoder();
  MessageDecoder(MessageDecoder&&) noexcept;
  MessageDecoder& operator=(MessageDecoder&&) noexcept;
  MessageDecoder(const MessageDecoder&) = delete;
  MessageDecoder& operator=(const MessageDecoder&) = delete;

  std::optional<GridAxesDescription> native_grid_axes() const;
  std::optional<GridDescription> output_grid(const CropParams& cp,
    const InterpParams& ip) const;
  std::optional<DecodedValues> decode_values(DataDType dtype,
    const CropParams& cp, const InterpParams& ip) const;

private:
  struct Impl;
  explicit MessageDecoder(std::unique_ptr<Impl> implementation);
  std::unique_ptr<Impl> implementation_;
};

// Returns native regular-grid axes in storage order without materializing the
// Cartesian coordinate product.
std::optional<GridAxesDescription> describe_native_grid_axes(
  const MsgInfo& mi, const ReadObjectPartFunc& reader,
  const std::string& cache_token = {});

// Describes the grid produced by the same crop/interpolation parameters used
// by the decoder. Coordinate arrays contain one compact axis per dimension;
// callers materialize the Cartesian product only for projected output batches.
std::optional<GridDescription> describe_output_grid(const MsgInfo& mi,
  const CropParams& cp, const InterpParams& ip,
  const ReadObjectPartFunc& reader, const std::string& cache_token = {});

// Typed decode entry points used by query adapters. The byte-returning APIs
// above remain the encoded-file/FUSE boundary and serialize these values only
// for callers that explicitly require bytes.
std::optional<DecodedValues> decode_values_typed(const MsgInfo& mi,
  DataDType dtype, const ReadObjectPartFunc& reader,
  const std::string& cache_token = {});

std::optional<DecodedValues> decode_and_maybe_crop_values_typed(
  const MsgInfo& mi, DataDType dtype, const CropParams& cp,
  const ReadObjectPartFunc& reader, const std::string& cache_token = {});

std::optional<DecodedValues> decode_crop_and_bilinear_resample_typed(
  const MsgInfo& mi, DataDType dtype, const CropParams& cp,
  const InterpParams& ip, const ReadObjectPartFunc& reader,
  const std::string& cache_token = {});

// Decode, crop and bilinearly resample the data if required.
std::optional<std::vector<uint8_t>> decode_crop_and_bilinear_resample(
  const MsgInfo& mi, DataDType dtype, const CropParams& cp,
  const InterpParams& ip, const ReadObjectPartFunc& reader);

std::optional<std::vector<uint8_t>> decode_crop_and_bilinear_resample(
  const MsgInfo& mi, DataDType dtype, const CropParams& cp,
  const InterpParams& ip, const ReadObjectPartFunc& reader,
  const std::string& cache_token);

// Configure the global in-memory cache for decoded message payloads.
void configure_message_cache(size_t max_entries);

} // namespace grib_utils
