#include "iotdbfs/formats/grib/GribQueryKernel.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <optional>

namespace iotdbfs {
namespace {

QueryStatus kernel_failure(QueryError error, std::string message,
    const QuerySource* source = nullptr) {
  QueryErrorDetail detail;
  detail.operation = "grib-query-kernel";
  if (source) {
    const auto& manifest = source->manifest();
    detail.source = QuerySourceReference{manifest.key.path,
      manifest.key.version, manifest.format_id.value_or("")};
  }
  return QueryStatus::failure(error, QueryPhase::Execution,
    std::move(message), std::move(detail));
}

double spacing_value(const LogicalValue& value) {
  return value.type() == LogicalTypeId::Float32
    ? static_cast<double>(std::get<float>(value.value()))
    : std::get<double>(value.value());
}

bool projected_once(const QueryFragment& fragment, const char* field_id) {
  return std::count_if(fragment.projection.begin(), fragment.projection.end(),
    [&](const auto& field) { return field.field_id == field_id; }) == 1;
}

double bound_value(const DomainBound& bound) {
  return std::get<double>(bound.value.value());
}

bool inside_slice(double value, const DomainSlice& slice) {
  if (slice.lower) {
    const auto lower = bound_value(*slice.lower);
    if (value < lower ||
        (value == lower && slice.lower->kind == BoundKind::Exclusive))
      return false;
  }
  if (slice.upper) {
    const auto upper = bound_value(*slice.upper);
    if (value > upper ||
        (value == upper && slice.upper->kind == BoundKind::Exclusive))
      return false;
  }
  return true;
}

std::optional<std::pair<std::size_t, std::size_t>> selected_extent(
    const std::vector<double>& coordinates, const DomainSlice* slice) {
  if (coordinates.empty()) return std::nullopt;
  if (!slice) return std::pair<std::size_t, std::size_t>{
    0, coordinates.size() - 1};
  std::optional<std::size_t> first;
  std::size_t last = 0;
  for (std::size_t index = 0; index < coordinates.size(); ++index) {
    if (!inside_slice(coordinates[index], *slice)) continue;
    if (!first) first = index;
    last = index;
  }
  if (!first) return std::nullopt;
  return std::pair<std::size_t, std::size_t>{*first, last};
}

bool is_cyclic_longitude_axis(const std::vector<double>& coordinates) {
  if (coordinates.size() < 2) return false;
  const double increment = coordinates[1] - coordinates[0];
  if (!std::isfinite(increment) || increment <= 0.0) return false;
  const double circumference = increment *
    static_cast<double>(coordinates.size());
  return std::fabs(circumference - 360.0) <=
    std::max(1e-6, increment * 1e-6);
}

bool inside_cyclic_longitude_slice(double value, const DomainSlice& slice) {
  if (!slice.lower || !slice.upper) return inside_slice(value, slice);
  const double lower = bound_value(*slice.lower);
  const double upper = bound_value(*slice.upper);
  if (upper - lower > 360.0) return true;
  const double center = lower + (upper - lower) / 2.0;
  const double equivalent = center + std::remainder(
    std::remainder(value, 360.0) - std::remainder(center, 360.0), 360.0);
  return inside_slice(equivalent, slice);
}

std::optional<std::pair<std::size_t, std::size_t>>
selected_longitude_extent(const std::vector<double>& coordinates,
    const DomainSlice* slice) {
  if (!slice || !slice->lower || !slice->upper ||
      !is_cyclic_longitude_axis(coordinates))
    return selected_extent(coordinates, slice);

  std::vector<bool> selected(coordinates.size(), false);
  std::size_t selected_count = 0;
  for (std::size_t index = 0; index < coordinates.size(); ++index) {
    selected[index] = inside_cyclic_longitude_slice(coordinates[index], *slice);
    if (selected[index]) ++selected_count;
  }
  if (selected_count == 0) return std::nullopt;
  if (selected_count == coordinates.size())
    return std::pair<std::size_t, std::size_t>{0, coordinates.size() - 1};

  for (std::size_t index = 0; index < coordinates.size(); ++index) {
    const std::size_t previous =
      (index + coordinates.size() - 1) % coordinates.size();
    if (!selected[index] || selected[previous]) continue;
    std::size_t last = index;
    while (selected[(last + 1) % coordinates.size()])
      last = (last + 1) % coordinates.size();
    return std::pair<std::size_t, std::size_t>{index, last};
  }
  return std::nullopt;
}

struct GridWindow {
  std::size_t i0{0};
  std::size_t j0{0};
  std::size_t width{0};
  std::size_t height{0};
  bool wraps_longitude{false};
};

double normalize_longitude(double longitude, double minimum,
    double maximum) {
  if (longitude >= minimum && longitude <= maximum) return longitude;
  const double center = minimum + (maximum - minimum) / 2.0;
  return center + std::remainder(
    std::remainder(longitude, 360.0) - center, 360.0);
}

std::size_t window_column(const GridWindow& window, std::size_t offset,
    std::size_t ni) {
  const auto column = window.i0 + offset;
  return window.wraps_longitude && column >= ni ? column - ni : column;
}

std::optional<GridWindow> crop_window(const GribNativeField& field,
    const grib_utils::CropParams& input) {
  if (field.ni == 0 || field.nj == 0 ||
      field.longitude_axis.size() != field.ni ||
      field.latitude_axis.size() != field.nj ||
      !std::isfinite(input.lat0) || !std::isfinite(input.lon0) ||
      !std::isfinite(input.lat1) || !std::isfinite(input.lon1) ||
      !std::isfinite(field.longitude_increment) ||
      !std::isfinite(field.latitude_increment) ||
      field.longitude_increment <= 0.0 ||
      field.latitude_increment <= 0.0)
    return std::nullopt;

  auto crop = input;
  const double latitude_first = field.latitude_axis.front();
  const double latitude_last = field.latitude_axis.back();
  const double longitude_first = field.longitude_axis.front();
  const double longitude_last = field.longitude_axis.back();
  if (input.extend_equal_corners_to_grid_end &&
      input.lat0 == input.lat1 && input.lon0 == input.lon1) {
    crop.lat1 = latitude_last;
    crop.lon1 = longitude_last;
  }

  const double longitude_min = std::min(longitude_first, longitude_last);
  const double longitude_max = std::max(longitude_first, longitude_last);
  const bool cyclic = is_cyclic_longitude_axis(field.longitude_axis);
  const double requested_min = std::min(crop.lon0, crop.lon1);
  const double requested_max = std::max(crop.lon0, crop.lon1);
  const double requested_span = requested_max - requested_min;
  if (!std::isfinite(requested_span)) return std::nullopt;
  if (cyclic) {
    crop.lon0 = normalize_longitude(requested_min, longitude_min,
      longitude_max);
    crop.lon1 = crop.lon0 + requested_span;
  } else {
    crop.lon0 = normalize_longitude(crop.lon0, longitude_min,
      longitude_max);
    crop.lon1 = normalize_longitude(crop.lon1, longitude_min,
      longitude_max);
  }

  const auto longitude_index = [&](double longitude) {
    if (cyclic) {
      double offset = std::fmod(longitude - longitude_first, 360.0);
      if (offset < 0.0) offset += 360.0;
      longitude = longitude_first + offset;
    }
    const auto index = std::clamp(
      (longitude - longitude_first) / field.longitude_increment, 0.0,
      static_cast<double>(field.ni - 1));
    return static_cast<std::size_t>(std::floor(index + 0.5));
  };
  const bool latitude_descending = latitude_first >= latitude_last;
  const auto latitude_index = [&](double latitude) {
    const double delta = latitude_descending
      ? latitude_first - latitude : latitude - latitude_first;
    const auto index = std::clamp(delta / field.latitude_increment, 0.0,
      static_cast<double>(field.nj - 1));
    return static_cast<std::size_t>(std::floor(index + 0.5));
  };

  std::size_t i0 = 0;
  std::size_t i1 = static_cast<std::size_t>(field.ni - 1);
  bool wraps = false;
  if (!cyclic || requested_span < 360.0) {
    i0 = longitude_index(crop.lon0);
    i1 = longitude_index(crop.lon1);
    wraps = cyclic && i0 > i1;
  }
  const auto north = latitude_index(std::max(crop.lat0, crop.lat1));
  const auto south = latitude_index(std::min(crop.lat0, crop.lat1));
  GridWindow window;
  window.i0 = wraps ? i0 : std::min(i0, i1);
  window.j0 = std::min(north, south);
  window.wraps_longitude = wraps;
  window.width = wraps
    ? static_cast<std::size_t>(field.ni) - window.i0 + i1 + 1
    : std::max(i0, i1) - window.i0 + 1;
  window.height = std::max(north, south) - window.j0 + 1;
  if (window.width == 0 || window.height == 0) return std::nullopt;
  return window;
}

std::vector<double> selected_values(const GribNativeField& field,
    const GridWindow& window) {
  std::vector<double> output;
  output.reserve(window.width * window.height);
  for (std::size_t j = 0; j < window.height; ++j) {
    const auto row = (window.j0 + j) * static_cast<std::size_t>(field.ni);
    for (std::size_t i = 0; i < window.width; ++i)
      output.push_back(field.values[row +
        window_column(window, i, static_cast<std::size_t>(field.ni))]);
  }
  return output;
}

std::optional<std::vector<double>> interpolated_values(
    const GribNativeField& field,
    const grib_utils::InterpParams& interpolation, std::size_t output_ni,
    std::size_t output_nj, double longitude_min, double longitude_max,
    double latitude_min, double latitude_max) {
  if (output_ni == 0 || output_nj == 0 ||
      output_ni > std::numeric_limits<std::size_t>::max() / output_nj)
    return std::nullopt;
  const double longitude_first = field.longitude_axis.front();
  const double latitude_first = field.latitude_axis.front();
  const bool latitude_descending = latitude_first >=
    field.latitude_axis.back();
  const bool cyclic = is_cyclic_longitude_axis(field.longitude_axis);
  const double inverse_i = 1.0 / field.longitude_increment;
  const double inverse_j = 1.0 / field.latitude_increment;
  const double maximum_i = static_cast<double>(field.ni - 1);
  const double maximum_j = static_cast<double>(field.nj - 1);
  const bool clamp = interpolation.border ==
    grib_utils::InterpParams::Border::CLAMP;
  const double nan = std::numeric_limits<double>::quiet_NaN();

  const auto sample = [&](double latitude, double longitude) {
    double u = (longitude - longitude_first) * inverse_i;
    double v = (latitude_descending ? latitude_first - latitude
                                    : latitude - latitude_first) * inverse_j;
    if (cyclic) {
      u = std::fmod(u, static_cast<double>(field.ni));
      if (u < 0.0) u += static_cast<double>(field.ni);
      if (clamp) v = std::clamp(v, 0.0, maximum_j);
      else if (v < 0.0 || v > maximum_j) return nan;
    } else if (clamp) {
      u = std::clamp(u, 0.0, maximum_i);
      v = std::clamp(v, 0.0, maximum_j);
    } else if (u < 0.0 || u > maximum_i || v < 0.0 || v > maximum_j) {
      return nan;
    }

    const auto u_floor = std::floor(u);
    const auto v_floor = std::floor(v);
    const auto left = static_cast<std::size_t>(std::clamp(u_floor, 0.0,
      maximum_i));
    const auto top = static_cast<std::size_t>(std::clamp(v_floor, 0.0,
      maximum_j));
    const auto right = cyclic ? (left + 1) % field.ni
      : std::min<std::size_t>(left + 1, field.ni - 1);
    const auto bottom = std::min<std::size_t>(top + 1, field.nj - 1);
    const double horizontal = u - u_floor;
    const double vertical = v - v_floor;
    const auto row_width = static_cast<std::size_t>(field.ni);
    const double top_left = field.values[top * row_width + left];
    const double top_right = field.values[top * row_width + right];
    const double bottom_left = field.values[bottom * row_width + left];
    const double bottom_right = field.values[bottom * row_width + right];
    const double upper = top_left + horizontal * (top_right - top_left);
    const double lower = bottom_left + horizontal *
      (bottom_right - bottom_left);
    return upper + vertical * (lower - upper);
  };

  const double longitude_step = output_ni > 1
    ? (interpolation.dlon > 0.0 ? interpolation.dlon
       : (longitude_max - longitude_min) /
         static_cast<double>(output_ni - 1))
    : 0.0;
  const double latitude_step = output_nj > 1
    ? (interpolation.dlat > 0.0 ? interpolation.dlat
       : (latitude_max - latitude_min) /
         static_cast<double>(output_nj - 1))
    : 0.0;
  std::vector<double> output;
  output.reserve(output_ni * output_nj);
  for (std::size_t j = 0; j < output_nj; ++j) {
    const double latitude = latitude_max - static_cast<double>(j) *
      latitude_step;
    for (std::size_t i = 0; i < output_ni; ++i) {
      const double longitude = longitude_min + static_cast<double>(i) *
        longitude_step;
      output.push_back(sample(latitude, longitude));
    }
  }
  return output;
}

GribGridResult empty_grid_result(grib_utils::DataDType type) {
  GribGridResult result;
  if (type == grib_utils::DataDType::F32) {
    result.values = {{LogicalTypeId::Float32, false, std::nullopt},
      std::vector<float>{}, {}};
  } else {
    result.values = {{LogicalTypeId::Float64, false, std::nullopt},
      std::vector<double>{}, {}};
  }
  result.domain = {DomainKind::DenseGrid,
    {{{grib_query_fields::kLatitude},
       {LogicalTypeId::Float64, false, std::nullopt}, "degree_north"},
     {{grib_query_fields::kLongitude},
       {LogicalTypeId::Float64, false, std::nullopt}, "degree_east"}},
    {0, 0}, {0, 0}};
  return result;
}

} // namespace

QueryResult<GribTypedOutputPlan> plan_grib_typed_output(
    const QueryFragment& fragment) {
  using namespace grib_query_fields;
  GribTypedOutputPlan plan;
  if (fragment.output.mode == QueryFragment::ResultMode::BinaryView) {
    if (fragment.projection.size() != 1 ||
        !projected_once(fragment, kMessageBytes) ||
        !fragment.output.type_overrides.empty() ||
        fragment.output.interpolation || !fragment.slices.empty())
      return QueryResult<GribTypedOutputPlan>::failure(kernel_failure(
        QueryError::UnsupportedCapability,
        "GRIB binary-view mode requires only an untransformed "
        "grib.message.bytes projection"));
    plan.kind = GribResultKind::SourceMessage;
    return QueryResult<GribTypedOutputPlan>::success(std::move(plan));
  }

  for (const auto& type_override : fragment.output.type_overrides) {
    if (type_override.field.field_id != kValue)
      return QueryResult<GribTypedOutputPlan>::failure(kernel_failure(
        QueryError::UnsupportedCapability,
        "GRIB output type can be overridden only for grib.data.value"));
    if (type_override.type != LogicalTypeId::Float32 &&
        type_override.type != LogicalTypeId::Float64)
      return QueryResult<GribTypedOutputPlan>::failure(kernel_failure(
        QueryError::UnsupportedCapability,
        "GRIB values support only Float32 and Float64 output"));
    plan.value_type = type_override.type == LogicalTypeId::Float32
      ? grib_utils::DataDType::F32 : grib_utils::DataDType::F64;
  }
  if (!fragment.output.interpolation)
    return QueryResult<GribTypedOutputPlan>::success(std::move(plan));

  plan.interpolation.enabled = true;
  plan.interpolation.border =
    fragment.output.interpolation->border ==
      QueryFragment::BorderMode::NaN
    ? grib_utils::InterpParams::Border::AS_NAN
    : grib_utils::InterpParams::Border::CLAMP;
  bool latitude = false;
  bool longitude = false;
  for (const auto& axis : fragment.output.interpolation->axes) {
    if (axis.dimension.field_id == kLatitude) {
      latitude = true;
      if (axis.count &&
          *axis.count > static_cast<std::uint64_t>(
            std::numeric_limits<long>::max()))
        return QueryResult<GribTypedOutputPlan>::failure(kernel_failure(
          QueryError::ResourceExhausted,
          "GRIB latitude interpolation count is too large"));
      if (axis.count)
        plan.interpolation.outNj = static_cast<long>(*axis.count);
      if (axis.spacing)
        plan.interpolation.dlat = spacing_value(*axis.spacing);
    } else if (axis.dimension.field_id == kLongitude) {
      longitude = true;
      if (axis.count &&
          *axis.count > static_cast<std::uint64_t>(
            std::numeric_limits<long>::max()))
        return QueryResult<GribTypedOutputPlan>::failure(kernel_failure(
          QueryError::ResourceExhausted,
          "GRIB longitude interpolation count is too large"));
      if (axis.count)
        plan.interpolation.outNi = static_cast<long>(*axis.count);
      if (axis.spacing)
        plan.interpolation.dlon = spacing_value(*axis.spacing);
    } else {
      return QueryResult<GribTypedOutputPlan>::failure(kernel_failure(
        QueryError::UnsupportedCapability,
        "GRIB interpolation supports only latitude and longitude"));
    }
  }
  if (!latitude || !longitude)
    return QueryResult<GribTypedOutputPlan>::failure(kernel_failure(
      QueryError::InvalidQuery,
      "GRIB interpolation requires latitude and longitude axes"));
  return QueryResult<GribTypedOutputPlan>::success(std::move(plan));
}

QueryResult<GribLayoutDocument> GribQueryKernel::read_layout(
    const QuerySource& source) {
  const auto size = source.manifest().layout_size;
  if (size == 0)
    return QueryResult<GribLayoutDocument>::failure(kernel_failure(
      QueryError::LayoutUnavailable,
      "GRIB File Version has no Layout Artifact", &source));
  auto bytes = source.read_layout(0, size);
  if (!bytes.ok())
    return QueryResult<GribLayoutDocument>::failure(bytes.status());
  if (bytes.value().size() != size)
    return QueryResult<GribLayoutDocument>::failure(kernel_failure(
      QueryError::CorruptLayout,
      "GRIB Layout Artifact read returned an unexpected byte count",
      &source));
  return parse_grib_layout(bytes.value());
}

QueryResult<BinaryValue> GribQueryKernel::read_source_message(
    const QuerySource& source, const grib_utils::MsgInfo& message) {
  if (!message.ok || message.len == 0)
    return QueryResult<BinaryValue>::failure(kernel_failure(
      QueryError::CorruptLayout,
      "GRIB physical plan contains an invalid message range", &source));
  auto bytes = source.read_payload(message.off, message.len);
  if (!bytes.ok()) return QueryResult<BinaryValue>::failure(bytes.status());
  if (bytes.value().size() != message.len)
    return QueryResult<BinaryValue>::failure(kernel_failure(
      QueryError::CorruptPayload,
      "GRIB message read returned an unexpected byte count", &source));
  return QueryResult<BinaryValue>::success(std::move(bytes.value()));
}

std::size_t GribNativeField::memory_bytes() const {
  return sizeof(GribNativeField) +
    latitude_axis.capacity() * sizeof(double) +
    longitude_axis.capacity() * sizeof(double) +
    values.capacity() * sizeof(double);
}

QueryResult<GribNativeField> GribQueryKernel::decode_native_field(
    const QuerySource& source, const GribGridPlan& plan) {
  if (!plan.message.ok || plan.message.len == 0)
    return QueryResult<GribNativeField>::failure(kernel_failure(
      QueryError::CorruptLayout,
      "GRIB physical plan contains an invalid message range", &source));
  std::optional<QueryStatus> read_failure;
  grib_utils::ReadObjectPartFunc reader =
    [&](std::uint64_t offset, std::uint64_t length)
      -> std::optional<std::vector<std::uint8_t>> {
      auto bytes = source.read_payload(offset, length);
      if (!bytes.ok()) {
        read_failure = bytes.status();
        return std::nullopt;
      }
      return std::move(bytes.value());
    };
  std::string cache_token = plan.cache_token;
  if (cache_token.empty()) {
    const auto& manifest = source.manifest();
    cache_token = manifest.key.path + "@" +
      std::to_string(manifest.key.version);
  }
  auto decoder = grib_utils::MessageDecoder::open(plan.message, reader,
    cache_token);
  if (!decoder) {
    if (read_failure)
      return QueryResult<GribNativeField>::failure(*read_failure);
    return QueryResult<GribNativeField>::failure(kernel_failure(
      QueryError::CorruptPayload,
      "GRIB message cannot be opened for typed decoding", &source));
  }
  auto axes = decoder->native_grid_axes();
  if (!axes) {
    if (read_failure)
      return QueryResult<GribNativeField>::failure(*read_failure);
    return QueryResult<GribNativeField>::failure(kernel_failure(
      QueryError::CorruptPayload,
      "GRIB native grid axes cannot be decoded", &source));
  }
  auto decoded = decoder->decode_values(grib_utils::DataDType::F64, {}, {});
  if (!decoded) {
    if (read_failure)
      return QueryResult<GribNativeField>::failure(*read_failure);
    return QueryResult<GribNativeField>::failure(kernel_failure(
      QueryError::CorruptPayload,
      "GRIB native values cannot be decoded", &source));
  }
  auto* values = std::get_if<std::vector<double>>(&*decoded);
  if (!values || axes->ni == 0 || axes->nj == 0 ||
      axes->ni > std::numeric_limits<std::size_t>::max() / axes->nj ||
      values->size() != static_cast<std::size_t>(axes->ni * axes->nj) ||
      axes->latitudes.size() != axes->nj ||
      axes->longitudes.size() != axes->ni)
    return QueryResult<GribNativeField>::failure(kernel_failure(
      QueryError::CorruptPayload,
      "GRIB native values and grid geometry disagree", &source));

  GribNativeField field;
  field.ni = axes->ni;
  field.nj = axes->nj;
  field.longitude_increment = axes->longitude_increment;
  field.latitude_increment = axes->latitude_increment;
  field.latitude_axis = std::move(axes->latitudes);
  field.longitude_axis = std::move(axes->longitudes);
  field.values = std::move(*values);
  return QueryResult<GribNativeField>::success(std::move(field));
}

QueryResult<GribGridResult> GribQueryKernel::render_grid(
    const QuerySource& source, const GribGridPlan& plan,
    const GribNativeField& field) {
  if (field.ni == 0 || field.nj == 0 ||
      field.ni > std::numeric_limits<std::size_t>::max() / field.nj ||
      field.values.size() != static_cast<std::size_t>(field.ni * field.nj) ||
      field.latitude_axis.size() != field.nj ||
      field.longitude_axis.size() != field.ni)
    return QueryResult<GribGridResult>::failure(kernel_failure(
      QueryError::CorruptPayload,
      "GRIB cached native field has an invalid shape", &source));
  if (plan.crop.enabled && !plan.slices.empty())
    return QueryResult<GribGridResult>::failure(kernel_failure(
      QueryError::Internal,
      "GRIB grid plan cannot mix private crop state and Domain Slices",
      &source));

  GridWindow window{0, 0, static_cast<std::size_t>(field.ni),
    static_cast<std::size_t>(field.nj), false};
  if (!plan.slices.empty()) {
    const DomainSlice* latitude = nullptr;
    const DomainSlice* longitude = nullptr;
    for (const auto& slice : plan.slices) {
      if (slice.dimension.field_id == grib_query_fields::kLatitude)
        latitude = &slice;
      else if (slice.dimension.field_id == grib_query_fields::kLongitude)
        longitude = &slice;
      else
        return QueryResult<GribGridResult>::failure(kernel_failure(
          QueryError::InvalidDomainSlice,
          "GRIB grid plan contains an unsupported Domain Slice", &source));
    }
    const auto latitudes = selected_extent(field.latitude_axis, latitude);
    const auto longitudes = selected_longitude_extent(field.longitude_axis,
      longitude);
    if (!latitudes || !longitudes)
      return QueryResult<GribGridResult>::success(
        empty_grid_result(plan.value_type));
    window.j0 = latitudes->first;
    window.height = latitudes->second - latitudes->first + 1;
    window.i0 = longitudes->first;
    window.wraps_longitude = longitudes->first > longitudes->second;
    window.width = window.wraps_longitude
      ? static_cast<std::size_t>(field.ni) - longitudes->first +
        longitudes->second + 1
      : longitudes->second - longitudes->first + 1;
  } else if (plan.crop.enabled) {
    auto cropped = crop_window(field, plan.crop);
    if (!cropped)
      return QueryResult<GribGridResult>::failure(kernel_failure(
        QueryError::InvalidDomainSlice,
        "GRIB crop cannot be mapped to the native grid", &source));
    window = *cropped;
  }

  const double longitude_min = field.longitude_axis[window.i0];
  const double longitude_max = longitude_min +
    static_cast<double>(window.width - 1) * field.longitude_increment;
  const double first_latitude = field.latitude_axis[window.j0];
  const double last_latitude = field.latitude_axis[
    window.j0 + window.height - 1];
  const double latitude_min = std::min(first_latitude, last_latitude);
  const double latitude_max = std::max(first_latitude, last_latitude);

  std::size_t output_ni = window.width;
  std::size_t output_nj = window.height;
  std::vector<double> values;
  if (plan.interpolation.enabled) {
    long requested_ni = plan.interpolation.outNi;
    long requested_nj = plan.interpolation.outNj;
    if (requested_ni <= 0) {
      requested_ni = plan.interpolation.dlon > 0.0
        ? std::max(1L, static_cast<long>(std::round(
            std::fabs(longitude_max - longitude_min) /
            plan.interpolation.dlon)) + 1)
        : static_cast<long>(window.width);
    }
    if (requested_nj <= 0) {
      requested_nj = plan.interpolation.dlat > 0.0
        ? std::max(1L, static_cast<long>(std::round(
            std::fabs(latitude_max - latitude_min) /
            plan.interpolation.dlat)) + 1)
        : static_cast<long>(window.height);
    }
    if (requested_ni <= 0 || requested_nj <= 0)
      return QueryResult<GribGridResult>::failure(kernel_failure(
        QueryError::InvalidQuery,
        "GRIB interpolation has an invalid output shape", &source));
    output_ni = static_cast<std::size_t>(requested_ni);
    output_nj = static_cast<std::size_t>(requested_nj);
    auto interpolated = interpolated_values(field, plan.interpolation,
      output_ni, output_nj, longitude_min,
      longitude_max, latitude_min, latitude_max);
    if (!interpolated)
      return QueryResult<GribGridResult>::failure(kernel_failure(
        QueryError::ResourceExhausted,
        "GRIB interpolation output is too large", &source));
    values = std::move(*interpolated);
  } else {
    if (window.width > std::numeric_limits<std::size_t>::max() /
        window.height)
      return QueryResult<GribGridResult>::failure(kernel_failure(
        QueryError::ResourceExhausted,
        "GRIB crop output is too large", &source));
    values = selected_values(field, window);
  }

  GribGridResult result;
  result.latitude_axis.reserve(output_nj);
  result.longitude_axis.reserve(output_ni);
  if (plan.interpolation.enabled) {
    const double longitude_step = output_ni > 1
      ? (plan.interpolation.dlon > 0.0 ? plan.interpolation.dlon
         : (longitude_max - longitude_min) /
           static_cast<double>(output_ni - 1))
      : 0.0;
    const double latitude_step = output_nj > 1
      ? (plan.interpolation.dlat > 0.0 ? plan.interpolation.dlat
         : (latitude_max - latitude_min) /
           static_cast<double>(output_nj - 1))
      : 0.0;
    for (std::size_t j = 0; j < output_nj; ++j)
      result.latitude_axis.push_back(latitude_max -
        static_cast<double>(j) * latitude_step);
    for (std::size_t i = 0; i < output_ni; ++i) {
      double longitude = longitude_min + static_cast<double>(i) *
        longitude_step;
      if (window.wraps_longitude) {
        double offset = std::fmod(longitude - field.longitude_axis.front(),
          360.0);
        if (offset < 0.0) offset += 360.0;
        longitude = field.longitude_axis.front() + offset;
      }
      result.longitude_axis.push_back(longitude);
    }
  } else {
    for (std::size_t j = 0; j < window.height; ++j)
      result.latitude_axis.push_back(field.latitude_axis[window.j0 + j]);
    for (std::size_t i = 0; i < window.width; ++i)
      result.longitude_axis.push_back(field.longitude_axis[
        window_column(window, i, static_cast<std::size_t>(field.ni))]);
  }

  if (plan.value_type == grib_utils::DataDType::F32) {
    std::vector<float> converted;
    converted.reserve(values.size());
    for (const auto value : values)
      converted.push_back(static_cast<float>(value));
    result.values = {{LogicalTypeId::Float32, false, std::nullopt},
      std::move(converted), {}};
  } else {
    result.values = {{LogicalTypeId::Float64, false, std::nullopt},
      std::move(values), {}};
  }
  result.domain = {DomainKind::DenseGrid,
    {{{grib_query_fields::kLatitude},
       {LogicalTypeId::Float64, false, std::nullopt}, "degree_north"},
     {{grib_query_fields::kLongitude},
       {LogicalTypeId::Float64, false, std::nullopt}, "degree_east"}},
    {output_nj, output_ni},
    {plan.interpolation.enabled ? 0 : window.j0,
     plan.interpolation.enabled ? 0 : window.i0}};
  return QueryResult<GribGridResult>::success(std::move(result));
}

QueryResult<GribGridResult> GribQueryKernel::decode_grid(
    const QuerySource& source, const GribGridPlan& plan) {
  auto field = decode_native_field(source, plan);
  if (!field.ok())
    return QueryResult<GribGridResult>::failure(field.status());
  return render_grid(source, plan, field.value());
}

} // namespace iotdbfs
