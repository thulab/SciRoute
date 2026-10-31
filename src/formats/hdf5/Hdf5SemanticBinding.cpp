#include "iotdbfs/formats/hdf5/Hdf5SemanticBinding.h"
#include "iotdbfs/formats/hdf5/Hdf5Layout.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <regex>
#include <set>
#include <sstream>
#include <stdexcept>

namespace iotdbfs {
namespace {
QueryStatus unsupported(const char *message) {
  return QueryStatus::failure(QueryError::UnsupportedCapability,
                              QueryPhase::Planning, message);
}
} // namespace
QueryResult<Hdf5GpsTimeAxis>
Hdf5GpsTimeAxis::create(double start, double spacing, std::uint64_t count) {
  using Result = QueryResult<Hdf5GpsTimeAxis>;
  if (!std::isfinite(start) || !std::isfinite(spacing) || spacing <= 0)
    return Result::failure(unsupported(
        "GPS grid requires finite origin and positive finite spacing"));
  int exponent = 0;
  if (std::frexp(spacing, &exponent) != 0.5)
    return Result::failure(
        unsupported("GPS grid spacing must be an exact power of two"));
  const auto unit_start = std::ldexp(start, 1 - exponent);
  constexpr std::int64_t exact_limit = std::int64_t{1} << 53;
  if (!std::isfinite(unit_start) || std::trunc(unit_start) != unit_start ||
      unit_start < -double(exact_limit) || unit_start > double(exact_limit) ||
      count > std::uint64_t(exact_limit))
    return Result::failure(unsupported(
        "GPS grid origin or length exceeds exact Float64 grid support"));
  const auto first = static_cast<std::int64_t>(unit_start);
  // The sum cannot overflow Int64 after the bounds above. Keeping arithmetic
  // integral avoids accepting an endpoint rounded back inside the safe range.
  const auto last = first + static_cast<std::int64_t>(count ? count - 1 : 0);
  if (last > exact_limit ||
      !std::isfinite(std::ldexp(double(last), exponent - 1)))
    return Result::failure(
        unsupported("GPS grid endpoint exceeds exact Float64 grid support"));
  return Result::success(Hdf5GpsTimeAxis(start, spacing, count));
}
double Hdf5GpsTimeAxis::coordinate(std::uint64_t ordinal) const {
  if (ordinal >= count_)
    throw std::out_of_range("GPS ordinal exceeds bound grid");
  return std::fma(static_cast<double>(ordinal), spacing_, start_);
}
std::uint64_t Hdf5GpsTimeAxis::lower_bound(double value) const {
  if (std::isnan(value))
    throw std::invalid_argument("GPS lower_bound requires an ordered value");
  std::uint64_t lo = 0, hi = count_;
  while (lo < hi) {
    const auto mid = lo + (hi - lo) / 2;
    if (coordinate(mid) < value)
      lo = mid + 1;
    else
      hi = mid;
  }
  return lo;
}

QueryResult<Hdf5GpsTimeAxis>
compile_hdf5_gwosc_axis(const Hdf5LayoutObject &object) {
  using Result = QueryResult<Hdf5GpsTimeAxis>;
  if (!object.dataset || object.dataset->descriptor.shape.size() != 1)
    return Result::failure(
        unsupported("GWOSC binding requires a rank-one dataset"));
  if (!object.attributes_complete)
    return Result::failure(unsupported(
        "GWOSC binding requires a complete attribute-name inventory"));
  auto attribute = [&](const char *name) -> const Hdf5Attribute * {
    const Hdf5Attribute *found = nullptr;
    for (const auto &item : object.attributes) {
      if (item.name != name)
        continue;
      if (found)
        return nullptr;
      found = &item;
    }
    return found;
  };
  const auto *start = attribute("Xstart");
  const auto *spacing = attribute("Xspacing");
  const auto *count = attribute("Npoints");
  auto scalar = [](const Hdf5Attribute *attr) {
    return attr && attr->state == Hdf5AttributeState::Complete &&
           !attr->null_dataspace && attr->shape.empty() &&
           attr->values.size() == 8;
  };
  auto integral = [&](const Hdf5Attribute *attr) {
    return scalar(attr) && (attr->type == LogicalTypeId::Int64 ||
                            attr->type == LogicalTypeId::UInt64);
  };
  if (!integral(start) || !integral(count) || !scalar(spacing) ||
      spacing->type != LogicalTypeId::Float64)
    return Result::failure(
        unsupported("GWOSC numeric attributes require scalar integral "
                    "Xstart/Npoints and Float64 Xspacing"));
  auto bits = [](const Hdf5Attribute &attr) {
    std::uint64_t out = 0;
    for (unsigned i = 0; i < 8; ++i)
      out |= std::uint64_t(attr.values[i]) << (8 * i);
    return out;
  };
  auto as_signed = [&](const Hdf5Attribute &attr) {
    const auto raw = bits(attr);
    std::int64_t out;
    std::memcpy(&out, &raw, sizeof out);
    return out;
  };
  constexpr std::uint64_t exact_limit = std::uint64_t{1} << 53;
  double origin;
  if (start->type == LogicalTypeId::Int64) {
    const auto value = as_signed(*start);
    if (value < -std::int64_t(exact_limit) || value > std::int64_t(exact_limit))
      return Result::failure(
          unsupported("GWOSC Xstart cannot be represented exactly in Float64"));
    origin = static_cast<double>(value);
  } else {
    const auto value = bits(*start);
    if (value > exact_limit)
      return Result::failure(
          unsupported("GWOSC Xstart cannot be represented exactly in Float64"));
    origin = static_cast<double>(value);
  }
  if (count->type == LogicalTypeId::Int64 && as_signed(*count) < 0)
    return Result::failure(unsupported("GWOSC Npoints must be nonnegative"));
  const auto length = bits(*count);
  if (length != object.dataset->descriptor.shape[0])
    return Result::failure(
        QueryStatus::failure(QueryError::SchemaMismatch, QueryPhase::Planning,
                             "GWOSC Npoints differs from dataset extent"));
  const auto spacing_bits = bits(*spacing);
  double increment;
  std::memcpy(&increment, &spacing_bits, sizeof increment);
  auto compiled = Hdf5GpsTimeAxis::create(origin, increment, length);
  if (!compiled.ok())
    return compiled;
  auto label = [&](const char *name, const char *expected, bool &verified) {
    const auto *attr = attribute(name);
    if (!attr || attr->null_dataspace || !attr->shape.empty())
      return false;
    if (attr->state == Hdf5AttributeState::UnsupportedVariableLength) {
      // The application explicitly declares these two strings through the
      // GWOSC binding. Never infer them from unread arbitrary attributes.
      verified = false;
      return attr->native_type_class ==
             3; // Native H5T_STRING, not a vlen sequence.
    }
    if (attr->state != Hdf5AttributeState::Complete ||
        attr->type != LogicalTypeId::Utf8 || attr->strings.size() != 1 ||
        attr->strings[0] != expected)
      return false;
    verified = true;
    return true;
  };
  if (!label("Xunits", "second", compiled.value().units_verified_) ||
      !label("Xlabel", "GPS time", compiled.value().label_verified_))
    return Result::failure(
        unsupported("GWOSC units/label must match explicit GPS seconds "
                    "declaration or be bounded-unread variable strings"));
  return compiled;
}

namespace {
const Hdf5Attribute *cf_attribute(const Hdf5LayoutObject &o, const char *name) {
  for (const auto &a : o.attributes)
    if (a.name == name)
      return &a;
  return nullptr;
}
std::string cf_string(const Hdf5Attribute *a) {
  if (!a || a->state != Hdf5AttributeState::Complete || a->null_dataspace ||
      a->type != LogicalTypeId::Utf8 || a->strings.size() != 1)
    throw std::runtime_error("CF attribute requires one available string");
  return a->strings.front();
}
std::vector<double> cf_numbers(const Hdf5Attribute &a, bool missing = false) {
  if (a.state != Hdf5AttributeState::Complete || a.null_dataspace ||
      a.values.empty() || a.values.size() % 8 ||
      (a.type != LogicalTypeId::Int64 && a.type != LogicalTypeId::UInt64 &&
       a.type != LogicalTypeId::Float64))
    throw std::runtime_error("CF attribute requires available numeric values");
  std::vector<double> result;
  for (std::size_t j = 0; j < a.values.size(); j += 8) {
    std::uint64_t bits = 0;
    for (unsigned i = 0; i < 8; ++i)
      bits |= std::uint64_t(a.values[j + i]) << (8 * i);
    double value;
    if (a.type == LogicalTypeId::Float64)
      std::memcpy(&value, &bits, 8);
    else if (a.type == LogicalTypeId::UInt64) {
      if (bits > (std::uint64_t{1} << 53))
        throw std::runtime_error("CF integer exceeds exact Float64 range");
      value = static_cast<double>(bits);
    } else {
      std::int64_t v;
      std::memcpy(&v, &bits, 8);
      if (v < -(std::int64_t{1} << 53) || v > (std::int64_t{1} << 53))
        throw std::runtime_error("CF integer exceeds exact Float64 range");
      value = static_cast<double>(v);
    }
    if (!std::isfinite(value) && !(missing && std::isnan(value)))
      throw std::runtime_error("CF numeric attribute is nonfinite");
    result.push_back(value);
  }
  return result;
}
double cf_scalar(const Hdf5Attribute &a) {
  auto values = cf_numbers(a);
  if (values.size() != 1)
    throw std::runtime_error("CF transform requires scalar attributes");
  return values[0];
}
QueryStatus cf_error(const std::exception &e) {
  return QueryStatus::failure(QueryError::UnsupportedCapability,
                              QueryPhase::Planning, e.what());
}
} // namespace
bool hdf5_declares_cf(const Hdf5LayoutDocument &doc) {
  for (const auto &o : doc.objects)
    if (o.id == doc.root_object_id) {
      try {
        auto v = cf_string(cf_attribute(o, "Conventions"));
        for (char &c : v)
          if (c == ',')
            c = ' ';
        std::istringstream input(v);
        std::string token;
        while (input >> token) {
          if (token == "CF-1") return true;
          if (token.rfind("CF-1.", 0) != 0)
            continue;
          auto minor = token.substr(5);
          for (unsigned i = 0; i <= 12; ++i)
            if (minor == std::to_string(i))
              return true;
        }
      } catch (...) {
        return false;
      }
    }
  return false;
}
bool hdf5_declares_geos_pressure(const Hdf5LayoutDocument &doc) {
  if (!doc.directory_complete || !hdf5_declares_cf(doc)) return false;
  for (const auto &o : doc.objects) if (o.id == doc.root_object_id) {
    if (!o.attributes_complete) return false;
    try {
      return cf_string(cf_attribute(o, "Source")) == "GEOSadas-5_7_2" &&
             cf_string(cf_attribute(o, "VersionID")) == "5.7.2";
    } catch (...) { return false; }
  }
  return false;
}
QueryResult<Hdf5CoordinateIndex>
inspect_hdf5_geos_pressure_coordinate(const Hdf5LayoutObject &o) {
  using R = QueryResult<Hdf5CoordinateIndex>;
  try {
    if (!o.dataset || o.dataset->descriptor.shape.size() != 1 ||
        !o.attributes_complete ||
        (o.dataset->descriptor.value_type != LogicalTypeId::Float32 &&
         o.dataset->descriptor.value_type != LogicalTypeId::Float64) ||
        cf_string(cf_attribute(o, "CLASS")) != "DIMENSION_SCALE" ||
        cf_string(cf_attribute(o, "units")) != "hPa" ||
        cf_string(cf_attribute(o, "positive")) != "down" ||
        cf_string(cf_attribute(o, "standard_name")) != "PLE_level")
      throw std::runtime_error("GEOS pressure scale evidence is incomplete");
    return R::success({"pressure", "hPa", "", {}, true});
  } catch (const std::exception &e) { return R::failure(cf_error(e)); }
}
QueryResult<Hdf5CfBinding>
compile_hdf5_geos_pressure_binding(const Hdf5LayoutDocument &doc,
                                   const std::string &id) {
  using R = QueryResult<Hdf5CfBinding>;
  try {
    if (!hdf5_declares_geos_pressure(doc))
      throw std::runtime_error("GEOS-5.7.2 profile is not declared");
    const Hdf5LayoutObject *data = nullptr;
    for (const auto &o : doc.objects) if (o.id == id) data = &o;
    if (!data || !data->dataset || !data->attributes_complete ||
        data->dataset->descriptor.shape.size() != 4 ||
        data->dataset->descriptor.value_type != LogicalTypeId::Float32 ||
        cf_string(cf_attribute(*data, "standard_name")) != "air_temperature" ||
        cf_string(cf_attribute(*data, "units")) != "K")
      throw std::runtime_error("GEOS temperature dataset evidence is incomplete");
    auto base = compile_hdf5_cf_binding(doc, id);
    if (!base.ok()) return base;
    auto result = std::move(base.value());
    auto role_at = [&](std::uint32_t dimension, const char *role) {
      return std::any_of(result.axes.begin(), result.axes.end(), [&](const auto &axis) {
        return axis.dimension == dimension && axis.role == role;
      });
    };
    if (result.axes.size() != 3 || !role_at(0, "time") ||
        !role_at(2, "latitude") || !role_at(3, "longitude") ||
        data->axis_associations.size() != 4)
      throw std::runtime_error("GEOS time/latitude/longitude scale relation is incomplete");
    const Hdf5LayoutObject *pressure = nullptr;
    for (const auto &relation : data->axis_associations) {
      if (relation.axis != 1) continue;
      if (relation.evidence != "forward_dimension_list/1" || pressure)
        throw std::runtime_error("GEOS pressure association is ambiguous");
      for (const auto &o : doc.objects)
        if (o.id == relation.scale_object_id) pressure = &o;
    }
    if (!pressure) throw std::runtime_error("GEOS pressure scale is absent");
    auto metadata = inspect_hdf5_geos_pressure_coordinate(*pressure);
    if (!metadata.ok() || !pressure->coordinate_index)
      throw std::runtime_error("GEOS pressure coordinate is unavailable");
    const auto &index = *pressure->coordinate_index;
    if (index.role != "pressure" || index.units != "hPa" ||
        index.values.size() != data->dataset->descriptor.shape[1] ||
        index.values.empty())
      throw std::runtime_error("GEOS pressure coordinate evidence disagrees");
    for (std::size_t i = 0; i < index.values.size(); ++i)
      if (!std::isfinite(index.values[i]) || index.values[i] <= 0 ||
          (i && !(index.ascending ? index.values[i] > index.values[i-1]
                                 : index.values[i] < index.values[i-1])))
        throw std::runtime_error("GEOS pressure values are invalid");
    result.axes.push_back({1, "pressure", pressure->id, index.units, "",
                           "forward_dimension_list/1", index.values, index.ascending});
    std::sort(result.axes.begin(), result.axes.end(),
              [](const auto &a, const auto &b) { return a.dimension < b.dimension; });
    result.profile = "geos572-pressure-grid/1";
    return R::success(std::move(result));
  } catch (const std::exception &e) { return R::failure(cf_error(e)); }
}
std::optional<std::int64_t>
verified_hdf5_geos_utc_second(const Hdf5LayoutDocument &doc) {
  if (!hdf5_declares_geos_pressure(doc)) return std::nullopt;
  const Hdf5LayoutObject *temperature = nullptr;
  for (const auto &object : doc.objects)
    if (object.canonical_path == "/T") {
      if (temperature) return std::nullopt;
      temperature = &object;
    }
  if (!temperature) return std::nullopt;
  auto binding = compile_hdf5_geos_pressure_binding(doc, temperature->id);
  if (!binding.ok()) return std::nullopt;
  const Hdf5CfAxis *time = nullptr;
  for (const auto &axis : binding.value().axes)
    if (axis.role == "time" && axis.dimension == 0) time = &axis;
  if (!time || time->values.size() != 1 || time->calendar != "standard")
    return std::nullopt;
  constexpr const char *prefix = "minutes since ";
  if (time->units.compare(0, 14, prefix) != 0) return std::nullopt;
  const std::string epoch = time->units.substr(14);
  if (epoch.size() != 19 || epoch[4] != '-' || epoch[7] != '-' ||
      epoch[10] != ' ' || epoch[13] != ':' || epoch[16] != ':')
    return std::nullopt;
  for (std::size_t i = 0; i < epoch.size(); ++i)
    if (i != 4 && i != 7 && i != 10 && i != 13 && i != 16 &&
        (epoch[i] < '0' || epoch[i] > '9')) return std::nullopt;
  const auto part = [&](std::size_t at, std::size_t n) {
    int x = 0;
    for (std::size_t i = at; i < at + n; ++i) x = x * 10 + epoch[i] - '0';
    return x;
  };
  const int year = part(0, 4), month = part(5, 2), day = part(8, 2);
  const int hour = part(11, 2), minute = part(14, 2), second = part(17, 2);
  if (year < 2000 || year > 2099 || month < 1 || month > 12 ||
      hour > 23 || minute > 59 || second > 59) return std::nullopt;
  const bool leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
  const int month_days[] = {0,31,28 + int(leap),31,30,31,30,31,31,30,31,30,31};
  if (day < 1 || day > month_days[month]) return std::nullopt;
  // Gregorian civil date to days since 1970-01-01; GEOS product dates are
  // modern UTC instants, so there is no Gregorian cutover ambiguity.
  int y = year - (month <= 2);
  const int era = y / 400;
  const unsigned yoe = unsigned(y - era * 400);
  const unsigned mp = unsigned(month + (month > 2 ? -3 : 9));
  const unsigned doy = (153 * mp + 2) / 5 + unsigned(day) - 1;
  const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  const auto days = std::int64_t(era) * 146097 + doe - 719468;
  const double native_minutes = time->values.front();
  if (!std::isfinite(native_minutes) || std::floor(native_minutes) != native_minutes ||
      native_minutes < -100000000.0 || native_minutes > 100000000.0)
    return std::nullopt;
  return days * 86400 + hour * 3600 + minute * 60 + second +
         static_cast<std::int64_t>(native_minutes) * 60;
}
namespace {
class CfProfile final : public Hdf5SemanticProfile {
public:
  std::string id() const override { return "cf-rectilinear/1"; }
  bool declares(const Hdf5LayoutDocument &doc) const override {
    return hdf5_declares_cf(doc);
  }
  QueryResult<Hdf5CoordinateIndex> inspect_coordinate(
      const Hdf5LayoutObject &o) const override {
    return inspect_hdf5_cf_coordinate(o);
  }
  QueryResult<Hdf5CfBinding> compile(const Hdf5LayoutDocument &doc,
      const std::string &id) const override {
    return compile_hdf5_cf_binding(doc, id);
  }
};
class GeosPressureProfile final : public Hdf5SemanticProfile {
public:
  std::string id() const override { return "geos572-pressure-grid/1"; }
  bool declares(const Hdf5LayoutDocument &doc) const override {
    return hdf5_declares_geos_pressure(doc);
  }
  QueryResult<Hdf5CoordinateIndex> inspect_coordinate(
      const Hdf5LayoutObject &o) const override {
    auto pressure = inspect_hdf5_geos_pressure_coordinate(o);
    return pressure.ok() ? pressure : inspect_hdf5_cf_coordinate(o);
  }
  QueryResult<Hdf5CfBinding> compile(const Hdf5LayoutDocument &doc,
      const std::string &id) const override {
    return compile_hdf5_geos_pressure_binding(doc, id);
  }
};
} // namespace
std::vector<std::shared_ptr<const Hdf5SemanticProfile>>
default_hdf5_semantic_profiles() {
  return {std::make_shared<GeosPressureProfile>(),
          std::make_shared<CfProfile>()};
}
QueryResult<Hdf5CoordinateIndex>
inspect_hdf5_cf_coordinate(const Hdf5LayoutObject &o) {
  using R = QueryResult<Hdf5CoordinateIndex>;
  try {
    if (!o.dataset || o.dataset->descriptor.shape.size() != 1 ||
        !o.attributes_complete)
      throw std::runtime_error(
          "CF coordinate requires rank one and complete attributes");
    Hdf5CoordinateIndex c;
    c.units = cf_string(cf_attribute(o, "units"));
    const std::set<std::string> lat{"degrees_north", "degree_north",
                                    "degree_N",      "degrees_N",
                                    "degreeN",       "degreesN"};
    const std::set<std::string> lon{"degrees_east", "degree_east", "degree_E",
                                    "degrees_E",    "degreeE",     "degreesE"};
    if (lat.count(c.units))
      c.role = "latitude";
    else if (lon.count(c.units))
      c.role = "longitude";
    else {
      auto at = c.units.find(" since ");
      const std::set<std::string> units{
          "s",    "sec",    "secs",    "second", "seconds", "min",
          "mins", "minute", "minutes", "h",      "hr",      "hrs",
          "hour", "hours",  "d",       "day",    "days"};
      if (at == std::string::npos || !units.count(c.units.substr(0, at)) ||
          at + 7 >= c.units.size())
        throw std::runtime_error("CF coordinate units are unsupported");
      // This profile compares native coordinate numbers; it never parses an
      // epoch into UTC. Preserve the entire units/calendar strings verbatim.
      const auto epoch = c.units.substr(at + 7);
      static const std::regex epoch_grammar(
          R"(^[+-]?[0-9]{1,9}-[0-9]{1,2}-[0-9]{1,2}([ T][0-9]{1,2}:[0-9]{1,2}(:[0-9]{1,2}(\.[0-9]+)?)?( ?[+-][0-9]{1,2}(:?[0-9]{2})?)?)?$)");
      if (!std::regex_match(epoch, epoch_grammar))
        throw std::runtime_error("CF reference time syntax is unsupported");
      c.role = "time";
      c.calendar = "standard";
      if (auto a = cf_attribute(o, "calendar"))
        c.calendar = cf_string(a);
      const std::set<std::string> calendars{
          "standard", "gregorian", "proleptic_gregorian",
          "julian",   "noleap",    "365_day",
          "all_leap", "366_day",   "360_day",
          "none"};
      if (!calendars.count(c.calendar))
        throw std::runtime_error("CF calendar is unsupported");
    }
    if (auto a = cf_attribute(o, "standard_name"))
      if (cf_string(a) != c.role)
        throw std::runtime_error(
            "CF coordinate standard_name conflicts with units");
    if (auto a = cf_attribute(o, "axis")) {
      const auto expected = c.role == "time"       ? "T"
                            : c.role == "latitude" ? "Y"
                                                   : "X";
      if (cf_string(a) != expected)
        throw std::runtime_error("CF axis conflicts with units");
    }
    for (const char *name :
         {"_FillValue", "missing_value", "scale_factor", "add_offset",
          "valid_min", "valid_max", "valid_range", "month_lengths"})
      if (cf_attribute(o, name))
        throw std::runtime_error("CF coordinate packing, missing, or custom "
                                 "calendar is unsupported");
    return R::success(std::move(c));
  } catch (const std::exception &e) {
    return R::failure(cf_error(e));
  }
}
QueryResult<Hdf5CfBinding>
compile_hdf5_cf_binding(const Hdf5LayoutDocument &doc, const std::string &id) {
  using R = QueryResult<Hdf5CfBinding>;
  try {
    if (!doc.directory_complete || !hdf5_declares_cf(doc))
      throw std::runtime_error("CF binding requires a declared supported "
                               "convention and complete directory");
    const Hdf5LayoutObject *object = nullptr;
    for (const auto &o : doc.objects)
      if (o.id == id)
        object = &o;
    if (!object || !object->dataset || !object->dataset->descriptor.supported ||
        !object->attributes_complete)
      throw std::runtime_error(
          "CF binding requires supported dataset and complete attributes");
    if (!object->axis_associations_complete)
      throw std::runtime_error("CF forward dimension evidence is unavailable");
    Hdf5CfBinding result;
    std::set<std::string> roles;
    std::set<std::uint32_t> dimensions;
    for (const auto &a : object->axis_associations) {
      const Hdf5LayoutObject *scale = nullptr;
      for (const auto &o : doc.objects)
        if (o.id == a.scale_object_id)
          scale = &o;
      if (!scale || !scale->dataset ||
          a.axis >= object->dataset->descriptor.shape.size() ||
          a.evidence != "forward_dimension_list/1")
        throw std::runtime_error("CF dimension association is invalid");
      if (cf_string(cf_attribute(*scale, "CLASS")) != "DIMENSION_SCALE")
        throw std::runtime_error(
            "CF forward reference target is not a dimension scale");
      if (const auto units = cf_attribute(*scale, "units"))
        if (units->state != Hdf5AttributeState::Complete)
          throw std::runtime_error("CF scale units are unavailable");
      auto metadata = inspect_hdf5_cf_coordinate(*scale);
      if (!metadata.ok()) {
        // Unrecognized vertical or non-spatial dimensions remain physical.
        // A declared spatial/time coordinate with unreadable evidence must
        // not silently disappear from the science view.
        auto standard = cf_attribute(*scale, "standard_name"),
             axis = cf_attribute(*scale, "axis");
        const auto standard_value =
            standard ? cf_string(standard) : std::string{};
        const auto axis_value = axis ? cf_string(axis) : std::string{};
        if (standard_value == "time" || standard_value == "latitude" ||
            standard_value == "longitude" ||
            standard_value == "grid_latitude" ||
            standard_value == "grid_longitude" || axis_value == "T" ||
            axis_value == "X" || axis_value == "Y")
          throw std::runtime_error(
              "CF declared axis metadata is unavailable or unsupported");
        continue;
      }
      if (!scale->coordinate_index)
        throw std::runtime_error("CF coordinate values are unavailable for " +
                                 scale->canonical_path + ": " +
                                 scale->coordinate_index_reason);
      const auto &c = *scale->coordinate_index;
      if (c.role != metadata.value().role ||
          c.units != metadata.value().units ||
          c.calendar != metadata.value().calendar ||
          c.values.size() != object->dataset->descriptor.shape[a.axis] ||
          !roles.insert(c.role).second || !dimensions.insert(a.axis).second)
        throw std::runtime_error(
            "CF coordinate shape, semantic evidence, or uniqueness mismatch");
      for (std::size_t i = 0; i < c.values.size(); ++i) {
        if (!std::isfinite(c.values[i]) ||
            (c.role == "latitude" && (c.values[i] < -90 || c.values[i] > 90)) ||
            (i && !(c.ascending ? c.values[i] > c.values[i - 1]
                                : c.values[i] < c.values[i - 1])))
          throw std::runtime_error(
              "CF coordinate values are invalid or nonmonotonic");
      }
      result.axes.push_back({a.axis, c.role, scale->id, c.units, c.calendar,
                             a.evidence, c.values, c.ascending});
    }
    if (result.axes.empty())
      throw std::runtime_error(
          "CF dataset has no validated time/latitude/longitude axes");
    if (auto a = cf_attribute(*object, "units"))
      result.units = cf_string(a);
    if (auto a = cf_attribute(*object, "scale_factor"))
      result.scale_factor = cf_scalar(*a);
    if (auto a = cf_attribute(*object, "add_offset"))
      result.add_offset = cf_scalar(*a);
    for (const char *name : {"_FillValue", "missing_value"})
      if (auto a = cf_attribute(*object, name)) {
        auto v = cf_numbers(*a, true);
        if (std::string(name) == "_FillValue" && v.size() != 1)
          throw std::runtime_error("CF fill value must be scalar");
        result.missing_values.insert(result.missing_values.end(), v.begin(),
                                     v.end());
      }
    const auto range = cf_attribute(*object, "valid_range");
    const auto lo = cf_attribute(*object, "valid_min"),
               hi = cf_attribute(*object, "valid_max");
    if (range && (lo || hi))
      throw std::runtime_error("CF valid_range conflicts with valid_min/max");
    if (range) {
      auto v = cf_numbers(*range);
      if (v.size() != 2)
        throw std::runtime_error("CF valid_range requires two values");
      result.valid_min = v[0];
      result.valid_max = v[1];
    }
    if (lo)
      result.valid_min = cf_scalar(*lo);
    if (hi)
      result.valid_max = cf_scalar(*hi);
    if (result.valid_min && result.valid_max &&
        *result.valid_min > *result.valid_max)
      throw std::runtime_error("CF valid range is reversed");
    std::sort(
        result.axes.begin(), result.axes.end(),
        [](const auto &a, const auto &b) { return a.dimension < b.dimension; });
    return R::success(std::move(result));
  } catch (const std::exception &e) {
    return R::failure(cf_error(e));
  }
}
} // namespace iotdbfs
