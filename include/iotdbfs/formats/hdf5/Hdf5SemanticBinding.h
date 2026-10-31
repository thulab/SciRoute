#pragma once

#include "iotdbfs/query/core/QueryPlanning.h"
#include <memory>
#include <optional>

namespace iotdbfs {

inline constexpr char kHdf5GpsSecondsField[] = "hdf5.coord.gps_seconds";
inline constexpr char kHdf5GwoscProfile[] = "gwosc-gps-seconds/1";

// Explicit application declaration. A path name or attribute name alone never
// enables scientific semantics. Namespace + exact source version prevent this
// declaration from silently applying to a different storage backend. This spec
// declares GPS time in seconds; unavailable variable-length unit/label values
// are never read and are recorded as application-declared, not verified.
struct Hdf5GwoscBindingSpec {
  StructuredBlobVersionKey source;
  std::string dataset_path;
  std::uint64_t storage_namespace{0};
};
struct Hdf5SemanticConfig {
  std::vector<Hdf5GwoscBindingSpec> gwosc_bindings;
};

// Validated, immutable CF rectilinear evidence. Coordinates retain the source
// order and native time units/calendar; no UTC conversion or resampling.
struct Hdf5CfAxis {
  std::uint32_t dimension{0};
  std::string role, coordinate_object_id, units, calendar, evidence;
  std::vector<double> values;
  bool ascending{true};
};
struct Hdf5CfBinding {
  std::vector<Hdf5CfAxis> axes;
  double scale_factor{1}, add_offset{0};
  std::vector<double> missing_values;
  std::optional<double> valid_min, valid_max;
  std::string units, profile{"cf-rectilinear/1"};
};
struct Hdf5LayoutObject;
struct Hdf5CoordinateIndex;
struct Hdf5LayoutDocument;
class Hdf5SemanticProfile {
public:
  virtual ~Hdf5SemanticProfile() = default;
  virtual std::string id() const = 0;
  virtual bool declares(const Hdf5LayoutDocument &) const = 0;
  virtual QueryResult<Hdf5CoordinateIndex>
  inspect_coordinate(const Hdf5LayoutObject &) const = 0;
  virtual QueryResult<Hdf5CfBinding>
  compile(const Hdf5LayoutDocument &, const std::string &dataset_object_id) const = 0;
};
// Profiles live in the format layer. The query core sees only bound fields,
// ordinal ranges and the resulting schema; applications can replace this list.
std::vector<std::shared_ptr<const Hdf5SemanticProfile>>
default_hdf5_semantic_profiles();
bool hdf5_declares_cf(const Hdf5LayoutDocument &);
bool hdf5_declares_geos_pressure(const Hdf5LayoutDocument &);
QueryResult<Hdf5CoordinateIndex>
inspect_hdf5_cf_coordinate(const Hdf5LayoutObject &);
QueryResult<Hdf5CoordinateIndex>
inspect_hdf5_geos_pressure_coordinate(const Hdf5LayoutObject &);
QueryResult<Hdf5CfBinding>
compile_hdf5_cf_binding(const Hdf5LayoutDocument &,
                        const std::string &dataset_object_id);
QueryResult<Hdf5CfBinding>
compile_hdf5_geos_pressure_binding(const Hdf5LayoutDocument &,
                                   const std::string &dataset_object_id);
// Only the verified GEOS-5.7.2 /T dimension-scale profile yields a UTC
// instant. GEOS product specification declares the 3-hourly samples in UTC.
// Other CF calendars and ambiguous reference-time syntax remain unindexed.
std::optional<std::int64_t>
verified_hdf5_geos_utc_second(const Hdf5LayoutDocument &);

// An immutable rank-one GPS-seconds grid. H07 supports exact dyadic grids only:
// every coordinate is representable in Float64 and strictly increasing. This
// is GPS elapsed seconds, without leap-second, UTC or nanosecond conversion.
struct Hdf5LayoutObject;
class Hdf5GpsTimeAxis {
public:
  static QueryResult<Hdf5GpsTimeAxis> create(double start, double spacing,
                                             std::uint64_t count);
  double start() const { return start_; }
  double spacing() const { return spacing_; }
  std::uint64_t count() const { return count_; }
  bool units_verified() const { return units_verified_; }
  bool label_verified() const { return label_verified_; }
  double coordinate(std::uint64_t ordinal) const;
  // Finds the first coordinate >= value, or count(). No division/ceil rounding.
  // Infinite values are outside-grid sentinels; NaN throws invalid_argument.
  std::uint64_t lower_bound(double value) const;

private:
  friend QueryResult<Hdf5GpsTimeAxis>
  compile_hdf5_gwosc_axis(const Hdf5LayoutObject &);
  Hdf5GpsTimeAxis(double start, double spacing, std::uint64_t count)
      : start_(start), spacing_(spacing), count_(count) {}
  double start_, spacing_;
  std::uint64_t count_;
  bool units_verified_{false}, label_verified_{false};
};

QueryResult<Hdf5GpsTimeAxis>
compile_hdf5_gwosc_axis(const Hdf5LayoutObject &object);

} // namespace iotdbfs
