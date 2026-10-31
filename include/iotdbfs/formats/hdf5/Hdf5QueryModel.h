#pragma once

#include "iotdbfs/formats/hdf5/Hdf5SemanticBinding.h"

namespace iotdbfs {

inline constexpr char kHdf5ValueFieldPrefix[] = "hdf5.value:";
inline constexpr char kHdf5ScienceValueFieldPrefix[] = "hdf5.science.value:";
inline constexpr char kHdf5TimeField[] = "hdf5.coord.time";
inline constexpr char kHdf5LatitudeField[] = "hdf5.coord.latitude";
inline constexpr char kHdf5LongitudeField[] = "hdf5.coord.longitude";
inline constexpr char kHdf5PressureField[] = "hdf5.coord.pressure";
inline constexpr char kHdf5AxisFieldPrefix[] = "hdf5.axis.";
inline constexpr std::size_t kHdf5MaxDatasetPathBytes = 4096;

// Field IDs preserve valid UTF-8 bytes without Unicode normalization. Only URI
// unreserved bytes remain literal; every other byte has an uppercase %XX form.
QueryResult<FieldRef> make_hdf5_value_field(const std::string& dataset_path);
QueryResult<FieldRef> make_hdf5_attribute_field(const std::string& attribute_name);
QueryResult<FieldRef> make_hdf5_science_value_field(const std::string& dataset_path);
bool is_hdf5_science_value_field(const FieldRef& field);
QueryResult<std::string> parse_hdf5_value_field(const FieldRef& field);
// Throws std::out_of_range for indices outside the H01 ordinal axes 0..3.
FieldRef hdf5_axis_field(std::size_t axis);

struct Hdf5DatasetDescriptor {
  std::string path;
  LogicalTypeId value_type{LogicalTypeId::Float32};
  std::vector<std::uint64_t> shape;
  bool supported{true};
  std::string unsupported_reason;
};

// Dataset metadata seam. No native-library handles or value reads cross it.
class Hdf5DatasetProvider {
public:
  virtual ~Hdf5DatasetProvider() = default;
  virtual QueryResult<Hdf5DatasetDescriptor> describe(
    const std::string& dataset_path) const = 0;
};

bool has_hdf5_query_fields(const QueryFragment& fragment);
// No-op for non-HDF5 requests. Checks the exact-source requirement and request
// grammar before catalog/schema filtering; descriptor bounds are checked below.
QueryStatus validate_hdf5_query_request(const QueryRequest& request);

class Hdf5QueryBinding {
public:
  const Hdf5DatasetDescriptor& descriptor() const { return descriptor_; }
  const FieldRef& value_field() const { return value_field_; }
  const LogicalSchema& schema() const { return schema_; }
  const std::vector<FieldRef>& projection() const { return projection_; }
  const std::vector<std::uint64_t>& origin() const { return origin_; }
  const std::vector<std::uint64_t>& shape() const { return shape_; }
  std::uint64_t row_count() const { return row_count_; }
  std::uint64_t projected_row_bytes() const { return projected_row_bytes_; }
  const std::optional<Hdf5GpsTimeAxis>& gps_axis() const { return gps_axis_; }
  const std::optional<Hdf5CfBinding>& cf_binding() const { return cf_binding_; }
  bool scientific_values() const { return is_hdf5_science_value_field(value_field_); }

private:
  friend QueryResult<Hdf5QueryBinding> bind_hdf5_query(
    const Hdf5DatasetProvider&, const QueryFragment&, const QueryLimits&,
    std::optional<Hdf5GpsTimeAxis>, std::optional<Hdf5CfBinding>,
    std::vector<LogicalField>, bool);
  Hdf5QueryBinding(Hdf5DatasetDescriptor descriptor, FieldRef value_field,
    LogicalSchema schema, std::vector<FieldRef> projection,
    std::vector<std::uint64_t> origin, std::vector<std::uint64_t> shape,
    std::uint64_t row_count, std::uint64_t projected_row_bytes,
    std::optional<Hdf5GpsTimeAxis> gps_axis, std::optional<Hdf5CfBinding> cf_binding);
  Hdf5DatasetDescriptor descriptor_;
  FieldRef value_field_;
  LogicalSchema schema_;
  std::vector<FieldRef> projection_;
  std::vector<std::uint64_t> origin_, shape_;
  std::uint64_t row_count_, projected_row_bytes_;
  std::optional<Hdf5GpsTimeAxis> gps_axis_;
  std::optional<Hdf5CfBinding> cf_binding_;
};

// Binds the value field to a dataset before building the full logical schema.
// Exactly one value field is mandatory; optional axis projections retain their
// request order. Physical slices are UInt64 [inclusive, exclusive), without
// coercion. An explicitly supplied rank-one GPS grid additionally enables
// Float64 GPS-seconds slices and projection. The distinct science value view
// requires compiled CF evidence, emits nullable Float64 after raw-domain masking
// and scaling, and enables original-unit coordinate slices. Domains always
// retain physical ordinals and source axis order. Attribute predicates are
// evaluated before binding; predicate_matches=false preserves a zero-row schema.
QueryResult<Hdf5QueryBinding> bind_hdf5_query(
  const Hdf5DatasetProvider& provider, const QueryFragment& fragment,
  const QueryLimits& limits = {},
  std::optional<Hdf5GpsTimeAxis> gps_axis = std::nullopt,
  std::optional<Hdf5CfBinding> cf_binding = std::nullopt,
  std::vector<LogicalField> attribute_fields = {}, bool predicate_matches = true);
QueryResult<QueryCapabilities> make_hdf5_query_capabilities(
  const Hdf5QueryBinding& binding);

struct Hdf5Slab {
  DomainDescriptor domain;
  std::uint64_t row_count{0};
};

// Each lazy result is a genuine rectangle. Concatenation follows C order even
// when the byte/row target forces splits within the innermost axis. Empty
// selections yield no slabs; the binding still supplies the result schema.
// An optional storage alignment only shortens splits within those targets;
// it never expands the ROI, reorders rows, or promises fixed batch boundaries.
class Hdf5SlabIterator {
public:
  static QueryResult<Hdf5SlabIterator> create(
    const Hdf5QueryBinding& binding, const QueryLimits& limits = {},
    const std::vector<std::uint64_t>& alignment = {});
  QueryResult<std::optional<Hdf5Slab>> next();

private:
  Hdf5SlabIterator(const Hdf5QueryBinding& binding,
    std::uint64_t capacity, const std::vector<std::uint64_t>& alignment);
  std::vector<std::uint64_t> origin_, shape_, position_;
  std::uint64_t capacity_, suffix_rows_{1};
  std::uint64_t alignment_{1};
  std::size_t pivot_{0};
  bool done_{false};
};

} // namespace iotdbfs
