#pragma once

#include "iotdbfs/formats/arrow/ArrowQuerySubject.h"
#include "iotdbfs/query/core/QueryPlanning.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace iotdbfs {

// Arrow List structure discovered from the native schema, independently of dataset annotations.
// A02 supports one or two List levels with float32 leaves. Nullability is
// represented by the offset reader and the leaf validity bitmap, not inferred
// from the physical coordinates.
enum class PhysicalArrayLeafType { Float32, Float64 };

struct PhysicalArrayShape {
  std::uint32_t list_depth{0};
  PhysicalArrayLeafType leaf_type{PhysicalArrayLeafType::Float32};
};

struct ArrayElementRange {
  std::uint64_t begin{0};
  std::uint64_t end{0};
};

struct ArrayListSpan {
  std::uint64_t begin{0};
  std::uint64_t end{0};
  bool valid{true};
};

// A format adapter supplies bounded offset/validity reads. This interface
// does not require all offsets, or any float values, to be decoded at once.
class ArrayListIndex {
public:
  virtual ~ArrayListIndex() = default;
  virtual std::uint32_t list_depth() const = 0;
  virtual std::uint64_t list_count(std::uint32_t level) const = 0;
  virtual std::uint64_t leaf_count() const = 0;
  virtual QueryResult<ArrayListSpan> list_span(
    std::uint32_t level, std::uint64_t index) const = 0;
};

struct ResolvedArrayValue {
  // A null List stops traversal. null_list_level identifies the first null
  // List (0 is the row); child ordinals beneath it have no observable bound.
  // An empty, valid List has valid=true, no null level, and begin=end.
  bool valid{true};
  std::uint64_t begin{0};
  std::uint64_t end{0};
  std::optional<std::uint32_t> null_list_level;
};

// Physical binding only. Null levels refer to native List nesting, and never
// imply a missing channel or time series. Values follow subject.values order.
struct ArrowPhysicalBinding {
  PhysicalArraySubject subject;
  std::vector<ResolvedArrayValue> values;
  std::uint64_t length{0};
};

// Positional logical schema/capabilities for a validated physical binding.
// These are format contracts, independent of layout I/O and cursor execution.
std::string arrow_query_value_field_id(const ArrayValuePath& path);
QueryResult<LogicalSchema> make_arrow_logical_schema(
  const ArrowPhysicalBinding& binding);
QueryResult<QueryCapabilities> make_arrow_query_capabilities(
  const ArrowPhysicalBinding& binding);

QueryStatus validate_physical_array_subject(
  const PhysicalArraySubject& subject);
QueryStatus validate_physical_array_shape(const PhysicalArrayShape& shape);

// Resolves a row/path to the innermost float range. The optional range is
// relative to that innermost List; it is positional and has no time semantics.
QueryResult<ResolvedArrayValue> resolve_array_value(
  const PhysicalArrayShape& shape, const ArrayListIndex& index,
  std::uint64_t row_index, const ArrayValuePath& path,
  const std::optional<ArrayElementRange>& range = std::nullopt);

} // namespace iotdbfs
