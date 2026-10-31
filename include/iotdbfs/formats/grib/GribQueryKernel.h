#pragma once

#include "iotdbfs/formats/grib/GribLayout.h"
#include "iotdbfs/formats/grib/GribQueryModel.h"
#include "iotdbfs/formats/grib/gribUtils.h"

#include <cstddef>

namespace iotdbfs {

enum class GribResultKind { GridValues, SourceMessage };

struct GribTypedOutputPlan {
  GribResultKind kind{ GribResultKind::GridValues };
  grib_utils::DataDType value_type{ grib_utils::DataDType::F32 };
  grib_utils::InterpParams interpolation;
};

QueryResult<GribTypedOutputPlan> plan_grib_typed_output(
  const QueryFragment& fragment);

struct GribGridPlan {
  grib_utils::MsgInfo message;
  grib_utils::DataDType value_type{ grib_utils::DataDType::F32 };
  grib_utils::CropParams crop;
  std::vector<DomainSlice> slices;
  grib_utils::InterpParams interpolation;
  std::string cache_token;
};

struct GribGridResult {
  std::vector<double> latitude_axis;
  std::vector<double> longitude_axis;
  LogicalArray values;
  DomainDescriptor domain;
};

// Immutable native grid retained by the typed GRIB query cache. Values use a
// canonical Float64 representation so output dtype, crop, and interpolation
// do not create separate cache entries for the same encoded message.
struct GribNativeField {
  std::uint64_t ni{0};
  std::uint64_t nj{0};
  double longitude_increment{0.0};
  double latitude_increment{0.0};
  std::vector<double> latitude_axis;
  std::vector<double> longitude_axis;
  std::vector<double> values;

  std::size_t memory_bytes() const;
};

// Adapter-private execution Module. It accepts only a fixed QuerySource and an
// already selected message, preserves storage failures, and returns stable
// typed grid state instead of FUSE-oriented byte buffers.
class GribQueryKernel {
public:
  static QueryResult<GribLayoutDocument> read_layout(
    const QuerySource& source);
  static QueryResult<BinaryValue> read_source_message(
    const QuerySource& source, const grib_utils::MsgInfo& message);
  static QueryResult<GribNativeField> decode_native_field(
    const QuerySource& source, const GribGridPlan& plan);
  static QueryResult<GribGridResult> render_grid(
    const QuerySource& source, const GribGridPlan& plan,
    const GribNativeField& field);
  static QueryResult<GribGridResult> decode_grid(
    const QuerySource& source, const GribGridPlan& plan);
};

} // namespace iotdbfs
