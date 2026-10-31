#pragma once

#include "iotdbfs/query/core/QueryPlanning.h"

namespace iotdbfs {
namespace grib_query_fields {

inline constexpr char kMessageBytes[] = "grib.message.bytes";
inline constexpr char kShortName[] = "grib.message.short_name";
inline constexpr char kTypeOfLevel[] = "grib.message.type_of_level";
inline constexpr char kLevel[] = "grib.message.level";
inline constexpr char kStep[] = "grib.message.step";
inline constexpr char kStepRange[] = "grib.message.step_range";
inline constexpr char kEdition[] = "grib.message.edition";
inline constexpr char kUnit[] = "grib.message.unit";
inline constexpr char kParamId[] = "grib.message.param_id";
inline constexpr char kValidityDate[] = "grib.message.validity_date";
inline constexpr char kValidityTime[] = "grib.message.validity_time";
inline constexpr char kOffset[] = "grib.message.offset";
inline constexpr char kLatitude[] = "grib.grid.latitude";
inline constexpr char kLongitude[] = "grib.grid.longitude";
inline constexpr char kValue[] = "grib.data.value";

} // namespace grib_query_fields

// Stable first-generation GRIB query contract. The schema names scientific
// fields and dimensions; offsets, messages, ecCodes handles, and layout keys
// remain private to the GRIB Implementation.
QueryResult<LogicalSchema> make_grib_logical_schema();
QueryResult<QueryCapabilities> make_grib_query_capabilities();

} // namespace iotdbfs
