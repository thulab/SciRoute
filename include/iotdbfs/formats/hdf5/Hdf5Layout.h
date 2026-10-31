#pragma once

#include "iotdbfs/formats/hdf5/Hdf5QueryModel.h"
#include <array>
#include <functional>
#include <map>
#include <memory>
#include <optional>

namespace iotdbfs {
inline constexpr char kHdf5LayoutMagic[] = "IOTDBFS-HDF5-LAYOUT/5\n";
inline constexpr char kHdf5PreviousLayoutMagic[] = "IOTDBFS-HDF5-LAYOUT/4\n";
inline constexpr std::size_t kHdf5LayoutHeaderBytes = sizeof(kHdf5LayoutMagic) - 1 + 16 + 32;
using Hdf5LayoutDigest = std::array<std::uint8_t, 32>;
enum class Hdf5ObjectKind : std::uint8_t { Group, Dataset, Other };
enum class Hdf5LinkKind : std::uint8_t { Hard, Soft, External, Other };
enum class Hdf5StorageLayout : std::uint8_t {
  Contiguous,
  Chunked,
  Compact,
  Virtual,
  Other
};
enum class Hdf5ByteOrder : std::uint8_t { Little, Big, Other };
enum class Hdf5FillStatus : std::uint8_t { Undefined, Default, Explicit };
struct Hdf5FilterDescription {
  std::uint32_t id{0}, flags{0};
  std::vector<std::uint32_t> parameters;
};
struct Hdf5ChunkRecord {
  std::vector<std::uint64_t> origin;
  std::uint64_t offset{0}, encoded_size{0};
  std::uint32_t filter_mask{0};
};
struct Hdf5DatasetLayout {
  Hdf5DatasetDescriptor descriptor;
  Hdf5StorageLayout storage{Hdf5StorageLayout::Other};
  Hdf5ByteOrder byte_order{Hdf5ByteOrder::Other};
  std::uint32_t native_type_class{0};
  std::uint64_t element_bytes{0};
  Hdf5FillStatus fill_status{Hdf5FillStatus::Undefined};
  // Canonical little-endian scalar bytes, empty for undefined/unsupported
  // types.
  BinaryValue fill_value;
  std::uint32_t allocation_time{0}, fill_time{0}, allocation_status{0};
  std::optional<std::uint64_t> contiguous_offset;
  std::uint64_t contiguous_size{0};
  // Native H5Pget_chunk_opts: bit 0x2 bypasses filters on partial edge chunks.
  std::uint32_t chunk_options{0};
  std::vector<std::uint64_t> chunk_shape;
  std::vector<Hdf5FilterDescription> filters;
  bool index_complete{false};
  std::string index_unavailable_reason;
  // Native allocated count is independent of records, to detect lost records.
  std::uint64_t allocated_chunk_count{0};
  std::vector<Hdf5ChunkRecord> chunks;
};
// Attribute values are independent evidence, never implicit scientific
// semantics.
enum class Hdf5AttributeState : std::uint8_t {
  Complete,
  UnsupportedType,
  TooLarge,
  InvalidValue,
  UnsupportedVariableLength
};
struct Hdf5Attribute {
  std::string name;
  LogicalTypeId type{LogicalTypeId::Binary};
  std::vector<std::uint64_t> shape;
  bool null_dataspace{false};
  std::uint32_t native_type_class{0};
  std::uint64_t native_element_bytes{0};
  Hdf5AttributeState state{Hdf5AttributeState::UnsupportedType};
  std::string reason;
  // Int64 / UInt64 / Float64: canonical little-endian 8-byte elements.
  BinaryValue values;
  // Fixed native strings, normalized according to their declared padding.
  std::vector<std::string> strings;
};
struct Hdf5DimensionScaleCandidate {
  // Reverse REFERENCE_LIST evidence; not proof of a consistent forward link.
  std::string dataset_object_id;
  std::uint32_t axis{0};
};
struct Hdf5AxisAssociation {
  std::uint32_t axis{0};
  std::string scale_object_id;
  // Only a validated forward legacy local-object reference is admitted.
  std::string evidence;
};
struct Hdf5CoordinateIndex {
  std::string role, units, calendar;
  std::vector<double> values;
  bool ascending{true};
};
struct Hdf5LayoutObject {
  std::string id; // Native object token, scoped to this exact immutable source.
  Hdf5ObjectKind kind{Hdf5ObjectKind::Other};
  std::optional<Hdf5DatasetLayout> dataset;
  std::uint64_t attribute_count{0};
  bool attribute_count_known{true};
  bool attributes_complete{true}; // Complete name directory, not all values.
  std::string attributes_unavailable_reason;
  std::vector<Hdf5Attribute> attributes;
  bool dimension_scales_complete{true};
  std::string dimension_scales_unavailable_reason;
  std::vector<Hdf5DimensionScaleCandidate> dimension_scales;
  bool axis_associations_complete{false};
  std::string axis_associations_reason{"not_inspected"};
  std::vector<Hdf5AxisAssociation> axis_associations;
  std::optional<Hdf5CoordinateIndex> coordinate_index;
  std::string coordinate_index_reason{"not_coordinate"};
  std::string canonical_path;
};
struct Hdf5LayoutLink {
  std::string parent_id, name;
  Hdf5LinkKind kind{Hdf5LinkKind::Hard};
  std::string
      target_id; // Only hard links resolve; other links remain inventory.
  std::string unsupported_reason;
};
struct Hdf5LayoutDocument {
  std::uint64_t source_size{0};
  std::string file_profile{"HDF5"}, parser_version{"1.14.4"}, root_object_id;
  bool directory_complete{true};
  std::string index_unavailable_reason;
  std::vector<Hdf5LayoutObject> objects;
  std::vector<Hdf5LayoutLink> links;
};
struct Hdf5LayoutLimits {
  std::uint64_t max_layout_bytes{64ULL * 1024 * 1024};
  std::uint64_t max_directory_bytes{8ULL * 1024 * 1024};
  std::uint64_t max_read_bytes{64ULL * 1024 * 1024};
  std::size_t max_read_calls{65536}, max_page_scans{1048576};
  std::size_t max_objects{65536}, max_links{131072}, max_pages{65536};
  std::size_t max_chunks{1000000}, chunks_per_page{256};
  // Optional metadata has its own limits; exhausting them preserves chunk
  // indexes.
  std::uint64_t max_attribute_bytes{64ULL * 1024},
      max_metadata_bytes{2ULL * 1024 * 1024};
  std::size_t max_attributes_per_object{256}, max_attributes{16384},
      max_attribute_elements{1024}, max_dimension_scale_candidates{4096};
  std::size_t max_coordinate_elements{1048576};
  std::uint64_t max_coordinate_bytes{8ULL * 1024 * 1024};
  std::size_t max_string_bytes{4096}, max_depth{256},
      max_filter_parameters{1024};
};
struct Hdf5ChunkPage {
  std::string object_id;
  std::uint64_t offset{0}, length{0}, record_count{0};
  std::vector<std::uint64_t> lower, upper, first_origin, last_origin;
  Hdf5LayoutDigest checksum{};
};
// One checksum- and bounds-validated page, independent of query selection.
struct Hdf5ValidatedChunkPage {
  std::vector<Hdf5ChunkRecord> records;
  std::uint64_t retained_bytes() const;
};
struct Hdf5ChunkPageCache {
  std::function<std::shared_ptr<const Hdf5ValidatedChunkPage>(const Hdf5ChunkPage&)> find;
  std::function<void(const Hdf5ChunkPage&,
    std::shared_ptr<const Hdf5ValidatedChunkPage>)> insert;
};
// Directory datasets contain no chunk records; records are separately paged.
struct Hdf5LayoutReadBudget {
  std::uint64_t bytes{0}, read_calls{0}, page_scans{0};
};
using Hdf5LayoutRead =
    std::function<QueryResult<BinaryValue>(std::uint64_t, std::uint64_t)>;
class Hdf5LayoutDirectory {
public:
  std::uint32_t protocol_version() const { return protocol_version_; }
  const Hdf5LayoutDocument &document() const { return document_; }
  const std::vector<Hdf5ChunkPage> &pages() const { return pages_; }
  std::uint64_t layout_size() const { return layout_size_; }
  std::uint64_t directory_read_bytes() const { return bytes_read_; }
  std::uint64_t directory_read_calls() const { return read_calls_; }
  std::uint64_t directory_page_scans() const { return pages_.size(); }
  const Hdf5LayoutDigest &checksum() const { return checksum_; }
  const Hdf5LayoutObject* object(const std::string& id) const;
  const Hdf5LayoutLink* link(const std::string& parent_id,
    const std::string& name) const;
  // Conservative retained storage accounting, including nested capacities.
  std::uint64_t retained_bytes() const;

private:
  std::uint32_t protocol_version_{5};
  Hdf5LayoutDocument document_;
  std::uint64_t layout_size_{0}, bytes_read_{0}, read_calls_{0};
  Hdf5LayoutDigest checksum_{};
  std::vector<Hdf5ChunkPage> pages_;
  std::map<std::string, std::size_t> object_lookup_;
  std::map<std::pair<std::string, std::string>, std::size_t> link_lookup_;
  friend QueryResult<Hdf5LayoutDirectory>
  read_hdf5_layout_directory(const Hdf5LayoutRead &, std::uint64_t,
                             std::uint64_t, const Hdf5LayoutLimits &);
};
bool valid_hdf5_metadata_text(const std::string &);
QueryStatus validate_hdf5_layout(const Hdf5LayoutDocument &,
                                 const Hdf5LayoutLimits & = {});
QueryResult<BinaryValue> serialize_hdf5_layout(const Hdf5LayoutDocument &,
                                               const Hdf5LayoutLimits & = {});
QueryResult<Hdf5LayoutDirectory>
read_hdf5_layout_directory(const Hdf5LayoutRead &, std::uint64_t layout_size,
                           std::uint64_t source_size,
                           const Hdf5LayoutLimits & = {});
// Reads only pages whose bounding boxes intersect the selection. Success proves
// all candidate pages were read and checked; absent chunks are sparse only when
// dataset.index_complete is true. budget includes directory reads and
// structural page scans.
QueryResult<std::vector<Hdf5ChunkRecord>> read_hdf5_layout_chunks(
    const Hdf5LayoutRead &, const Hdf5LayoutDirectory &,
    const std::string &object_id, const std::vector<std::uint64_t> &origin,
    const std::vector<std::uint64_t> &shape, Hdf5LayoutReadBudget &budget,
    const Hdf5LayoutLimits & = {}, const Hdf5ChunkPageCache* cache = nullptr);
QueryResult<std::string> resolve_hdf5_layout_path(const Hdf5LayoutDirectory &,
                                                  const std::string &path);
} // namespace iotdbfs
