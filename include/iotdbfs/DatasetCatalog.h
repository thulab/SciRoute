#pragma once
#include "iotdbfs/MetadataConfig.h"
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace iotdbfs {
using DatasetAttributeValue =
    std::variant<std::string, std::int64_t, std::uint64_t, double, bool>;
enum class DatasetAttributeState { Complete, Unsupported, TooLarge, Invalid };
struct DatasetCatalogAttribute {
  std::string name;
  DatasetAttributeState state{DatasetAttributeState::Complete};
  std::optional<DatasetAttributeValue> value;
  std::vector<std::uint64_t> shape;
  std::string reason;
};
struct DatasetCatalogRecord {
  std::string object_id;
  std::string canonical_path;
  bool attributes_complete{true};
  std::vector<DatasetCatalogAttribute> attributes;
};
struct DatasetCatalogArtifact {
  std::string protocol{"iotdbfs-dataset-catalog/1"};
  std::string format_id;
  std::string layout_protocol;
  std::string layout_digest;
  bool directory_complete{true};
  std::vector<DatasetCatalogRecord> records;
};
inline constexpr std::size_t kMaxDatasetCatalogBytes = 16ULL * 1024 * 1024;
MetadataResult<bool> validate_dataset_catalog(const DatasetCatalogArtifact &);
MetadataResult<std::vector<std::uint8_t>>
serialize_dataset_catalog(const DatasetCatalogArtifact &);
MetadataResult<DatasetCatalogArtifact>
deserialize_dataset_catalog(const std::vector<std::uint8_t> &);
// Stable type labels and exact scalar encoding for backend rows. Unsigned
// values use fixed-width decimal so database STRING ordering preserves all 64
// bits.
std::string dataset_attribute_type(const DatasetAttributeValue &);
std::string dataset_attribute_text(const DatasetAttributeValue &);
} // namespace iotdbfs
