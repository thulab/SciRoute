#pragma once

#include "iotdbfs/StructuredBlobStorage.h"

namespace iotdbfs {

inline constexpr const char* kDatasetSubstructureIndexProtocol =
  "iotdbfs-dataset-substructure-index/1";

// An immutable catalog's publication envelope. The digest covers canonical
// catalog bytes AND the exact source identity; it is not an authenticity MAC.
// Namespace isolation is supplied by the backend's process-local cache key.
struct DatasetSubstructureSeal {
  std::string digest;
  std::uint64_t bytes{0}, records{0}, attributes{0};
};

// Called after deriving the full catalog and before committing Described.
// The published phase/revision and unrelated metadata are deliberately not
// hashed: the envelope survives publication and POSIX permission changes.
StructuredBlobStatus seal_dataset_substructure_index(
  StructuredBlobManifest&, const DatasetCatalogArtifact&);
StructuredBlobResult<DatasetSubstructureSeal> read_dataset_substructure_seal(
  const StructuredBlobManifest&);
StructuredBlobStatus validate_dataset_substructure_index(
  const StructuredBlobManifest&, const DatasetCatalogArtifact&);

} // namespace iotdbfs
