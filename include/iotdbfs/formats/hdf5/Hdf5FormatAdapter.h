#pragma once

#include "iotdbfs/DatasetCatalog.h"
#include "iotdbfs/FormatAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5Layout.h"
#include "iotdbfs/formats/hdf5/Hdf5SourceDriver.h"

namespace iotdbfs {

DatasetCatalogRecord
hdf5_dataset_catalog_record(const Hdf5LayoutObject &,
                            const std::string &canonical_path);

// H03 content detection, metadata inventory and persisted format identity.
// Original file bytes are copied only by IngestionModule. This adapter does
// not register a typed query implementation or decode dataset value payloads.
class Hdf5FormatAdapter final : public FormatAdapter {
public:
  explicit Hdf5FormatAdapter(Hdf5LayoutLimits limits = {},
                             Hdf5SourceOptions source_options = {});
  const std::string &id() const override;
  const std::vector<std::string> &extensions() const override;
  bool matches(const FormatSource &source) const override;
  std::unique_ptr<FormatIngestionSession>
  begin_ingestion(const FormatSource &source) const override;
  FormatStatus validate_layout_artifact(const std::vector<std::uint8_t>& layout,
                                       std::uint64_t payload_size) const override;
  std::optional<std::string> get_xattr(long long row_time,
                                       const std::string &path,
                                       const Inode &inode,
                                       const std::string &name) const override;
  FormatXattrList list_xattrs(long long row_time, const std::string &path,
                              const Inode &inode) const override;

private:
  Hdf5LayoutLimits limits_;
  Hdf5SourceOptions source_options_;
};

} // namespace iotdbfs
