#pragma once

#include "iotdbfs/FormatAdapter.h"
#include "iotdbfs/formats/grib/GribExtensionService.h"
#include "iotdbfs/formats/grib/GribWriteStreamer.h"

namespace iotdbfs {


class GribFormatAdapter final : public FormatAdapter {
public:
  struct Config {
    GribWriteStreamer::Config ingestion;
    GribExtensionService::Dependencies queries;
  };

  explicit GribFormatAdapter(Config config);

  const std::string& id() const override;
  const std::vector<std::string>& extensions() const override;
  bool matches(const FormatSource& source) const override;
  std::unique_ptr<FormatIngestionSession> begin_ingestion(
    const FormatSource& source) const override;

  FormatStatus validate_layout_artifact(const std::vector<std::uint8_t>& layout,
    std::uint64_t payload_size) const override;

  FormatQueryPtr parse_query(const std::string& query) const override;
  bool can_handle_virtual_query(const std::string& path) const override;
  FormatVirtualAttr get_virtual_attr(long long row_time,
    const std::string& path, const FormatQuery& query,
    const std::string& raw_query) const override;
  FormatOpenResult open_virtual(long long row_time,
    const std::string& path, const FormatQuery& query,
    const std::string& raw_query) const override;
  std::optional<std::string> get_xattr(long long row_time,
    const std::string& path, const Inode& inode,
    const std::string& name) const override;
  FormatXattrList list_xattrs(long long row_time,
    const std::string& path, const Inode& inode) const override;

private:
  Config config_;
  GribExtensionService query_service_;
};

} // namespace iotdbfs
