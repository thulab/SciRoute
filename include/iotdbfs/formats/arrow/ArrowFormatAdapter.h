#pragma once

#include "iotdbfs/formats/arrow/ArrowLayout.h"
#include "iotdbfs/FormatAdapter.h"

namespace iotdbfs {

class ArrowFormatAdapter final : public FormatAdapter {
public:
  explicit ArrowFormatAdapter(ArrowLayoutLimits limits = {});
  const std::string& id() const override;
  const std::vector<std::string>& extensions() const override;
  bool matches(const FormatSource& source) const override;
  std::unique_ptr<FormatIngestionSession> begin_ingestion(
    const FormatSource& source) const override;
  FormatStatus validate_layout_artifact(const std::vector<std::uint8_t>& layout,
    std::uint64_t payload_size) const override;
  std::optional<std::string> get_xattr(long long row_time,
    const std::string& path, const Inode& inode,
    const std::string& name) const override;
  FormatXattrList list_xattrs(long long row_time,
    const std::string& path, const Inode& inode) const override;
private:
  ArrowLayoutLimits limits_;
};

} // namespace iotdbfs
