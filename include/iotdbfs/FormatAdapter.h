#pragma once

#include "iotdbfs/InodeTypes.h"
#include "iotdbfs/DatasetCatalog.h"
#include "iotdbfs/MetadataConfig.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace iotdbfs {

struct FormatSource {
  std::uint64_t size{ 0 };
  // The bytes must remain stable and repeatable throughout synchronous
  // ingestion. During content detection, an out-of-range request means that
  // candidate format does not match. Real short/failed reads remain fatal;
  // selected ingestion sessions must use valid, exact ranges.
  std::function<std::optional<std::vector<std::uint8_t>>(
    std::uint64_t offset, std::uint64_t length)> read;
};

// Format-derived state produced during ingestion. The payload is deliberately
// absent: source bytes are copied by the Ingestion Module, while an adapter
// derives only its typed metadata and private layout representation.
struct FormatIngestionArtifacts {
  MetadataMap metadata;
  std::vector<std::uint8_t> layout;
  std::optional<DatasetCatalogArtifact> dataset_catalog;
};

class FormatStatus {
public:
  static FormatStatus success() { return {}; }
  static FormatStatus failure(std::string error) {
    FormatStatus result;
    result.error_ = std::move(error);
    return result;
  }
  bool ok() const { return error_.empty(); }
  const std::string& error() const { return error_; }

private:
  std::string error_;
};

template<typename T>
class FormatResult {
public:
  static FormatResult success(T value) {
    FormatResult result;
    result.value_ = std::move(value);
    return result;
  }
  static FormatResult failure(std::string error) {
    FormatResult result;
    result.error_ = std::move(error);
    return result;
  }
  bool ok() const { return error_.empty(); }
  const std::string& error() const { return error_; }
  const T& value() const { return *value_; }
  T& value() { return *value_; }

private:
  std::string error_;
  std::optional<T> value_;
};

// Adapter-private streaming parser. It has no persistence callbacks and cannot
// publish, write payload bytes, or expose format-specific flush semantics to
// the filesystem core.
class FormatIngestionSession {
public:
  virtual ~FormatIngestionSession() = default;

  virtual FormatStatus consume_chunk(std::uint64_t offset,
    const std::uint8_t* data, std::size_t length) = 0;
  virtual FormatResult<FormatIngestionArtifacts> finish() = 0;
};

enum class FormatViewType { None, Range, Materialized };

struct FormatQuery {
  explicit FormatQuery(FormatViewType view_type) : type(view_type) {}
  virtual ~FormatQuery() = default;

  FormatViewType type{ FormatViewType::None };
};

using FormatQueryPtr = std::shared_ptr<const FormatQuery>;

struct FormatVirtualAttr {
  bool ok{ false };
  std::uint64_t size{ 0 };
};

struct FormatRangeView {
  std::uint64_t offset{ 0 };
  std::uint64_t length{ 0 };
};

struct FormatOpenResult {
  bool ok{ false };
  FormatViewType type{ FormatViewType::None };
  FormatRangeView range;
  std::vector<std::uint8_t> data;
};

using FormatXattrList =
  std::vector<std::pair<std::string, std::string>>;

class FormatAdapter {
public:
  virtual ~FormatAdapter() = default;

  // Persisted identity selects read/query behavior. Content matching selects
  // an optional ingestion adapter; no match means byte pass-through.
  virtual const std::string& id() const = 0;
  virtual const std::vector<std::string>& extensions() const = 0;
  virtual bool matches(const FormatSource& source) const;
  virtual FormatStatus validate_layout_artifact(const std::vector<std::uint8_t>&,
    std::uint64_t payload_size) const;

  // The source remains valid through consume_chunk/finish and session
  // destruction, all within the synchronous ingest call. Do not retain it.
  virtual std::unique_ptr<FormatIngestionSession> begin_ingestion(
    const FormatSource& source) const = 0;

  virtual FormatQueryPtr parse_query(const std::string& query) const;
  virtual bool can_handle_virtual_query(const std::string& path) const;
  virtual FormatVirtualAttr get_virtual_attr(long long row_time,
    const std::string& path, const FormatQuery& query,
    const std::string& raw_query) const;
  virtual FormatOpenResult open_virtual(long long row_time,
    const std::string& path, const FormatQuery& query,
    const std::string& raw_query) const;
  virtual std::optional<std::string> get_xattr(long long row_time,
    const std::string& path, const Inode& inode,
    const std::string& name) const;
  virtual FormatXattrList list_xattrs(long long row_time,
    const std::string& path, const Inode& inode) const;
};

} // namespace iotdbfs
