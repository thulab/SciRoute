#pragma once
#include "iotdbfs/StructuredBlobStorage.h"
#include <map>
#include <mutex>
namespace iotdbfs::test {
// Explicit test-backend persistence. The backend must erase(key) alongside its
// other artifacts; production uses the durable IoTDB implementation.
class InMemoryDatasetCatalog {
public:
  StructuredBlobStatus write(const StructuredBlobVersionKey &key,
                             const DatasetCatalogArtifact &value) {
    auto valid = validate_dataset_catalog(value);
    if (!valid.ok())
      return StructuredBlobStatus::failure(StructuredBlobError::InvalidArgument,
                                           valid.error());
    std::lock_guard<std::mutex> lock(mu_);
    records_[key] = value;
    return StructuredBlobStatus::success();
  }
  StructuredBlobResult<DatasetCatalogArtifact>
  read(const StructuredBlobVersionKey &key) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto found = records_.find(key);
    if (found == records_.end())
      return StructuredBlobResult<DatasetCatalogArtifact>::failure(
          StructuredBlobError::NotFound, "dataset catalog not found");
    return StructuredBlobResult<DatasetCatalogArtifact>::success(found->second);
  }
  void erase(const StructuredBlobVersionKey &key) {
    std::lock_guard<std::mutex> lock(mu_);
    records_.erase(key);
  }

private:
  mutable std::mutex mu_;
  std::map<StructuredBlobVersionKey, DatasetCatalogArtifact> records_;
};
} // namespace iotdbfs::test
