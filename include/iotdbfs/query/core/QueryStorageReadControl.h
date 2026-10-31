#pragma once
#include "iotdbfs/query/core/QueryModel.h"
#include <functional>
#include <memory>
#include <stdexcept>

namespace iotdbfs {
// Optional storage residency is registered weakly with the runtime that pays
// its temporary reservation. Reclaim removes cache ownership, never live pins.
class StorageReadOptionalCache {
public:
  virtual ~StorageReadOptionalCache() = default;
  virtual void reclaim() noexcept = 0;
};
class QueryStorageReadInterrupted : public std::runtime_error {
public:
  explicit QueryStorageReadInterrupted(QueryStatus status)
    : std::runtime_error(status.message()), status_(std::move(status)) {}
  const QueryStatus& status() const { return status_; }
private:
  QueryStatus status_;
};
struct QueryStorageReadControl {
  std::function<void()> checkpoint;
  std::function<std::shared_ptr<void>(std::uint64_t)> reserve;
  std::function<void(std::uint64_t,std::uint64_t)> io;
  std::function<bool(const std::shared_ptr<StorageReadOptionalCache>&)> register_cache;
};
inline thread_local const QueryStorageReadControl* active_storage_read_control=nullptr;
// Optional immutable read reuse must yield before a real budget admission
// fails. Reclaim outside the budget lock: releasing a pin may reenter it.
struct StorageReadOptionalReclaim {
  void (*callback)(void*) noexcept;
  void* context;
};
inline thread_local const StorageReadOptionalReclaim* active_storage_read_optional_reclaim=nullptr;
class ScopedStorageReadOptionalReclaim {
public:
  ScopedStorageReadOptionalReclaim(void (*callback)(void*) noexcept,void* context) noexcept
    : previous_(active_storage_read_optional_reclaim),reclaim_{callback,context} {
    active_storage_read_optional_reclaim=&reclaim_;
  }
  ~ScopedStorageReadOptionalReclaim() { active_storage_read_optional_reclaim=previous_; }
  ScopedStorageReadOptionalReclaim(const ScopedStorageReadOptionalReclaim&)=delete;
  ScopedStorageReadOptionalReclaim& operator=(const ScopedStorageReadOptionalReclaim&)=delete;
private:
  const StorageReadOptionalReclaim* previous_;
  StorageReadOptionalReclaim reclaim_;
};
inline void reclaim_storage_read_optional() noexcept {
  if(active_storage_read_optional_reclaim&&active_storage_read_optional_reclaim->callback)
    active_storage_read_optional_reclaim->callback(active_storage_read_optional_reclaim->context);
}
class ScopedQueryStorageReadControl {
public:
  ScopedQueryStorageReadControl(std::function<void()> checkpoint,
      std::function<std::shared_ptr<void>(std::uint64_t)> reserve,
      std::function<void(std::uint64_t,std::uint64_t)> io = {},
      std::function<bool(const std::shared_ptr<StorageReadOptionalCache>&)> register_cache = {})
    : previous_(active_storage_read_control), control_{std::move(checkpoint),std::move(reserve),std::move(io),std::move(register_cache)} {
    active_storage_read_control=&control_;
  }
  ~ScopedQueryStorageReadControl() { active_storage_read_control=previous_; }
  ScopedQueryStorageReadControl(const ScopedQueryStorageReadControl&)=delete;
  ScopedQueryStorageReadControl& operator=(const ScopedQueryStorageReadControl&)=delete;
private:
  const QueryStorageReadControl* previous_;
  QueryStorageReadControl control_;
};
inline void storage_read_checkpoint() {
  if(active_storage_read_control&&active_storage_read_control->checkpoint)
    active_storage_read_control->checkpoint();
}
inline bool register_storage_read_optional_cache(
    const std::shared_ptr<StorageReadOptionalCache>& cache) {
  if (!active_storage_read_control) return false;
  return active_storage_read_control->register_cache && active_storage_read_control->register_cache(cache);
}
inline std::shared_ptr<void> reserve_storage_read_temporary(std::uint64_t bytes) {
  storage_read_checkpoint();
  return active_storage_read_control&&active_storage_read_control->reserve
    ? active_storage_read_control->reserve(bytes) : nullptr;
}
inline void record_storage_read_io(std::uint64_t bytes,std::uint64_t ranges) {
  storage_read_checkpoint();
  if(active_storage_read_control&&active_storage_read_control->io)
    active_storage_read_control->io(bytes,ranges);
}
} // namespace iotdbfs
