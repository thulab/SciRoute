#include "iotdbfs/query/frontend/TypedResultStore.h"

#include <openssl/sha.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <limits>
#include <mutex>
#include <new>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace iotdbfs {
namespace {
QueryStatus failure(QueryError code, const char* message) {
  return QueryStatus::failure(code, QueryPhase::Execution, message);
}
std::string content_id(const std::vector<std::uint8_t>& bytes) {
  std::array<unsigned char, SHA256_DIGEST_LENGTH> digest{};
  SHA256(bytes.data(), bytes.size(), digest.data());
  constexpr char digits[] = "0123456789abcdef";
  std::string id;
  id.reserve(digest.size() * 2);
  for (const auto byte : digest) {
    id.push_back(digits[byte >> 4]);
    id.push_back(digits[byte & 0x0f]);
  }
  return id;
}
} // namespace

struct TypedResultStore::Payload {
  Payload(std::vector<std::uint8_t> complete_bytes,
      std::weak_ptr<State> owner)
    : bytes(std::move(complete_bytes)), state(std::move(owner)) {}
  ~Payload();

  std::vector<std::uint8_t> bytes;
  std::weak_ptr<State> state;
  bool accounted{false};
};

struct TypedResultStore::State {
  struct Entry {
    std::shared_ptr<Payload> payload;
    Clock::time_point expires_at;
    std::uint64_t sequence;
  };

  State(Config limits, ClockFn read_clock)
    : config(std::move(limits)), clock(std::move(read_clock)) {}

  void expire_locked(Clock::time_point now) {
    for (auto it = entries.begin(); it != entries.end();) {
      if (it->second.expires_at <= now) it = entries.erase(it);
      else ++it;
    }
  }

  std::uint64_t reclaimable_locked() const {
    std::uint64_t bytes = 0;
    for (const auto& item : entries) {
      if (item.second.payload.use_count() == 1)
        bytes += item.second.payload->bytes.size();
    }
    return bytes;
  }

  bool evict_oldest_locked(bool require_unpinned) {
    auto victim = entries.end();
    for (auto it = entries.begin(); it != entries.end(); ++it) {
      if (require_unpinned && it->second.payload.use_count() != 1) continue;
      if (victim == entries.end() ||
          it->second.sequence < victim->second.sequence) victim = it;
    }
    if (victim == entries.end()) return false;
    entries.erase(victim);
    return true;
  }

  Config config;
  ClockFn clock;
  std::mutex mutex;
  std::unordered_map<std::string, Entry> entries;
  std::atomic<std::uint64_t> resident_bytes{0};
  std::atomic<std::uint64_t> open_handles{0};
  std::uint64_t next_sequence{0};
};

TypedResultStore::Payload::~Payload() {
  if (!accounted) return;
  if (auto owner = state.lock())
    owner->resident_bytes.fetch_sub(bytes.size(), std::memory_order_relaxed);
}

TypedResultStore::Handle::Handle(std::shared_ptr<State> state,
    std::shared_ptr<const Payload> payload, std::string id) noexcept
  : state_(std::move(state)), payload_(std::move(payload)), id_(std::move(id)) {
  state_->open_handles.fetch_add(1, std::memory_order_relaxed);
}

TypedResultStore::Handle::Handle(Handle&& other) noexcept
  : state_(std::move(other.state_)), payload_(std::move(other.payload_)),
    id_(std::move(other.id_)) {}

TypedResultStore::Handle& TypedResultStore::Handle::operator=(
    Handle&& other) noexcept {
  if (this != &other) {
    release();
    state_ = std::move(other.state_);
    payload_ = std::move(other.payload_);
    id_ = std::move(other.id_);
  }
  return *this;
}

TypedResultStore::Handle::~Handle() { release(); }

std::uint64_t TypedResultStore::Handle::size() const {
  return payload_ ? payload_->bytes.size() : 0;
}

QueryResult<std::vector<std::uint8_t>> TypedResultStore::Handle::read(
    std::uint64_t offset, std::size_t length) const {
  if (!payload_)
    return QueryResult<std::vector<std::uint8_t>>::failure(
      failure(QueryError::InvalidQuery, "result handle was released"));
  const auto& bytes = payload_->bytes;
  if (offset >= bytes.size() || !length)
    return QueryResult<std::vector<std::uint8_t>>::success({});
  const auto begin = static_cast<std::size_t>(offset);
  const auto count = std::min(length, bytes.size() - begin);
  try {
    return QueryResult<std::vector<std::uint8_t>>::success(
      std::vector<std::uint8_t>(bytes.begin() + begin,
        bytes.begin() + begin + count));
  } catch (const std::bad_alloc&) {
    return QueryResult<std::vector<std::uint8_t>>::failure(
      failure(QueryError::ResourceExhausted, "result read allocation failed"));
  }
}

QueryResult<TypedResultStore::ReadOnlySlice> TypedResultStore::Handle::slice(
    std::uint64_t offset, std::size_t length) const {
  if (!payload_) return QueryResult<ReadOnlySlice>::failure(
    failure(QueryError::InvalidQuery, "result handle was released"));
  ReadOnlySlice slice;
  slice.state_ = state_;
  slice.payload_ = payload_;
  if (offset < payload_->bytes.size() && length) {
    const auto begin = static_cast<std::size_t>(offset);
    slice.size_ = std::min(length, payload_->bytes.size() - begin);
    slice.data_ = payload_->bytes.data() + begin;
  }
  return QueryResult<ReadOnlySlice>::success(std::move(slice));
}

void TypedResultStore::Handle::release() noexcept {
  if (!payload_) return;
  payload_.reset();
  id_.clear();
  state_->open_handles.fetch_sub(1, std::memory_order_relaxed);
  state_.reset();
}

TypedResultStore::TypedResultStore(Config config, ClockFn clock)
  : state_(std::make_shared<State>(std::move(config), std::move(clock))) {
  const auto& limits = state_->config;
  if (!limits.max_result_bytes || !limits.max_resident_bytes ||
      !limits.max_entries || limits.ttl.count() <= 0 || !state_->clock)
    throw std::invalid_argument("invalid typed result store limits");
}

QueryResult<std::string> TypedResultStore::publish(
    std::vector<std::uint8_t> complete_bytes) {
  return publish_retryable(complete_bytes);
}

QueryResult<std::string> TypedResultStore::publish_retryable(
    std::vector<std::uint8_t>& complete_bytes, Handle* pinned) {
  const auto size = complete_bytes.size();
  if (pinned && pinned->valid())
    return QueryResult<std::string>::failure(failure(
      QueryError::InvalidQuery, "publication handle must be empty"));
  if (!size)
    return QueryResult<std::string>::failure(
      failure(QueryError::InvalidQuery, "encoded result is empty"));
  if (size > state_->config.max_result_bytes ||
      size > state_->config.max_resident_bytes)
    return QueryResult<std::string>::failure(
      failure(QueryError::ResourceExhausted, "encoded result exceeds store limits"));

  try {
    const auto id = content_id(complete_bytes);
    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->expire_locked(state_->clock());
    if (auto existing = state_->entries.find(id); existing != state_->entries.end()) {
      if (existing->second.payload->bytes != complete_bytes)
        return QueryResult<std::string>::failure(failure(
          QueryError::Internal, "encoded result digest collision"));
      existing->second.expires_at = state_->clock() + state_->config.ttl;
      existing->second.sequence = ++state_->next_sequence;
      if (pinned) *pinned = Handle(state_, existing->second.payload, id);
      complete_bytes.clear();
      return QueryResult<std::string>::success(id);
    }
    const auto resident = state_->resident_bytes.load(std::memory_order_relaxed);
    const auto reclaimable = state_->reclaimable_locked();
    if (size > state_->config.max_resident_bytes - (resident - reclaimable))
      return QueryResult<std::string>::failure(failure(
        QueryError::ResourceExhausted, "open result handles exhaust resident budget"));

    while (size > state_->config.max_resident_bytes -
        state_->resident_bytes.load(std::memory_order_relaxed)) {
      if (!state_->evict_oldest_locked(true))
        return QueryResult<std::string>::failure(failure(
          QueryError::ResourceExhausted, "result resident budget exhausted"));
    }
    while (state_->entries.size() >= state_->config.max_entries) {
      if (!state_->evict_oldest_locked(true))
        state_->evict_oldest_locked(false);
    }

    auto payload = std::make_shared<Payload>(std::vector<std::uint8_t>{}, state_);
    const bool inserted = state_->entries.emplace(id, State::Entry{payload,
      state_->clock() + state_->config.ttl, state_->next_sequence}).second;
    if (!inserted)
      return QueryResult<std::string>::failure(failure(
        QueryError::Internal, "encoded result ID collision"));
    // All fallible admission work is complete before consuming the buffer.
    payload->bytes = std::move(complete_bytes);
    payload->accounted = true;
    state_->resident_bytes.fetch_add(size, std::memory_order_relaxed);
    ++state_->next_sequence;
    if (pinned) *pinned = Handle(state_, payload, id);
    return QueryResult<std::string>::success(std::move(id));
  } catch (const std::bad_alloc&) {
    return QueryResult<std::string>::failure(failure(
      QueryError::ResourceExhausted, "result store allocation failed"));
  } catch (const std::exception&) {
    return QueryResult<std::string>::failure(failure(
      QueryError::Internal, "result digest publication failed"));
  }
}

QueryResult<TypedResultStore::PreparedPublication> TypedResultStore::prepare_publication(
    std::vector<std::uint8_t> complete_bytes) const {
  using Result = QueryResult<PreparedPublication>;
  if (complete_bytes.empty()) return Result::failure(failure(
    QueryError::InvalidQuery, "encoded result is empty"));
  if (complete_bytes.size() > max_publish_bytes()) return Result::failure(failure(
    QueryError::ResourceExhausted, "encoded result exceeds store limits"));
  try {
    PreparedPublication prepared;
    prepared.id_ = content_id(complete_bytes);
    prepared.bytes_ = std::move(complete_bytes);
    return Result::success(std::move(prepared));
  } catch (const std::exception&) {
    return Result::failure(failure(QueryError::ResourceExhausted,
      "result digest preparation failed"));
  }
}

QueryStatus TypedResultStore::prepare_capacity(PreparedPublication& prepared) {
  try {
    prepared.result_id_ = prepared.id_;
    prepared.handle_id_ = prepared.id_;
    prepared.identical_.reset();
    if (!prepared.spare_)
      prepared.spare_ = std::make_shared<Payload>(std::vector<std::uint8_t>{}, state_);
    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->expire_locked(state_->clock());
    if (auto existing = state_->entries.find(prepared.id_); existing != state_->entries.end()) {
      if (existing->second.payload->bytes != prepared.bytes_)
        return failure(QueryError::Internal, "encoded result digest collision");
      prepared.identical_ = existing->second.payload;
      return QueryStatus::success();
    }
    const auto size = prepared.bytes_.size();
    const auto resident = state_->resident_bytes.load(std::memory_order_relaxed);
    const auto reclaimable = state_->reclaimable_locked();
    if (size > state_->config.max_resident_bytes - (resident - reclaimable))
      return failure(QueryError::ResourceExhausted, "open result handles exhaust resident budget");
    while (size > state_->config.max_resident_bytes - state_->resident_bytes.load(std::memory_order_relaxed))
      if (!state_->evict_oldest_locked(true))
        return failure(QueryError::ResourceExhausted, "result resident budget exhausted");
    while (state_->entries.size() >= state_->config.max_entries) {
      if (!state_->evict_oldest_locked(true)) state_->evict_oldest_locked(false);
    }
    return QueryStatus::success();
  } catch (const std::exception&) {
    return failure(QueryError::ResourceExhausted, "result capacity preparation failed");
  }
}

QueryResult<std::string> TypedResultStore::publish_prepared_retryable(
    PreparedPublication& prepared, Handle* pinned) {
  auto& complete_bytes = prepared.bytes_;
  const auto& id = prepared.id_;
  const auto size = complete_bytes.size();
  if (pinned && pinned->valid())
    return QueryResult<std::string>::failure(failure(
      QueryError::InvalidQuery, "publication handle must be empty"));
  if (!size || id.empty() || prepared.result_id_ != id || prepared.handle_id_ != id) return QueryResult<std::string>::failure(failure(
    QueryError::InvalidQuery, "publication was not prepared"));
  if (size > max_publish_bytes()) return QueryResult<std::string>::failure(failure(
    QueryError::ResourceExhausted, "encoded result exceeds store limits"));
  try {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (auto existing = state_->entries.find(id); existing != state_->entries.end()) {
      if (existing->second.payload != prepared.identical_)
        return QueryResult<std::string>::failure(failure(
          QueryError::ResourceExhausted, "result store changed after preparation"));
      existing->second.expires_at = state_->clock() + state_->config.ttl;
      existing->second.sequence = ++state_->next_sequence;
      if (pinned) *pinned = Handle(state_, existing->second.payload, std::move(prepared.handle_id_));
      complete_bytes.clear();
      return QueryResult<std::string>::success(std::move(prepared.result_id_));
    }
    if (state_->entries.size() >= state_->config.max_entries ||
        size > state_->config.max_resident_bytes - state_->resident_bytes.load(std::memory_order_relaxed))
      return QueryResult<std::string>::failure(failure(
        QueryError::ResourceExhausted, "result store changed after preparation"));
    auto payload = prepared.spare_;
    if (!payload) return QueryResult<std::string>::failure(failure(
      QueryError::InvalidQuery, "result capacity was not prepared"));
    const bool inserted = state_->entries.emplace(id, State::Entry{payload,
      state_->clock() + state_->config.ttl, state_->next_sequence}).second;
    if (!inserted)
      return QueryResult<std::string>::failure(failure(
        QueryError::Internal, "encoded result ID collision"));
    // All fallible admission work is complete before consuming the buffer.
    payload->bytes = std::move(complete_bytes);
    payload->accounted = true;
    state_->resident_bytes.fetch_add(size, std::memory_order_relaxed);
    ++state_->next_sequence;
    if (pinned) *pinned = Handle(state_, payload, std::move(prepared.handle_id_));
    return QueryResult<std::string>::success(std::move(prepared.result_id_));
  } catch (const std::bad_alloc&) {
    return QueryResult<std::string>::failure(failure(
      QueryError::ResourceExhausted, "result store allocation failed"));
  } catch (const std::exception&) {
    return QueryResult<std::string>::failure(failure(
      QueryError::Internal, "result digest publication failed"));
  }
}

QueryResult<TypedResultStore::Handle> TypedResultStore::open(
    const std::string& result_id) {
  std::lock_guard<std::mutex> lock(state_->mutex);
  state_->expire_locked(state_->clock());
  auto found = state_->entries.find(result_id);
  if (found == state_->entries.end())
    return QueryResult<Handle>::failure(failure(
      QueryError::SourceNotFound, "encoded result ID was not found"));
  return QueryResult<Handle>::success(Handle(state_, found->second.payload, result_id));
}

std::uint64_t TypedResultStore::max_publish_bytes() const {
  return std::min(state_->config.max_result_bytes,
    state_->config.max_resident_bytes);
}

TypedResultStore::Stats TypedResultStore::stats() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  state_->expire_locked(state_->clock());
  return Stats{state_->resident_bytes.load(std::memory_order_relaxed),
    state_->entries.size(),
    state_->open_handles.load(std::memory_order_relaxed)};
}

} // namespace iotdbfs
