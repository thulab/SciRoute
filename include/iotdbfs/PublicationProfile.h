#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>

namespace iotdbfs {

enum class PublicationOperation {
  QuerySubmit, NonQuery, Tablet, QueryClose, QueryFetch,
  TabletBuild, ObjectEncode, SessionAcquire, SdkInsert
};

struct PublicationOperationMetrics {
  std::uint64_t calls{0}, failures{0}, nanoseconds{0};
};
struct PublicationStageMetrics {
  // Inclusive client wall time. Submission metrics include pool acquisition
  // and RPC wait; they are not server CPU time or result fetch/parse time.
  std::uint64_t wall_nanoseconds{0};
  PublicationOperationMetrics query_submit, nonquery, tablet, query_close;
  // hasNext()/next() method calls, not network RPCs. Includes any fetch wait
  // performed by those methods; caller field copies/parsing stay outside.
  PublicationOperationMetrics query_fetch;
  // Backend work BEFORE insertTablet(): base Tablet/path construction and
  // OBJECT EOF/offset/content encoding, measured separately and without RPC.
  PublicationOperationMetrics tablet_build, object_encode;
  // Nested within the client operation above. SDK insert includes its own
  // request construction/serialization, transport and server wait; it is NOT
  // pure serialization or wire time. Never add these to tablet/query totals.
  PublicationOperationMetrics session_acquire, sdk_insert;
  std::map<std::string, PublicationOperationMetrics> details;
};

class PublicationProfileRecord {
public:
  using Snapshot = std::map<std::string, PublicationStageMetrics>;
  Snapshot snapshot() const {
    std::lock_guard<std::mutex> lock(mu_);
    return stages_;
  }
  void observe(const std::string& stage, PublicationOperation operation,
               bool success, std::uint64_t nanoseconds) noexcept {
    try {
      std::lock_guard<std::mutex> lock(mu_);
      auto& entry = stages_[stage];
      PublicationOperationMetrics* metric = nullptr;
      switch (operation) {
      case PublicationOperation::QuerySubmit: metric = &entry.query_submit; break;
      case PublicationOperation::NonQuery: metric = &entry.nonquery; break;
      case PublicationOperation::Tablet: metric = &entry.tablet; break;
      case PublicationOperation::QueryClose: metric = &entry.query_close; break;
      case PublicationOperation::QueryFetch: metric = &entry.query_fetch; break;
      case PublicationOperation::TabletBuild: metric = &entry.tablet_build; break;
      case PublicationOperation::ObjectEncode: metric = &entry.object_encode; break;
      case PublicationOperation::SessionAcquire: metric = &entry.session_acquire; break;
      case PublicationOperation::SdkInsert: metric = &entry.sdk_insert; break;
      }
      if (!metric) return;
      ++metric->calls;
      if (!success) ++metric->failures;
      metric->nanoseconds += nanoseconds;
    } catch (...) { /* Profiling must not change publication behavior. */ }
  }
  void observe_detail(const std::string& stage, const std::string& detail,
                      bool success, std::uint64_t nanoseconds) noexcept {
    try {
      std::lock_guard<std::mutex> lock(mu_);
      auto& metric = stages_[stage].details[detail];
      ++metric.calls;
      if (!success) ++metric.failures;
      metric.nanoseconds += nanoseconds;
    } catch (...) { }
  }
  void observe_stage(const std::string& stage, std::uint64_t nanoseconds) noexcept {
    try {
      std::lock_guard<std::mutex> lock(mu_);
      stages_[stage].wall_nanoseconds += nanoseconds;
    } catch (...) { }
  }
  std::string to_json() const {
    const auto values = snapshot();
    std::ostringstream out;
    out << "{\"stages\":{";
    bool first = true;
    const auto metrics = [&](const PublicationOperationMetrics& metric) {
      out << "{\"calls\":" << metric.calls << ",\"failures\":" << metric.failures
          << ",\"nanoseconds\":" << metric.nanoseconds << '}';
    };
    for (const auto& value : values) {
      if (!first) out << ',';
      first = false;
      out << json_string(value.first) << ":{\"wall_nanoseconds\":" << value.second.wall_nanoseconds
          << ",\"query_submit\":";
      metrics(value.second.query_submit);
      out << ",\"nonquery\":"; metrics(value.second.nonquery);
      out << ",\"tablet\":"; metrics(value.second.tablet);
      out << ",\"query_close\":"; metrics(value.second.query_close);
      out << ",\"query_fetch\":"; metrics(value.second.query_fetch);
      out << ",\"tablet_build\":"; metrics(value.second.tablet_build);
      out << ",\"object_encode\":"; metrics(value.second.object_encode);
      out << ",\"session_acquire\":"; metrics(value.second.session_acquire);
      out << ",\"sdk_insert\":"; metrics(value.second.sdk_insert);
      out << ",\"details\":{";
      bool first_detail = true;
      for (const auto& detail : value.second.details) {
        if (!first_detail) out << ',';
        first_detail = false;
        out << json_string(detail.first) << ':';
        metrics(detail.second);
      }
      out << '}';
      out << '}';
    }
    out << "}}";
    return out.str();
  }
private:
  static std::string json_string(const std::string& text) {
    std::string result = "\"";
    constexpr char hex[] = "0123456789abcdef";
    for (unsigned char byte : text) {
      if (byte == '"' || byte == '\\') { result += '\\'; result += static_cast<char>(byte); }
      else if (byte < 0x20) {
        result += "\\u00"; result += hex[byte >> 4]; result += hex[byte & 15];
      } else result += static_cast<char>(byte);
    }
    return result + '"';
  }
  mutable std::mutex mu_;
  Snapshot stages_;
};

struct PublicationProfileContext {
  std::shared_ptr<PublicationProfileRecord> record;
  std::string stage;
};
inline thread_local PublicationProfileContext active_publication_profile;

class PublicationProfileScope {
public:
  explicit PublicationProfileScope(std::shared_ptr<PublicationProfileRecord> record) noexcept {
    try {
      PublicationProfileContext next{std::move(record), {}};
      if (next.record) next.stage = "unscoped";
      // Prepare every allocation before changing the caller's context.
      previous_ = std::move(active_publication_profile);
      active_publication_profile = std::move(next);
      installed_ = true;
    } catch (...) { }
  }
  ~PublicationProfileScope() {
    if (installed_) active_publication_profile = std::move(previous_);
  }
  PublicationProfileScope(const PublicationProfileScope&) = delete;
  PublicationProfileScope& operator=(const PublicationProfileScope&) = delete;
private:
  PublicationProfileContext previous_;
  bool installed_{false};
};

class PublicationStageScope {
public:
  explicit PublicationStageScope(const char* stage) noexcept { activate(stage ? stage : "unscoped"); }
  explicit PublicationStageScope(const std::string& stage) noexcept { activate(stage); }
  ~PublicationStageScope() {
    if (!record_) return;
    const auto elapsed = std::chrono::steady_clock::now() - start_;
    record_->observe_stage(stage_, std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
    active_publication_profile.stage = std::move(previous_stage_);
  }
  PublicationStageScope(const PublicationStageScope&) = delete;
  PublicationStageScope& operator=(const PublicationStageScope&) = delete;
private:
  template<class Stage> void activate(const Stage& stage) noexcept {
    // A disabled literal stage neither constructs a string nor samples a clock.
    if (!active_publication_profile.record) return;
    try {
      std::string next(stage);
      stage_ = next;
      record_ = active_publication_profile.record;
      start_ = std::chrono::steady_clock::now();
      previous_stage_ = std::move(active_publication_profile.stage);
      active_publication_profile.stage = std::move(next);
    } catch (...) { record_.reset(); }
  }
  std::shared_ptr<PublicationProfileRecord> record_;
  std::string stage_, previous_stage_;
  std::chrono::steady_clock::time_point start_;
};

// A deferred query close uses the captured context even after its publication
// and stage scopes end or the dataset is destroyed on another thread.
class PublicationOperationTimer {
public:
  explicit PublicationOperationTimer(PublicationOperation operation) noexcept
    : PublicationOperationTimer(operation, active_publication_profile) {}
  PublicationOperationTimer(PublicationOperation operation, const PublicationProfileContext& context) noexcept
    : operation_(operation) {
    try { context_ = context; }
    catch (...) { context_ = {}; }
    if (context_.record) start_ = std::chrono::steady_clock::now();
  }
  ~PublicationOperationTimer() { finish(false); }
  void finish(bool success) noexcept {
    if (!context_.record || finished_) return;
    const auto elapsed = std::chrono::steady_clock::now() - start_;
    finished_ = true;
    context_.record->observe(context_.stage, operation_, success,
      std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
  }
  const PublicationProfileContext& context() const { return context_; }
  std::shared_ptr<const PublicationProfileContext> deferred_context() const noexcept {
    if (!context_.record) return {};
    try { return std::make_shared<const PublicationProfileContext>(context_); }
    catch (...) { return {}; }
  }
  PublicationOperationTimer(const PublicationOperationTimer&) = delete;
  PublicationOperationTimer& operator=(const PublicationOperationTimer&) = delete;
private:
  PublicationProfileContext context_;
  PublicationOperation operation_;
  std::chrono::steady_clock::time_point start_;
  bool finished_{false};
};

// Attribute a named sub-operation to the current publication AND stage without
// installing a child stage. Disabled profiling does not allocate or read clocks.
class PublicationDetailTimer {
public:
  explicit PublicationDetailTimer(const char* detail) noexcept {
    if (!active_publication_profile.record) return;
    try {
      context_ = active_publication_profile;
      detail_ = detail ? detail : "unnamed";
      start_ = std::chrono::steady_clock::now();
    } catch (...) { context_ = {}; }
  }
  ~PublicationDetailTimer() { finish(false); }
  void finish(bool success) noexcept {
    if (!context_.record || finished_) return;
    const auto elapsed = std::chrono::steady_clock::now() - start_;
    finished_ = true;
    context_.record->observe_detail(context_.stage, detail_, success,
      std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
  }
  PublicationDetailTimer(const PublicationDetailTimer&) = delete;
  PublicationDetailTimer& operator=(const PublicationDetailTimer&) = delete;
private:
  PublicationProfileContext context_;
  std::string detail_;
  std::chrono::steady_clock::time_point start_;
  bool finished_{false};
};

} // namespace iotdbfs
