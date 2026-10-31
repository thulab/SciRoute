#include "iotdbfs/query/training/arrow/WindowAssembler.h"

#include <algorithm>
#include <atomic>
#include <future>
#include <limits>
#include <map>
#include <tuple>
#include <utility>

namespace iotdbfs {
namespace {

QueryStatus failure(QueryError code, const char* message) {
  return QueryStatus::failure(code, QueryPhase::Execution, message);
}

bool checked_add(std::uint64_t a, std::uint64_t b, std::uint64_t& out) {
  if (a > std::numeric_limits<std::uint64_t>::max() - b) return false;
  out = a + b;
  return true;
}

bool checked_mul(std::uint64_t a, std::uint64_t b, std::uint64_t& out) {
  if (b && a > std::numeric_limits<std::uint64_t>::max() / b) return false;
  out = a * b;
  return true;
}

std::uint64_t bitmap_bytes(std::uint64_t n) { return n / 8 + (n % 8 != 0); }

bool leaf_valid(const ArrowDecodedFragment& fragment, std::uint64_t leaf) {
  if (fragment.validity.empty()) return true;
  const auto offset = leaf - fragment.begin;
  return (fragment.validity[offset / 8] & (1u << (offset % 8))) != 0;
}

} // namespace

struct WindowAssembler::PreparedContext {
  struct Source {
    std::uint64_t metadata_bytes{0};
    std::shared_ptr<const BinaryValue> serialized_layout;
    std::shared_ptr<const ArrowLayoutDocument> layout;
    std::map<std::pair<std::uint64_t, std::uint32_t>,
      std::shared_ptr<const ArrayListIndex>> list_indexes;
  };
  std::vector<Source> sources;
  std::uint64_t metadata_bytes{0};
};

struct WindowAssembler::Impl {
  struct SourceState {
    std::shared_ptr<const QuerySource> source;
    std::shared_ptr<const BinaryValue> serialized_layout;
    std::shared_ptr<const ArrowLayoutDocument> layout;
    std::unique_ptr<ArrowFragmentProvider> provider;
    std::unique_ptr<ArrowFragmentProvider> block_provider;
    const PreparedContext::Source* prepared{nullptr};
    std::map<std::pair<std::uint64_t, std::uint32_t>,
      std::shared_ptr<const ArrayListIndex>> list_indexes;
  };
  using TileKey = std::tuple<std::size_t, std::uint64_t,
    std::uint32_t, std::uint64_t, bool>;
  struct Prefetched {
    std::shared_ptr<const ArrowDecodedFragment> fragment;
    std::string key;
  };
  struct TileDemand {
    std::uint64_t leaf_count{0};
    bool has_nulls{false};
    std::size_t uses{0};
    std::vector<StepInterval> ranges; // Physical leaf intervals, never a hull.
  };

  const DatasetSnapshot& snapshot;
  std::shared_ptr<const PreparedContext> prepared;
  std::shared_ptr<const StructuredBlobStorage> storage;
  std::shared_ptr<QueryRuntime> runtime;
  TrainingViewSpec view;
  TrainingQueryLimits limits;
  QueryExecutionOptions options;
  ArrowDecodeConfig config;
  CancellationToken cancellation;
  std::size_t read_parallelism;
  std::map<std::size_t, std::unique_ptr<SourceState>> sources;
  std::uint64_t layout_bytes{0}, logical_bytes{0}, logical_ranges{0};
  std::uint64_t output_bytes{0}, provenance_spans{0};
  std::map<TileKey, Prefetched> lookahead;
  std::vector<std::pair<TileKey, ArrowFragmentDemand>> pending;
  std::map<TileKey, std::size_t> pending_uses;
  std::size_t pending_cursor{0};
  bool lookahead_complete{false};
  bool cancelled{false};

  Impl(const DatasetSnapshot& s, std::shared_ptr<const StructuredBlobStorage> st,
      std::shared_ptr<QueryRuntime> rt, TrainingViewSpec v,
      TrainingQueryLimits lim, QueryExecutionOptions opt,
      ArrowDecodeConfig cfg, CancellationToken token, std::size_t parallelism,
      std::shared_ptr<const PreparedContext> context)
    : snapshot(s), prepared(std::move(context)), storage(std::move(st)), runtime(std::move(rt)),
      view(std::move(v)), limits(lim), options(opt), config(cfg),
      cancellation(std::move(token)), read_parallelism(parallelism) {}

  QueryResult<SourceState*> source(std::size_t shard_index) {
    using Result = QueryResult<SourceState*>;
    if (cancelled || cancellation.cancelled())
      return Result::failure(failure(QueryError::Cancelled,
        "window assembly cancelled"));
    if (shard_index >= snapshot.shards.size())
      return Result::failure(failure(QueryError::InvalidQuery,
        "window shard index is out of range"));
    auto existing = sources.find(shard_index);
    if (existing != sources.end()) return Result::success(existing->second.get());
    const auto& expected = snapshot.manifests[shard_index];
    if (expected.layout_size > ArrowLayoutLimits{}.max_layout_bytes ||
        expected.layout_size > options.max_layout_bytes - layout_bytes ||
        logical_ranges >= options.max_ranges)
      return Result::failure(failure(QueryError::ResourceExhausted,
        "window layout or range budget exhausted"));
    auto opened = snapshot.read_context
      ? open_catalog_query_source(storage, expected, snapshot.storage_namespace)
      : open_structured_blob_query_source(storage, snapshot.shards[shard_index].key);
    if (!opened.ok()) return Result::failure(opened.status());
    const auto& actual = opened.value()->manifest();
    if (opened.value()->storage_namespace() != snapshot.storage_namespace ||
        actual.key.path != expected.key.path ||
        actual.key.version != expected.key.version ||
        actual.revision != expected.revision ||
        actual.format_id != expected.format_id ||
        actual.payload_size != expected.payload_size ||
        actual.layout_size != expected.layout_size)
      return Result::failure(failure(QueryError::StalePlan,
        "window source no longer matches the snapshot"));
    auto state = std::make_unique<SourceState>();
    state->source = std::move(opened.value());
    if (prepared && prepared->sources.at(shard_index).layout) {
      // Published revisions are immutable. Manifest validation above is the
      // request boundary; do not re-read or re-parse all layout bytes here.
      state->prepared = &prepared->sources.at(shard_index);
      state->serialized_layout = state->prepared->serialized_layout;
      state->layout = state->prepared->layout;
    } else {
      auto read = state->source->read_layout(0, expected.layout_size);
      if (!read.ok()) return Result::failure(read.status());
      if (read.value().size() != expected.layout_size)
        return Result::failure(failure(QueryError::ReadFailure,
          "short Arrow layout read during window assembly"));
      auto parsed = parse_arrow_layout(read.value(), expected.payload_size);
      if (!parsed.ok()) return Result::failure(parsed.status());
      state->serialized_layout = std::make_shared<const BinaryValue>(
        std::move(read.value()));
      state->layout = std::make_shared<const ArrowLayoutDocument>(
        std::move(parsed.value()));
    }
    // The assembler owns the request-wide logical budget across all shards.
    // Provider-local accounting must not reject speculative prefetches or
    // retain charges from a window later rejected for a null leaf.
    auto provider_options = options;
    provider_options.max_ranges = std::numeric_limits<std::size_t>::max();
    provider_options.max_payload_bytes =
      std::numeric_limits<std::uint64_t>::max();
    state->provider = std::make_unique<ArrowFragmentProvider>(state->source,
      state->serialized_layout, runtime, provider_options, config,
      cancellation);
    layout_bytes += expected.layout_size;
    ++logical_ranges;
    auto* pointer = state.get();
    sources.emplace(shard_index, std::move(state));
    return Result::success(pointer);
  }

  QueryResult<std::shared_ptr<const ArrayListIndex>> list_index(
      SourceState& state, std::uint64_t batch, std::uint32_t field) {
    const auto key = std::make_pair(batch, field);
    if (state.prepared) {
      auto existing = state.prepared->list_indexes.find(key);
      if (existing != state.prepared->list_indexes.end())
        return QueryResult<std::shared_ptr<const ArrayListIndex>>::success(
          existing->second);
    }
    auto existing = state.list_indexes.find(key);
    if (existing != state.list_indexes.end())
      return QueryResult<std::shared_ptr<const ArrayListIndex>>::success(
        existing->second);
    auto built = make_arrow_list_index(state.layout, batch, field);
    if (!built.ok()) return built;
    state.list_indexes.emplace(key, built.value());
    return built;
  }

  ArrowDecodeConfig block_config() const {
    auto result = config;
    // A declared step-major batch is a training block. Keep that entire leaf
    // buffer as one immutable fragment, without changing ordinary Arrow tiles.
    result.tile_elements = std::max<std::uint64_t>(1, config.max_buffer_bytes / 4);
    return result;
  }

  ArrowFragmentProvider& provider(SourceState& state, bool block) {
    if (!block) return *state.provider;
    if (!state.block_provider) {
      auto provider_options = options;
      provider_options.max_ranges = std::numeric_limits<std::size_t>::max();
      provider_options.max_payload_bytes = std::numeric_limits<std::uint64_t>::max();
      state.block_provider = std::make_unique<ArrowFragmentProvider>(state.source,
        state.serialized_layout, runtime, provider_options, block_config(),
        cancellation);
    }
    return *state.block_provider;
  }

  std::size_t batch_tile_limit() const {
    std::uint64_t tile_bytes = 0;
    if (!checked_mul(config.tile_elements, 4, tile_bytes) ||
        !checked_add(tile_bytes, bitmap_bytes(config.tile_elements), tile_bytes))
      return 1;
    const auto by_bytes = tile_bytes > kMaxPayloadBytesPerBatch ? 1 :
      std::max<std::uint64_t>(1, kMaxPayloadBytesPerBatch / tile_bytes);
    const auto by_entries = runtime->config().max_entries > sources.size()
      ? runtime->config().max_entries - sources.size() : 1;
    return std::max<std::size_t>(1,
      std::min({std::size_t{64}, by_entries,
        runtime->config().max_inflight,
        static_cast<std::size_t>(by_bytes)}));
  }

  QueryStatus prepare_lookahead(const std::vector<PlannedWindow>& windows) {
    lookahead.clear();
    pending.clear();
    pending_uses.clear();
    pending_cursor = 0;
    lookahead_complete = false;
    if (cancelled || cancellation.cancelled()) return failure(QueryError::Cancelled,
      "window assembly cancelled");
    std::uint64_t input_elements, target_elements, per_window_bytes,
      lookahead_bytes;
    if (!checked_mul(view.input_length, view.input_channels.size(),
          input_elements) ||
        !checked_mul(view.target_length, view.target_channels.size(),
          target_elements) ||
        !checked_add(input_elements, target_elements, per_window_bytes) ||
        !checked_mul(per_window_bytes, 5, per_window_bytes) ||
        !checked_mul(per_window_bytes, windows.size(), lookahead_bytes) ||
        per_window_bytes > limits.max_result_bytes - output_bytes)
      return failure(QueryError::ResourceExhausted,
        "window output budget cannot admit lookahead");
    if (lookahead_bytes > limits.max_result_bytes - output_bytes)
      return QueryStatus::success();
    std::map<TileKey, TileDemand> demands;
    std::vector<TileKey> first_use_order;
    std::uint64_t resident = 0;
    bool group_fits = true;
    for (const auto& window : windows) {
      if (cancellation.cancelled()) return failure(QueryError::Cancelled,
        "window assembly cancelled");
      for (const auto& planned : window.spans) {
        if (cancellation.cancelled()) return failure(QueryError::Cancelled,
          "window assembly cancelled");
        const auto& span = planned.coverage;
        if (span.null_series || span.null_channel || span.missing_channel ||
            !span.physical_ordinal) continue;
        if (span.steps.begin >= span.steps.end ||
            span.source_step_begin > span.steps.begin ||
            span.shard_index >= snapshot.shards.size())
          return failure(QueryError::InvalidQuery,
            "lookahead coverage span is invalid");
        auto opened = source(span.shard_index);
        if (!opened.ok()) return opened.status();
        auto& state = *opened.value();
        if (span.batch_index >= state.layout->batches.size() ||
            span.logical_row >= snapshot.rows.size())
          return failure(QueryError::CorruptLayout,
            "lookahead batch or logical row is out of range");
        auto field = std::find_if(state.layout->fields.begin(),
          state.layout->fields.end(), [&](const ArrowNativeField& f) {
            return f.name == span.field_id;
          });
        if (field == state.layout->fields.end())
          return failure(QueryError::UnknownField,
            "lookahead field is absent from Arrow layout");
        const auto field_index = static_cast<std::uint32_t>(
          field - state.layout->fields.begin());
        const auto& row = snapshot.rows[span.logical_row];
        const bool step_major = row.value_layout == SnapshotValueLayout::StepMajorFlat;
        if (step_major) {
          const TileKey key{span.shard_index, span.batch_index, field_index, 0, true};
          auto existing = demands.find(key);
          if (existing != demands.end()) {
            ++existing->second.uses;
            continue;
          }
          auto index = list_index(state, span.batch_index, field_index);
          if (!index.ok()) return index.status();
          auto resolved = resolve_array_value({field->list_depth,
            PhysicalArrayLeafType::Float32}, *index.value(), span.row_index, {});
          if (!resolved.ok()) return resolved.status();
          std::uint64_t count;
          const auto channels = row.declared_n_channels.value_or(0);
          if (field->list_depth != 1 || !channels ||
              *span.physical_ordinal >= channels || !resolved.value().valid ||
              !checked_mul(row.steps.end - row.steps.begin, channels, count) ||
              resolved.value().end - resolved.value().begin != count ||
              span.steps.end - span.source_step_begin > row.steps.end - row.steps.begin)
            return failure(QueryError::StalePlan,
              "step-major block differs from snapshot coverage");
          const auto& batch = state.layout->batches[span.batch_index];
          auto array = std::find_if(batch.arrays.begin(), batch.arrays.end(),
            [&](const ArrowArrayIndex& a) { return a.field_index == field_index; });
          if (array == batch.arrays.end() || array->leaf_count > block_config().tile_elements)
            return failure(QueryError::ResourceExhausted,
              "step-major block exceeds decoded buffer limit");
          std::uint64_t charge;
          if (!checked_mul(array->leaf_count, 4, charge) ||
              !checked_add(charge, array->leaf_null_count ? bitmap_bytes(array->leaf_count) : 0, charge) ||
              !checked_add(resident, charge, resident))
            return failure(QueryError::ResourceExhausted,
              "step-major block size overflows");
          auto& demand = demands[key];
          first_use_order.push_back(key);
          demand.leaf_count = array->leaf_count;
          demand.has_nulls = array->leaf_null_count != 0;
          demand.uses = 1;
          demand.ranges.push_back({0, array->leaf_count});
          const auto max_tiles = runtime->config().max_entries > sources.size()
            ? runtime->config().max_entries - sources.size() : 0;
          if (resident > runtime->config().memory_budget_bytes / 4 ||
              demands.size() > max_tiles) group_fits = false;
          continue;
        }
        auto index = list_index(state, span.batch_index, field_index);
        if (!index.ok()) return index.status();
        ArrayValuePath path;
        if (field->list_depth == 2)
          path.child_indices.push_back(*span.physical_ordinal);
        else if (field->list_depth != 1 || *span.physical_ordinal != 0)
          return failure(QueryError::InvalidQuery,
            "lookahead channel cannot resolve to a List child");
        auto resolved = resolve_array_value({field->list_depth,
          PhysicalArrayLeafType::Float32}, *index.value(), span.row_index, path);
        if (!resolved.ok()) return resolved.status();
        const auto row_length = snapshot.rows[span.logical_row].steps.end -
          snapshot.rows[span.logical_row].steps.begin;
        if (!resolved.value().valid ||
            resolved.value().end - resolved.value().begin !=
              row_length || span.steps.end - span.source_step_begin > row_length)
          return failure(QueryError::StalePlan,
            "lookahead List differs from snapshot coverage");
        const auto& batch = state.layout->batches[span.batch_index];
        auto array = std::find_if(batch.arrays.begin(), batch.arrays.end(),
          [&](const ArrowArrayIndex& a) { return a.field_index == field_index; });
        if (array == batch.arrays.end())
          return failure(QueryError::CorruptLayout,
            "lookahead leaf array is absent");
        const bool compressed = state.layout->messages[batch.message_index].compression !=
          ArrowBufferCompression::None;
        auto leaf = resolved.value().begin +
          (span.steps.begin - span.source_step_begin);
        const auto end = resolved.value().begin +
          (span.steps.end - span.source_step_begin);
        if (end > resolved.value().end || end > array->leaf_count)
          return failure(QueryError::StalePlan,
            "lookahead demand escapes declared List length");
        while (leaf < end) {
          if (cancellation.cancelled()) return failure(QueryError::Cancelled,
            "window assembly cancelled");
          const auto tile_begin = compressed ? 0 :
            leaf - leaf % config.tile_elements;
          const auto tile_length = compressed ? array->leaf_count :
            std::min(config.tile_elements, array->leaf_count - tile_begin);
          const auto tile_end = tile_begin + tile_length;
          const auto range_end = std::min(end, tile_end);
          auto [it, inserted] = demands.try_emplace(
            TileKey{span.shard_index, span.batch_index, field_index,
              tile_begin, false});
          if (inserted) {
            first_use_order.push_back(it->first);
            it->second.leaf_count = tile_length;
            it->second.has_nulls = array->leaf_null_count != 0;
            std::uint64_t charge;
            if (!checked_mul(tile_length, 4, charge) ||
                !checked_add(charge, it->second.has_nulls
                  ? bitmap_bytes(tile_length) : 0, charge) ||
                !checked_add(resident, charge, resident))
              return failure(QueryError::ResourceExhausted,
                "lookahead fragment size overflows");
            // Layout fragments stay leased by each source. Retain at most the
            // remaining cache entry capacity for this window group; larger
            // groups are fetched one window at a time by the caller.
            const auto max_tiles = runtime->config().max_entries > sources.size()
              ? runtime->config().max_entries - sources.size() : 0;
            if (resident > runtime->config().memory_budget_bytes / 4 ||
                demands.size() > max_tiles)
              group_fits = false;
          }
          ++it->second.uses;
          it->second.ranges.push_back({leaf, range_end});
          leaf = range_end;
        }
      }
    }
    // Check the entire group before issuing any physical read. A group that
    // exceeds its logical lookahead allowance is fetched per window instead.
    std::uint64_t speculative_bytes = 0, speculative_ranges = 0;
    bool speculative_fits = true;
    std::map<std::pair<std::size_t, bool>, std::vector<std::pair<TileKey,
      ArrowFragmentDemand>>> by_shard;
    std::map<TileKey, ArrowFragmentDemand> canonical_demands;
    for (auto& entry : demands) {
      if (cancellation.cancelled()) return failure(QueryError::Cancelled,
        "window assembly cancelled");
      auto& ranges = entry.second.ranges;
      std::sort(ranges.begin(), ranges.end(),
        [](StepInterval a, StepInterval b) {
          return std::tie(a.begin, a.end) < std::tie(b.begin, b.end);
        });
      std::vector<StepInterval> merged;
      for (const auto range : ranges) {
        if (!merged.empty() && range.begin <= merged.back().end)
          merged.back().end = std::max(merged.back().end, range.end);
        else merged.push_back(range);
      }
      std::uint64_t demand_bytes = 0;
      for (const auto range : merged) {
        std::uint64_t part;
        if (!checked_mul(range.end - range.begin, 4, part) ||
            (entry.second.has_nulls &&
             !checked_add(part, bitmap_bytes(range.end - range.begin), part)) ||
            !checked_add(demand_bytes, part, demand_bytes))
          return failure(QueryError::ResourceExhausted,
            "lookahead logical demand overflows");
      }
      const auto remaining = options.max_payload_bytes - logical_bytes;
      if (logical_ranges >= options.max_ranges ||
          speculative_ranges >= options.max_ranges - logical_ranges ||
          speculative_bytes > remaining ||
          demand_bytes > remaining - speculative_bytes)
        speculative_fits = false;
      const auto& [shard, batch, field, tile, block] = entry.first;
      if (speculative_bytes <= remaining &&
          demand_bytes <= remaining - speculative_bytes)
        speculative_bytes += demand_bytes;
      if (speculative_ranges < options.max_ranges)
        ++speculative_ranges;
      ArrowFragmentDemand request{batch, field, merged.front().begin,
        merged.front().end - merged.front().begin};
      canonical_demands.emplace(entry.first, request);
      by_shard[{shard, block}].push_back({entry.first, request});
    }
    // Logical limits apply when a window is assembled. A prefetch that cannot
    // fit the remaining request allowance must not issue speculative reads
    // from later shards before that window reaches its budget check.
    if (!speculative_fits) return QueryStatus::success();
    if (!group_fits) {
      // Preserve first-use order for a large single window. The assembler
      // acquires bounded consecutive waves as it reaches their first tile.
      pending.reserve(first_use_order.size());
      for (const auto& key : first_use_order) {
        pending.push_back({key, canonical_demands.at(key)});
        pending_uses.emplace(key, demands.at(key).uses);
      }
      return QueryStatus::success();
    }
    // One provider is bound to one exact source version. Its batch method
    // claims only missing canonical tiles, coalesces adjacent physical spans,
    // and transports disjoint spans in bounded multi-range requests.
    for (auto& [source_group, entries] : by_shard) {
      const auto [shard, block] = source_group;
      auto& state = *sources.at(shard);
      auto max_tiles_per_call = batch_tile_limit();
      if (block) {
        std::uint64_t largest = 1;
        for (const auto& item : entries) {
          const auto& demand = demands.at(item.first);
          largest = std::max(largest, demand.leaf_count * 4 +
            (demand.has_nulls ? bitmap_bytes(demand.leaf_count) : 0) + 32);
        }
        max_tiles_per_call = std::min(max_tiles_per_call,
          static_cast<std::size_t>(std::max<std::uint64_t>(1,
            kMaxPayloadBytesPerBatch / largest)));
      }
      const auto lanes = std::min({read_parallelism,
        runtime->config().worker_threads, runtime->config().max_inflight});
      if (lanes > 1 && entries.size() > max_tiles_per_call) {
        // Bound aggregate in-flight tile admission, not only thread count.
        // The entire group's resident bytes were capped at memory_budget/4;
        // the cache additionally reserves each wave's two transport copies.
        // Nonresident groups use the sequential pending-wave path above.
        const auto wave_tiles = std::min(max_tiles_per_call,
          std::max<std::size_t>(1, runtime->config().max_inflight / lanes));
        using LaneItems = std::vector<std::pair<TileKey, Prefetched>>;
        using LaneResult = QueryResult<LaneItems>;
        std::atomic<bool> failed{false};
        std::vector<std::future<LaneResult>> tasks;
        tasks.reserve(lanes);
        const auto& wave_entries = entries;
        auto run_lane = [&](std::size_t lane) -> LaneResult {
          try {
            auto provider_options = options;
            provider_options.max_ranges = std::numeric_limits<std::size_t>::max();
            provider_options.max_payload_bytes =
              std::numeric_limits<std::uint64_t>::max();
            ArrowFragmentProvider provider(state.source, state.serialized_layout,
              runtime, provider_options, block ? block_config() : config, cancellation);
            LaneItems items;
            for (auto begin = lane * wave_tiles; begin < wave_entries.size();
                 begin += lanes * wave_tiles) {
              if (failed.load(std::memory_order_relaxed)) break;
              if (cancellation.cancelled()) {
                failed.store(true, std::memory_order_relaxed);
                return LaneResult::failure(failure(QueryError::Cancelled,
                  "window assembly cancelled"));
              }
              const auto end = std::min(wave_entries.size(), begin + wave_tiles);
              std::vector<ArrowFragmentDemand> request;
              request.reserve(end - begin);
              for (auto i = begin; i < end; ++i)
                request.push_back(wave_entries[i].second);
              auto fetched = provider.get_batch(request);
              if (!fetched.ok()) {
                failed.store(true, std::memory_order_relaxed);
                return LaneResult::failure(fetched.status());
              }
              if (fetched.value().size() != request.size()) {
                failed.store(true, std::memory_order_relaxed);
                return LaneResult::failure(failure(QueryError::Internal,
                  "Arrow fragment batch returned an incorrect item count"));
              }
              for (std::size_t i = 0; i < request.size(); ++i) {
                const auto& demand = request[i];
                auto& item = fetched.value()[i];
                if (!item.fragment || item.fragment->begin > demand.leaf_position ||
                    item.fragment->end < demand.leaf_position + demand.demand_elements) {
                  failed.store(true, std::memory_order_relaxed);
                  return LaneResult::failure(failure(QueryError::CorruptPayload,
                    "lookahead fragment does not cover its demand"));
                }
                items.push_back({wave_entries[begin + i].first,
                  {std::move(item.fragment), std::move(item.key_hex)}});
              }
            }
            return LaneResult::success(std::move(items));
          } catch (const std::bad_alloc&) {
            failed.store(true, std::memory_order_relaxed);
            return LaneResult::failure(failure(QueryError::ResourceExhausted,
              "parallel window lookahead allocation failed"));
          } catch (...) {
            failed.store(true, std::memory_order_relaxed);
            return LaneResult::failure(failure(QueryError::Internal,
              "parallel window lookahead failed"));
          }
        };
        try {
          for (std::size_t lane = 0; lane < lanes; ++lane)
            tasks.push_back(std::async(std::launch::async, run_lane, lane));
        } catch (...) {
          failed.store(true, std::memory_order_relaxed);
          // Join before returning: tasks borrow immutable planning state.
          for (auto& task : tasks) task.wait();
          return failure(QueryError::ResourceExhausted,
            "cannot start parallel window read wave");
        }
        QueryStatus status;
        for (auto& task : tasks) {
          auto fetched = task.get();
          if (!fetched.ok()) {
            if (status.ok()) status = fetched.status();
          } else {
            for (auto& item : fetched.value())
              lookahead.emplace(std::move(item.first), std::move(item.second));
          }
        }
        if (!status.ok()) return status;
        continue;
      }
      for (std::size_t begin = 0; begin < entries.size();
           begin += max_tiles_per_call) {
        if (cancellation.cancelled()) return failure(QueryError::Cancelled,
          "window assembly cancelled");
        const auto end = std::min(entries.size(), begin + max_tiles_per_call);
        std::vector<ArrowFragmentDemand> request;
        request.reserve(end - begin);
        for (auto i = begin; i < end; ++i)
          request.push_back(entries[i].second);
        auto fetched = provider(state, block).get_batch(request);
        if (!fetched.ok()) return fetched.status();
        if (fetched.value().size() != request.size())
          return failure(QueryError::Internal,
            "Arrow fragment batch returned an incorrect item count");
        for (std::size_t i = 0; i < request.size(); ++i) {
          const auto& demand = request[i];
          auto& item = fetched.value()[i];
          if (!item.fragment || item.fragment->begin > demand.leaf_position ||
              item.fragment->end < demand.leaf_position + demand.demand_elements)
            return failure(QueryError::CorruptPayload,
              "lookahead fragment does not cover its demand");
          lookahead.emplace(entries[begin + i].first,
            Prefetched{std::move(item.fragment), std::move(item.key_hex)});
        }
      }
    }
    lookahead_complete = true;
    return QueryStatus::success();
  }

  QueryStatus fetch_pending_wave() {
    if (pending_cursor >= pending.size())
      return failure(QueryError::Internal,
        "window tile was absent from its pending batch plan");
    const auto max_tiles_per_call = batch_tile_limit();
    const auto shard = std::get<0>(pending[pending_cursor].first);
    const auto block = std::get<4>(pending[pending_cursor].first);
    const auto begin = pending_cursor;
    std::vector<ArrowFragmentDemand> request;
    std::uint64_t wave_bytes = 0;
    while (pending_cursor < pending.size() &&
           request.size() < max_tiles_per_call &&
           std::get<0>(pending[pending_cursor].first) == shard &&
           std::get<4>(pending[pending_cursor].first) == block) {
      const auto& next = pending[pending_cursor].second;
      const auto charge = block ? next.demand_elements * 4 +
        bitmap_bytes(next.demand_elements) + 32 : 0;
      if (!request.empty() && charge > kMaxPayloadBytesPerBatch -
          std::min(wave_bytes, std::uint64_t{kMaxPayloadBytesPerBatch})) break;
      wave_bytes += charge;
      request.push_back(pending[pending_cursor++].second);
    }
    auto& state = *sources.at(shard);
    auto fetched = provider(state, block).get_batch(request);
    if (!fetched.ok()) return fetched.status();
    if (fetched.value().size() != request.size())
      return failure(QueryError::Internal,
        "window tile batch returned an incorrect item count");
    for (std::size_t i = 0; i < request.size(); ++i) {
      auto& item = fetched.value()[i];
      const auto& demand = request[i];
      if (!item.fragment || item.fragment->begin > demand.leaf_position ||
          item.fragment->end < demand.leaf_position + demand.demand_elements)
        return failure(QueryError::CorruptPayload,
          "window tile batch did not cover its demand");
      lookahead.emplace(pending[begin + i].first,
        Prefetched{std::move(item.fragment), std::move(item.key_hex)});
    }
    return QueryStatus::success();
  }

  QueryResult<std::optional<WindowSample>> assemble(PlannedWindow planned) {
    using Result = QueryResult<std::optional<WindowSample>>;
    if (cancelled || cancellation.cancelled())
      return Result::failure(failure(QueryError::Cancelled,
        "window assembly cancelled"));
    auto& descriptor = planned.descriptor;
    if (descriptor.view_id != view.view_id ||
        descriptor.input.begin > descriptor.input.end ||
        descriptor.target.begin > descriptor.target.end ||
        descriptor.input.end - descriptor.input.begin != view.input_length ||
        descriptor.target.end - descriptor.target.begin != view.target_length ||
        descriptor.input_channels.size() != view.input_channels.size() ||
        descriptor.target_channels.size() != view.target_channels.size())
      return Result::failure(failure(QueryError::InvalidQuery,
        "planned window does not match the training view"));
    std::uint64_t input_elements, target_elements, total_elements, value_bytes;
    if (!checked_mul(view.input_length, view.input_channels.size(), input_elements) ||
        !checked_mul(view.target_length, view.target_channels.size(), target_elements) ||
        !checked_add(input_elements, target_elements, total_elements) ||
        !checked_mul(total_elements, 5, value_bytes) ||
        value_bytes > limits.max_result_bytes - output_bytes ||
        total_elements > std::vector<float>().max_size())
      return Result::failure(failure(QueryError::ResourceExhausted,
        "window result exceeds output budget"));
    WindowSample sample;
    sample.descriptor = std::move(descriptor);
    sample.descriptor.source_spans.clear();
    sample.input_values.resize(static_cast<std::size_t>(input_elements), 0.0f);
    sample.input_validity.resize(static_cast<std::size_t>(input_elements), 0);
    sample.target_values.resize(static_cast<std::size_t>(target_elements), 0.0f);
    sample.target_validity.resize(static_cast<std::size_t>(target_elements), 0);
    const auto provenance_before = provenance_spans;
    std::uint64_t window_logical_bytes = 0, window_logical_ranges = 0;
    auto reject_window = [&]() -> Result {
      provenance_spans = provenance_before;
      logical_bytes -= window_logical_bytes;
      logical_ranges -= window_logical_ranges;
      return Result::success(std::nullopt);
    };

    // Compile selected channels once per physical block/role interval. A
    // step-major fragment is acquired once, then copied with time as the outer
    // loop; channel selection never performs a cache lookup for every value.
    using BlockGroupKey = std::tuple<WindowRole, std::size_t, std::uint64_t,
      std::uint64_t, std::uint64_t, std::string, std::uint64_t, std::uint64_t>;
    std::map<BlockGroupKey, std::vector<const PlannedWindowSpan*>> block_groups;
    for (const auto& demand : planned.spans) {
      const auto& span = demand.coverage;
      if (span.logical_row >= snapshot.rows.size())
        return Result::failure(failure(QueryError::InvalidQuery,
          "planned logical row is out of range"));
      if (snapshot.rows[span.logical_row].value_layout != SnapshotValueLayout::StepMajorFlat ||
          span.null_series || span.null_channel || span.missing_channel || !span.physical_ordinal)
        continue;
      block_groups[{demand.role, span.shard_index, span.batch_index,
        span.row_index, span.logical_row, span.field_id, span.steps.begin,
        span.steps.end}].push_back(&demand);
    }
    for (const auto& [group_key, group] : block_groups) {
      if (cancellation.cancelled()) return Result::failure(failure(QueryError::Cancelled,
        "window assembly cancelled"));
      const auto& first = *group.front();
      const auto& span = first.coverage;
      const auto& row = snapshot.rows[span.logical_row];
      const auto input = first.role == WindowRole::Input;
      const auto& refs = input ? view.input_channels : view.target_channels;
      const auto interval = input ? sample.descriptor.input : sample.descriptor.target;
      auto& values = input ? sample.input_values : sample.target_values;
      auto& validity = input ? sample.input_validity : sample.target_validity;
      const auto channels = row.declared_n_channels.value_or(0);
      if (!channels || span.steps.begin >= span.steps.end ||
          span.steps.begin < interval.begin || span.steps.end > interval.end ||
          span.source_step_begin > span.steps.begin ||
          span.steps.end - span.source_step_begin > row.steps.end - row.steps.begin)
        return Result::failure(failure(QueryError::InvalidQuery,
          "step-major coverage escapes its window or source block"));
      auto opened = source(span.shard_index);
      if (!opened.ok()) return Result::failure(opened.status());
      auto& state = *opened.value();
      if (span.batch_index >= state.layout->batches.size())
        return Result::failure(failure(QueryError::CorruptLayout,
          "step-major batch is out of range"));
      auto field = std::find_if(state.layout->fields.begin(), state.layout->fields.end(),
        [&](const ArrowNativeField& f) { return f.name == span.field_id; });
      if (field == state.layout->fields.end() || field->list_depth != 1)
        return Result::failure(failure(QueryError::InvalidQuery,
          "step-major training block requires a flat List field"));
      const auto field_index = static_cast<std::uint32_t>(field - state.layout->fields.begin());
      auto index = list_index(state, span.batch_index, field_index);
      if (!index.ok()) return Result::failure(index.status());
      auto resolved = resolve_array_value({1, PhysicalArrayLeafType::Float32},
        *index.value(), span.row_index, {});
      if (!resolved.ok()) return Result::failure(resolved.status());
      std::uint64_t declared_values;
      if (!resolved.value().valid ||
          !checked_mul(row.steps.end - row.steps.begin, channels, declared_values) ||
          resolved.value().end - resolved.value().begin != declared_values)
        return Result::failure(failure(QueryError::StalePlan,
          "step-major List length or validity differs from the snapshot"));
      const auto& batch = state.layout->batches[span.batch_index];
      auto array = std::find_if(batch.arrays.begin(), batch.arrays.end(),
        [&](const ArrowArrayIndex& a) { return a.field_index == field_index; });
      if (array == batch.arrays.end())
        return Result::failure(failure(QueryError::CorruptLayout,
          "step-major leaf array is absent"));
      if (array->leaf_count > block_config().tile_elements)
        return Result::failure(failure(QueryError::ResourceExhausted,
          "step-major block exceeds decoded buffer limit"));
      const auto steps = span.steps.end - span.steps.begin;
      std::uint64_t per_channel, logical;
      if (!checked_mul(steps, 4, per_channel) ||
          !checked_add(per_channel, array->leaf_null_count ? bitmap_bytes(steps) : 0,
            per_channel) || !checked_mul(per_channel, group.size(), logical) ||
          logical_ranges > options.max_ranges ||
          group.size() > options.max_ranges - logical_ranges ||
          logical > options.max_payload_bytes - logical_bytes)
        return Result::failure(failure(QueryError::ResourceExhausted,
          "window logical fragment budget exhausted"));
      bool contiguous = group.size() == refs.size();
      std::uint64_t first_physical = *group.front()->coverage.physical_ordinal;
      for (std::size_t i = 0; i < group.size(); ++i) {
        const auto& selected = *group[i];
        if (selected.channel_index >= refs.size() ||
            *selected.coverage.physical_ordinal >= channels ||
            selected.coverage.source_step_begin != span.source_step_begin)
          return Result::failure(failure(QueryError::InvalidQuery,
            "step-major selected channel is out of range"));
        contiguous = contiguous && selected.channel_index == i &&
          *selected.coverage.physical_ordinal == first_physical + i;
      }
      const TileKey tile{span.shard_index, span.batch_index, field_index, 0, true};
      auto prefetched = lookahead.find(tile);
      while (prefetched == lookahead.end() && !pending.empty()) {
        auto fetched = fetch_pending_wave();
        if (!fetched.ok()) return Result::failure(fetched);
        prefetched = lookahead.find(tile);
      }
      std::shared_ptr<const ArrowDecodedFragment> fragment;
      std::string fragment_key;
      auto& blocks = provider(state, true);
      if (prefetched != lookahead.end()) {
        fragment = prefetched->second.fragment;
        fragment_key = prefetched->second.key;
      } else {
        auto fetched = blocks.get(span.batch_index, field_index, resolved.value().begin,
          declared_values);
        if (!fetched.ok()) return Result::failure(fetched.status());
        fragment = std::move(fetched.value());
        fragment_key = blocks.last_fragment_key();
      }
      logical_bytes += logical; logical_ranges += group.size();
      window_logical_bytes += logical; window_logical_ranges += group.size();
      const auto& decoded = *fragment;
      if (decoded.begin > resolved.value().begin || decoded.end < resolved.value().end ||
          decoded.values.size() < resolved.value().end - decoded.begin)
        return Result::failure(failure(QueryError::CorruptPayload,
          "decoded training block does not cover its declared List"));
      for (std::uint64_t step = span.steps.begin; step < span.steps.end; ++step) {
        if ((step - span.steps.begin) % 4096 == 0 && cancellation.cancelled())
          return Result::failure(failure(QueryError::Cancelled, "window assembly cancelled"));
        const auto physical = resolved.value().begin +
          (step - span.source_step_begin) * channels;
        const auto output = (step - interval.begin) * refs.size();
        if (contiguous && decoded.validity.empty()) {
          std::copy_n(decoded.values.begin() + physical + first_physical - decoded.begin,
            group.size(), values.begin() + output);
          std::fill_n(validity.begin() + output, group.size(), std::uint8_t{1});
        } else {
          for (const auto* selected : group) {
            const auto leaf = physical + *selected->coverage.physical_ordinal;
            if (leaf_valid(decoded, leaf)) {
              values[output + selected->channel_index] = decoded.values[leaf - decoded.begin];
              validity[output + selected->channel_index] = 1;
            } else if (view.missing.null_value == MissingTreatment::RejectWindow)
              return reject_window();
          }
        }
      }
      if (provenance_spans > limits.max_provenance_spans ||
          group.size() > limits.max_provenance_spans - provenance_spans)
        return Result::failure(failure(QueryError::ResourceExhausted,
          "window provenance span budget exhausted"));
      provenance_spans += group.size();
      const auto& shard = snapshot.shards[span.shard_index];
      for (const auto* selected : group)
        sample.descriptor.source_spans.push_back({first.role,
          {shard.key.path, shard.key.version, "arrow"}, span.field_id,
          span.batch_index, span.row_index, refs[selected->channel_index].ordinal,
          span.steps, fragment_key});
      blocks.account_output(steps * group.size() * 5);
      if (!pending.empty()) {
        auto remaining = pending_uses.find(tile);
        if (remaining == pending_uses.end() || remaining->second < group.size())
          return Result::failure(failure(QueryError::Internal,
            "training block use count is inconsistent"));
        remaining->second -= group.size();
        if (!remaining->second) {
          pending_uses.erase(remaining);
          lookahead.erase(tile);
        }
      }
    }

    for (const auto& demand : planned.spans) {
      if (cancellation.cancelled())
        return Result::failure(failure(QueryError::Cancelled,
          "window assembly cancelled"));
      const auto& span = demand.coverage;
      const bool input = demand.role == WindowRole::Input;
      const auto& refs = input ? view.input_channels : view.target_channels;
      const auto interval = input ? sample.descriptor.input : sample.descriptor.target;
      auto& values = input ? sample.input_values : sample.target_values;
      auto& validity = input ? sample.input_validity : sample.target_validity;
      if (demand.channel_index >= refs.size() || span.steps.begin < interval.begin ||
          span.steps.end > interval.end || span.steps.begin >= span.steps.end ||
          span.shard_index >= snapshot.shards.size() ||
          span.source_step_begin > span.steps.begin)
        return Result::failure(failure(QueryError::InvalidQuery,
          "planned coverage span escapes its window"));
      if (span.null_series || span.missing_channel || span.null_channel ||
          !span.physical_ordinal) {
        const auto treatment = span.null_series ? view.missing.null_series :
          span.missing_channel || !span.physical_ordinal
            ? view.missing.missing_channel : view.missing.null_value;
        if (treatment == MissingTreatment::RejectWindow)
          return reject_window();
        if (++provenance_spans > limits.max_provenance_spans)
          return Result::failure(failure(QueryError::ResourceExhausted,
            "window provenance span budget exhausted"));
        const auto& shard = snapshot.shards[span.shard_index];
        sample.descriptor.source_spans.push_back({demand.role,
          {shard.key.path, shard.key.version, "arrow"}, span.field_id,
          span.batch_index, span.row_index, refs[demand.channel_index].ordinal,
          span.steps, {}});
        continue;
      }
      if (snapshot.rows[span.logical_row].value_layout == SnapshotValueLayout::StepMajorFlat)
        continue; // Already assembled once per block/channel group above.
      auto opened = source(span.shard_index);
      if (!opened.ok()) return Result::failure(opened.status());
      auto& state = *opened.value();
      if (span.batch_index >= state.layout->batches.size() ||
          span.logical_row >= snapshot.rows.size())
        return Result::failure(failure(QueryError::CorruptLayout,
          "window batch index exceeds Arrow layout"));
      auto field = std::find_if(state.layout->fields.begin(),
        state.layout->fields.end(), [&](const ArrowNativeField& f) {
          return f.name == span.field_id;
        });
      if (field == state.layout->fields.end())
        return Result::failure(failure(QueryError::UnknownField,
          "window field is absent from Arrow layout"));
      const auto field_index = static_cast<std::uint32_t>(
        field - state.layout->fields.begin());
      auto index = list_index(state, span.batch_index, field_index);
      if (!index.ok()) return Result::failure(index.status());
      ArrayValuePath path;
      if (field->list_depth == 2) path.child_indices.push_back(*span.physical_ordinal);
      else if (field->list_depth != 1 || *span.physical_ordinal != 0)
        return Result::failure(failure(QueryError::InvalidQuery,
          "window channel cannot resolve to a List child"));
      auto resolved = resolve_array_value({field->list_depth,
        PhysicalArrayLeafType::Float32}, *index.value(), span.row_index, path);
      if (!resolved.ok()) return Result::failure(resolved.status());
      if (!resolved.value().valid || resolved.value().begin > resolved.value().end)
        return Result::failure(failure(QueryError::StalePlan,
          "window List null state differs from coverage index"));
      const auto& row = snapshot.rows[span.logical_row];
      const auto row_length = row.steps.end - row.steps.begin;
      if (resolved.value().end - resolved.value().begin != row_length ||
          span.steps.end - span.source_step_begin > row_length)
        return Result::failure(failure(QueryError::StalePlan,
          "window List length differs from snapshot step coverage"));
      const auto& batch = state.layout->batches[span.batch_index];
      auto array = std::find_if(batch.arrays.begin(), batch.arrays.end(),
        [&](const ArrowArrayIndex& a) { return a.field_index == field_index; });
      if (array == batch.arrays.end())
        return Result::failure(failure(QueryError::CorruptLayout,
          "window leaf array is absent"));
      const auto compressed = state.layout->messages[batch.message_index].compression !=
        ArrowBufferCompression::None;
      std::uint64_t leaf = resolved.value().begin +
        (span.steps.begin - span.source_step_begin);
      const std::uint64_t end = resolved.value().begin +
        (span.steps.end - span.source_step_begin);
      while (leaf < end) {
        if (cancellation.cancelled())
          return Result::failure(failure(QueryError::Cancelled,
            "window assembly cancelled"));
        const auto tile_begin = compressed ? 0 : leaf - leaf % config.tile_elements;
        const auto tile_end = compressed ? array->leaf_count :
          std::min(array->leaf_count, tile_begin +
            std::min(config.tile_elements, array->leaf_count - tile_begin));
        const auto count = std::min(end - leaf, tile_end - leaf);
        if (!count)
          return Result::failure(failure(QueryError::CorruptLayout,
            "window tile has zero available leaf values"));
        const TileKey tile{span.shard_index, span.batch_index,
          field_index, tile_begin, false};
        auto prefetched = lookahead.find(tile);
        if (prefetched == lookahead.end() && !pending.empty()) {
          auto fetched = fetch_pending_wave();
          if (!fetched.ok()) return Result::failure(fetched);
          prefetched = lookahead.find(tile);
        }
        std::shared_ptr<const ArrowDecodedFragment> fragment;
        std::string fragment_key;
        std::uint64_t logical;
        if (!checked_mul(count, 4, logical) ||
            (array->leaf_null_count &&
             !checked_add(logical, bitmap_bytes(count), logical)) ||
            logical_ranges >= options.max_ranges ||
            logical > options.max_payload_bytes - logical_bytes)
          return Result::failure(failure(QueryError::ResourceExhausted,
            "window logical fragment budget exhausted"));
        if (prefetched != lookahead.end()) {
          fragment = prefetched->second.fragment;
          fragment_key = prefetched->second.key;
        } else {
          auto fetched = state.provider->get(span.batch_index, field_index,
            leaf, count);
          if (!fetched.ok()) return Result::failure(fetched.status());
          fragment = std::move(fetched.value());
          fragment_key = state.provider->last_fragment_key();
        }
        logical_bytes += logical; ++logical_ranges;
        window_logical_bytes += logical; ++window_logical_ranges;
        const auto& decoded = *fragment;
        if (decoded.begin > leaf || decoded.end < leaf + count)
          return Result::failure(failure(QueryError::CorruptPayload,
            "decoded fragment does not cover planned leaf range"));
        const auto steps_begin = span.source_step_begin +
          (leaf - resolved.value().begin);
        for (std::uint64_t i = 0; i < count; ++i) {
          if (i % 4096 == 0 && cancellation.cancelled())
            return Result::failure(failure(QueryError::Cancelled,
              "window assembly cancelled"));
          const auto step = steps_begin + i;
          const auto output = (step - interval.begin) * refs.size() +
            demand.channel_index;
          const auto position = leaf + i - decoded.begin;
          if (output >= values.size() || position >= decoded.values.size())
            return Result::failure(failure(QueryError::CorruptPayload,
              "decoded fragment escapes window output"));
          if (leaf_valid(decoded, leaf + i)) {
            values[static_cast<std::size_t>(output)] =
              decoded.values[static_cast<std::size_t>(position)];
            validity[static_cast<std::size_t>(output)] = 1;
          } else if (view.missing.null_value == MissingTreatment::RejectWindow)
            return reject_window();
        }
        if (++provenance_spans > limits.max_provenance_spans)
          return Result::failure(failure(QueryError::ResourceExhausted,
            "window provenance span budget exhausted"));
        const auto& shard = snapshot.shards[span.shard_index];
        sample.descriptor.source_spans.push_back({demand.role,
          {shard.key.path, shard.key.version, "arrow"}, span.field_id,
          span.batch_index, span.row_index, refs[demand.channel_index].ordinal,
          {steps_begin, steps_begin + count},
          std::move(fragment_key)});
        state.provider->account_output(count * 5);
        if (!pending.empty()) {
          auto remaining = pending_uses.find(tile);
          if (remaining == pending_uses.end() || !remaining->second)
            return Result::failure(failure(QueryError::Internal,
              "window tile use count is inconsistent"));
          if (--remaining->second == 0) {
            pending_uses.erase(remaining);
            lookahead.erase(tile);
          }
        }
        leaf += count;
      }
    }
    output_bytes += value_bytes;
    return Result::success(std::move(sample));
  }
};

WindowAssembler::WindowAssembler(std::unique_ptr<Impl> impl)
  : impl_(std::move(impl)) {}
WindowAssembler::WindowAssembler(WindowAssembler&&) noexcept = default;
WindowAssembler& WindowAssembler::operator=(WindowAssembler&&) noexcept = default;
WindowAssembler::~WindowAssembler() = default;

QueryResult<std::shared_ptr<const WindowAssembler::PreparedContext>>
WindowAssembler::prepare_context(const DatasetSnapshot& snapshot,
    std::shared_ptr<const StructuredBlobStorage> storage,
    std::uint64_t max_metadata_bytes) {
  using Result = QueryResult<std::shared_ptr<const PreparedContext>>;
  auto status = validate_dataset_snapshot_runtime(snapshot, storage);
  if (!status.ok()) return Result::failure(status);
  try {
    const auto base_bytes = sizeof(PreparedContext) +
      snapshot.shards.size() * sizeof(PreparedContext::Source);
    if (base_bytes > max_metadata_bytes) return Result::success({});
    auto context = std::make_shared<PreparedContext>();
    context->sources.resize(snapshot.shards.size());
    context->metadata_bytes = sizeof(PreparedContext) +
      context->sources.capacity() * sizeof(PreparedContext::Source);
    if (context->metadata_bytes > max_metadata_bytes) return Result::success({});
    std::vector<bool> needed(snapshot.shards.size(), false);
    for (const auto& row : snapshot.rows) needed.at(row.shard_index) = true;
    for (std::size_t shard = 0; shard < snapshot.shards.size(); ++shard) {
      if (!needed[shard]) continue;
      const auto& expected = snapshot.manifests[shard];
      std::uint64_t charge;
      // Compiled metadata is an optimization. Bound both the storage read
      // and retained expansion before allocation; over-budget sources keep
      // the existing request-local, budgeted layout path.
      if (expected.layout_size > ArrowLayoutLimits{}.max_layout_bytes ||
          !checked_mul(expected.layout_size, 17, charge) ||
          !checked_add(charge, 65536, charge) ||
          charge > max_metadata_bytes - context->metadata_bytes) continue;
      auto opened = snapshot.read_context
        ? open_catalog_query_source(storage, expected, snapshot.storage_namespace)
        : open_structured_blob_query_source(storage, snapshot.shards[shard].key);
      if (!opened.ok()) {
        // A deleted unused shard must not disable windows that only reach
        // other shards. Keep an empty compiled slot; execution will reopen
        // and report the exact source error if that shard is demanded.
        if (opened.status().code() == QueryError::SourceNotFound) continue;
        return Result::failure(opened.status());
      }
      const auto& actual = opened.value()->manifest();
      if (opened.value()->storage_namespace() != snapshot.storage_namespace ||
          actual.revision != expected.revision || actual.format_id != expected.format_id ||
          actual.layout_size != expected.layout_size || actual.payload_size != expected.payload_size)
        return Result::failure(failure(QueryError::StalePlan,
          "source changed while compiling training layouts"));
      auto bytes = opened.value()->read_layout(0, expected.layout_size);
      if (!bytes.ok()) return Result::failure(bytes.status());
      if (bytes.value().size() != expected.layout_size)
        return Result::failure(failure(QueryError::ReadFailure,
          "short Arrow layout read while compiling training context"));
      auto parsed = parse_arrow_layout(bytes.value(), expected.payload_size);
      if (!parsed.ok()) return Result::failure(parsed.status());
      auto& target = context->sources[shard];
      target.serialized_layout = std::make_shared<const BinaryValue>(std::move(bytes.value()));
      target.layout = std::make_shared<const ArrowLayoutDocument>(std::move(parsed.value()));
      target.metadata_bytes = charge;
      context->metadata_bytes += charge;
    }
    for (const auto& row : snapshot.rows) {
      auto& target = context->sources.at(row.shard_index);
      if (!target.layout) continue;
      for (const auto& binding : row.channel_bindings()) {
        if (!binding.physical_ordinal) continue;
        auto field = std::find_if(target.layout->fields.begin(), target.layout->fields.end(),
          [&](const ArrowNativeField& f) { return f.name == binding.field_id; });
        if (field == target.layout->fields.end())
          return Result::failure(failure(QueryError::UnknownField,
            "prepared training field is absent from Arrow layout"));
        const auto field_index = static_cast<std::uint32_t>(field - target.layout->fields.begin());
        const auto key = std::make_pair(row.batch_index, field_index);
        if (target.list_indexes.find(key) != target.list_indexes.end()) continue;
        if (max_metadata_bytes - context->metadata_bytes < 256) {
          context->metadata_bytes -= target.metadata_bytes;
          target = PreparedContext::Source{};
          break;
        }
        auto index = detail::make_validated_arrow_list_index(target.layout, row.batch_index, field_index);
        if (!index.ok()) return Result::failure(index.status());
        target.list_indexes.emplace(key, std::move(index.value()));
        context->metadata_bytes += 256;
        target.metadata_bytes += 256;
      }
    }
    return Result::success(std::move(context));
  } catch (const std::bad_alloc&) {
    return Result::failure(failure(QueryError::ResourceExhausted,
      "prepared training context allocation failed"));
  }
}

std::uint64_t WindowAssembler::prepared_metadata_bytes(
    const std::shared_ptr<const PreparedContext>& context) noexcept {
  return context ? context->metadata_bytes : 0;
}

QueryResult<WindowAssembler> WindowAssembler::create(
    const DatasetSnapshot& snapshot,
    std::shared_ptr<const StructuredBlobStorage> storage,
    std::shared_ptr<QueryRuntime> runtime,
    TrainingViewSpec view, TrainingQueryLimits limits,
    QueryExecutionOptions options, ArrowDecodeConfig config,
    CancellationToken cancellation, std::uint64_t read_parallelism) {
  return create_impl(snapshot, std::move(storage), std::move(runtime),
    std::move(view), limits, options, config, std::move(cancellation),
    read_parallelism, false);
}

QueryResult<WindowAssembler> WindowAssembler::create_prepared(
    const DatasetSnapshot& snapshot,
    std::shared_ptr<const StructuredBlobStorage> storage,
    std::shared_ptr<QueryRuntime> runtime,
    TrainingViewSpec view, TrainingQueryLimits limits,
    QueryExecutionOptions options, ArrowDecodeConfig config,
    CancellationToken cancellation, std::uint64_t read_parallelism,
    std::shared_ptr<const PreparedContext> context) {
  return create_impl(snapshot, std::move(storage), std::move(runtime),
    std::move(view), limits, options, config, std::move(cancellation),
    read_parallelism, true, std::move(context));
}

QueryResult<WindowAssembler> WindowAssembler::create_impl(
    const DatasetSnapshot& snapshot,
    std::shared_ptr<const StructuredBlobStorage> storage,
    std::shared_ptr<QueryRuntime> runtime,
    TrainingViewSpec view, TrainingQueryLimits limits,
    QueryExecutionOptions options, ArrowDecodeConfig config,
    CancellationToken cancellation, std::uint64_t read_parallelism,
    bool prepared, std::shared_ptr<const PreparedContext> context) {
  if (!storage || !runtime || !config.tile_elements ||
      !config.max_buffer_bytes || !options.max_ranges ||
      !options.max_layout_bytes || !options.max_payload_bytes ||
      !limits.max_result_bytes || !limits.max_provenance_spans ||
      !read_parallelism || read_parallelism > 4 ||
      view.snapshot_fingerprint != snapshot.fingerprint)
    return QueryResult<WindowAssembler>::failure(failure(QueryError::InvalidQuery,
      "invalid window assembler dependencies or budgets"));
  if (!prepared) {
    auto runtime_status = validate_dataset_snapshot_runtime(snapshot, storage);
    if (!runtime_status.ok())
      return QueryResult<WindowAssembler>::failure(runtime_status);
  } else if (storage->storage_namespace() != snapshot.storage_namespace) {
    return QueryResult<WindowAssembler>::failure(failure(QueryError::StalePlan,
      "dataset snapshot belongs to a different storage namespace"));
  }
  return QueryResult<WindowAssembler>::success(WindowAssembler(
    std::make_unique<Impl>(snapshot, std::move(storage), std::move(runtime),
      std::move(view), limits, options, config, std::move(cancellation),
      static_cast<std::size_t>(read_parallelism), std::move(context))));
}

QueryResult<std::optional<WindowSample>> WindowAssembler::assemble(
    PlannedWindow planned) {
  try { return impl_->assemble(std::move(planned)); }
  catch (const std::bad_alloc&) {
    return QueryResult<std::optional<WindowSample>>::failure(failure(
      QueryError::ResourceExhausted, "window assembly allocation failed"));
  }
}

QueryStatus WindowAssembler::prepare_lookahead(
    const std::vector<PlannedWindow>& windows) {
  try { return impl_->prepare_lookahead(windows); }
  catch (const std::bad_alloc&) {
    return failure(QueryError::ResourceExhausted,
      "window lookahead allocation failed");
  }
}

void WindowAssembler::release_lookahead() {
  impl_->lookahead.clear();
  impl_->pending.clear();
  impl_->pending_uses.clear();
  impl_->pending_cursor = 0;
  impl_->lookahead_complete = false;
}

bool WindowAssembler::lookahead_complete() const {
  return impl_->lookahead_complete;
}

void WindowAssembler::cancel() {
  impl_->cancelled = true;
  impl_->cancellation.cancel();
  for (auto& source : impl_->sources) {
    source.second->provider->cancel();
    if (source.second->block_provider) source.second->block_provider->cancel();
  }
}

} // namespace iotdbfs
