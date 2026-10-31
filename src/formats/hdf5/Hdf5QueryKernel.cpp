#include "iotdbfs/formats/hdf5/Hdf5QueryKernel.h"
#include "iotdbfs/formats/hdf5/Hdf5SourceDriver.h"
#include "iotdbfs/query/core/QueryProfile.h"

#include <algorithm>
#include <limits>
#include <cmath>
#include <cstring>
#include <mutex>

namespace iotdbfs {
namespace {
QueryStatus failure(QueryError code, const char* message) {
  return QueryStatus::failure(code, QueryPhase::Execution, message);
}
using BatchResult = QueryResult<std::optional<QueryBatch>>;
constexpr std::size_t kInt16Domain = 1U << 16;
constexpr std::uint64_t kInt16LookupBytes = kInt16Domain * (sizeof(double) + sizeof(std::uint8_t));
// Building every possible raw value is profitable only for a sufficiently
// large selection; tiny point queries retain the direct path.
constexpr std::uint64_t kInt16LookupMinRows = 8 * kInt16Domain;

bool scientific_valid(double value, const Hdf5CfBinding& cf) {
  if (!std::isfinite(value) || (cf.valid_min && value < *cf.valid_min) ||
      (cf.valid_max && value > *cf.valid_max)) return false;
  for (double missing : cf.missing_values)
    if (value == missing || (std::isnan(value) && std::isnan(missing))) return false;
  return true;
}

struct Int16Lookup {
  // States: 0 = masked, 1 = valid, 2 = transform overflow. An overflow in
  // an unselected raw value must not fail a query that never encounters it.
  std::vector<double> values = std::vector<double>(kInt16Domain);
  std::vector<std::uint8_t> states = std::vector<std::uint8_t>(kInt16Domain);
  explicit Int16Lookup(const Hdf5CfBinding& cf) {
    for (int raw = -32768; raw <= 32767; ++raw) {
      const auto key = static_cast<std::uint16_t>(raw);
      if (!scientific_valid(raw, cf)) continue;
      const double scaled = static_cast<double>(raw) * cf.scale_factor;
      const double transformed = scaled + cf.add_offset;
      if (!std::isfinite(transformed)) { states[key] = 2; continue; }
      values[key] = transformed;
      states[key] = 1;
    }
  }
};

template<class Native, bool Lookup = false>
QueryStatus convert_scientific(std::vector<double>& values,
    std::vector<std::uint8_t>& validity, std::uint64_t run_length,
    const Hdf5CfBinding& cf, const Int16Lookup* lookup,
    const CancellationToken& cancelled) {
  for (std::uint64_t row = 0; row < values.size(); row += run_length) {
    if (cancelled.cancelled())
      return failure(QueryError::Cancelled, "HDF5 query cancelled during conversion");
    const auto* raw = reinterpret_cast<const std::uint8_t*>(values.data() + row);
    // Native values occupy the start of each logical Float64 row. Reading
    // backwards keeps unconverted bytes intact while expanding in place.
    for (std::uint64_t i = run_length; i-- > 0;) {
      Native native;
      std::memcpy(&native, raw + i * sizeof(Native), sizeof(Native));
      double transformed = 0;
      bool valid;
      if constexpr (Lookup) {
        const auto key = static_cast<std::uint16_t>(native);
        const auto state = lookup->states[key];
        if (state == 2)
          return failure(QueryError::CorruptPayload, "HDF5 CF transform overflowed");
        valid = state == 1;
        if (valid) transformed = lookup->values[key];
      } else {
        const double value = static_cast<double>(native);
        valid = scientific_valid(value, cf);
        if (valid) {
          const double scaled = value * cf.scale_factor;
          transformed = scaled + cf.add_offset;
          if (!std::isfinite(transformed))
            return failure(QueryError::CorruptPayload, "HDF5 CF transform overflowed");
        }
      }
      if (!valid) validity[(row + i) / 8] &=
        static_cast<std::uint8_t>(~(1U << ((row + i) % 8)));
      values[row + i] = transformed;
    }
  }
  return QueryStatus::success();
}

QueryStatus expand_int16_physical(std::vector<std::int32_t>& values,
    std::uint64_t run_length, const CancellationToken& cancelled) {
  for (std::uint64_t row = 0; row < values.size(); row += run_length) {
    if (cancelled.cancelled())
      return failure(QueryError::Cancelled, "HDF5 query cancelled during conversion");
    const auto* raw = reinterpret_cast<const std::uint8_t*>(values.data() + row);
    for (std::uint64_t i = run_length; i-- > 0;) {
      std::int16_t native;
      std::memcpy(&native, raw + i * sizeof(native), sizeof(native));
      values[row + i] = native;
    }
  }
  return QueryStatus::success();
}

class Hdf5Cursor final : public QueryBatchCursor {
public:
  Hdf5Cursor(Hdf5QueryBinding binding, LogicalSchema schema,
      Hdf5SlabIterator slabs, std::unique_ptr<Hdf5FragmentProvider> provider,
      CancellationToken cancelled, QuerySourceReference source, bool chunked,
      std::function<void(const Hdf5FragmentMetrics&)> observer,
      bool use_int16_lookup, std::unique_ptr<Hdf5SourceDriver> native = {},
      std::uint64_t native_output_limit = 0)
    : binding_(std::move(binding)), schema_(std::move(schema)),
      slabs_(std::move(slabs)), provider_(std::move(provider)),
      cancelled_(std::move(cancelled)), source_(std::move(source)),
      chunked_(chunked), observer_(std::move(observer)), native_(std::move(native)),
      native_output_limit_(native_output_limit) {
    if (use_int16_lookup) {
      QueryProfileTimer build("hdf5.science.int16_lookup.build");
      int16_lookup_ = std::make_unique<Int16Lookup>(*binding_.cf_binding());
      query_profile_count("hdf5.science.int16_lookup.builds", 1);
    }
  }
  const LogicalSchema& schema() const override { return schema_; }
  void cancel() override {
    // Safe even when called reentrantly by read_payload on this same thread.
    // The active next() or subsequent call performs cleanup while owning mu_.
    cancelled_.cancel();
  }
  BatchResult next() override {
    std::lock_guard<std::mutex> lock(mu_);
    try {
      auto result = advance();
      observe();
      if (!result.ok()) { sticky_ = result.status(); clear(); }
      else if (cancelled_.cancelled()) {
        sticky_ = failure(QueryError::Cancelled, "HDF5 query cancelled");
        clear(); return BatchResult::failure(sticky_);
      }
      return result;
    } catch (const std::bad_alloc&) {
      sticky_ = failure(QueryError::ResourceExhausted, "HDF5 output allocation failed");
    } catch (const std::exception&) {
      sticky_ = failure(QueryError::Internal, "HDF5 cursor failed");
    } catch (...) {
      sticky_ = failure(QueryError::Internal, "HDF5 cursor threw");
    }
    observe(); clear(); return BatchResult::failure(sticky_);
  }
private:
  void observe() noexcept {
    if (observer_ && provider_) try { observer_(provider_->metrics()); } catch (...) {}
  }
  void clear() {
    if (provider_) provider_->clear();
    native_.reset();
  }
  BatchResult advance() {
    QueryProfileTimer assembly("hdf5.assemble");
    if (!sticky_.ok()) return BatchResult::failure(sticky_);
    if (cancelled_.cancelled())
      return BatchResult::failure(failure(QueryError::Cancelled, "HDF5 query cancelled"));
    auto slab = slabs_.next();
    if (!slab.ok()) return BatchResult::failure(slab.status());
    if (!slab.value()) { clear(); return BatchResult::success(std::nullopt); }
    const auto& rectangle = *slab.value();
    if (provider_) provider_->begin_slab(rectangle.domain.origin, rectangle.domain.shape);
    const auto rows = rectangle.row_count;
    const auto output_bytes = rows * binding_.projected_row_bytes();
    if (provider_) {
      auto reservation = provider_->reserve_output(output_bytes);
      if (!reservation.ok()) return BatchResult::failure(reservation);
    } else if (!native_ || output_bytes > native_output_limit_) {
      return BatchResult::failure(failure(QueryError::ResourceExhausted,
        "HDF5 native slab exceeds reserved output/subset budget"));
    }
    std::vector<LogicalArray> columns;
    columns.reserve(schema_.fields().size());
    std::size_t value_column = 0;
    std::optional<std::size_t> gps_column;
    std::vector<std::pair<std::size_t, const Hdf5CfAxis*>> cf_columns;
    std::vector<std::pair<std::size_t, std::size_t>> axes;
    for (std::size_t i = 0; i < schema_.fields().size(); ++i) {
      const auto& field = schema_.fields()[i];
      if (field.field_id == binding_.value_field().field_id) {
        value_column = i;
        // H5Dread already returns owned physical values in this exact logical
        // type (including Int16 -> Int32). Do not allocate another full array
        // just to copy it immediately after the native library call.
        const auto count = native_ && !binding_.scientific_values() ? 0 : rows;
        if (field.type.id == LogicalTypeId::Int32)
          columns.push_back({field.type, std::vector<std::int32_t>(count), {}});
        else if (field.type.id == LogicalTypeId::Float32)
          columns.push_back({field.type, std::vector<float>(count), {}});
        else columns.push_back({field.type, std::vector<double>(count), binding_.scientific_values() ? std::vector<std::uint8_t>((rows + 7) / 8, 0xff) : std::vector<std::uint8_t>{}});
      } else if (field.field_id == kHdf5GpsSecondsField) {
        if (!binding_.gps_axis())
          return BatchResult::failure(failure(QueryError::Internal, "HDF5 GPS projection has no bound coordinate grid"));
        gps_column = i;
        columns.push_back({field.type, std::vector<double>(rows), {}});
      } else if (field.field_id == kHdf5TimeField || field.field_id == kHdf5LatitudeField ||
                 field.field_id == kHdf5LongitudeField || field.field_id == kHdf5PressureField) {
        const std::string role = field.field_id == kHdf5TimeField ? "time" :
          field.field_id == kHdf5LatitudeField ? "latitude" :
          field.field_id == kHdf5LongitudeField ? "longitude" : "pressure";
        if (!binding_.cf_binding()) return BatchResult::failure(failure(QueryError::Internal, "HDF5 CF projection has no binding"));
        const auto& bound = binding_.cf_binding()->axes;
        auto found = std::find_if(bound.begin(), bound.end(), [&](const auto& axis) { return axis.role == role; });
        if (found == bound.end()) return BatchResult::failure(failure(QueryError::Internal, "HDF5 CF projection has no coordinate"));
        cf_columns.emplace_back(i, &*found);
        columns.push_back({field.type, std::vector<double>(rows), {}});
      } else {
        std::size_t axis = 0;
        for (; axis < rectangle.domain.shape.size(); ++axis)
          if (field.field_id == hdf5_axis_field(axis).field_id) break;
        if (axis == rectangle.domain.shape.size())
          return BatchResult::failure(failure(QueryError::Internal, "HDF5 bound projection has an invalid axis"));
        axes.emplace_back(i, axis);
        columns.push_back({field.type, std::vector<std::uint64_t>(rows), {}});
      }
    }
    auto& value_array = columns[value_column];
    void* value_destination;
    std::uint64_t logical_width;
    if (value_array.type.id == LogicalTypeId::Int32) {
      value_destination = std::get<std::vector<std::int32_t>>(value_array.values).data();
      logical_width = sizeof(std::int32_t);
    } else if (value_array.type.id == LogicalTypeId::Float32) {
      value_destination = std::get<std::vector<float>>(value_array.values).data();
      logical_width = sizeof(float);
    } else {
      value_destination = std::get<std::vector<double>>(value_array.values).data();
      logical_width = sizeof(double);
    }
    const auto& shape = rectangle.domain.shape;
    const auto run_length = shape.back();
    auto next_coordinate = [&](std::vector<std::uint64_t>& coordinate) {
      // The innermost dimension is copied as one logical run.
      for (std::size_t axis = shape.size() - 1; axis > 0;) {
        --axis;
        if (++coordinate[axis] < rectangle.domain.origin[axis] + shape[axis]) break;
        coordinate[axis] = rectangle.domain.origin[axis];
      }
    };
    {
      // This phase includes fragment acquisition and native-value copying.
      // One timer per batch avoids clock and profile-map work for each run.
      QueryProfileTimer copy("hdf5.assemble.copy");
      if (native_) {
        auto subset = native_->read(binding_.descriptor().path,
          rectangle.domain.origin, rectangle.domain.shape);
        if (!subset.ok()) return BatchResult::failure(subset.status());
        const auto& actual = subset.value().descriptor;
        if (actual.shape != binding_.descriptor().shape ||
            actual.value_type != binding_.descriptor().value_type)
          return BatchResult::failure(failure(QueryError::CorruptPayload,
            "HDF5 native dataset differs from sealed descriptor"));
        if (!binding_.scientific_values()) {
          if (cancelled_.cancelled())
            return BatchResult::failure(failure(QueryError::Cancelled,
              "HDF5 native query cancelled during assembly"));
          const auto actual_rows = std::visit([](const auto& values) { return values.size(); },
            subset.value().values.values);
          if (subset.value().values.type.id != value_array.type.id ||
              subset.value().values.type.nullable != value_array.type.nullable ||
              subset.value().values.type.timezone != value_array.type.timezone ||
              actual_rows != rows || subset.value().origin != rectangle.domain.origin ||
              subset.value().shape != rectangle.domain.shape ||
              !subset.value().values.validity.empty())
            return BatchResult::failure(failure(QueryError::CorruptPayload,
              "HDF5 native output differs from physical schema"));
          value_array.values = std::move(subset.value().values.values);
          query_profile_count("hdf5.native.output.moved.bytes", rows * logical_width);
        } else {
          // Keep precisely the indexed engine's packed native-row representation
          // so the shared physical Int16 expansion / CF masking / conversion below
          // has identical inputs. Native H5Dread has already normalized endianness.
          auto* destination = static_cast<std::uint8_t*>(value_destination);
          for (std::uint64_t row = 0; row < rows; row += run_length) {
            if (cancelled_.cancelled())
              return BatchResult::failure(failure(QueryError::Cancelled,
                "HDF5 native query cancelled during assembly"));
            auto* out = destination + row * logical_width;
            if (actual.value_type == LogicalTypeId::Int32) {
              const auto& values = std::get<std::vector<std::int32_t>>(subset.value().values.values);
              for (std::uint64_t i = 0; i < run_length; ++i) {
                const auto raw = static_cast<std::int16_t>(values[row + i]);
                std::memcpy(out + i * sizeof(raw), &raw, sizeof(raw));
              }
            } else if (actual.value_type == LogicalTypeId::Float32) {
              const auto& values = std::get<std::vector<float>>(subset.value().values.values);
              std::memcpy(out, values.data() + row, run_length * sizeof(float));
            } else {
              const auto& values = std::get<std::vector<double>>(subset.value().values.values);
              std::memcpy(out, values.data() + row, run_length * sizeof(double));
            }
          }
          query_profile_count("hdf5.native.output.copied.bytes", rows *
            (actual.value_type == LogicalTypeId::Int32 ? sizeof(std::int16_t) :
             actual.value_type == LogicalTypeId::Float32 ? sizeof(float) : sizeof(double)));
        }
        query_profile_count("hdf5.native.slabs");
        query_profile_count("hdf5.native.output.rows", rows);
      } else if (chunked_) {
        auto copied = provider_->copy_chunked_slab(value_destination, rows * logical_width,
          logical_width, output_bytes);
        if (!copied.ok()) return BatchResult::failure(copied);
      } else {
        auto coordinate = rectangle.domain.origin;
        auto* destination = static_cast<std::uint8_t*>(value_destination);
        for (std::uint64_t row = 0; row < rows; row += run_length) {
          if (cancelled_.cancelled())
            return BatchResult::failure(failure(QueryError::Cancelled, "HDF5 query cancelled during assembly"));
          auto copied = provider_->copy_run(coordinate, run_length,
            destination + row * logical_width, output_bytes);
          if (!copied.ok()) return BatchResult::failure(copied);
          next_coordinate(coordinate);
        }
      }
    }
    if (provider_) provider_->finish_slab();
    if (binding_.scientific_values() || (!native_ && binding_.descriptor().value_type == LogicalTypeId::Int32)) {
      QueryProfileTimer conversion("hdf5.assemble.scientific_conversion");
      QueryStatus converted;
      if (!binding_.scientific_values()) {
        converted = expand_int16_physical(
          std::get<std::vector<std::int32_t>>(value_array.values), run_length, cancelled_);
      } else {
        auto& values = std::get<std::vector<double>>(value_array.values);
        auto& validity = value_array.validity;
        const auto& cf = *binding_.cf_binding();
        switch (binding_.descriptor().value_type) {
          case LogicalTypeId::Int32:
            converted = int16_lookup_ ?
              convert_scientific<std::int16_t, true>(values, validity, run_length, cf,
                int16_lookup_.get(), cancelled_) :
              convert_scientific<std::int16_t>(values, validity, run_length, cf,
                nullptr, cancelled_);
            break;
          case LogicalTypeId::Float32:
            converted = convert_scientific<float>(values, validity, run_length, cf,
              nullptr, cancelled_);
            break;
          case LogicalTypeId::Float64:
            converted = convert_scientific<double>(values, validity, run_length, cf,
              nullptr, cancelled_);
            break;
          default:
            return BatchResult::failure(failure(QueryError::Internal,
              "HDF5 scientific view has invalid native type"));
        }
      }
      if (!converted.ok()) return BatchResult::failure(converted);
    }
    if (!axes.empty() || !cf_columns.empty() || gps_column) {
      QueryProfileTimer coordinates("hdf5.assemble.coordinates");
      auto coordinate = rectangle.domain.origin;
      for (std::uint64_t row = 0; row < rows; row += run_length) {
        if (cancelled_.cancelled())
          return BatchResult::failure(failure(QueryError::Cancelled, "HDF5 query cancelled during coordinates"));
        for (const auto& axis : axes) {
          auto& values = std::get<std::vector<std::uint64_t>>(columns[axis.first].values);
          if (axis.second + 1 == shape.size()) {
            for (std::uint64_t step = 0; step < run_length; ++step)
              values[row + step] = coordinate.back() + step;
          } else std::fill_n(values.begin() + row, run_length, coordinate[axis.second]);
        }
        for (const auto& column : cf_columns) {
          auto& values = std::get<std::vector<double>>(columns[column.first].values);
          const auto& axis = *column.second;
          for (std::uint64_t step = 0; step < run_length; ++step)
            values[row + step] = axis.values[coordinate[axis.dimension] + (axis.dimension + 1 == shape.size() ? step : 0)];
        }
        if (gps_column) {
          auto& values = std::get<std::vector<double>>(columns[*gps_column].values);
          for (std::uint64_t step = 0; step < run_length; ++step)
            values[row + step] = binding_.gps_axis()->coordinate(coordinate[0] + step);
        }
        next_coordinate(coordinate);
      }
    }
    auto batch = QueryBatch::create(schema_, rows, std::move(columns), rectangle.domain, {source_});
    if (!batch.ok()) return BatchResult::failure(batch.status());
    query_profile_count("hdf5.output.rows", rows);
    query_profile_count("hdf5.output.bytes", binding_.scientific_values() ? output_bytes - rows + (rows + 7) / 8 : output_bytes);
    return BatchResult::success(std::move(batch.value()));
  }
  Hdf5QueryBinding binding_;
  LogicalSchema schema_;
  Hdf5SlabIterator slabs_;
  std::unique_ptr<Hdf5FragmentProvider> provider_;
  CancellationToken cancelled_;
  QuerySourceReference source_;
  bool chunked_{false};
  QueryStatus sticky_;
  std::function<void(const Hdf5FragmentMetrics&)> observer_;
  std::unique_ptr<Int16Lookup> int16_lookup_;
  std::unique_ptr<Hdf5SourceDriver> native_;
  std::uint64_t native_output_limit_{0};
  std::mutex mu_;
};
} // namespace

QueryResult<std::unique_ptr<QueryBatchCursor>> make_hdf5_query_cursor(
    std::shared_ptr<const QuerySource> source, Hdf5QueryBinding binding,
    Hdf5DatasetLayout dataset, const QueryExecutionOptions& options,
    Hdf5QueryKernelConfig config, std::shared_ptr<QueryRuntime> runtime, std::string identity,
    std::optional<Hdf5PreparedSelection> prepared, Hdf5QueryMode mode) {
  using Result = QueryResult<std::unique_ptr<QueryBatchCursor>>;
  if (!source || !options.target_batch_rows || !options.target_batch_bytes)
    return Result::failure(failure(QueryError::InvalidQuery, "HDF5 execution source and batch limits are required"));
  if (binding.row_count() > options.max_result_rows)
    return Result::failure(failure(QueryError::ResourceExhausted, "HDF5 execution result row limit exceeded"));
  if (mode == Hdf5QueryMode::NativeHdf5) {
    try {
    // All visible buffers are reserved together. libhdf5's filter internals
    // retain the SourceDriver contract: logical chunk bound, not a hard RSS cap.
    const std::uint64_t metadata_bytes = (config.native_session_cache_enabled ? 2ULL : 8ULL) * 1024 * 1024;
    const std::uint64_t raw_cache_bytes = config.native_session_cache_enabled ? 256ULL * 1024 : 0;
    constexpr std::uint64_t conversion_bytes = 1024ULL * 1024;
    std::uint64_t chunk_bytes = dataset.element_bytes;
    if (dataset.storage == Hdf5StorageLayout::Chunked) {
      if (dataset.chunk_shape.size() != binding.descriptor().shape.size())
        return Result::failure(failure(QueryError::CorruptLayout, "HDF5 native chunk rank is invalid"));
      for (auto dimension : dataset.chunk_shape) {
        if (!dimension || !chunk_bytes || chunk_bytes > config.max_decoded_chunk_bytes / dimension)
          return Result::failure(failure(QueryError::ResourceExhausted, "HDF5 native chunk exceeds decoded budget"));
        chunk_bytes *= dimension;
      }
    } else chunk_bytes = 0;
    if (!options.max_ranges || !options.max_payload_bytes || !config.max_encoded_chunk_bytes ||
        !config.max_decoded_chunk_bytes || config.max_buffer_bytes <= metadata_bytes + raw_cache_bytes + conversion_bytes ||
        chunk_bytes >= config.max_buffer_bytes - metadata_bytes - raw_cache_bytes - conversion_bytes)
      return Result::failure(failure(QueryError::ResourceExhausted, "HDF5 native working budget exhausted"));
    const auto output_limit = (config.max_buffer_bytes - metadata_bytes - raw_cache_bytes - conversion_bytes - chunk_bytes) / 2;
    QueryLimits limits;
    limits.max_result_rows = options.max_result_rows;
    limits.target_batch_rows = options.target_batch_rows;
    limits.target_batch_bytes = std::min(options.target_batch_bytes, output_limit);
    if (binding.row_count() && limits.target_batch_bytes < binding.projected_row_bytes())
      return Result::failure(failure(QueryError::ResourceExhausted, "HDF5 native budget cannot hold one output row"));
    const auto alignment = dataset.storage == Hdf5StorageLayout::Chunked ?
      dataset.chunk_shape : std::vector<std::uint64_t>{};
    auto slabs = Hdf5SlabIterator::create(binding, limits, alignment);
    if (!slabs.ok()) return Result::failure(slabs.status());
    if (!alignment.empty()) query_profile_count("hdf5.native.chunk_aligned.cursors");
    std::vector<LogicalField> fields;
    for (const auto& field : binding.projection()) fields.push_back(*binding.schema().find(field.field_id));
    auto schema = LogicalSchema::create(std::move(fields));
    if (!schema.ok()) return Result::failure(schema.status());
    CancellationToken cancelled;
    std::unique_ptr<Hdf5SourceDriver> driver;
    if (binding.row_count()) {
      Hdf5SourceOptions native_options;
      native_options.max_read_calls = std::min<std::size_t>(options.max_ranges, 1048576);
      native_options.max_read_bytes = options.max_payload_bytes;
      native_options.max_single_read_bytes = std::min<std::uint64_t>({config.max_encoded_chunk_bytes,
        options.max_payload_bytes, source->payload_read_capabilities().max_bytes, 1024ULL * 1024 * 1024});
      native_options.max_result_bytes = std::min<std::uint64_t>(output_limit, 1024ULL * 1024 * 1024);
      native_options.max_decoded_chunk_bytes = std::min<std::uint64_t>(config.max_decoded_chunk_bytes, 1024ULL * 1024 * 1024);
      native_options.metadata_cache_bytes = metadata_bytes;
      native_options.raw_chunk_cache_bytes = raw_cache_bytes;
      native_options.enable_cf_inventory = false;
      native_options.native_selection_identity = std::to_string(binding.descriptor().path.size()) +
        ":" + binding.descriptor().path;
      for (const auto &part : {binding.origin(), binding.shape()}) {
        native_options.native_selection_identity += ":" + std::to_string(part.size());
        for (auto value : part) native_options.native_selection_identity += ":" + std::to_string(value);
      }
      for (auto value : {std::uint64_t(limits.target_batch_rows), limits.target_batch_bytes, binding.projected_row_bytes()})
        native_options.native_selection_identity += ":" + std::to_string(value);
      native_options.native_selection_identity += ":alignment";
      for (auto value : alignment) native_options.native_selection_identity += ":" + std::to_string(value);
      native_options.cancelled = [cancelled] { return cancelled.cancelled(); };
      auto opened = Hdf5SourceDriver::open_local(source, std::move(native_options),
        config.native_session_cache_enabled ? config.native_session_pool : nullptr);
      if (!opened.ok()) return Result::failure(opened.status());
      driver = std::move(opened.value());
      auto actual = driver->describe(binding.descriptor().path);
      if (!actual.ok()) return Result::failure(actual.status());
      if (!actual.value().supported || actual.value().shape != binding.descriptor().shape ||
          actual.value().value_type != binding.descriptor().value_type)
        return Result::failure(failure(QueryError::CorruptPayload,
          "HDF5 native dataset differs from sealed descriptor"));
    }
    const auto& m = source->manifest();
    QuerySourceReference reference{m.key.path, m.key.version, m.format_id.value_or("hdf5")};
    return Result::success(std::make_unique<Hdf5Cursor>(std::move(binding), std::move(schema.value()),
      std::move(slabs.value()), nullptr, std::move(cancelled), std::move(reference), false,
      config.metrics_observer, false, std::move(driver), output_limit));
    } catch (const std::bad_alloc&) {
      return Result::failure(failure(QueryError::ResourceExhausted, "HDF5 native cursor allocation failed"));
    }
  }
  if (!prepared) {
    auto checked = prepare_hdf5_selection(dataset, binding, config,
      source->payload_read_capabilities());
    if (!checked.ok()) return Result::failure(checked.status());
    prepared = std::move(checked.value());
  }
  if (prepared->config_signature != hdf5_selection_config_signature(config))
    return Result::failure(failure(QueryError::StalePlan,
      "HDF5 execution decoder limits differ from prepared selection"));
  const auto largest = prepared->working_bytes;
  const bool use_int16_lookup = binding.scientific_values() &&
    binding.descriptor().value_type == LogicalTypeId::Int32 &&
    binding.row_count() >= kInt16LookupMinRows &&
    config.max_buffer_bytes - largest >= binding.projected_row_bytes() &&
    config.max_buffer_bytes - largest - binding.projected_row_bytes() >= kInt16LookupBytes;
  if (use_int16_lookup) config.max_buffer_bytes -= kInt16LookupBytes;
  QueryLimits limits;
  limits.max_result_rows = options.max_result_rows;
  limits.target_batch_rows = options.target_batch_rows;
  limits.target_batch_bytes = std::min(options.target_batch_bytes, config.max_buffer_bytes - largest);
  if (binding.row_count() && limits.target_batch_bytes < binding.projected_row_bytes())
    return Result::failure(failure(QueryError::ResourceExhausted, "HDF5 batch budget cannot hold one row and fragment"));
  // Hdf5SlabIterator validates a positive target even for zero-row schemas.
  if (!limits.target_batch_bytes) limits.target_batch_bytes = 1;
  auto slabs = Hdf5SlabIterator::create(binding, limits);
  if (!slabs.ok()) return Result::failure(slabs.status());
  try {
    std::vector<LogicalField> fields;
    for (const auto& field : binding.projection()) fields.push_back(*binding.schema().find(field.field_id));
    auto schema = LogicalSchema::create(std::move(fields));
    if (!schema.ok()) return Result::failure(schema.status());
    CancellationToken cancelled;
    const bool chunked = dataset.storage == Hdf5StorageLayout::Chunked;
    auto provider = Hdf5FragmentProvider::create(source, std::move(dataset), binding,
      options, config, cancelled, std::move(runtime), std::move(identity),
      std::move(*prepared));
    if (!provider.ok()) return Result::failure(provider.status());
    const auto& m = source->manifest();
    QuerySourceReference reference{m.key.path, m.key.version, m.format_id.value_or("hdf5")};
    return Result::success(std::make_unique<Hdf5Cursor>(std::move(binding), std::move(schema.value()),
      std::move(slabs.value()), std::move(provider.value()), std::move(cancelled),
      std::move(reference), chunked, config.metrics_observer, use_int16_lookup));
  } catch (const std::bad_alloc&) {
    return Result::failure(failure(QueryError::ResourceExhausted, "HDF5 cursor allocation failed"));
  }
}
} // namespace iotdbfs
