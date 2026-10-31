#include "iotdbfs/formats/hdf5/Hdf5Layout.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <map>
#include <openssl/sha.h>
#include <set>
#include <stdexcept>
#include <type_traits>

namespace iotdbfs {
namespace {
constexpr std::size_t kHeaderBytes = kHdf5LayoutHeaderBytes;
struct Failure {
  QueryStatus status;
};
[[noreturn]] void fail(QueryError code, const char *message) {
  throw Failure{QueryStatus::failure(code, QueryPhase::Planning, message)};
}
void check(bool yes, const char *message) {
  if (!yes)
    fail(QueryError::CorruptLayout, message);
}
void limit(bool yes, const char *message) {
  if (!yes)
    fail(QueryError::ResourceExhausted, message);
}
std::uint64_t add(std::uint64_t a, std::uint64_t b) {
  check(b <= UINT64_MAX - a, "HDF5 layout integer overflow");
  return a + b;
}
Hdf5LayoutDigest digest(const BinaryValue &v) {
  Hdf5LayoutDigest d{};
  SHA256(v.data(), v.size(), d.data());
  return d;
}
struct Encoder {
  BinaryValue b;
  std::uint64_t cap;
  void bytes(const void *p, std::size_t n) {
    limit(b.size() <= cap && n <= cap - b.size(),
          "HDF5 layout encoding exceeds byte budget");
    if (n)
      b.insert(b.end(), static_cast<const std::uint8_t *>(p),
               static_cast<const std::uint8_t *>(p) + n);
  }
  void u(std::uint64_t v, unsigned n = 8) {
    for (unsigned i = 0; i < n; ++i) {
      auto c = std::uint8_t(v >> (8 * i));
      bytes(&c, 1);
    }
  }
  void str(const std::string &s) {
    u(s.size());
    bytes(s.data(), s.size());
  }
  void vec(const std::vector<std::uint64_t> &v) {
    u(v.size());
    for (auto x : v)
      u(x);
  }
  void blob(const BinaryValue &v) {
    u(v.size());
    bytes(v.data(), v.size());
  }
};
struct Decoder {
  const BinaryValue &b;
  std::size_t p{0};
  const Hdf5LayoutLimits &limits;
  void need(std::size_t n) {
    check(n <= b.size() - p, "truncated HDF5 layout");
  }
  std::uint64_t u(unsigned n = 8) {
    need(n);
    std::uint64_t v = 0;
    for (unsigned i = 0; i < n; ++i)
      v |= std::uint64_t(b[p++]) << (8 * i);
    return v;
  }
  std::size_t count(std::size_t cap) {
    auto n = u();
    limit(n <= cap, "HDF5 layout collection exceeds limit");
    check(n <= b.size() - p, "impossible HDF5 layout collection count");
    return static_cast<std::size_t>(n);
  }
  std::string str() {
    auto n = count(limits.max_string_bytes);
    need(n);
    std::string s(reinterpret_cast<const char *>(b.data() + p), n);
    p += n;
    return s;
  }
  std::vector<std::uint64_t> vec() {
    auto n = count(32);
    check(n <= (b.size() - p) / 8, "truncated HDF5 extent vector");
    std::vector<std::uint64_t> v;
    v.reserve(n);
    while (n--)
      v.push_back(u());
    return v;
  }
  BinaryValue blob() {
    auto n = count(limits.max_directory_bytes);
    need(n);
    BinaryValue v(b.begin() + p, b.begin() + p + n);
    p += n;
    return v;
  }
  bool flag() {
    auto v = u(1);
    check(v <= 1, "invalid HDF5 layout boolean");
    return v != 0;
  }
  template <class E> E en(unsigned max) {
    auto n = u(1);
    check(n <= max, "invalid HDF5 layout enum");
    return static_cast<E>(n);
  }
  Hdf5LayoutDigest sha() {
    need(32);
    Hdf5LayoutDigest d{};
    std::copy_n(b.data() + p, 32, d.data());
    p += 32;
    return d;
  }
  void end() { check(p == b.size(), "trailing HDF5 layout bytes"); }
};
void put_attribute(Encoder &e, const Hdf5Attribute &a) {
  e.str(a.name);
  e.u(static_cast<unsigned>(a.type), 1);
  e.vec(a.shape);
  e.u(a.null_dataspace, 1);
  e.u(a.native_type_class, 4);
  e.u(a.native_element_bytes);
  e.u(static_cast<unsigned>(a.state), 1);
  e.str(a.reason);
  e.blob(a.values);
  e.u(a.strings.size());
  for (const auto &v : a.strings)
    e.str(v);
}
Hdf5Attribute get_attribute(Decoder &r) {
  Hdf5Attribute a;
  a.name = r.str();
  auto t = r.u(1);
  check(t <= static_cast<unsigned>(LogicalTypeId::TimestampNs),
        "invalid HDF5 attribute type");
  a.type = static_cast<LogicalTypeId>(t);
  a.shape = r.vec();
  a.null_dataspace = r.flag();
  a.native_type_class = r.u(4);
  a.native_element_bytes = r.u();
  a.state = r.en<Hdf5AttributeState>(4);
  a.reason = r.str();
  a.values = r.blob();
  auto n = r.count(r.limits.max_attribute_elements);
  while (n--)
    a.strings.push_back(r.str());
  return a;
}
void put_metadata(Encoder &e, const Hdf5LayoutObject &o) {
  e.str(o.canonical_path);
  e.u(o.attribute_count);
  e.u(o.attribute_count_known, 1);
  e.u(o.attributes_complete, 1);
  e.str(o.attributes_unavailable_reason);
  e.u(o.attributes.size());
  for (const auto &a : o.attributes)
    put_attribute(e, a);
  e.u(o.dimension_scales_complete, 1);
  e.str(o.dimension_scales_unavailable_reason);
  e.u(o.dimension_scales.size());
  for (const auto &c : o.dimension_scales) {
    e.str(c.dataset_object_id);
    e.u(c.axis, 4);
  }
  e.u(o.axis_associations_complete, 1);
  e.str(o.axis_associations_reason);
  e.u(o.axis_associations.size());
  for (const auto &a : o.axis_associations) {
    e.u(a.axis, 4);
    e.str(a.scale_object_id);
    e.str(a.evidence);
  }
  e.str(o.coordinate_index_reason);
  e.u(o.coordinate_index.has_value(), 1);
  if (o.coordinate_index) {
    const auto &c = *o.coordinate_index;
    e.str(c.role);
    e.str(c.units);
    e.str(c.calendar);
    e.u(c.ascending, 1);
    e.u(c.values.size());
    for (double value : c.values) {
      std::uint64_t bits;
      std::memcpy(&bits, &value, 8);
      e.u(bits);
    }
  }
}
void get_metadata(Decoder &r, Hdf5LayoutObject &o) {
  o.canonical_path = r.str();
  o.attribute_count = r.u();
  o.attribute_count_known = r.flag();
  o.attributes_complete = r.flag();
  o.attributes_unavailable_reason = r.str();
  auto n = r.count(r.limits.max_attributes_per_object);
  while (n--)
    o.attributes.push_back(get_attribute(r));
  o.dimension_scales_complete = r.flag();
  o.dimension_scales_unavailable_reason = r.str();
  n = r.count(r.limits.max_dimension_scale_candidates);
  while (n--)
    o.dimension_scales.push_back({r.str(), static_cast<std::uint32_t>(r.u(4))});
  o.axis_associations_complete = r.flag();
  o.axis_associations_reason = r.str();
  n = r.count(r.limits.max_dimension_scale_candidates);
  while (n--)
    o.axis_associations.push_back(
        {static_cast<std::uint32_t>(r.u(4)), r.str(), r.str()});
  o.coordinate_index_reason = r.str();
  if (r.flag()) {
    Hdf5CoordinateIndex c;
    c.role = r.str();
    c.units = r.str();
    c.calendar = r.str();
    c.ascending = r.flag();
    n = r.count(r.limits.max_coordinate_elements);
    limit(n <= r.limits.max_coordinate_bytes / 8, "HDF5 coordinate byte limit");
    r.need(n * 8);
    c.values.reserve(n);
    while (n--) {
      auto bits = r.u();
      double v;
      std::memcpy(&v, &bits, 8);
      c.values.push_back(v);
    }
    o.coordinate_index = std::move(c);
  }
}
void put_dataset(Encoder &e, const Hdf5DatasetLayout &d) {
  e.str(d.descriptor.path);
  e.u(static_cast<unsigned>(d.descriptor.value_type), 1);
  e.vec(d.descriptor.shape);
  e.u(d.descriptor.supported, 1);
  e.str(d.descriptor.unsupported_reason);
  e.u(static_cast<unsigned>(d.storage), 1);
  e.u(static_cast<unsigned>(d.byte_order), 1);
  e.u(d.native_type_class, 4);
  e.u(d.element_bytes);
  e.u(static_cast<unsigned>(d.fill_status), 1);
  e.blob(d.fill_value);
  e.u(d.allocation_time, 4);
  e.u(d.fill_time, 4);
  e.u(d.allocation_status, 4);
  e.u(d.contiguous_offset.has_value(), 1);
  if (d.contiguous_offset)
    e.u(*d.contiguous_offset);
  e.u(d.contiguous_size);
  e.u(d.chunk_options, 4);
  e.vec(d.chunk_shape);
  e.u(d.filters.size());
  for (const auto &f : d.filters) {
    e.u(f.id, 4);
    e.u(f.flags, 4);
    e.u(f.parameters.size());
    for (auto v : f.parameters)
      e.u(v, 4);
  }
  e.u(d.index_complete, 1);
  e.str(d.index_unavailable_reason);
  e.u(d.allocated_chunk_count);
}
Hdf5DatasetLayout get_dataset(Decoder &r) {
  Hdf5DatasetLayout d;
  d.descriptor.path = r.str();
  auto t = r.u(1);
  check(t <= static_cast<unsigned>(LogicalTypeId::TimestampNs),
        "invalid HDF5 logical type");
  d.descriptor.value_type = static_cast<LogicalTypeId>(t);
  d.descriptor.shape = r.vec();
  d.descriptor.supported = r.flag();
  d.descriptor.unsupported_reason = r.str();
  d.storage = r.en<Hdf5StorageLayout>(4);
  d.byte_order = r.en<Hdf5ByteOrder>(2);
  d.native_type_class = r.u(4);
  d.element_bytes = r.u();
  d.fill_status = r.en<Hdf5FillStatus>(2);
  d.fill_value = r.blob();
  d.allocation_time = r.u(4);
  d.fill_time = r.u(4);
  d.allocation_status = r.u(4);
  if (r.flag())
    d.contiguous_offset = r.u();
  d.contiguous_size = r.u();
  d.chunk_options = r.u(4);
  d.chunk_shape = r.vec();
  auto n = r.count(32);
  while (n--) {
    Hdf5FilterDescription f;
    f.id = r.u(4);
    f.flags = r.u(4);
    auto k = r.count(r.limits.max_filter_parameters);
    check(k <= (r.b.size() - r.p) / 4, "truncated filter parameters");
    while (k--)
      f.parameters.push_back(r.u(4));
    d.filters.push_back(std::move(f));
  }
  d.index_complete = r.flag();
  d.index_unavailable_reason = r.str();
  d.allocated_chunk_count = r.u();
  return d;
}
void put_page(Encoder &e, const Hdf5ChunkPage &p) {
  e.str(p.object_id);
  e.u(p.offset);
  e.u(p.length);
  e.u(p.record_count);
  e.vec(p.lower);
  e.vec(p.upper);
  e.vec(p.first_origin);
  e.vec(p.last_origin);
  e.bytes(p.checksum.data(), 32);
}
Hdf5ChunkPage get_page(Decoder &r) {
  Hdf5ChunkPage p;
  p.object_id = r.str();
  p.offset = r.u();
  p.length = r.u();
  p.record_count = r.u();
  p.lower = r.vec();
  p.upper = r.vec();
  p.first_origin = r.vec();
  p.last_origin = r.vec();
  p.checksum = r.sha();
  return p;
}
BinaryValue directory_bytes(const Hdf5LayoutDocument &d,
                            const std::vector<Hdf5ChunkPage> &pages,
                            const Hdf5LayoutLimits &l) {
  Encoder e{{}, l.max_directory_bytes};
  e.u(d.source_size);
  e.str(d.file_profile);
  e.str(d.parser_version);
  e.str(d.root_object_id);
  e.u(d.directory_complete, 1);
  e.str(d.index_unavailable_reason);
  e.u(d.objects.size());
  for (const auto &o : d.objects) {
    e.str(o.id);
    e.u(static_cast<unsigned>(o.kind), 1);
    e.u(o.dataset.has_value(), 1);
    if (o.dataset)
      put_dataset(e, *o.dataset);
    put_metadata(e, o);
  }
  e.u(d.links.size());
  for (const auto &k : d.links) {
    e.str(k.parent_id);
    e.str(k.name);
    e.u(static_cast<unsigned>(k.kind), 1);
    e.str(k.target_id);
    e.str(k.unsupported_reason);
  }
  e.u(pages.size());
  for (const auto &p : pages)
    put_page(e, p);
  return std::move(e.b);
}
std::uint64_t shape_bytes(const std::vector<std::uint64_t> &shape,
                          std::uint64_t element_bytes) {
  if (std::find(shape.begin(), shape.end(), 0) != shape.end())
    return 0;
  auto bytes = element_bytes;
  for (auto n : shape) {
    check(n == 0 || bytes <= UINT64_MAX / n,
          "HDF5 logical byte product overflows");
    bytes *= n;
  }
  return bytes;
}
void text_ok(const std::string &s, const Hdf5LayoutLimits &l) {
  limit(s.size() <= l.max_string_bytes, "HDF5 layout string exceeds limit");
  check(s.find('\0') == std::string::npos, "HDF5 layout string contains NUL");
}
std::uint64_t attribute_ok(const Hdf5Attribute &a, const Hdf5LayoutLimits &l) {
  text_ok(a.name, l);
  text_ok(a.reason, l);
  check(!a.name.empty() && valid_hdf5_metadata_text(a.name),
        "invalid HDF5 attribute name");
  check(static_cast<unsigned>(a.state) <= 4, "invalid HDF5 attribute state");
  check(a.native_type_class <= 10 && a.shape.size() <= 32,
        "invalid HDF5 attribute native type/rank");
  check(!a.null_dataspace || a.shape.empty(),
        "null HDF5 attribute has dimensions");
  check(static_cast<unsigned>(a.type) <=
            static_cast<unsigned>(LogicalTypeId::TimestampNs),
        "invalid HDF5 attribute logical type");
  if (a.state != Hdf5AttributeState::Complete) {
    check(!a.reason.empty() && a.values.empty() && a.strings.empty(),
          "incomplete HDF5 attribute has value or lacks reason");
    return sizeof(Hdf5Attribute) + a.name.size() + a.reason.size() +
           a.shape.size() * 8;
  }
  check(a.reason.empty(), "complete HDF5 attribute has failure reason");
  auto count = a.null_dataspace ? 0 : shape_bytes(a.shape, 1);
  limit(count <= l.max_attribute_elements,
        "HDF5 attribute element limit exceeded");
  check(a.native_element_bytes > 0,
        "complete HDF5 attribute has zero native width");
  limit(!count || a.native_element_bytes <= l.max_attribute_bytes / count,
        "HDF5 attribute native bytes exceed limit");
  std::uint64_t bytes = 0;
  if (a.type == LogicalTypeId::Utf8) {
    check(a.native_type_class == 3 && a.values.empty() &&
              a.strings.size() == count,
          "HDF5 string attribute cardinality/type mismatch");
    for (const auto &v : a.strings) {
      text_ok(v, l);
      check(valid_hdf5_metadata_text(v), "invalid HDF5 attribute UTF-8");
      bytes = add(bytes, v.size());
    }
  } else {
    check((a.type == LogicalTypeId::Int64 || a.type == LogicalTypeId::UInt64 ||
           a.type == LogicalTypeId::Float64) &&
              a.strings.empty(),
          "unsupported complete HDF5 attribute type");
    check(a.native_type_class == (a.type == LogicalTypeId::Float64 ? 1U : 0U),
          "HDF5 attribute native/logical type mismatch");
    check(a.type == LogicalTypeId::Float64
              ? (a.native_element_bytes == 4 || a.native_element_bytes == 8)
              : a.native_element_bytes <= 8,
          "invalid HDF5 complete attribute native width");
    check(count <= UINT64_MAX / 8 && a.values.size() == count * 8,
          "HDF5 numeric attribute cardinality mismatch");
    bytes = a.values.size();
  }
  limit(bytes <= l.max_attribute_bytes,
        "HDF5 attribute value bytes exceed limit");
  return add(bytes, sizeof(Hdf5Attribute) + a.name.size() + a.shape.size() * 8 +
                        a.strings.size() * sizeof(std::string));
}
void chunk_ok(const Hdf5ChunkRecord &c, const Hdf5DatasetLayout &d,
              std::uint64_t size) {
  check(c.origin.size() == d.chunk_shape.size(), "HDF5 chunk rank mismatch");
  for (std::size_t i = 0; i < c.origin.size(); ++i)
    check(c.origin[i] < d.descriptor.shape[i] &&
              c.origin[i] % d.chunk_shape[i] == 0,
          "invalid HDF5 chunk coordinate");
  check(c.encoded_size > 0 && c.offset <= size &&
            c.encoded_size <= size - c.offset,
        "HDF5 chunk lies outside source");
  check(d.filters.size() == 32 || (c.filter_mask >> d.filters.size()) == 0,
        "HDF5 chunk filter mask exceeds pipeline");
}
void validate(const Hdf5LayoutDocument &d, const Hdf5LayoutLimits &l,
              bool records) {
  limit(l.chunks_per_page > 0 && l.max_directory_bytes > 0,
        "invalid HDF5 layout limits");
  limit(d.objects.size() <= l.max_objects && d.links.size() <= l.max_links,
        "HDF5 graph exceeds limit");
  text_ok(d.file_profile, l);
  text_ok(d.parser_version, l);
  text_ok(d.root_object_id, l);
  text_ok(d.index_unavailable_reason, l);
  check(d.directory_complete || !d.index_unavailable_reason.empty(),
        "incomplete HDF5 directory lacks reason");
  std::map<std::string, Hdf5ObjectKind> ids;
  std::uint64_t total = 0, attribute_total = 0, metadata_bytes = 0,
                candidate_total = 0;
  for (const auto &o : d.objects) {
    text_ok(o.id, l);
    check(!o.id.empty() && ids.emplace(o.id, o.kind).second,
          "duplicate/empty HDF5 object identity");
    check(static_cast<unsigned>(o.kind) <= 2, "invalid HDF5 object kind");
    check((o.kind == Hdf5ObjectKind::Dataset) == o.dataset.has_value(),
          "HDF5 object kind/dataset mismatch");
    text_ok(o.attributes_unavailable_reason, l);
    text_ok(o.dimension_scales_unavailable_reason, l);
    check(o.attributes_complete ? (o.attribute_count_known &&
                                   o.attributes.size() == o.attribute_count &&
                                   o.attributes_unavailable_reason.empty())
                                : !o.attributes_unavailable_reason.empty(),
          "HDF5 attribute directory completeness mismatch");
    check(o.attribute_count_known
              ? o.attributes.size() <= o.attribute_count
              : (o.attribute_count == 0 && o.attributes.empty()),
          "invalid unknown/excess HDF5 attribute count");
    limit(o.attributes.size() <= l.max_attributes_per_object,
          "HDF5 object attribute count exceeds limit");
    attribute_total = add(attribute_total, o.attributes.size());
    limit(attribute_total <= l.max_attributes,
          "HDF5 attribute count exceeds limit");
    std::set<std::string> attribute_names;
    for (const auto &a : o.attributes) {
      check(attribute_names.insert(a.name).second,
            "duplicate HDF5 attribute name");
      metadata_bytes = add(metadata_bytes, attribute_ok(a, l));
    }
    check(o.dimension_scales_complete
              ? o.dimension_scales_unavailable_reason.empty()
              : !o.dimension_scales_unavailable_reason.empty(),
          "HDF5 dimension-scale completeness mismatch");
    check(o.kind == Hdf5ObjectKind::Dataset || o.dimension_scales.empty(),
          "non-dataset has dimension-scale candidates");
    std::set<std::pair<std::string, std::uint32_t>> candidates;
    for (const auto &c : o.dimension_scales) {
      text_ok(c.dataset_object_id, l);
      check(!c.dataset_object_id.empty() &&
                candidates.emplace(c.dataset_object_id, c.axis).second,
            "duplicate/empty dimension-scale candidate");
      metadata_bytes = add(metadata_bytes, sizeof(Hdf5DimensionScaleCandidate) +
                                               c.dataset_object_id.size());
    }
    candidate_total = add(candidate_total, o.dimension_scales.size());
    limit(candidate_total <= l.max_dimension_scale_candidates &&
              metadata_bytes <= l.max_metadata_bytes,
          "HDF5 optional metadata budget exceeded");
    if (!o.dataset)
      continue;
    const auto &x = *o.dataset;
    const auto &v = x.descriptor;
    text_ok(v.path, l);
    text_ok(v.unsupported_reason, l);
    text_ok(x.index_unavailable_reason, l);
    check(v.supported ? make_hdf5_value_field(v.path).ok()
                      : (!v.path.empty() && v.path.front() == '/'),
          "invalid HDF5 dataset inventory path");
    limit(v.shape.size() <= 42, "HDF5 dataset rank exceeds metadata limit");
    check(static_cast<unsigned>(x.storage) <= 4 &&
              static_cast<unsigned>(x.byte_order) <= 2 &&
              static_cast<unsigned>(x.fill_status) <= 2,
          "invalid HDF5 dataset enum");
    check(v.supported || !v.unsupported_reason.empty(),
          "unsupported HDF5 dataset lacks reason");
    check(x.index_complete || !x.index_unavailable_reason.empty(),
          "incomplete HDF5 dataset index lacks reason");
    check(static_cast<unsigned>(v.value_type) <=
              static_cast<unsigned>(LogicalTypeId::TimestampNs),
          "invalid HDF5 logical dtype enum");
    check(x.native_type_class <= 10 && x.allocation_time <= 3 &&
              x.fill_time <= 2 && x.allocation_status <= 2,
          "invalid HDF5 native metadata enum");
    check(x.index_complete || x.chunks.empty(),
          "incomplete HDF5 index retains partial records");
    if (v.supported) {
      check(x.native_type_class ==
                (v.value_type == LogicalTypeId::Int32 ? 0U : 1U),
            "supported HDF5 native datatype is not floating point");
      const auto logical_bytes = shape_bytes(v.shape, x.element_bytes);
      if (x.storage == Hdf5StorageLayout::Contiguous && x.contiguous_offset)
        check(x.contiguous_size == logical_bytes,
              "HDF5 contiguous size disagrees with dataset shape");
      if (x.storage == Hdf5StorageLayout::Chunked)
        (void)shape_bytes(x.chunk_shape, x.element_bytes);
      for (const auto &f : x.filters)
        check(f.id >= 1 && f.id <= 3,
              "supported HDF5 dataset has unsupported filter");
      check(v.shape.size() >= 1 && v.shape.size() <= 4,
            "supported HDF5 dataset rank invalid");
      check(v.value_type == LogicalTypeId::Float32 ||
                v.value_type == LogicalTypeId::Float64 ||
                v.value_type == LogicalTypeId::Int32,
            "supported HDF5 dtype invalid");
      check(x.element_bytes == (v.value_type == LogicalTypeId::Float32 ? 4U
                                : v.value_type == LogicalTypeId::Int32 ? 2U
                                                                       : 8U) &&
                x.byte_order != Hdf5ByteOrder::Other,
            "supported HDF5 type metadata invalid");
      check(x.storage == Hdf5StorageLayout::Contiguous ||
                x.storage == Hdf5StorageLayout::Chunked,
            "supported HDF5 storage invalid");
      check(x.fill_status == Hdf5FillStatus::Undefined
                ? x.fill_value.empty()
                : x.fill_value.size() == x.element_bytes,
            "HDF5 fill scalar size mismatch");
    }
    limit(x.fill_value.size() <= l.max_directory_bytes,
          "HDF5 fill exceeds metadata limit");
    limit(x.filters.size() <= 32, "too many HDF5 filters");
    for (const auto &f : x.filters)
      limit(f.parameters.size() <= l.max_filter_parameters,
            "HDF5 filter parameters exceed limit");
    if (x.storage != Hdf5StorageLayout::Chunked)
      check(x.filters.empty() && x.chunk_options == 0,
            "nonchunked HDF5 dataset has chunk options or filter pipeline");
    if (v.supported)
      check((x.chunk_options & ~2U) == 0,
            "supported HDF5 dataset has unknown chunk options");
    if (x.storage != Hdf5StorageLayout::Contiguous)
      check(!x.contiguous_offset && x.contiguous_size == 0,
            "noncontiguous HDF5 dataset has contiguous range");
    if (x.contiguous_offset)
      check(*x.contiguous_offset <= d.source_size &&
                x.contiguous_size <= d.source_size - *x.contiguous_offset,
            "HDF5 contiguous range outside source");
    else
      check(x.contiguous_size == 0,
            "unallocated HDF5 contiguous storage has bytes");
    if (x.storage == Hdf5StorageLayout::Chunked) {
      check(x.chunk_shape.size() == v.shape.size() && !x.chunk_shape.empty(),
            "HDF5 chunk shape rank mismatch");
      for (auto n : x.chunk_shape)
        check(n > 0, "zero HDF5 chunk dimension");
    } else
      check(x.chunks.empty() && x.allocated_chunk_count == 0 &&
                x.chunk_shape.empty(),
            "nonchunked HDF5 dataset has chunk index");
    total = add(total, x.chunks.size());
    limit(total <= l.max_chunks, "HDF5 chunk index exceeds limit");
    if (records) {
      check(!x.index_complete || x.chunks.size() == x.allocated_chunk_count,
            "HDF5 allocated chunk count mismatch");
      check(x.chunks.size() <= x.allocated_chunk_count,
            "excess HDF5 chunk records");
      for (std::size_t i = 0; i < x.chunks.size(); ++i) {
        chunk_ok(x.chunks[i], x, d.source_size);
        if (i)
          check(x.chunks[i - 1].origin < x.chunks[i].origin,
                "HDF5 chunk records are duplicated or unordered");
      }
    }
  }
  std::map<std::string, std::size_t> dataset_ranks;
  for (const auto &o : d.objects)
    if (o.dataset)
      dataset_ranks.emplace(o.id, o.dataset->descriptor.shape.size());
  for (const auto &o : d.objects)
    for (const auto &c : o.dimension_scales) {
      const auto target = dataset_ranks.find(c.dataset_object_id);
      check(target != dataset_ranks.end() && c.axis < target->second,
            "invalid dimension-scale target/axis");
    }
  std::uint64_t coordinate_bytes = 0;
  for (const auto &o : d.objects) {
    text_ok(o.canonical_path, l);
    text_ok(o.axis_associations_reason, l);
    text_ok(o.coordinate_index_reason, l);
    check(o.canonical_path.empty() || o.canonical_path.front() == '/',
          "invalid canonical object path");
    check(o.axis_associations_complete ? o.axis_associations_reason.empty()
                                       : !o.axis_associations_reason.empty(),
          "invalid axis association status");
    check(o.axis_associations_complete || o.axis_associations.empty(),
          "incomplete axis associations retain evidence");
    std::set<std::pair<std::uint32_t, std::string>> seen;
    limit(o.axis_associations.size() <= l.max_dimension_scale_candidates,
          "axis association limit");
    for (const auto &a : o.axis_associations) {
      text_ok(a.scale_object_id, l);
      text_ok(a.evidence, l);
      check(o.dataset && a.axis < o.dataset->descriptor.shape.size() &&
                dataset_ranks.count(a.scale_object_id) &&
                a.evidence == "forward_dimension_list/1" &&
                seen.emplace(a.axis, a.scale_object_id).second,
            "invalid forward axis association");
    }
    if (!o.coordinate_index)
      continue;
    const auto &c = *o.coordinate_index;
    check(o.dataset && o.dataset->descriptor.shape.size() == 1 &&
              c.values.size() == o.dataset->descriptor.shape[0] &&
              o.coordinate_index_reason.empty(),
          "coordinate index cardinality mismatch");
    check(c.role == "time" || c.role == "latitude" ||
              c.role == "longitude" || c.role == "pressure",
          "invalid coordinate role");
    text_ok(c.units, l);
    text_ok(c.calendar, l);
    check(!c.units.empty(), "coordinate index lacks units");
    limit(c.values.size() <= l.max_coordinate_elements,
          "coordinate element limit");
    coordinate_bytes = add(coordinate_bytes, c.values.size() * 8);
    limit(coordinate_bytes <= l.max_coordinate_bytes, "coordinate byte limit");
    for (std::size_t i = 0; i < c.values.size(); ++i) {
      check(std::isfinite(c.values[i]), "nonfinite coordinate");
      if (i)
        check(c.ascending ? c.values[i] > c.values[i - 1]
                          : c.values[i] < c.values[i - 1],
              "nonmonotonic coordinate index");
    }
  }
  if (d.directory_complete)
    check(ids.count(d.root_object_id) &&
              ids.at(d.root_object_id) == Hdf5ObjectKind::Group,
          "HDF5 root group is absent");
  std::set<std::pair<std::string, std::string>> names;
  std::map<std::pair<std::string, std::string>, const Hdf5LayoutLink *> links;
  std::map<std::string, std::vector<std::string>> children;
  for (const auto &k : d.links) {
    text_ok(k.parent_id, l);
    text_ok(k.name, l);
    text_ok(k.target_id, l);
    text_ok(k.unsupported_reason, l);
    check(static_cast<unsigned>(k.kind) <= 3, "invalid HDF5 link kind");
    check(!k.name.empty() && k.name != "." && k.name != ".." &&
              k.name.find('/') == std::string::npos,
          "invalid HDF5 link name");
    check(ids.count(k.parent_id) &&
              ids.at(k.parent_id) == Hdf5ObjectKind::Group,
          "HDF5 link parent is absent/not group");
    check(names.emplace(k.parent_id, k.name).second,
          "duplicate HDF5 graph link");
    links.emplace(std::make_pair(k.parent_id, k.name), &k);
    if (k.kind == Hdf5LinkKind::Hard) {
      check(ids.count(k.target_id), "dangling HDF5 hard link");
      children[k.parent_id].push_back(k.target_id);
    } else
      check(!k.unsupported_reason.empty(),
            "unsupported HDF5 link lacks reason");
  }
  if (d.directory_complete) {
    std::set<std::string> reachable;
    std::vector<std::pair<std::string, std::size_t>> pending{
        {d.root_object_id, 0}};
    for (std::size_t i = 0; i < pending.size(); ++i) {
      const auto item = pending[i];
      if (!reachable.insert(item.first).second)
        continue;
      limit(item.second <= l.max_depth,
            "HDF5 object graph depth exceeds limit");
      for (const auto &child : children[item.first])
        if (!reachable.count(child))
          pending.emplace_back(child, item.second + 1);
    }
    check(reachable.size() == d.objects.size(),
          "HDF5 object directory contains unreachable objects");
    for (const auto &o : d.objects)
      if (o.dataset) {
        const auto &path = o.dataset->descriptor.path;
        std::string id = d.root_object_id;
        std::size_t start = 1, depth = 0;
        while (start < path.size()) {
          limit(++depth <= l.max_depth,
                "HDF5 dataset path exceeds depth limit");
          auto end = path.find('/', start);
          if (end == std::string::npos)
            end = path.size();
          auto it = links.find({id, path.substr(start, end - start)});
          check(it != links.end() && it->second->kind == Hdf5LinkKind::Hard,
                "HDF5 dataset inventory path has no hard-link mapping");
          id = it->second->target_id;
          start = end + 1;
        }
        check(id == o.id,
              "HDF5 dataset inventory path identifies another object");
      }
  }
}
void charge_read(std::uint64_t length, Hdf5LayoutReadBudget &used,
                 const Hdf5LayoutLimits &l, bool page) {
  limit(used.bytes <= l.max_read_bytes &&
            length <= l.max_read_bytes - used.bytes,
        "HDF5 layout read budget exhausted");
  limit(used.read_calls < l.max_read_calls,
        "HDF5 layout read call budget exhausted");
  ++used.read_calls;
  used.bytes += length;
  query_profile_count(page ? "hdf5.layout.pages.calls"
                           : "hdf5.layout.directory.calls");
  query_profile_count(
      page ? "hdf5.layout.pages.bytes" : "hdf5.layout.directory.bytes", length);
}
BinaryValue read_exact(const Hdf5LayoutRead &read, std::uint64_t offset,
                       std::uint64_t length, Hdf5LayoutReadBudget &used,
                       const Hdf5LayoutLimits &l, bool page = false) {
  charge_read(length, used, l, page);
  auto r = [&] {
    QueryProfileTimer timer(page ? "hdf5.layout.pages.read"
                                 : "hdf5.layout.directory.read");
    return read(offset, length);
  }();
  if (!r.ok())
    throw Failure{r.status()};
  check(r.value().size() == length, "short HDF5 layout read");
  return std::move(r.value());
}
void pages_ok(const Hdf5LayoutDirectory &dir, const Hdf5LayoutLimits &l) {
  const auto &d = dir.document();
  std::map<std::string, const Hdf5DatasetLayout *> datasets;
  for (const auto &o : d.objects)
    if (o.dataset)
      datasets.emplace(o.id, &*o.dataset);
  std::map<std::string, std::uint64_t> counts;
  std::map<std::string, std::vector<std::uint64_t>> last;
  std::uint64_t offset = dir.directory_read_bytes(), total_records = 0;
  for (const auto &p : dir.pages()) {
    auto it = datasets.find(p.object_id);
    check(it != datasets.end(), "HDF5 chunk page references missing dataset");
    const auto &x = *it->second;
    check(x.storage == Hdf5StorageLayout::Chunked,
          "HDF5 page belongs to nonchunked dataset");
    auto rank = x.chunk_shape.size();
    check(p.record_count > 0 && p.record_count <= l.chunks_per_page &&
              p.record_count <= (UINT64_MAX - 8) / (rank * 8 + 20),
          "invalid HDF5 chunk page count");
    check(p.length == 8 + p.record_count * (rank * 8 + 20),
          "invalid HDF5 chunk page length");
    check(p.offset == offset && p.offset <= dir.layout_size() &&
              p.length <= dir.layout_size() - p.offset,
          "missing/overlapping HDF5 chunk page");
    offset = add(offset, p.length);
    check(p.lower.size() == rank && p.upper.size() == rank &&
              p.first_origin.size() == rank && p.last_origin.size() == rank,
          "HDF5 page bound rank mismatch");
    for (std::size_t i = 0; i < rank; ++i) {
      check(p.lower[i] < p.upper[i] && p.upper[i] <= x.descriptor.shape[i],
            "invalid HDF5 page bounds");
      check(p.first_origin[i] >= p.lower[i] && p.first_origin[i] < p.upper[i] &&
                p.last_origin[i] >= p.lower[i] && p.last_origin[i] < p.upper[i],
            "HDF5 page endpoints outside bounds");
    }
    check(p.first_origin <= p.last_origin, "reversed HDF5 page coordinates");
    if (last.count(p.object_id))
      check(last.at(p.object_id) < p.first_origin,
            "HDF5 chunk pages overlap/unordered");
    last[p.object_id] = p.last_origin;
    check(x.index_complete, "incomplete HDF5 dataset has chunk pages");
    counts[p.object_id] = add(counts[p.object_id], p.record_count);
    total_records = add(total_records, p.record_count);
    limit(total_records <= l.max_chunks, "HDF5 page records exceed limit");
  }
  check(offset == dir.layout_size(),
        "HDF5 layout has unindexed trailing bytes");
  for (const auto &entry : datasets) {
    const auto &x = *entry.second;
    check(counts[entry.first] <= x.allocated_chunk_count,
          "excess HDF5 page records");
    if (x.index_complete)
      check(counts[entry.first] == x.allocated_chunk_count,
            "HDF5 chunk page directory is incomplete");
  }
}
template <class T, class F> QueryResult<T> guarded(F &&f) {
  try {
    return QueryResult<T>::success(f());
  } catch (const Failure &e) {
    return QueryResult<T>::failure(e.status);
  } catch (const std::bad_alloc &) {
    return QueryResult<T>::failure(QueryStatus::failure(
        QueryError::ResourceExhausted, QueryPhase::Planning,
        "HDF5 layout allocation failed"));
  } catch (const std::exception &) {
    return QueryResult<T>::failure(
        QueryStatus::failure(QueryError::Internal, QueryPhase::Planning,
                             "HDF5 layout callback failed"));
  }
}
} // namespace

bool valid_hdf5_metadata_text(const std::string &s) {
  for (std::size_t i = 0; i < s.size();) {
    const auto c = static_cast<unsigned char>(s[i++]);
    if (!c)
      return false;
    if (c < 0x80)
      continue;
    unsigned n;
    std::uint32_t cp;
    if (c >= 0xc2 && c <= 0xdf) {
      n = 1;
      cp = c & 0x1f;
    } else if (c >= 0xe0 && c <= 0xef) {
      n = 2;
      cp = c & 0x0f;
    } else if (c >= 0xf0 && c <= 0xf4) {
      n = 3;
      cp = c & 7;
    } else
      return false;
    const unsigned tail = n;
    if (n > s.size() - i)
      return false;
    while (n--) {
      const auto b = static_cast<unsigned char>(s[i++]);
      if ((b & 0xc0) != 0x80)
        return false;
      cp = (cp << 6) | (b & 0x3f);
    }
    if ((tail == 2 && cp < 0x800) || (tail == 3 && cp < 0x10000) ||
        cp > 0x10ffff || (cp >= 0xd800 && cp <= 0xdfff))
      return false;
  }
  return true;
}
QueryStatus validate_hdf5_layout(const Hdf5LayoutDocument &d,
                                 const Hdf5LayoutLimits &l) {
  auto r = guarded<bool>([&] {
    validate(d, l, true);
    return true;
  });
  return r.status();
}
QueryResult<BinaryValue> serialize_hdf5_layout(const Hdf5LayoutDocument &d,
                                               const Hdf5LayoutLimits &l) {
  QueryProfileTimer timer("hdf5.layout.serialize");
  return guarded<BinaryValue>([&] {
    validate(d, l, true);
    std::vector<Hdf5ChunkPage> pages;
    std::vector<BinaryValue> payloads;
    std::uint64_t payload_bytes = 0;
    for (const auto &o : d.objects) {
      if (!o.dataset)
        continue;
      const auto &x = *o.dataset;
      for (std::size_t start = 0; start < x.chunks.size();) {
        auto n = std::min(l.chunks_per_page, x.chunks.size() - start);
        limit(pages.size() < l.max_pages, "HDF5 page directory exceeds limit");
        Hdf5ChunkPage p;
        p.object_id = o.id;
        p.record_count = n;
        p.first_origin = x.chunks[start].origin;
        p.last_origin = x.chunks[start + n - 1].origin;
        p.lower = p.first_origin;
        p.upper = p.first_origin;
        Encoder e{{}, l.max_layout_bytes};
        e.u(n);
        for (std::size_t j = start; j < start + n; ++j) {
          const auto &c = x.chunks[j];
          for (std::size_t i = 0; i < c.origin.size(); ++i) {
            e.u(c.origin[i]);
            p.lower[i] = std::min(p.lower[i], c.origin[i]);
            const auto end =
                c.origin[i] +
                std::min(x.chunk_shape[i], x.descriptor.shape[i] - c.origin[i]);
            p.upper[i] = std::max(p.upper[i], end);
          }
          e.u(c.offset);
          e.u(c.encoded_size);
          e.u(c.filter_mask, 4);
        }
        p.length = e.b.size();
        p.checksum = digest(e.b);
        payload_bytes = add(payload_bytes, p.length);
        limit(payload_bytes <= l.max_layout_bytes,
              "HDF5 chunk pages exceed layout budget");
        pages.push_back(std::move(p));
        payloads.push_back(std::move(e.b));
        start += n;
      }
    }
    auto dir = directory_bytes(d, pages, l);
    auto offset = add(kHeaderBytes, dir.size());
    for (auto &p : pages) {
      p.offset = offset;
      offset = add(offset, p.length);
    }
    limit(offset <= l.max_layout_bytes,
          "HDF5 artifact exceeds build byte budget");
    dir = directory_bytes(d, pages, l);
    Encoder e{{}, l.max_layout_bytes};
    e.bytes(kHdf5LayoutMagic, sizeof(kHdf5LayoutMagic) - 1);
    e.u(dir.size());
    e.u(offset);
    auto hash = digest(dir);
    e.bytes(hash.data(), hash.size());
    e.bytes(dir.data(), dir.size());
    for (const auto &p : payloads)
      e.bytes(p.data(), p.size());
    return std::move(e.b);
  });
}
const Hdf5LayoutObject* Hdf5LayoutDirectory::object(const std::string& id) const {
  const auto found = object_lookup_.find(id);
  return found == object_lookup_.end() ? nullptr : &document_.objects[found->second];
}
const Hdf5LayoutLink* Hdf5LayoutDirectory::link(
    const std::string& parent_id, const std::string& name) const {
  const auto found = link_lookup_.find({parent_id, name});
  return found == link_lookup_.end() ? nullptr : &document_.links[found->second];
}
std::uint64_t Hdf5LayoutDirectory::retained_bytes() const {
  std::uint64_t bytes = sizeof(*this);
  auto text = [&](const std::string& value) { bytes += value.capacity() + 1; };
  auto vector = [&](const auto& value) {
    bytes += value.capacity() * sizeof(typename std::decay_t<decltype(value)>::value_type);
  };
  const auto& d = document_;
  text(d.file_profile); text(d.parser_version); text(d.root_object_id);
  text(d.index_unavailable_reason); vector(d.objects); vector(d.links); vector(pages_);
  for (const auto& o : d.objects) {
    text(o.id); text(o.attributes_unavailable_reason);
    text(o.dimension_scales_unavailable_reason); text(o.axis_associations_reason);
    text(o.coordinate_index_reason); text(o.canonical_path);
    vector(o.attributes); vector(o.dimension_scales); vector(o.axis_associations);
    for (const auto& a : o.attributes) {
      text(a.name); text(a.reason); vector(a.shape); vector(a.values); vector(a.strings);
      for (const auto& s : a.strings) text(s);
    }
    for (const auto& scale : o.dimension_scales) text(scale.dataset_object_id);
    for (const auto& axis : o.axis_associations) { text(axis.scale_object_id); text(axis.evidence); }
    if (o.coordinate_index) {
      const auto& c = *o.coordinate_index;
      text(c.role); text(c.units); text(c.calendar); vector(c.values);
    }
    if (o.dataset) {
      const auto& dataset = *o.dataset;
      text(dataset.descriptor.path); text(dataset.descriptor.unsupported_reason);
      vector(dataset.descriptor.shape); vector(dataset.fill_value); vector(dataset.chunk_shape);
      vector(dataset.filters); vector(dataset.chunks); text(dataset.index_unavailable_reason);
      for (const auto& filter : dataset.filters) vector(filter.parameters);
      for (const auto& chunk : dataset.chunks) vector(chunk.origin);
    }
  }
  for (const auto& link : d.links) {
    text(link.parent_id); text(link.name); text(link.target_id); text(link.unsupported_reason);
  }
  for (const auto& page : pages_) {
    text(page.object_id); vector(page.lower); vector(page.upper);
    vector(page.first_origin); vector(page.last_origin);
  }
  for (const auto& [id, position] : object_lookup_)
    bytes += sizeof(id) + sizeof(position) + 3 * sizeof(void*) + id.capacity() + 1;
  for (const auto& [name, position] : link_lookup_)
    bytes += sizeof(name) + sizeof(position) + 3 * sizeof(void*) +
      name.first.capacity() + name.second.capacity() + 2;
  return bytes;
}
std::uint64_t Hdf5ValidatedChunkPage::retained_bytes() const {
  std::uint64_t bytes = sizeof(*this) + records.capacity() * sizeof(Hdf5ChunkRecord);
  for (const auto& chunk : records)
    bytes += chunk.origin.capacity() * sizeof(std::uint64_t);
  return bytes;
}
QueryResult<Hdf5LayoutDirectory>
read_hdf5_layout_directory(const Hdf5LayoutRead &read,
                           std::uint64_t layout_size, std::uint64_t source_size,
                           const Hdf5LayoutLimits &l) {
  QueryProfileTimer total("hdf5.layout.directory");
  return guarded<Hdf5LayoutDirectory>([&] {
    check(layout_size >= kHeaderBytes, "truncated HDF5 layout header");
    Hdf5LayoutDirectory result;
    Hdf5LayoutReadBudget read_budget;
    result.layout_size_ = layout_size;
    auto head = read_exact(read, 0, kHeaderBytes, read_budget, l);
    static_assert(sizeof(kHdf5LayoutMagic) == sizeof(kHdf5PreviousLayoutMagic));
    const bool current = std::memcmp(head.data(), kHdf5LayoutMagic,
                                     sizeof(kHdf5LayoutMagic) - 1) == 0;
    const bool previous = std::memcmp(head.data(), kHdf5PreviousLayoutMagic,
                                      sizeof(kHdf5PreviousLayoutMagic) - 1) == 0;
    if (!current && !previous)
      fail(QueryError::UnsupportedLayoutVersion,
           "unsupported HDF5 layout version");
    result.protocol_version_ = current ? 5 : 4;
    Decoder h{head, sizeof(kHdf5LayoutMagic) - 1, l};
    auto bytes = h.u();
    check(h.u() == layout_size, "HDF5 layout size mismatch");
    result.checksum_ = h.sha();
    limit(bytes <= l.max_directory_bytes, "HDF5 directory exceeds read limit");
    check(bytes <= layout_size - kHeaderBytes, "truncated HDF5 directory");
    auto body = read_exact(read, kHeaderBytes, bytes, read_budget, l);
    QueryProfileTimer parse("hdf5.layout.directory.parse");
    check(digest(body) == result.checksum_, "HDF5 directory checksum mismatch");
    Decoder r{body, 0, l};
    auto &d = result.document_;
    d.source_size = r.u();
    check(d.source_size == source_size, "HDF5 layout source size mismatch");
    d.file_profile = r.str();
    d.parser_version = r.str();
    d.root_object_id = r.str();
    d.directory_complete = r.flag();
    d.index_unavailable_reason = r.str();
    auto n = r.count(l.max_objects);
    while (n--) {
      Hdf5LayoutObject o;
      o.id = r.str();
      o.kind = r.en<Hdf5ObjectKind>(2);
      if (r.flag())
        o.dataset = get_dataset(r);
      get_metadata(r, o);
      d.objects.push_back(std::move(o));
    }
    n = r.count(l.max_links);
    while (n--) {
      Hdf5LayoutLink k;
      k.parent_id = r.str();
      k.name = r.str();
      k.kind = r.en<Hdf5LinkKind>(3);
      k.target_id = r.str();
      k.unsupported_reason = r.str();
      d.links.push_back(std::move(k));
    }
    n = r.count(l.max_pages);
    while (n--)
      result.pages_.push_back(get_page(r));
    r.end();
    result.bytes_read_ = read_budget.bytes;
    result.read_calls_ = read_budget.read_calls;
    limit(result.pages_.size() <= l.max_page_scans,
          "HDF5 directory page scan budget exhausted");
    validate(d, l, false);
    if (previous)
      for (const auto &object : d.objects)
        check(!object.coordinate_index ||
                  object.coordinate_index->role != "pressure",
              "layout /4 cannot contain pressure coordinates");
    pages_ok(result, l);
    for (std::size_t i = 0; i < d.objects.size(); ++i)
      result.object_lookup_.emplace(d.objects[i].id, i);
    for (std::size_t i = 0; i < d.links.size(); ++i)
      result.link_lookup_.emplace(
        std::make_pair(d.links[i].parent_id, d.links[i].name), i);
    return result;
  });
}
QueryResult<std::vector<Hdf5ChunkRecord>>
read_hdf5_layout_chunks(const Hdf5LayoutRead &read,
                        const Hdf5LayoutDirectory &dir, const std::string &id,
                        const std::vector<std::uint64_t> &origin,
                        const std::vector<std::uint64_t> &shape,
                        Hdf5LayoutReadBudget &used, const Hdf5LayoutLimits &l,
                        const Hdf5ChunkPageCache* cache) {
  QueryProfileTimer total("hdf5.layout.pages");
  return guarded<std::vector<Hdf5ChunkRecord>>([&] {
    used.bytes = std::max(used.bytes, dir.directory_read_bytes());
    used.read_calls = std::max(used.read_calls, dir.directory_read_calls());
    used.page_scans = std::max(used.page_scans, dir.directory_page_scans());
    limit(used.bytes <= l.max_read_bytes &&
              used.read_calls <= l.max_read_calls &&
              used.page_scans <= l.max_page_scans,
          "HDF5 layout read budget exhausted");
    const auto* object = dir.object(id);
    const Hdf5DatasetLayout *d = object && object->dataset ?
      &*object->dataset : nullptr;
    if (!d)
      fail(QueryError::UnknownField, "HDF5 dataset object not found");
    if (!dir.document().directory_complete || !d->index_complete)
      fail(QueryError::LayoutUnavailable, "HDF5 chunk index is incomplete");
    check(d->storage == Hdf5StorageLayout::Chunked,
          "HDF5 chunk lookup requires chunked dataset");
    if (origin.size() != d->descriptor.shape.size() ||
        shape.size() != origin.size())
      fail(QueryError::InvalidDomainSlice, "HDF5 selection rank mismatch");
    std::vector<std::uint64_t> end;
    bool empty = false;
    for (std::size_t i = 0; i < shape.size(); ++i) {
      if (origin[i] > d->descriptor.shape[i] ||
          shape[i] > d->descriptor.shape[i] - origin[i])
        fail(QueryError::InvalidDomainSlice, "HDF5 selection exceeds dataset");
      end.push_back(origin[i] + shape[i]);
      empty |= shape[i] == 0;
    }
    std::vector<Hdf5ChunkRecord> out;
    if (empty)
      return out;
    for (const auto &p : dir.pages()) {
      limit(used.page_scans < l.max_page_scans,
            "HDF5 candidate page scan budget exhausted");
      ++used.page_scans;
      if (p.object_id != id)
        continue;
      bool hit = true;
      for (std::size_t i = 0; i < shape.size(); ++i)
        hit &= origin[i] < p.upper[i] && p.lower[i] < end[i];
      if (!hit)
        continue;
      auto parsed = cache && cache->find ? cache->find(p) : nullptr;
      if (parsed) {
        // A cache hit avoids physical I/O and parsing, but still consumes the
        // request's logical page-read budget.
        charge_read(p.length, used, l, true);
        query_profile_count("hdf5.layout.cache.page.hits");
      } else {
        if (cache) query_profile_count("hdf5.layout.cache.page.misses");
        auto b = read_exact(read, p.offset, p.length, used, l, true);
        QueryProfileTimer parse("hdf5.layout.pages.parse");
        check(digest(b) == p.checksum, "HDF5 chunk page checksum mismatch");
        Decoder r{b, 0, l};
        check(r.u() == p.record_count, "HDF5 page record count mismatch");
        auto fresh = std::make_shared<Hdf5ValidatedChunkPage>();
        fresh->records.reserve(p.record_count);
        std::vector<std::uint64_t> last, lower, upper;
        for (std::uint64_t j = 0; j < p.record_count; ++j) {
          Hdf5ChunkRecord c;
          for (std::size_t i = 0; i < shape.size(); ++i)
            c.origin.push_back(r.u());
          c.offset = r.u();
          c.encoded_size = r.u();
          c.filter_mask = r.u(4);
          chunk_ok(c, *d, dir.document().source_size);
          if (j)
            check(last < c.origin, "HDF5 page chunks duplicated/unordered");
          else {
            check(c.origin == p.first_origin,
                  "HDF5 page first coordinate mismatch");
            lower = c.origin;
            upper = c.origin;
          }
          last = c.origin;
          for (std::size_t i = 0; i < shape.size(); ++i) {
            lower[i] = std::min(lower[i], c.origin[i]);
            auto edge = c.origin[i] +
                std::min(d->chunk_shape[i], d->descriptor.shape[i] - c.origin[i]);
            upper[i] = std::max(upper[i], edge);
          }
          fresh->records.push_back(std::move(c));
        }
        r.end();
        check(last == p.last_origin && lower == p.lower && upper == p.upper,
              "HDF5 page bounds do not match records");
        parsed = std::move(fresh);
        if (cache && cache->insert) cache->insert(p, parsed);
      }
      for (const auto& c : parsed->records) {
        bool selected = true;
        for (std::size_t i = 0; i < shape.size(); ++i) {
          const auto edge = c.origin[i] +
            std::min(d->chunk_shape[i], d->descriptor.shape[i] - c.origin[i]);
          selected &= origin[i] < edge && c.origin[i] < end[i];
        }
        if (selected) out.push_back(c);
      }
    }
    return out;
  });
}
QueryResult<std::string>
resolve_hdf5_layout_path(const Hdf5LayoutDirectory &dir,
                         const std::string &path) {
  return guarded<std::string>([&] {
    if (!make_hdf5_value_field(path).ok())
      fail(QueryError::InvalidQuery, "invalid HDF5 dataset path");
    if (!dir.document().directory_complete)
      fail(QueryError::LayoutUnavailable,
           "HDF5 object directory is incomplete");
    std::string current = dir.document().root_object_id;
    std::size_t begin = 1;
    while (begin < path.size()) {
      auto end = path.find('/', begin);
      if (end == std::string::npos)
        end = path.size();
      auto name = path.substr(begin, end - begin);
      const Hdf5LayoutLink *found = dir.link(current, name);
      if (!found)
        fail(QueryError::UnknownField, "HDF5 dataset path was not found");
      if (found->kind != Hdf5LinkKind::Hard)
        fail(QueryError::UnsupportedCapability,
             "HDF5 non-hard link is unsupported");
      current = found->target_id;
      begin = end + 1;
    }
    if (const auto* object = dir.object(current)) {
      if (!object->dataset)
        fail(QueryError::UnsupportedCapability, "HDF5 path is not a dataset");
      return current;
    }
    fail(QueryError::CorruptLayout, "HDF5 path references absent object");
  });
}
} // namespace iotdbfs
