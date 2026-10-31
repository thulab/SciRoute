// H00 standalone spike. No production adapter, source driver, or cache is
// implemented here. HDF5 owns metadata parsing; recorded ranges below are raw
// value payload reads only.
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <functional>
#include <hdf5.h>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
#include <zlib.h>

namespace {
constexpr uint64_t kMaxBytes = 64ULL * 1024 * 1024;
constexpr uint64_t kMaxElements = 1024 * 1024;
constexpr uint64_t kMaxRanges = 65536;
constexpr size_t kMaxObjects = 4096;
using Bytes = std::vector<unsigned char>;
using Dims = std::vector<hsize_t>;
struct Unsupported : std::runtime_error {
  using std::runtime_error::runtime_error;
};
void check(herr_t value, const char *what) {
  if (value < 0)
    throw std::runtime_error(what);
}
struct Id {
  hid_t value;
  herr_t (*close)(hid_t);
  Id(hid_t v, herr_t (*c)(hid_t)) : value(v), close(c) {
    if (v < 0)
      throw std::runtime_error("HDF5 object open/create failed");
  }
  ~Id() { close(value); }
  operator hid_t() const { return value; }
  Id(const Id &) = delete;
};
std::string quote(const std::string &s) {
  std::ostringstream out;
  out << '"';
  for (unsigned char c : s) {
    if (c == '"' || c == '\\')
      out << '\\' << c;
    else if (c < 32)
      out << "\\u" << std::hex << std::setw(4) << std::setfill('0')
          << unsigned(c) << std::dec;
    else
      out << c;
  }
  out << '"';
  return out.str();
}
std::string dims_json(const Dims &dims) {
  std::ostringstream out;
  out << '[';
  for (size_t i = 0; i < dims.size(); ++i) {
    if (i)
      out << ',';
    out << dims[i];
  }
  out << ']';
  return out.str();
}
std::string hex(const Bytes &bytes) {
  static const char *digits = "0123456789abcdef";
  std::string s;
  s.reserve(bytes.size() * 2);
  for (auto b : bytes) {
    s += digits[b >> 4];
    s += digits[b & 15];
  }
  return s;
}
uint64_t mul(uint64_t a, uint64_t b) {
  if (b && a > std::numeric_limits<uint64_t>::max() / b)
    throw std::runtime_error("size overflow");
  return a * b;
}
uint64_t add(uint64_t a, uint64_t b) {
  if (a > std::numeric_limits<uint64_t>::max() - b)
    throw std::runtime_error("offset overflow");
  return a + b;
}
uint64_t product(const Dims &d) {
  uint64_t n = 1;
  for (auto x : d)
    n = mul(n, x);
  return n;
}
Dims parse_dims(const std::string &text) {
  if (text.empty())
    return {}; // Allows unsupported scalar/null datasets to be classified
               // first.
  Dims result;
  std::stringstream s(text);
  std::string part;
  while (std::getline(s, part, ',')) {
    if (part.empty() ||
        part.find_first_not_of("0123456789") != std::string::npos)
      throw std::runtime_error("invalid dimension list");
    result.push_back(std::stoull(part));
  }
  if (result.empty() || result.size() > 3 || text.back() == ',')
    throw std::runtime_error("rank must be 1..3");
  return result;
}
uint64_t flat(const Dims &coord, const Dims &shape) {
  uint64_t n = 0;
  for (size_t i = 0; i < shape.size(); ++i)
    n = add(mul(n, shape[i]), coord[i]);
  return n;
}
void each_coord(const Dims &shape,
                const std::function<void(const Dims &)> &fn) {
  if (!product(shape))
    return;
  Dims c(shape.size(), 0);
  while (true) {
    fn(c);
    size_t i = shape.size();
    while (i) {
      --i;
      if (++c[i] < shape[i])
        break;
      c[i] = 0;
    }
    if (i == 0 && c[0] == 0)
      break;
  }
}
struct Filter {
  H5Z_filter_t id;
  std::vector<unsigned> params;
};
struct Dataset {
  Dims shape, chunk;
  size_t bytes = 0;
  H5T_order_t order = H5T_ORDER_ERROR;
  H5D_layout_t layout = H5D_LAYOUT_ERROR;
  std::vector<Filter> filters;
  H5D_fill_value_t fill = H5D_FILL_VALUE_ERROR;
  H5D_fill_time_t fill_time = H5D_FILL_TIME_ERROR;
  uint64_t storage_bytes = 0, allocated_chunks = 0;
  std::string reason;
};
Dataset describe(hid_t d) {
  Dataset out;
  Id sp(H5Dget_space(d), H5Sclose), ty(H5Dget_type(d), H5Tclose),
      dc(H5Dget_create_plist(d), H5Pclose);
  auto kind = H5Sget_simple_extent_type(sp);
  int rank = H5Sget_simple_extent_ndims(sp);
  if (rank < 0)
    throw std::runtime_error("cannot read dataspace rank");
  out.shape.resize(rank);
  check(H5Sget_simple_extent_dims(sp, out.shape.data(), nullptr),
        "cannot read dimensions");
  out.storage_bytes = H5Dget_storage_size(d);
  out.bytes = H5Tget_size(ty);
  out.order = H5Tget_order(ty);
  out.layout = H5Pget_layout(dc);
  check(H5Pfill_value_defined(dc, &out.fill), "cannot inspect fill value");
  check(H5Pget_fill_time(dc, &out.fill_time), "cannot inspect fill time");
  bool ieee =
      H5Tequal(ty, H5T_IEEE_F32LE) > 0 || H5Tequal(ty, H5T_IEEE_F32BE) > 0 ||
      H5Tequal(ty, H5T_IEEE_F64LE) > 0 || H5Tequal(ty, H5T_IEEE_F64BE) > 0;
  if (kind != H5S_SIMPLE || rank < 1 || rank > 3)
    out.reason = "requires rank 1..3 simple dataspace";
  else if (!ieee)
    out.reason = "requires standard IEEE Float32/Float64";
  else if (out.layout != H5D_CONTIGUOUS && out.layout != H5D_CHUNKED)
    out.reason = "unsupported dataset layout";
  else if (H5Pget_external_count(dc) != 0)
    out.reason = "external raw storage is unsupported";
  if (out.layout == H5D_CHUNKED) {
    hsize_t allocated = 0;
    check(H5Dget_num_chunks(d, sp, &allocated),
          "cannot count allocated chunks");
    out.allocated_chunks = allocated;
    out.chunk.resize(rank);
    if (H5Pget_chunk(dc, rank, out.chunk.data()) != rank)
      throw std::runtime_error("cannot read chunk dimensions");
    if (mul(product(out.chunk), out.bytes) > kMaxBytes)
      out.reason = "chunk exceeds 64 MiB spike limit";
  }
  int nf = H5Pget_nfilters(dc);
  if (nf < 0 || nf > 32)
    throw std::runtime_error("invalid filter count");
  for (int i = 0; i < nf; ++i) {
    unsigned flags = 0, config = 0;
    size_t n = 32;
    unsigned p[32]{};
    auto id = H5Pget_filter2(dc, i, &flags, &n, p, 0, nullptr, &config);
    if (id < 0 || n > 32)
      throw std::runtime_error("invalid filter metadata");
    out.filters.push_back({id, std::vector<unsigned>(p, p + n)});
    if (id != H5Z_FILTER_DEFLATE && id != H5Z_FILTER_SHUFFLE &&
        id != H5Z_FILTER_FLETCHER32)
      out.reason = "unsupported filter";
    if (id == H5Z_FILTER_SHUFFLE && (n != 1 || p[0] != out.bytes))
      out.reason = "unsupported shuffle parameters";
  }
  return out;
}
std::string dataset_json(const std::string &path, const Dataset &d) {
  std::ostringstream o;
  o << "{\"path\":" << quote(path) << ",\"shape\":" << dims_json(d.shape)
    << ",\"element_bytes\":" << d.bytes << ",\"byte_order\":"
    << quote(d.order == H5T_ORDER_LE   ? "little"
             : d.order == H5T_ORDER_BE ? "big"
                                       : "other")
    << ",\"layout\":"
    << quote(d.layout == H5D_CHUNKED      ? "chunked"
             : d.layout == H5D_CONTIGUOUS ? "contiguous"
             : d.layout == H5D_COMPACT    ? "compact"
             : d.layout == H5D_VIRTUAL    ? "virtual"
                                          : "other")
    << ",\"storage_bytes\":" << d.storage_bytes
    << ",\"allocated_chunks\":" << d.allocated_chunks
    << ",\"chunk_shape\":" << dims_json(d.chunk)
    << ",\"fill_status\":" << int(d.fill)
    << ",\"fill_time\":" << int(d.fill_time)
    << ",\"supported\":" << (d.reason.empty() ? "true" : "false")
    << ",\"reason\":" << quote(d.reason) << ",\"filters\":[";
  for (size_t i = 0; i < d.filters.size(); ++i) {
    if (i)
      o << ',';
    o << "{\"id\":" << d.filters[i].id << ",\"parameters\":[";
    for (size_t j = 0; j < d.filters[i].params.size(); ++j) {
      if (j)
        o << ',';
      o << d.filters[i].params[j];
    }
    o << "]}";
  }
  o << "]}";
  return o.str();
}
struct Inventory {
  size_t objects = 0;
  std::vector<std::string> datasets, links;
  std::string error;
};
herr_t visit_object(hid_t root, const char *name, const H5O_info2_t *info,
                    void *ctx) noexcept {
  auto &inv = *static_cast<Inventory *>(ctx);
  try {
    if (++inv.objects > kMaxObjects || std::strlen(name) > 4096 ||
        std::count(name, name + std::strlen(name), '/') > 64)
      throw std::runtime_error("inventory graph bound exceeded");
    if (info->type == H5O_TYPE_DATASET) {
      Id d(H5Dopen2(root, name, H5P_DEFAULT), H5Dclose);
      inv.datasets.push_back(
          dataset_json(std::string("/") + name, describe(d)));
    }
    return 0;
  } catch (const std::exception &e) {
    inv.error = e.what();
    return -1;
  }
}
herr_t visit_link(hid_t, const char *name, const H5L_info2_t *info,
                  void *ctx) noexcept {
  auto &inv = *static_cast<Inventory *>(ctx);
  try {
    if (inv.links.size() >= kMaxObjects || std::strlen(name) > 4096)
      throw std::runtime_error("link graph bound exceeded");
    inv.links.push_back(
        "{\"path\":" + quote(std::string("/") + name) + ",\"kind\":" +
        quote(info->type == H5L_TYPE_HARD   ? "hard"
              : info->type == H5L_TYPE_SOFT ? "soft"
                                            : "external_or_user") +
        "}");
    return 0;
  } catch (const std::exception &e) {
    inv.error = e.what();
    return -1;
  }
}
struct Reader {
  std::ifstream stream;
  uint64_t size = 0, bytes = 0;
  std::vector<std::string> ranges;
  explicit Reader(const std::string &path) : stream(path, std::ios::binary) {
    if (!stream)
      throw std::runtime_error("cannot open raw file");
    stream.seekg(0, std::ios::end);
    auto n = stream.tellg();
    if (n < 0)
      throw std::runtime_error("cannot size file");
    size = n;
  }
  Bytes read(uint64_t offset, uint64_t length, const std::string &purpose) {
    if (ranges.size() >= kMaxRanges || length > kMaxBytes ||
        bytes > kMaxBytes - length)
      throw std::runtime_error("raw I/O budget exceeded");
    if (offset > size || length > size - offset)
      throw std::runtime_error("raw range outside file");
    Bytes b(length);
    stream.clear();
    stream.seekg(offset);
    stream.read(reinterpret_cast<char *>(b.data()), length);
    if (!stream || uint64_t(stream.gcount()) != length)
      throw std::runtime_error("short raw read");
    bytes += length;
    ranges.push_back("{\"offset\":" + std::to_string(offset) +
                     ",\"length\":" + std::to_string(length) +
                     ",\"purpose\":" + quote(purpose) + "}");
    return b;
  }
};
uint32_t checksum(const Bytes &b, size_t n) {
  uint32_t a = 0, c = 0;
  for (size_t i = 0; i < n; i += 2) {
    a += (uint32_t(b[i]) << 8) | (i + 1 < n ? b[i + 1] : 0);
    a = (a & 65535) + (a >> 16);
    c += a;
    c = (c & 65535) + (c >> 16);
  }
  a = (a & 65535) + (a >> 16);
  c = (c & 65535) + (c >> 16);
  return (c << 16) | a;
}
Bytes decode(Bytes b, const Dataset &d, uint32_t mask, size_t expected) {
  if (d.filters.size() < 32 && (mask >> d.filters.size()))
    throw std::runtime_error("filter mask has unknown bits");
  for (size_t j = d.filters.size(); j-- > 0;) {
    if (mask & (uint32_t(1) << j))
      continue;
    auto id = d.filters[j].id;
    if (id == H5Z_FILTER_FLETCHER32) {
      if (b.size() < 4)
        throw std::runtime_error("short Fletcher32 chunk");
      size_t n = b.size() - 4;
      uint32_t stored = uint32_t(b[n]) | (uint32_t(b[n + 1]) << 8) |
                        (uint32_t(b[n + 2]) << 16) | (uint32_t(b[n + 3]) << 24);
      if (checksum(b, n) != stored)
        throw std::runtime_error("Fletcher32 checksum mismatch");
      b.resize(n);
    } else if (id == H5Z_FILTER_DEFLATE) {
      Bytes out(expected + 128);
      uLongf len = out.size();
      int rc = uncompress(out.data(), &len, b.data(), b.size());
      if (rc != Z_OK || len > expected + 128)
        throw std::runtime_error("DEFLATE decode failed or exceeds bound");
      out.resize(len);
      b = std::move(out);
    } else if (id == H5Z_FILTER_SHUFFLE) {
      if (b.size() % d.bytes)
        throw std::runtime_error("shuffle byte count mismatch");
      Bytes out(b.size());
      size_t n = b.size() / d.bytes;
      for (size_t k = 0; k < d.bytes; ++k)
        for (size_t i = 0; i < n; ++i)
          out[i * d.bytes + k] = b[k * n + i];
      b = std::move(out);
    } else
      throw Unsupported("unsupported active filter");
  }
  if (b.size() != expected)
    throw std::runtime_error("decoded chunk size mismatch");
  return b;
}
void validate_path(hid_t file, const std::string &path) {
  if (path.empty() || path[0] != '/' || path.size() > 4096)
    throw Unsupported("requires absolute bounded dataset path");
  size_t at = 1, depth = 0;
  std::string prefix;
  while (at < path.size()) {
    size_t end = path.find('/', at);
    if (end == std::string::npos)
      end = path.size();
    auto part = path.substr(at, end - at);
    if (part.empty() || part == "." || part == ".." || ++depth > 64)
      throw Unsupported("noncanonical dataset path");
    prefix += "/" + part;
    H5L_info2_t info{};
    check(H5Lget_info2(file, prefix.c_str(), &info, H5P_DEFAULT),
          "dataset path does not exist");
    if (info.type != H5L_TYPE_HARD)
      throw Unsupported("soft/external links are not followed");
    at = end + 1;
  }
}
std::string query(hid_t file, Reader &reader, const std::string &path,
                  const Dims &start, const Dims &count,
                  const std::string &values_output) {
  validate_path(file, path);
  Id ds(H5Dopen2(file, path.c_str(), H5P_DEFAULT), H5Dclose);
  auto d = describe(ds);
  if (!d.reason.empty())
    throw Unsupported(d.reason);
  if (start.size() != d.shape.size() || count.size() != d.shape.size())
    throw std::runtime_error("selection rank mismatch");
  for (size_t i = 0; i < start.size(); ++i)
    if (start[i] > d.shape[i] || count[i] > d.shape[i] - start[i])
      throw std::runtime_error("selection outside dataset");
  uint64_t elements = product(count), nbytes = mul(elements, d.bytes);
  if (elements > kMaxElements || nbytes > kMaxBytes)
    throw std::runtime_error("selection exceeds spike limit");
  Id type(H5Dget_type(ds), H5Tclose), dc(H5Dget_create_plist(ds), H5Pclose),
      space(H5Dget_space(ds), H5Sclose);
  Bytes output(nbytes), oracle(nbytes), fill(d.bytes);
  uint64_t chunks = 0, fill_elements = 0;
  std::vector<std::string> chunk_records;
  auto fill_bytes = [&]() {
    if (d.fill == H5D_FILL_VALUE_UNDEFINED ||
        d.fill_time == H5D_FILL_TIME_NEVER)
      throw Unsupported("unallocated data has indeterminate fill");
    check(H5Pget_fill_value(dc, type, fill.data()), "cannot read fill value");
  };
  if (elements && d.layout == H5D_CONTIGUOUS) {
    auto addr = H5Dget_offset(ds);
    if (addr == HADDR_UNDEF) {
      fill_bytes();
      for (uint64_t i = 0; i < elements; ++i)
        std::memcpy(output.data() + i * d.bytes, fill.data(), d.bytes);
      fill_elements = elements;
    } else {
      Dims rows = count;
      rows.back() = 1;
      each_coord(rows, [&](const Dims &row) {
        Dims c = row;
        for (size_t i = 0; i < c.size(); ++i)
          c[i] += start[i];
        auto b = reader.read(add(addr, mul(flat(c, d.shape), d.bytes)),
                             mul(count.back(), d.bytes), "contiguous_slab");
        std::memcpy(output.data() + mul(flat(row, count), d.bytes), b.data(),
                    b.size());
      });
    }
  } else if (elements) {
    Dims first(start.size()), nc(start.size());
    for (size_t i = 0; i < start.size(); ++i) {
      first[i] = start[i] / d.chunk[i];
      nc[i] = (start[i] + count[i] - 1) / d.chunk[i] - first[i] + 1;
    }
    if (product(nc) > kMaxRanges)
      throw std::runtime_error("chunk count exceeds spike limit");
    each_coord(nc, [&](const Dims &index) {
      Dims origin(index.size()), lo(index.size()), extent(index.size());
      for (size_t i = 0; i < index.size(); ++i) {
        origin[i] = (first[i] + index[i]) * d.chunk[i];
        lo[i] = std::max(origin[i], start[i]);
        extent[i] = std::min(add(origin[i],
                                 std::min(d.chunk[i], d.shape[i] - origin[i])),
                             start[i] + count[i]) -
                    lo[i];
      }
      unsigned mask = 0;
      haddr_t addr = HADDR_UNDEF;
      hsize_t size = 0;
      check(H5Dget_chunk_info_by_coord(ds, origin.data(), &mask, &addr, &size),
            "chunk lookup failed");
      Bytes decoded;
      bool allocated = addr != HADDR_UNDEF && size;
      if (allocated) {
        if (size > kMaxBytes)
          throw std::runtime_error("encoded chunk exceeds bound");
        auto raw = reader.read(addr, size, "chunk_payload");
        Bytes libraw(size);
        uint32_t libmask = 0;
        check(H5Dread_chunk(ds, H5P_DEFAULT, origin.data(), &libmask,
                            libraw.data()),
              "native raw chunk read failed");
        if (raw != libraw || mask != libmask)
          throw std::runtime_error(
              "absolute chunk offset or mask differs from H5Dread_chunk");
        decoded =
            decode(std::move(raw), d, mask, mul(product(d.chunk), d.bytes));
        ++chunks;
      } else
        fill_bytes();
      chunk_records.push_back(
          "{\"origin\":" + dims_json(origin) +
          ",\"allocated\":" + (allocated ? "true" : "false") +
          ",\"address\":" + (allocated ? std::to_string(addr) : "null") +
          ",\"encoded_bytes\":" + std::to_string(size) +
          ",\"filter_mask\":" + std::to_string(mask) + "}");
      each_coord(extent, [&](const Dims &rel) {
        Dims src(rel.size()), dst(rel.size());
        for (size_t i = 0; i < rel.size(); ++i) {
          src[i] = lo[i] + rel[i] - origin[i];
          dst[i] = lo[i] + rel[i] - start[i];
        }
        auto p = allocated ? decoded.data() + mul(flat(src, d.chunk), d.bytes)
                           : fill.data();
        std::memcpy(output.data() + mul(flat(dst, count), d.bytes), p, d.bytes);
        if (!allocated)
          ++fill_elements;
      });
    });
  }
  if (elements) {
    check(H5Sselect_hyperslab(space, H5S_SELECT_SET, start.data(), nullptr,
                              count.data(), nullptr),
          "native selection failed");
    Id mem(H5Screate_simple(count.size(), count.data(), nullptr), H5Sclose);
    check(H5Dread(ds, type, mem, space, H5P_DEFAULT, oracle.data()),
          "native H5Dread failed");
  }
  if (output != oracle)
    throw std::runtime_error(
        "raw decoded selection differs bitwise from H5Dread");
  Bytes canonical = output;
  if (d.order == H5T_ORDER_BE)
    for (size_t i = 0; i < canonical.size(); i += d.bytes)
      std::reverse(canonical.begin() + i, canonical.begin() + i + d.bytes);
  if (!values_output.empty()) {
    std::ofstream values(values_output, std::ios::binary);
    values.write(reinterpret_cast<const char *>(canonical.data()),
                 canonical.size());
    if (!values)
      throw std::runtime_error("cannot write selected values");
  }
  std::ostringstream o;
  o << "{\"dataset\":" << dataset_json(path, d)
    << ",\"start\":" << dims_json(start) << ",\"count\":" << dims_json(count)
    << ",\"output_elements\":" << elements
    << ",\"exact_match\":true,\"output_canonical_le_bytes_hex\":"
    << quote(values_output.empty() ? hex(canonical) : std::string())
    << ",\"decoded_chunks\":" << chunks
    << ",\"fill_elements\":" << fill_elements << ",\"chunks\":[";
  for (size_t i = 0; i < chunk_records.size(); ++i) {
    if (i)
      o << ',';
    o << chunk_records[i];
  }
  o << "]}";
  return o.str();
}
} // namespace
int main(int argc, char **argv) {
  std::string file, dataset, output, values_output, start_text, count_text,
      status = "ok", error, result = "null";
  std::ostringstream report;
  try {
    for (int i = 1; i < argc; ++i) {
      std::string arg = argv[i];
      if (i + 1 >= argc)
        throw std::runtime_error("missing option value");
      std::string value = argv[++i];
      if (arg == "--file")
        file = value;
      else if (arg == "--dataset")
        dataset = value;
      else if (arg == "--start")
        start_text = value;
      else if (arg == "--count")
        count_text = value;
      else if (arg == "--output")
        output = value;
      else if (arg == "--values-output")
        values_output = value;
      else
        throw std::runtime_error("unknown argument " + arg);
    }
    if (file.empty() || output.empty())
      throw std::runtime_error("required: --file PATH --output JSON [--dataset "
                               "PATH --start CSV --count CSV]");
    check(H5PLset_loading_state(0), "cannot disable dynamic filter plugins");
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
    unsigned major = 0, minor = 0, release = 0;
    hbool_t threadsafe = false;
    check(H5get_libversion(&major, &minor, &release), "cannot read version");
    check(H5is_library_threadsafe(&threadsafe), "cannot read thread safety");
    std::ostringstream filters;
    filters << '[';
    for (unsigned id = 1; id <= 3; ++id) {
      if (id != 1)
        filters << ',';
      const auto available = H5Zfilter_avail(id);
      if (available < 0)
        throw std::runtime_error("cannot inspect builtin filter");
      unsigned flags = 0;
      if (available)
        check(H5Zget_filter_info(id, &flags),
              "cannot inspect filter capabilities");
      filters << "{\"id\":" << id
              << ",\"available\":" << (available ? "true" : "false")
              << ",\"encode\":"
              << ((flags & H5Z_FILTER_CONFIG_ENCODE_ENABLED) ? "true" : "false")
              << ",\"decode\":"
              << ((flags & H5Z_FILTER_CONFIG_DECODE_ENABLED) ? "true" : "false")
              << '}';
    }
    filters << ']';
    Id f(H5Fopen(file.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT), H5Fclose),
        fc(H5Fget_create_plist(f), H5Pclose);
    hsize_t userblock = 0;
    check(H5Pget_userblock(fc, &userblock), "cannot read user block");
    Inventory inventory;
    auto rc = H5Ovisit3(f, H5_INDEX_NAME, H5_ITER_INC, visit_object, &inventory,
                        H5O_INFO_BASIC);
    if (rc < 0)
      throw std::runtime_error(inventory.error.empty()
                                   ? "object inventory failed"
                                   : inventory.error);
    rc = H5Lvisit2(f, H5_INDEX_NAME, H5_ITER_INC, visit_link, &inventory);
    if (rc < 0)
      throw std::runtime_error(inventory.error.empty() ? "link inventory failed"
                                                       : inventory.error);
    Reader reader(file);
    try {
      if (!dataset.empty())
        result = query(f, reader, dataset, parse_dims(start_text),
                       parse_dims(count_text), values_output);
      else if (!start_text.empty() || !count_text.empty())
        throw std::runtime_error("selection requires dataset");
    } catch (const Unsupported &e) {
      status = "unsupported";
      error = e.what();
    } catch (const std::exception &e) {
      status = "error";
      error = e.what();
    }
    report << "{\"schema_version\":\"iotdbfs-hdf5-h00-probe/1\",\"status\":"
           << quote(status) << ",\"error\":" << quote(error)
           << ",\"file\":" << quote(file) << ",\"library_version\":"
           << quote(std::to_string(major) + "." + std::to_string(minor) + "." +
                    std::to_string(release))
           << ",\"library_threadsafe\":" << (threadsafe ? "true" : "false")
           << ",\"zlib_version\":" << quote(zlibVersion())
           << ",\"dynamic_plugins_enabled\":false,\"builtin_filters\":"
           << filters.str() << ",\"userblock_bytes\":" << userblock
           << ",\"source_bytes\":" << reader.size
           << ",\"raw_payload_bytes\":" << reader.bytes
           << ",\"raw_payload_calls\":" << reader.ranges.size()
           << ",\"trace_scope\":\"ifstream value payload only; libhdf5 "
              "metadata/oracle I/O excluded\",\"object_count\":"
           << inventory.objects << ",\"datasets\":[";
    for (size_t i = 0; i < inventory.datasets.size(); ++i) {
      if (i)
        report << ',';
      report << inventory.datasets[i];
    }
    report << "],\"links\":[";
    for (size_t i = 0; i < inventory.links.size(); ++i) {
      if (i)
        report << ',';
      report << inventory.links[i];
    }
    report << "],\"ranges\":[";
    for (size_t i = 0; i < reader.ranges.size(); ++i) {
      if (i)
        report << ',';
      report << reader.ranges[i];
    }
    report << "],\"query\":" << result << "}\n";
  } catch (const std::exception &e) {
    status = "error";
    report.str("");
    report << "{\"schema_version\":\"iotdbfs-hdf5-h00-probe/"
              "1\",\"status\":\"error\",\"error\":"
           << quote(e.what()) << "}\n";
  }
  if (output.empty()) {
    std::cerr << report.str();
    return 1;
  }
  std::ofstream out(output);
  out << report.str();
  if (!out) {
    std::cerr << "cannot write output\n";
    return 1;
  }
  return status == "error" ? 1 : 0;
}
