#pragma once
#include "iotdbfs/FormatAdapter.h"
#include "iotdbfs/query/core/QueryModel.h"
#include <cstring>
#include <fstream>
#include <hdf5.h>
#include <stdexcept>
#include <unistd.h>
namespace hdf5_cf_test {
using namespace iotdbfs;
struct Native {
  hid_t h;
  herr_t (*close)(hid_t);
  Native(hid_t h, herr_t (*c)(hid_t)) : h(h), close(c) {
    if (h < 0)
      throw std::runtime_error("fixture HDF failure");
  }
  ~Native() { close(h); }
  operator hid_t() const { return h; }
};
inline void attr(hid_t obj, const char *name, hid_t type, const void *value,
                 const std::vector<hsize_t> &dims = {}) {
  Native space(dims.empty()
                   ? H5Screate(H5S_SCALAR)
                   : H5Screate_simple(dims.size(), dims.data(), nullptr),
               H5Sclose);
  Native a(H5Acreate2(obj, name, type, space, H5P_DEFAULT, H5P_DEFAULT),
           H5Aclose);
  if (H5Awrite(a, type, value) < 0)
    throw std::runtime_error("attribute write");
}
inline void str(hid_t obj, const char *name, const char *value) {
  Native type(H5Tcopy(H5T_C_S1), H5Tclose);
  H5Tset_size(type, std::strlen(value) + 1);
  attr(obj, name, type, value);
}
inline BinaryValue fixture(bool reversed = false, bool int32_time = false) {
  char path[] = "/tmp/iotdbfs-cf-XXXXXX";
  int fd = mkstemp(path);
  if (fd < 0)
    throw std::runtime_error("mkstemp");
  close(fd);
  struct Remove {
    char *p;
    ~Remove() { unlink(p); }
  } remove{path};
  {
    Native f(H5Fcreate(path, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT),
             H5Fclose);
    str(f, "Conventions", "CF-1.6");
    hsize_t shape[] = {1, 1, 3, 4};
    Native space(H5Screate_simple(4, shape, nullptr), H5Sclose);
    Native data(H5Dcreate2(f, "/data", H5T_STD_I16LE, space, H5P_DEFAULT,
                           H5P_DEFAULT, H5P_DEFAULT),
                H5Dclose);
    std::int16_t values[12] = {0, 1, -999, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    H5Dwrite(data, H5T_NATIVE_INT16, H5S_ALL, H5S_ALL, H5P_DEFAULT, values);
    str(data, "units", "Celsius");
    double scale = .01, offset = 2;
    attr(data, "scale_factor", H5T_NATIVE_DOUBLE, &scale, {1});
    attr(data, "add_offset", H5T_NATIVE_DOUBLE, &offset, {1});
    std::int16_t fill = -999;
    attr(data, "_FillValue", H5T_NATIVE_INT16, &fill, {1});
    hobj_ref_t refs[4]{};
    hvl_t lists[4]{};
    const char *names[] = {"/time", "/lat", "/lon"};
    const char *units[] = {"days since 1978-01-01 12:00:00", "degrees_north",
                           "degrees_east"};
    for (unsigned j = 0; j < 3; ++j) {
      unsigned axis = j == 0 ? 0 : j + 1;
      hsize_t n = shape[axis];
      Native sp(H5Screate_simple(1, &n, nullptr), H5Sclose);
      Native dcpl(H5Pcreate(H5P_DATASET_CREATE), H5Pclose);
      if (j == 0 && int32_time) {
        H5Pset_chunk(dcpl, 1, &n);
        H5Pset_deflate(dcpl, 1);
      }
      Native ds(H5Dcreate2(f, names[j], j == 0 && int32_time
                                           ? H5T_STD_I32LE
                                           : H5T_IEEE_F64LE,
                           sp, H5P_DEFAULT, dcpl, H5P_DEFAULT),
                H5Dclose);
      double v[4] = {0, 1, 2, 3};
      if (j == 0)
        v[0] = 16801;
      if (j == 1) {
        v[0] = 2;
        v[1] = reversed ? 3 : 1;
        v[2] = 0;
      }
      if (j == 0 && int32_time) {
        std::int32_t ordinal = 16801;
        H5Dwrite(ds, H5T_NATIVE_INT32, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                 &ordinal);
      } else {
        H5Dwrite(ds, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, v);
      }
      str(ds, "CLASS", "DIMENSION_SCALE");
      str(ds, "units", units[j]);
      H5Rcreate(&refs[axis], f, names[j], H5R_OBJECT, H5I_INVALID_HID);
      lists[axis] = {1, &refs[axis]};
    }
    Native vl(H5Tvlen_create(H5T_STD_REF_OBJ), H5Tclose);
    attr(data, "DIMENSION_LIST", vl, lists, {4});
  }
  std::ifstream input(path, std::ios::binary);
  return {std::istreambuf_iterator<char>(input), {}};
}

} // namespace hdf5_cf_test
