"""Same-library native readers for section 4.2; no service/resource mutation.

Every result owns its values, mask and coordinates. Metadata-only preparation
never retains encoded messages or decoded values. Hash/audit work is separate
from the recorded entry -> owned output -> handle-release clock.
"""
from __future__ import annotations

import ctypes as C
import math
import os
import time
from pathlib import Path

import numpy as np


def need(condition, message):
    if not condition:
        raise ValueError(message)


def clock_record(start, discovered, owned, **fields):
    return dict(t0_ns=start, t1_ns=discovered, t2_ns=owned,
                discovery_ns=discovered-start, value_ns=owned-discovered,
                total_ns=owned-start, **fields)


class NativeHdf5:
    """Explicit HDF5 C ABI, loading the daemon's pinned thread-safe core."""
    def __init__(self, library_path):
        for key in ('HDF5_DRIVER', 'HDF5_VOL_CONNECTOR', 'HDF5_PLUGIN_PATH'):
            need(not os.environ.get(key), 'HDF5 runtime override forbidden')
        self.library_path = str(Path(library_path).resolve(strict=True))
        self.lib = C.CDLL(self.library_path)
        hid, size, ptr = C.c_int64, C.c_uint64, C.c_void_p
        signatures = {
            'H5open': (C.c_int, []),
            'H5get_libversion': (C.c_int, [C.POINTER(C.c_uint)]*3),
            'H5is_library_threadsafe': (C.c_int, [C.POINTER(C.c_bool)]),
            'H5PLset_loading_state': (C.c_int, [C.c_uint]),
            'H5Fopen': (hid, [C.c_char_p, C.c_uint, hid]),
            'H5Fclose': (C.c_int, [hid]),
            'H5Dopen2': (hid, [hid, C.c_char_p, hid]),
            'H5Dclose': (C.c_int, [hid]),
            'H5Dget_space': (hid, [hid]),
            'H5Dget_type': (hid, [hid]),
            'H5Dread': (C.c_int, [hid, hid, hid, hid, hid, ptr]),
            'H5Sget_simple_extent_ndims': (C.c_int, [hid]),
            'H5Sget_simple_extent_dims': (C.c_int, [hid, C.POINTER(size), C.POINTER(size)]),
            'H5Sget_simple_extent_npoints': (C.c_int64, [hid]),
            'H5Screate_simple': (hid, [C.c_int, C.POINTER(size), C.POINTER(size)]),
            'H5Sselect_hyperslab': (C.c_int, [hid, C.c_int, C.POINTER(size),
                                           C.POINTER(size), C.POINTER(size), C.POINTER(size)]),
            'H5Sclose': (C.c_int, [hid]),
            'H5Tget_class': (C.c_int, [hid]),
            'H5Tget_size': (C.c_size_t, [hid]),
            'H5Tget_sign': (C.c_int, [hid]),
            'H5Tclose': (C.c_int, [hid]),
            'H5Aexists': (C.c_int, [hid, C.c_char_p]),
            'H5Aopen': (hid, [hid, C.c_char_p, hid]),
            'H5Aget_space': (hid, [hid]),
            'H5Aread': (C.c_int, [hid, hid, ptr]),
            'H5Aclose': (C.c_int, [hid]),
        }
        for name, (result, arguments) in signatures.items():
            function = getattr(self.lib, name)
            function.restype, function.argtypes = result, arguments
        self._check(self.lib.H5open(), 'initialize')
        major, minor, release = C.c_uint(), C.c_uint(), C.c_uint()
        self._check(self.lib.H5get_libversion(C.byref(major), C.byref(minor), C.byref(release)), 'version')
        need((major.value, minor.value, release.value) == (1, 14, 4), 'HDF5 version differs')
        safe = C.c_bool()
        self._check(self.lib.H5is_library_threadsafe(C.byref(safe)), 'thread safety')
        need(safe.value, 'HDF5 library must be thread-safe')
        self._check(self.lib.H5PLset_loading_state(0), 'disable plugins')
        names = {'<f4': 'FLOAT', '<f8': 'DOUBLE', '<i2': 'SHORT', '<i4': 'INT32',
                 '<i8': 'INT64', '<u4': 'UINT32', '<u8': 'UINT64'}
        self.types = {dtype: hid.in_dll(self.lib, 'H5T_NATIVE_'+name+'_g').value
                      for dtype, name in names.items()}

    @staticmethod
    def _check(status, operation):
        need(status >= 0, 'HDF5 '+operation+' failed')
        return status

    def open_file(self, path):
        return self._check(self.lib.H5Fopen(os.fsencode(path), 0, 0), 'open file')

    def close_file(self, file_id):
        self._check(self.lib.H5Fclose(file_id), 'close file')

    def _open_dataset(self, file_id, dataset):
        return self._check(self.lib.H5Dopen2(file_id, dataset.encode(), 0), 'open dataset')

    def describe(self, file_id, dataset):
        did = self._open_dataset(file_id, dataset)
        space = type_id = -1
        try:
            space = self._check(self.lib.H5Dget_space(did), 'dataset space')
            type_id = self._check(self.lib.H5Dget_type(did), 'dataset type')
            rank = self._check(self.lib.H5Sget_simple_extent_ndims(space), 'rank')
            need(1 <= rank <= 8, 'unsupported dataset rank')
            dims = (C.c_uint64*rank)()
            self._check(self.lib.H5Sget_simple_extent_dims(space, dims, None), 'shape')
            kind, width = self.lib.H5Tget_class(type_id), self.lib.H5Tget_size(type_id)
            prefix = 'f' if kind == 1 else ('i' if self.lib.H5Tget_sign(type_id) == 1 else 'u')
            need(kind in (0, 1), 'unsupported nonnumeric dataset type')
            dtype = '<'+prefix+str(width)
            need(dtype in self.types, 'unsupported numeric dataset type')
            return dict(dataset=dataset, source_shape=list(dims), dtype=dtype)
        finally:
            if type_id >= 0:
                self._check(self.lib.H5Tclose(type_id), 'close type')
            if space >= 0:
                self._check(self.lib.H5Sclose(space), 'close space')
            self._check(self.lib.H5Dclose(did), 'close dataset')

    def numeric_attributes(self, file_id, dataset, names):
        """Numeric attributes converted by HDF5, suitable for CF scale/fill."""
        did = self._open_dataset(file_id, dataset)
        result = {}
        try:
            for name in names:
                present = self.lib.H5Aexists(did, name.encode())
                self._check(present, 'attribute existence')
                if not present:
                    continue
                aid = self._check(self.lib.H5Aopen(did, name.encode(), 0), 'open attribute')
                sid = -1
                try:
                    sid = self._check(self.lib.H5Aget_space(aid), 'attribute space')
                    count = self._check(self.lib.H5Sget_simple_extent_npoints(sid), 'attribute length')
                    need(0 < count <= 64, 'numeric attribute exceeds bound')
                    array = np.empty(count, dtype='<f8')
                    self._check(self.lib.H5Aread(aid, self.types['<f8'], C.c_void_p(array.ctypes.data)),
                                'read numeric attribute')
                    result[name] = array.tolist()
                finally:
                    if sid >= 0:
                        self._check(self.lib.H5Sclose(sid), 'close attribute space')
                    self._check(self.lib.H5Aclose(aid), 'close attribute')
        finally:
            self._check(self.lib.H5Dclose(did), 'close dataset')
        return result

    def read_dataset(self, file_id, dataset, origin, shape, dtype, *, dataset_id=None,
                     max_result_bytes=512*1024*1024):
        need(len(origin) == len(shape) and 1 <= len(shape) <= 8 and
             all(type(v) is int and v >= 0 for v in origin) and
             all(type(v) is int and v > 0 for v in shape), 'invalid hyperslab')
        need(dtype in self.types and math.prod(shape)*np.dtype(dtype).itemsize <= max_result_bytes,
             'native result exceeds output bound')
        did = self._open_dataset(file_id, dataset) if dataset_id is None else dataset_id
        space = memory = -1
        try:
            space = self._check(self.lib.H5Dget_space(did), 'read space')
            rank = self._check(self.lib.H5Sget_simple_extent_ndims(space), 'read rank')
            need(rank == len(shape), 'selection rank differs')
            dims = (C.c_uint64*rank)()
            self._check(self.lib.H5Sget_simple_extent_dims(space, dims, None), 'read shape')
            need(all(o <= d and s <= d-o for o,s,d in zip(origin, shape, dims)),
                 'hyperslab outside dataset')
            starts, counts = (C.c_uint64*rank)(*origin), (C.c_uint64*rank)(*shape)
            self._check(self.lib.H5Sselect_hyperslab(space, 0, starts, None, counts, None), 'select')
            memory = self._check(self.lib.H5Screate_simple(rank, counts, None), 'memory space')
            values = np.empty(math.prod(shape), dtype=dtype)
            self._check(self.lib.H5Dread(did, self.types[dtype], memory, space, 0,
                                       C.c_void_p(values.ctypes.data)), 'read values')
            need(values.flags.owndata, 'native output is not owned')
            return values
        finally:
            if memory >= 0:
                self._check(self.lib.H5Sclose(memory), 'close memory space')
            if space >= 0:
                self._check(self.lib.H5Sclose(space), 'close read space')
            if dataset_id is None:
                self._check(self.lib.H5Dclose(did), 'close read dataset')

    def prepare(self, spec):
        start = time.perf_counter_ns()
        fid = self.open_file(spec['raw'])
        try:
            metadata = self.describe(fid, spec.get('dataset', '/T'))
        finally:
            self.close_file(fid)
        self._verify_metadata(spec, metadata)
        return metadata, time.perf_counter_ns()-start

    @staticmethod
    def _verify_metadata(spec, metadata):
        for name in ('dataset', 'source_shape', 'dtype'):
            if name in spec:
                need(metadata[name] == spec[name], 'native metadata differs: '+name)

    def open_session(self, spec, metadata=None):
        start = time.perf_counter_ns()
        fid, did = self.open_file(spec['raw']), -1
        try:
            if metadata is None:
                metadata = self.describe(fid, spec.get('dataset', '/T'))
            self._verify_metadata(spec, metadata)
            did = self._open_dataset(fid, spec.get('dataset', '/T'))
            return dict(file_id=fid, dataset_id=did, metadata=metadata), time.perf_counter_ns()-start
        except BaseException:
            if did >= 0:
                self._check(self.lib.H5Dclose(did), 'close failed session dataset')
            self.close_file(fid)
            raise

    def close_session(self, session):
        start = time.perf_counter_ns()
        try:
            self._check(self.lib.H5Dclose(session.pop('dataset_id')), 'close session dataset')
        finally:
            self.close_file(session.pop('file_id'))
        return time.perf_counter_ns()-start

    def read(self, spec, metadata=None, session=None):
        start = time.perf_counter_ns()
        fid = session['file_id'] if session else None
        cached = session['metadata'] if session else metadata
        close_needed = False
        try:
            if cached is None:
                fid = self.open_file(spec['raw'])
                close_needed = True
                cached = self.describe(fid, spec.get('dataset', '/T'))
            self._verify_metadata(spec, cached)
            discovered = time.perf_counter_ns()
            if fid is None:
                fid = self.open_file(spec['raw'])
                close_needed = True
            values = self.read_dataset(fid, cached['dataset'], spec['origin'], spec['shape'],
                                       spec.get('physical_output_dtype', cached['dtype']),
                                       dataset_id=session['dataset_id'] if session else None,
                                       max_result_bytes=spec.get('max_result_bytes', 64*1024*1024))
            mask = np.ones(values.size, dtype='u1')
            result = dict(values=values, mask=mask)
        finally:
            if close_needed:
                self.close_file(fid)
        end = time.perf_counter_ns()
        return clock_record(start, discovered, end, rows=values.size, dtype=values.dtype.str,
                            native_value_read_calls=1, decoded_units=1,
                            metadata_reused=cached is not None and (metadata is not None or session is not None),
                            handles_reused=session is not None), result


class NativeGrib:
    """Headers-only scan plus direct message decode; metadata-only reuse."""
    def __init__(self, library_path):
        self.library_path = str(Path(library_path).resolve(strict=True))
        self.lib = C.CDLL(self.library_path)
        self.libc = C.CDLL(None)
        p, size = C.c_void_p, C.c_size_t
        signatures = {
            'grib_new_from_file': (p, [p,p,C.c_int,C.POINTER(C.c_int)]),
            'codes_get_long': (C.c_int,[p,C.c_char_p,C.POINTER(C.c_long)]),
            'codes_get_double': (C.c_int,[p,C.c_char_p,C.POINTER(C.c_double)]),
            'codes_get_string': (C.c_int,[p,C.c_char_p,C.c_char_p,C.POINTER(size)]),
            'codes_get_message_offset': (C.c_int,[p,C.POINTER(C.c_int64)]),
            'codes_handle_new_from_message_copy': (p,[p,p,size]),
            'codes_get_size': (C.c_int,[p,C.c_char_p,C.POINTER(size)]),
            'codes_get_double_array': (C.c_int,[p,C.c_char_p,C.POINTER(C.c_double),C.POINTER(size)]),
            'codes_handle_delete': (C.c_int,[p]),
            'codes_get_api_version': (C.c_long,[]),
        }
        for name,(restype,args) in signatures.items():
            function = getattr(self.lib,name)
            function.restype,function.argtypes = restype,args
        for name,restype,args in (
            ('fopen',p,[C.c_char_p,C.c_char_p]), ('fclose',C.c_int,[p]),
            ('fseeko',C.c_int,[p,C.c_int64,C.c_int]),
            ('fread',size,[p,size,size,p])):
            function = getattr(self.libc,name)
            function.restype,function.argtypes=restype,args
        need(self.lib.codes_get_api_version() >= 20000, 'unsupported ecCodes version')

    @staticmethod
    def _check(status, operation):
        need(status == 0, 'ecCodes '+operation+' failed: '+str(status))

    def _long(self, handle, key):
        value=C.c_long()
        self._check(self.lib.codes_get_long(handle,key.encode(),C.byref(value)), key)
        return value.value

    def _double(self, handle, key):
        value=C.c_double()
        self._check(self.lib.codes_get_double(handle,key.encode(),C.byref(value)), key)
        return value.value

    def _string(self, handle, key):
        buffer,size=C.create_string_buffer(1024),C.c_size_t(1024)
        self._check(self.lib.codes_get_string(handle,key.encode(),buffer,C.byref(size)), key)
        return buffer.value.decode()

    def _open(self, raw):
        file=self.libc.fopen(os.fsencode(raw),b'rb')
        need(file, 'native GRIB open failed')
        return file

    def _close(self, file):
        need(self.libc.fclose(file) == 0, 'native GRIB close failed')

    def _discover(self, file, spec):
        need(self.libc.fseeko(file,0,0) == 0, 'GRIB rewind failed')
        source_size=Path(spec['raw']).stat().st_size
        position,candidates,selected=0,0,[]
        predicate=spec['filter']
        while position < source_size:
            error=C.c_int()
            handle=self.lib.grib_new_from_file(None,file,1,C.byref(error))
            self._check(error.value, 'headers-only discovery')
            need(handle, 'GRIB truncated before source EOF')
            try:
                offset=C.c_int64()
                self._check(self.lib.codes_get_message_offset(handle,C.byref(offset)), 'offset')
                length=self._long(handle,'totalLength')
                need(offset.value == position and 0 < length <= source_size-position,
                     'GRIB source range differs')
                values={key:self._long(handle,key) for key in ('level','step')}
                matches=all(values[k] == predicate[k] for k in values if k in predicate)
                if matches:
                    values.update({key:self._string(handle,key) for key in ('shortName','typeOfLevel')})
                    matches=all(values[k] == value for k,value in predicate.items())
                if matches:
                    selected.append(dict(offset=offset.value,length=length,**values))
                candidates+=1
                need(candidates <= 100000, 'GRIB candidate bound exceeded')
                position+=length
            finally:
                self._check(self.lib.codes_handle_delete(handle),'delete header')
        need(position == source_size and len(selected) == 1, 'GRIB expected exactly one selected message')
        return dict(candidates=candidates,selected=selected,filter=dict(predicate),source_bytes=source_size)

    def prepare(self, spec):
        start=time.perf_counter_ns()
        file=self._open(spec['raw'])
        try:
            metadata=self._discover(file,spec)
        finally:
            self._close(file)
        return metadata,time.perf_counter_ns()-start

    def open_session(self, spec, metadata=None):
        start=time.perf_counter_ns()
        file=self._open(spec['raw'])
        try:
            if metadata is None:
                metadata=self._discover(file,spec)
            need(metadata['filter'] == spec['filter'], 'GRIB predicate cache differs')
            return dict(file=file,metadata=metadata),time.perf_counter_ns()-start
        except BaseException:
            self._close(file)
            raise

    def close_session(self, session):
        start=time.perf_counter_ns()
        self._close(session.pop('file'))
        return time.perf_counter_ns()-start

    def _array(self, handle, name):
        count=C.c_size_t()
        self._check(self.lib.codes_get_size(handle,name.encode(),C.byref(count)),name+' size')
        need(0 < count.value <= 20000000,'GRIB array bound exceeded')
        values=np.empty(count.value,dtype='<f8')
        self._check(self.lib.codes_get_double_array(handle,name.encode(),
            values.ctypes.data_as(C.POINTER(C.c_double)),C.byref(count)),name+' decode')
        need(count.value == values.size,'GRIB decoded array size changed')
        return values

    def _decode(self, encoded, spec):
        if spec['view'] == 'binary':
            # bytes(memoryview) creates independent immutable ownership.
            return dict(values=[bytes(memoryview(encoded))],mask=np.ones(1,dtype='u1'))
        data=C.create_string_buffer(encoded)
        handle=self.lib.codes_handle_new_from_message_copy(None,data,len(encoded))
        need(handle,'ecCodes rejected selected message')
        try:
            need(self._long(handle,'bitmapPresent') == 0,'main GRIB workload must have no bitmap')
            need(self._long(handle,'Ni') == 1440 and self._long(handle,'Nj') == 721 and
                 self._long(handle,'jPointsAreConsecutive') == 0 and
                 self._long(handle,'alternativeRowScanning') == 0,'GRIB grid scanning differs')
            values=self._array(handle,'values')
            ni,nj=self._long(handle,'Ni'),self._long(handle,'Nj')
            lat0=self._double(handle,'latitudeOfFirstGridPointInDegrees')
            lon0=self._double(handle,'longitudeOfFirstGridPointInDegrees')
            di=self._double(handle,'iDirectionIncrementInDegrees')
            dj=self._double(handle,'jDirectionIncrementInDegrees')
            if not self._long(handle,'iScansNegatively'):
                longitudes=lon0+np.arange(ni,dtype='<f8')*di
            else:
                longitudes=lon0-np.arange(ni,dtype='<f8')*di
            latitudes=lat0+np.arange(nj,dtype='<f8')*dj*(1 if self._long(handle,'jScansPositively') else -1)
            lat_bounds,lon_bounds=spec['bounds']
            ys=np.flatnonzero((latitudes>=lat_bounds[0]) & (latitudes<=lat_bounds[1]))
            xs=np.flatnonzero((longitudes>=lon_bounds[0]) & (longitudes<=lon_bounds[1]))
            need(ys.size and xs.size,'GRIB empty crop differs from fixed case')
            selected=values.reshape(nj,ni)[np.ix_(ys,xs)].astype('<f4',copy=True).ravel().copy()
            latitude=np.repeat(latitudes[ys],xs.size).copy()
            longitude=np.tile(longitudes[xs],ys.size).copy()
            return dict(values=selected,mask=np.ones(selected.size,dtype='u1'),
                        latitude=latitude,longitude=longitude)
        finally:
            self._check(self.lib.codes_handle_delete(handle),'delete payload handle')

    def read(self, spec, metadata=None, session=None):
        start=time.perf_counter_ns()
        file=session['file'] if session else self._open(spec['raw'])
        close_needed=session is None
        try:
            cached=session['metadata'] if session else metadata
            if cached is None:
                cached=self._discover(file,spec)
            need(cached['filter'] == spec['filter'],'GRIB cached predicate differs')
            need(len(cached['selected']) == 1,'GRIB selected unit differs')
            row=cached['selected'][0]
            discovered=time.perf_counter_ns()
            need(self.libc.fseeko(file,row['offset'],0) == 0,'GRIB selected seek failed')
            need(0 < row['length'] <= 64*1024*1024,'GRIB encoded bound exceeded')
            buffer=C.create_string_buffer(row['length'])
            need(self.libc.fread(buffer,1,row['length'],file) == row['length'],'short GRIB message read')
            owned=self._decode(buffer.raw,spec)
        finally:
            if close_needed:
                self._close(file)
        end=time.perf_counter_ns()
        return clock_record(start,discovered,end,rows=owned['mask'].size,
            dtype='binary' if spec['view']=='binary' else '<f4',
            payload_read_calls=1,payload_read_bytes=row['length'],
            decoded_units=0 if spec['view']=='binary' else 1,
            candidate_units=cached['candidates'],selected_units=1,
            metadata_reused=metadata is not None or session is not None,
            handles_reused=session is not None),owned
