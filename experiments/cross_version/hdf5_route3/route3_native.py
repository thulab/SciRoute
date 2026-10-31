"""Owned-array ctypes boundary for the route-3 HDF5 experiment.

Discovery returns JSON metadata only. read() allocates actual NumPy-owned value
and mask arrays in the client and native H5Dread writes directly into them.
Calls are serial per context; native HDF5 handles are retained only by that arm.
"""
from __future__ import annotations

import ctypes
import json
from pathlib import Path

import numpy as np

PROTOCOL = "iotdbfs-route3-native/1"


class NativeError(RuntimeError):
    pass


def _bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


class NativeContext:
    def __init__(self, library, config):
        self.library_path = str(Path(library).resolve(strict=True))
        self.lib = ctypes.CDLL(self.library_path)
        void, text = ctypes.c_void_p, ctypes.c_char_p
        self.lib.route3_last_error.argtypes = []
        self.lib.route3_last_error.restype = text
        self.lib.route3_context_create.argtypes = [text]
        self.lib.route3_context_create.restype = void
        self.lib.route3_context_free.argtypes = [void]
        self.lib.route3_context_free.restype = None
        self.lib.route3_result_free.argtypes = [void]
        self.lib.route3_result_free.restype = None
        self.lib.route3_select_json.argtypes = [void, text]
        self.lib.route3_select_json.restype = void
        self.lib.route3_stats_json.argtypes = [void]
        self.lib.route3_stats_json.restype = void
        self.lib.route3_read_into.argtypes = [
            void, text, text, ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(ctypes.c_float),
            ctypes.POINTER(ctypes.c_uint8)]
        self.lib.route3_read_into.restype = ctypes.c_int
        self.config = {"protocol": PROTOCOL, **config}
        self.handle = self.lib.route3_context_create(_bytes(self.config))
        if not self.handle:
            raise NativeError(self._error())

    def _error(self):
        return (self.lib.route3_last_error() or b"unknown native error").decode()

    def _active(self):
        if not self.handle:
            raise NativeError("native context is closed")

    def _json(self, pointer):
        if not pointer:
            raise NativeError(self._error())
        try:
            return json.loads(ctypes.string_at(pointer))
        finally:
            self.lib.route3_result_free(pointer)

    def select(self, mode, selection, predicate):
        self._active()
        return self._json(self.lib.route3_select_json(self.handle, _bytes({
            "mode": mode, "selection": selection, "predicate": predicate})))

    def stats(self):
        self._active()
        return self._json(self.lib.route3_stats_json(self.handle))

    def read(self, path, dataset, origin, shape):
        """Return (Float32 values, uint8 validity), both NumPy-owned C arrays."""
        self._active()
        if (len(origin) != 4 or len(shape) != 4 or
                any(type(n) is not int or n < 0 for n in origin) or
                any(type(n) is not int or n <= 0 for n in shape)):
            raise ValueError("ROI must have rank four and positive integer shape")
        count = 1
        for n in shape:
            count *= n
        if count * 5 > 64 * 1024**2:
            raise ValueError("ROI exceeds 64 MiB owned value+mask bound")
        values = np.empty(tuple(shape), dtype=np.float32, order="C")
        mask = np.empty(tuple(shape), dtype=np.uint8, order="C")
        u64 = ctypes.c_uint64 * 4
        result = self.lib.route3_read_into(
            self.handle, str(path).encode(), dataset.encode(),
            u64(*origin), u64(*shape),
            values.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
            mask.ctypes.data_as(ctypes.POINTER(ctypes.c_uint8)))
        if result:
            raise NativeError(self._error())
        return values, mask

    def close(self):
        if getattr(self, "handle", None):
            self.lib.route3_context_free(self.handle)
            self.handle = None

    def __enter__(self):
        self._active()
        return self

    def __exit__(self, *_):
        self.close()

    def __del__(self):
        self.close()


def counter_delta(before, after):
    """Counters are cumulative; residency and declared budgets are gauges."""
    gauges = {"resident_files", "resident_datasets",
              "metadata_cache_bytes_per_file", "raw_chunk_cache_bytes_per_dataset"}
    return {key: value if key in gauges else value - before.get(key, 0)
            for key, value in after.items()}
