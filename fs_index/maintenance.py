"""Incrementally maintained GRIB/HDF5 catalog (protocol v2).

Handles are serial. Use a separate Writer and Reader, including across processes.
The native library owns publication, SQLite transactions and scientific readers.
"""
from __future__ import annotations

import ctypes
import json
import math
from pathlib import Path

from .native import NativeError

PROTOCOL = "iotdbfs-fs-index/2"


class _Handle:
    def __init__(self, library, root, *, writer=False, create=False, indexed=None):
        self._context = None
        self.library_path = str(Path(library).resolve(strict=True))
        self._library = ctypes.CDLL(self.library_path)
        lib = self._library
        lib.fixed_native_last_error.argtypes = []
        lib.fixed_native_last_error.restype = ctypes.c_char_p
        lib.fixed_native_result_free.argtypes = [ctypes.c_void_p]
        lib.fixed_native_result_free.restype = None
        lib.fs_index_open_json.argtypes = [ctypes.c_char_p]
        lib.fs_index_open_json.restype = ctypes.c_void_p
        lib.fs_index_close.argtypes = [ctypes.c_void_p]
        lib.fs_index_close.restype = None
        for name in ("publish", "select", "explain"):
            function = getattr(lib, "fs_index_" + name + "_json")
            function.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
            function.restype = ctypes.c_void_p
        for name in ("status", "checkpoint"):
            function = getattr(lib, "fs_index_" + name + "_json")
            function.argtypes = [ctypes.c_void_p]
            function.restype = ctypes.c_void_p
        lib.fs_index_read_into.argtypes = [ctypes.c_void_p, ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_float), ctypes.POINTER(ctypes.c_uint8), ctypes.c_uint64]
        lib.fs_index_read_into.restype = ctypes.c_int
        config = dict(protocol=PROTOCOL, root=str(Path(root).absolute()), writer=writer, create=create)
        if indexed is not None:
            config["indexed"] = bool(indexed)
        self._context = lib.fs_index_open_json(self._json(config))
        if not self._context:
            self._raise()

    @staticmethod
    def _json(value):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()

    def _raise(self):
        message = self._library.fixed_native_last_error()
        raise NativeError(message.decode() if message else "native maintenance error")

    def _live(self):
        if not self._context:
            raise NativeError("maintenance handle is closed")
        return self._context

    def _call(self, name, request=None):
        function = getattr(self._library, "fs_index_" + name + "_json")
        pointer = function(self._live()) if request is None else function(self._live(), self._json(request))
        if not pointer:
            self._raise()
        try:
            return json.loads(ctypes.string_at(pointer))
        finally:
            self._library.fixed_native_result_free(pointer)

    def status(self):
        """Diagnostic full catalog/space inventory; keep outside query clocks."""
        result = self._call("status")
        result["library_path"] = self.library_path
        return result

    def close(self):
        pointer = getattr(self, "_context", None)
        if pointer:
            self._library.fs_index_close(pointer)
            self._context = None

    def __enter__(self):
        self._live()
        return self

    def __exit__(self, *_):
        self.close()

    def __del__(self):
        self.close()


class Writer(_Handle):
    def __init__(self, library, root, *, create=False, indexed=None):
        super().__init__(library, root, writer=True, create=create, indexed=indexed)

    def publish(self, input, *, logical_path, format, version, ordinal, expected_version, op_id):
        """Copy/fsync one immutable version and atomically publish its catalog.

        New stores use expected_version=0 for the first version of a logical path.
        Retrying the same op_id requires the same content and semantic request.
        indexed=False stores register sources/head only, for native scan workloads.
        """
        for name, value in (("version", version), ("ordinal", ordinal), ("expected_version", expected_version)):
            if type(value) is not int or value < (0 if name == "expected_version" else 1):
                raise ValueError("invalid " + name)
        return self._call("publish", dict(input=str(Path(input).absolute()), logical_path=logical_path,
            format=format, version=version, ordinal=ordinal, expected_version=expected_version, op_id=op_id))

    def checkpoint(self):
        """Timed TRUNCATE checkpoint; inspect busy and remaining WAL explicitly."""
        return self._call("checkpoint")


class Reader(_Handle):
    def __init__(self, library, root):
        super().__init__(library, root)

    @staticmethod
    def _request(mode, selection, predicate, logical_scope):
        if mode not in ("scan", "index"):
            raise ValueError("mode must be scan/index")
        return dict(mode=mode, selection=selection, predicate=predicate, logical_scope=logical_scope)

    def select(self, mode, selection, predicate, logical_scope):
        return self._call("select", self._request(mode, selection, predicate, logical_scope))

    def explain(self, selection, predicate, logical_scope):
        return self._call("explain", self._request("index", selection, predicate, logical_scope))

    def read(self, row):
        import numpy as np
        shape = tuple(row["shape"])
        if not shape or any(type(n) is not int or n <= 0 for n in shape) or math.prod(shape) != 4096:
            raise ValueError("maintenance profile requires 4096 values per unit")
        values = np.empty(shape, dtype=np.float32, order="C")
        mask = np.empty(shape, dtype=np.uint8, order="C")
        code = self._library.fs_index_read_into(self._live(), self._json(row),
            values.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
            mask.ctypes.data_as(ctypes.POINTER(ctypes.c_uint8)), values.size)
        if code:
            self._raise()
        return values, mask
