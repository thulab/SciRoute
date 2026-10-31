"""Client-owned native baseline API for the approved fixed matrix.

The C ABI serializes discovery metadata only. Allocating arrays, crossing ctypes,
reading/decoding, and producing masks are all inside the controller's q clock.
"""
from __future__ import annotations

import ctypes
import json
import math
from pathlib import Path
from typing import Any


class NativeError(RuntimeError):
    pass


class NativeContext:
    """Independent retained native state for one baseline arm/campaign."""

    def __init__(self, library: str | Path, config: dict[str, Any]):
        self._library_path = str(Path(library).resolve(strict=True))
        self._library = ctypes.CDLL(self._library_path)
        lib = self._library
        lib.fixed_native_last_error.argtypes = []
        lib.fixed_native_last_error.restype = ctypes.c_char_p
        lib.fixed_native_context_create.argtypes = [ctypes.c_char_p]
        lib.fixed_native_context_create.restype = ctypes.c_void_p
        lib.fixed_native_context_free.argtypes = [ctypes.c_void_p]
        lib.fixed_native_context_free.restype = None
        lib.fixed_native_select_json.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
        lib.fixed_native_select_json.restype = ctypes.c_void_p
        lib.fixed_native_explain_json.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
        lib.fixed_native_explain_json.restype = ctypes.c_void_p
        lib.fixed_native_stats_json.argtypes = [ctypes.c_void_p]
        lib.fixed_native_stats_json.restype = ctypes.c_void_p
        lib.fixed_native_result_free.argtypes = [ctypes.c_void_p]
        lib.fixed_native_result_free.restype = None
        lib.fixed_native_read_into.argtypes = [
            ctypes.c_void_p, ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_float), ctypes.POINTER(ctypes.c_uint8),
            ctypes.c_uint64,
        ]
        lib.fixed_native_read_into.restype = ctypes.c_int
        normalized = dict(config)
        if normalized.get("protocol", "iotdbfs-fixed-native/1") != "iotdbfs-fixed-native/1":
            raise ValueError("native config protocol mismatch")
        normalized["protocol"] = "iotdbfs-fixed-native/1"
        self._context = lib.fixed_native_context_create(self._json(normalized))
        if not self._context:
            self._raise()

    @staticmethod
    def _json(value: Any) -> bytes:
        return json.dumps(value, sort_keys=True, separators=(",", ":"),
                          allow_nan=False).encode("utf-8")

    def _raise(self) -> None:
        message = self._library.fixed_native_last_error()
        raise NativeError(message.decode("utf-8") if message else "native error")

    def _live(self) -> int:
        if not self._context:
            raise NativeError("native context is closed")
        return self._context

    def _result(self, pointer: int) -> dict[str, Any]:
        if not pointer:
            self._raise()
        try:
            return json.loads(ctypes.string_at(pointer))
        finally:
            self._library.fixed_native_result_free(pointer)

    def select(self, mode: str, selection: dict[str, Any],
               predicate: dict[str, Any], logical_scope: list[str]) -> dict[str, Any]:
        """Discover all selected units before any call to read()."""
        if mode not in ("scan", "index"):
            raise ValueError("mode must be scan or index")
        request = {"mode": mode, "selection": selection,
                   "predicate": predicate, "logical_scope": logical_scope}
        return self._result(self._library.fixed_native_select_json(
            self._live(), self._json(request)))

    def explain(self, mode: str, selection: dict[str, Any],
                predicate: dict[str, Any], logical_scope: list[str]) -> dict[str, Any]:
        """Collect the actual index SQL, bindings and SQLite plan outside clocks.

        Uses the same SQL builder and bind routine as select(). It may prepare
        the normal scope/ordinal metadata tables, but never reads payload values.
        """
        if mode != "index":
            raise ValueError("explain requires index mode")
        request = {"mode": mode, "selection": selection,
                   "predicate": predicate, "logical_scope": logical_scope}
        return self._result(self._library.fixed_native_explain_json(
            self._live(), self._json(request)))

    def read(self, row: dict[str, Any]):
        """Return full frozen selected unit as owned Float32 and uint8 arrays."""
        import numpy as np

        shape = tuple(row["shape"])
        if not shape or any(type(n) is not int or n <= 0 for n in shape):
            raise ValueError("invalid unit shape")
        count = math.prod(shape)
        if count != 4096:
            raise ValueError("fixed matrix selected unit must contain 4096 elements")
        values = np.empty(shape, dtype=np.float32, order="C")
        mask = np.empty(shape, dtype=np.uint8, order="C")
        code = self._library.fixed_native_read_into(
            self._live(), self._json(row),
            values.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
            mask.ctypes.data_as(ctypes.POINTER(ctypes.c_uint8)), count)
        if code:
            self._raise()
        return values, mask

    def stats(self) -> dict[str, Any]:
        result = self._result(self._library.fixed_native_stats_json(self._live()))
        result["library_path"] = self._library_path
        return result

    def close(self) -> None:
        context = getattr(self, "_context", None)
        if context:
            self._library.fixed_native_context_free(context)
            self._context = None

    def __enter__(self):
        self._live()
        return self

    def __exit__(self, *_):
        self.close()

    def __del__(self):
        self.close()
