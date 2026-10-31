"""Native ecCodes generation and byte-preserving GRIB message regrouping."""
from __future__ import annotations

from contextlib import contextmanager
import ctypes as c
import ctypes.util
import hashlib

import numpy as np

from common import MAX_UNIT_BYTES, MAX_UNITS, TARGET, identity, generated_attributes


class Codes:
    def __init__(self):
        path = ctypes.util.find_library("eccodes")
        if not path:
            raise ValueError("native libeccodes is required")
        self.lib = c.CDLL(path)
        declarations = {
            "codes_grib_handle_new_from_samples": (c.c_void_p, [c.c_void_p, c.c_char_p]),
            "codes_handle_new_from_message_copy": (c.c_void_p, [c.c_void_p, c.c_void_p, c.c_size_t]),
            "codes_handle_delete": (c.c_int, [c.c_void_p]),
            "codes_set_long": (c.c_int, [c.c_void_p, c.c_char_p, c.c_long]),
            "codes_set_double": (c.c_int, [c.c_void_p, c.c_char_p, c.c_double]),
            "codes_set_string": (c.c_int, [c.c_void_p, c.c_char_p, c.c_char_p, c.POINTER(c.c_size_t)]),
            "codes_set_double_array": (c.c_int, [c.c_void_p, c.c_char_p, c.POINTER(c.c_double), c.c_size_t]),
            "codes_get_long": (c.c_int, [c.c_void_p, c.c_char_p, c.POINTER(c.c_long)]),
            "codes_get_double": (c.c_int, [c.c_void_p, c.c_char_p, c.POINTER(c.c_double)]),
            "codes_get_string": (c.c_int, [c.c_void_p, c.c_char_p, c.c_char_p, c.POINTER(c.c_size_t)]),
            "codes_get_size": (c.c_int, [c.c_void_p, c.c_char_p, c.POINTER(c.c_size_t)]),
            "codes_get_double_array": (c.c_int, [c.c_void_p, c.c_char_p, c.POINTER(c.c_double), c.POINTER(c.c_size_t)]),
            "codes_get_message": (c.c_int, [c.c_void_p, c.POINTER(c.c_void_p), c.POINTER(c.c_size_t)]),
            "codes_get_api_version": (c.c_long, []),
        }
        for name, (result, args) in declarations.items():
            function = getattr(self.lib, name)
            function.restype, function.argtypes = result, args
        self.path = path

    @staticmethod
    def check(code, operation):
        if code != 0:
            raise ValueError(f"ecCodes {operation} failed: {code}")

    @contextmanager
    def handle(self, encoded=None):
        if encoded is None:
            handle = self.lib.codes_grib_handle_new_from_samples(None, b"regular_ll_sfc_grib2")
        else:
            data = c.create_string_buffer(encoded)
            handle = self.lib.codes_handle_new_from_message_copy(None, data, len(encoded))
        if not handle:
            raise ValueError("ecCodes rejected GRIB source/sample")
        try:
            yield handle
        finally:
            self.lib.codes_handle_delete(handle)

    def get(self, handle, name, kind=int):
        if kind is str:
            size, value = c.c_size_t(1024), c.create_string_buffer(1024)
            self.check(self.lib.codes_get_string(handle, name.encode(), value, c.byref(size)), name)
            return value.value.decode()
        value = c.c_double() if kind is float else c.c_long()
        function = self.lib.codes_get_double if kind is float else self.lib.codes_get_long
        self.check(function(handle, name.encode(), c.byref(value)), name)
        return kind(value.value)

    def set(self, handle, name, value):
        if isinstance(value, str):
            encoded = value.encode()
            size = c.c_size_t(len(encoded))
            code = self.lib.codes_set_string(handle, name.encode(), encoded, c.byref(size))
        elif type(value) is float:
            code = self.lib.codes_set_double(handle, name.encode(), value)
        else:
            code = self.lib.codes_set_long(handle, name.encode(), value)
        self.check(code, name)

    def array(self, handle, name):
        size = c.c_size_t()
        self.check(self.lib.codes_get_size(handle, name.encode(), c.byref(size)), name)
        if not 0 < size.value <= MAX_UNIT_BYTES // 24:
            raise ValueError("GRIB numerical oracle exceeds working-memory bound")
        value = np.empty(size.value, dtype=np.float64)
        self.check(self.lib.codes_get_double_array(handle, name.encode(),
            value.ctypes.data_as(c.POINTER(c.c_double)), c.byref(size)), name)
        return value

    def encoded(self, handle):
        pointer, size = c.c_void_p(), c.c_size_t()
        self.check(self.lib.codes_get_message(handle, c.byref(pointer), c.byref(size)), "message")
        if not 0 < size.value <= MAX_UNIT_BYTES:
            raise ValueError("encoded message exceeds bound")
        return c.string_at(pointer, size.value)


def environment():
    codes = Codes()
    return {"eccodes_api_version": int(codes.lib.codes_get_api_version()),
            "eccodes_library": codes.path, "numpy": np.__version__}


def describe(codes, encoded, offset):
    with codes.handle(encoded) as handle:
        attributes = {name: codes.get(handle, name, str if name in ("shortName", "typeOfLevel") else int)
                      for name in TARGET}
        values = codes.array(handle, "values")
        mask = np.isfinite(values)
        if codes.get(handle, "bitmapPresent"):
            mask &= values != codes.get(handle, "missingValue", float)
        physical = np.where(mask, values, 0).astype("<f4")
        if not np.isfinite(physical).all():
            raise ValueError("GRIB values cannot be represented as Float32")
        oracle = {"physical_dtype": "<f4", "elements": len(values),
                  "value_sha256": hashlib.sha256(physical.tobytes()).hexdigest(),
                  "mask_sha256": hashlib.sha256(mask.astype("u1").tobytes()).hexdigest(),
                  "order": "native ecCodes scanning order; invalid values encoded as zero"}
        for name in ("latitudes", "longitudes"):
            array = codes.array(handle, name).astype("<f8", copy=False)
            oracle[name + "_sha256"] = hashlib.sha256(array.tobytes()).hexdigest()
        schema = {name: codes.get(handle, name) for name in ("edition", "Ni", "Nj", "bitsPerValue")}
        schema["packingType"] = codes.get(handle, "packingType", str)
        schema["gridType"] = codes.get(handle, "gridType", str)
        return {"unit_id": hashlib.sha256(encoded).hexdigest(), "offset": offset,
                "length": len(encoded), "attributes": attributes, "schema": schema, "oracle": oracle}


def inspect(path, selected=None):
    codes = Codes()
    units = []
    with open(path, "rb") as stream:
        offset = 0
        while True:
            header = stream.read(16)
            if not header:
                break
            if len(header) < 16 or header[:4] != b"GRIB" or header[7] not in (1, 2):
                raise ValueError(f"invalid/truncated GRIB header at {offset}")
            length = int.from_bytes(header[8:16] if header[7] == 2 else header[4:7], "big")
            if not 20 <= length <= MAX_UNIT_BYTES:
                raise ValueError("GRIB message length exceeds bounds (extended GRIB1 lengths unsupported)")
            encoded = header + stream.read(length - 16)
            if len(encoded) != length or encoded[-4:] != b"7777":
                raise ValueError("truncated GRIB message")
            if len(units) == MAX_UNITS:
                raise ValueError("GRIB source exceeds unit count bound")
            units.append(describe(codes, encoded, offset))
            offset += length
    if not units:
        raise ValueError("empty GRIB source")
    return {"controlled": False, "total_dataset_count": len(units), "units": units}


def create_file(path, spec, file_index):
    if spec["query"] != TARGET:
        raise ValueError("controlled generation uses the frozen target predicate; regroup may use other predicates")
    codes, p = Codes(), spec["payload"]
    with open(path, "xb") as output:
        for slot in range(spec["D"]):
            attrs = generated_attributes(file_index < spec["K"] and slot == 0, file_index * spec["D"] + slot)
            with codes.handle() as handle:
                for name, value in {"Ni": p["Ni"], "Nj": p["Nj"],
                        "latitudeOfFirstGridPointInDegrees": 0.0,
                        "latitudeOfLastGridPointInDegrees": (p["Nj"] - 1) * 0.25,
                        "longitudeOfFirstGridPointInDegrees": 0.0,
                        "longitudeOfLastGridPointInDegrees": (p["Ni"] - 1) * 0.25,
                        "iDirectionIncrementInDegrees": 0.25, "jDirectionIncrementInDegrees": 0.25,
                        "jScansPositively": 1, "iScansNegatively": 0,
                        "jPointsAreConsecutive": 0, "alternativeRowScanning": 0,
                        "typeOfLevel": attrs["typeOfLevel"], "level": attrs["level"],
                        "shortName": attrs["shortName"], "dataDate": TARGET["validityDate"],
                        "dataTime": TARGET["validityTime"], "step": attrs["step"],
                        "packingType": p["packing"], "bitsPerValue": p["bits_per_value"]}.items():
                    codes.set(handle, name, value)
                seed = int(identity({"seed": spec["seed"], "file": file_index, "slot": slot})[:32], 16)
                rng = np.random.Generator(np.random.PCG64(seed))
                values = (rng.integers(0, 65536, size=p["Ni"] * p["Nj"]).astype(np.float64) / 128 + 100)
                codes.check(codes.lib.codes_set_double_array(handle, b"values",
                    values.ctypes.data_as(c.POINTER(c.c_double)), len(values)), "set values")
                output.write(codes.encoded(handle))
    result = inspect(path)
    result["controlled"] = True
    for slot, unit in enumerate(result["units"]):
        if any(unit["schema"][k] != v for k, v in {
                "Ni": p["Ni"], "Nj": p["Nj"], "bitsPerValue": p["bits_per_value"],
                "packingType": p["packing"], "gridType": "regular_ll"}.items()):
            raise ValueError("native GRIB output differs from declared geometry/packing")
        unit["lineage"] = {"kind": "generated", "file_index": file_index,
                           "slot": slot, "seed": spec["seed"]}
    return result


def copy_units(path, chosen, resolver):
    with open(path, "xb") as output:
        for source, unit in chosen:
            with open(resolver(source), "rb") as raw:
                raw.seek(unit["offset"])
                encoded = raw.read(unit["length"])
            if len(encoded) != unit["length"] or hashlib.sha256(encoded).hexdigest() != unit["unit_id"]:
                raise ValueError("GRIB message changed since inventory")
            output.write(encoded)
    result = inspect(path)
    result["controlled"] = all(source["controlled"] for source, _ in chosen)
    for actual, (_, old) in zip(result["units"], chosen):
        if actual["unit_id"] != old["unit_id"] or actual["oracle"] != old["oracle"]:
            raise ValueError("GRIB regrouping changed bytes or native numerical oracle")
    return result
