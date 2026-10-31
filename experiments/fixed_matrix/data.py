"""Deterministic full raw corpora for the approved fixed matrix.

No database access or benchmark clocks live here.  The manifest describes every
unit, including nonmatches, using a fresh native read after the file is closed.
Existing directories are never resumed or replaced, including failed attempts.
"""
from __future__ import annotations

import ctypes as c
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import stat
import sys
import time

from experiments.multi_file.common import TARGET, generated_attributes


PROTOCOL = "iotdbfs-fixed-matrix-raw/1"
SEED = 20261013
SIDES = {"S0": 64, "S1": 384, "S2": 768}
FIELDS = ("format", "N", "V_stored", "size_profile", "history_scenario")


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode()


def _identity(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1048576), b""):
            digest.update(block)
    return digest.hexdigest()


def _write(path, value):
    with Path(path).open("xb") as stream:
        stream.write(_canonical(value) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


def corpus_key(group):
    """C and selection are case properties, never different raw corpora."""
    return (f"FM1-{group['format']}-N{group['N']}-V{group['V_stored']}-"
            f"{group['size_profile']}-{group['history_scenario']}")


def specifications(catalog, tier="primary"):
    if tier not in ("primary", "extension", "full"):
        raise ValueError("unknown fixed-matrix tier")
    result = {}
    for group in catalog["groups"]:
        if tier != "full" and group["collection_tier"] != tier:
            continue
        key = corpus_key(group)
        if key not in result:
            result[key] = {name: group[name] for name in FIELDS}
            result[key].update(corpus_key=key, D=4, seed=SEED, group_ids=[])
        result[key]["group_ids"].append(group["group_id"])
    return list(result.values())


def logical_file(fmt, file_id):
    """Opaque names, independent of shape, history, N, and predicate match."""
    code = _identity({"seed": SEED, "purpose": "path", "file_id": file_id})
    suffix = ".h5" if fmt == "hdf5" else ".grib"
    return f"b{int(code[:4], 16) % 16:02d}/b{int(code[4:8], 16) % 16:02d}/{code[8:32]}{suffix}"


def target_slot(file_id):
    return int(_identity({"seed": SEED, "purpose": "target-slot", "file_id": file_id})[:8], 16) % 4


def attributes(file_id, slot):
    return generated_attributes(file_id < 2 and slot == target_slot(file_id), file_id * 4 + slot)


def _matches(attrs):
    return all(type(attrs.get(k)) is type(v) and attrs[k] == v for k, v in TARGET.items())


def _side(spec, file_id, slot):
    return 64 if _matches(attributes(file_id, slot)) else SIDES[spec["size_profile"]]


def _base_values(file_id, slot, side):
    import numpy as np
    seed = int(_identity({"seed": SEED, "file_id": file_id, "slot": slot})[:32], 16)
    rng = np.random.Generator(np.random.PCG64(seed))
    # Binary-exact finite values avoid CF or precision-transform confounders.
    return (rng.integers(0, 65536, size=(side, side)).astype("<f4") / 128 + 100).astype("<f4")


def _change(values):
    import numpy as np
    result = np.asarray(values).copy()
    result.reshape(-1)[:8] += 1 / 128
    return result


def _oracle(values, valid, shape):
    import numpy as np
    mask = np.asarray(valid, dtype="u1").reshape(-1)
    physical = np.where(mask, np.asarray(values).reshape(-1), 0).astype("<f4")
    if not np.isfinite(physical).all():
        raise ValueError("native values exceed finite Float32 delivery")
    return {"elements": int(physical.size), "physical_dtype": "<f4", "shape": list(shape),
            "value_sha256": hashlib.sha256(physical.tobytes(order="C")).hexdigest(),
            "mask_sha256": hashlib.sha256(mask.tobytes(order="C")).hexdigest(),
            "valid_elements": int(mask.sum()), "order": "C/native scanning order",
            "invalid_value": 0, "mask_source": "native values and native missing-value metadata"}


def _hdf_environment():
    import h5py
    import numpy as np
    return {"h5py": h5py.__version__, "hdf5": h5py.version.hdf5_version, "numpy": np.__version__}


def _hdf_create(path, spec, file_id):
    import h5py
    import numpy as np
    # Disable object timestamps at every created level, including the root.
    fcpl = h5py.h5p.create(h5py.h5p.FILE_CREATE)
    fcpl.set_obj_track_times(False)
    handle = h5py.h5f.create(os.fsencode(path), h5py.h5f.ACC_EXCL, fcpl=fcpl)
    with h5py.File(handle) as output:
        gcpl = h5py.h5p.create(h5py.h5p.GROUP_CREATE)
        gcpl.set_obj_track_times(False)
        group = h5py.Group(h5py.h5g.create(output.id, b"data", gcpl=gcpl))
        for slot in range(4):
            side = _side(spec, file_id, slot)
            dataset = group.create_dataset(f"u{slot:04d}", data=_base_values(file_id, slot, side).reshape(1, 1, side, side),
                dtype="<f4", chunks=(1, 1, 32, 32), compression="gzip", compression_opts=1,
                shuffle=True, track_times=False)
            for name, value in attributes(file_id, slot).items():
                dataset.attrs[name] = np.bytes_(value) if isinstance(value, str) else np.int64(value)


def _hdf_change(path, slot):
    import h5py
    with h5py.File(path, "r+") as output:
        dataset = output[f"/data/u{slot:04d}"]
        dataset[...] = _change(dataset[...])


def _hdf_inspect(path):
    import h5py
    import numpy as np
    units = []
    with h5py.File(path, "r") as source:
        names = []
        source.visititems(lambda name, obj: names.append("/" + name) if isinstance(obj, h5py.Dataset) else None)
        if sorted(names) != [f"/data/u{x:04d}" for x in range(4)]:
            raise ValueError("native HDF5 inventory is not exactly four units")
        for name in sorted(names):
            dataset = source[name]
            values = dataset[...]
            valid = np.isfinite(values)
            for missing in ("_FillValue", "missing_value"):
                if missing in dataset.attrs:
                    valid &= values != dataset.attrs[missing]
            attrs = {}
            for key in TARGET:
                value = dataset.attrs[key]
                attrs[key] = bytes(value).decode() if isinstance(value, (bytes, np.bytes_)) else value.item()
            schema = {"shape": list(dataset.shape), "dtype": dataset.dtype.str,
                      "chunks": list(dataset.chunks), "compression": dataset.compression,
                      "compression_opts": dataset.compression_opts, "shuffle": bool(dataset.shuffle)}
            oracle = _oracle(values, valid, dataset.shape)
            units.append({"dataset": name, "attributes": attrs, "schema": schema, "oracle": oracle,
                          "unit_id": _identity({"schema": schema, "attributes": attrs, "oracle": oracle}),
                          "allocated_chunks": dataset.id.get_num_chunks(),
                          "stored_bytes": dataset.id.get_storage_size()})
    return units


def _codes():
    """Reuse the existing native binding without retaining its flat import alias."""
    from experiments.multi_file import common
    name = "_fixed_matrix_native_grib"
    module = sys.modules.get(name)
    if module is None:
        descriptor = importlib.util.spec_from_file_location(name, Path(__file__).parents[1] / "multi_file/grib_data.py")
        module = importlib.util.module_from_spec(descriptor)
        previous = sys.modules.get("common")
        sys.modules["common"] = common
        try:
            descriptor.loader.exec_module(module)
        finally:
            if previous is None:
                sys.modules.pop("common", None)
            else:
                sys.modules["common"] = previous
        sys.modules[name] = module
    return module.Codes()


def _grib_create(path, spec, file_id, codes):
    import numpy as np
    with path.open("xb") as output:
        for slot in range(4):
            side, attrs = _side(spec, file_id, slot), attributes(file_id, slot)
            with codes.handle() as handle:
                controls = {"Ni": side, "Nj": side,
                    "latitudeOfFirstGridPointInDegrees": 90.0,
                    "latitudeOfLastGridPointInDegrees": 90.0 - (side - 1) * .125,
                    "longitudeOfFirstGridPointInDegrees": 0.0,
                    "longitudeOfLastGridPointInDegrees": (side - 1) * .125,
                    "iDirectionIncrementInDegrees": .125, "jDirectionIncrementInDegrees": .125,
                    "jScansPositively": 0, "iScansNegatively": 0,
                    "jPointsAreConsecutive": 0, "alternativeRowScanning": 0,
                    "typeOfLevel": attrs["typeOfLevel"], "level": attrs["level"],
                    "shortName": attrs["shortName"], "dataDate": attrs["validityDate"],
                    "dataTime": attrs["validityTime"], "step": attrs["step"],
                    "packingType": "grid_simple", "bitsPerValue": 16}
                # validityTime describes the forecast, while dataTime is its reference.
                # generated_attributes encodes nonmatches with step*100 validity time.
                controls["dataTime"] = TARGET["validityTime"]
                for key, value in controls.items():
                    codes.set(handle, key, value)
                values = _base_values(file_id, slot, side).astype(np.float64).reshape(-1)
                codes.check(codes.lib.codes_set_double_array(handle, b"values",
                    values.ctypes.data_as(c.POINTER(c.c_double)), len(values)), "set values")
                output.write(codes.encoded(handle))


def _grib_messages(path):
    with path.open("rb") as source:
        while True:
            header = source.read(16)
            if not header:
                return
            if len(header) != 16 or header[:4] != b"GRIB" or header[7] != 2:
                raise ValueError("invalid generated GRIB2 header")
            length = int.from_bytes(header[8:16], "big")
            if not 20 <= length <= 64 * 1048576:
                raise ValueError("invalid generated GRIB2 length")
            message = header + source.read(length - 16)
            if len(message) != length or message[-4:] != b"7777":
                raise ValueError("truncated generated GRIB2")
            yield message


def _grib_change(path, slot, codes):
    import numpy as np
    messages = list(_grib_messages(path))
    with codes.handle(messages[slot]) as handle:
        values = _change(codes.array(handle, "values")).astype(np.float64)
        codes.check(codes.lib.codes_set_double_array(handle, b"values",
            values.ctypes.data_as(c.POINTER(c.c_double)), len(values)), "change values")
        messages[slot] = codes.encoded(handle)
    with path.open("wb") as output:
        output.writelines(messages)


def _grib_inspect(path, codes):
    import numpy as np
    units, offset = [], 0
    for message in _grib_messages(path):
        with codes.handle(message) as handle:
            attrs = {key: codes.get(handle, key, str if isinstance(value, str) else int) for key, value in TARGET.items()}
            values = codes.array(handle, "values")
            valid = np.isfinite(values)
            if codes.get(handle, "bitmapPresent"):
                valid &= values != codes.get(handle, "missingValue", float)
            ni, nj = codes.get(handle, "Ni"), codes.get(handle, "Nj")
            schema = {"Ni": ni, "Nj": nj, "shape": [nj, ni], "edition": codes.get(handle, "edition"),
                      "bitsPerValue": codes.get(handle, "bitsPerValue"),
                      "packingType": codes.get(handle, "packingType", str),
                      "gridType": codes.get(handle, "gridType", str)}
            for key in ("latitudeOfFirstGridPointInDegrees", "latitudeOfLastGridPointInDegrees",
                        "longitudeOfFirstGridPointInDegrees", "longitudeOfLastGridPointInDegrees",
                        "iDirectionIncrementInDegrees", "jDirectionIncrementInDegrees"):
                schema[key] = codes.get(handle, key, float)
            for key in ("jScansPositively", "iScansNegatively", "jPointsAreConsecutive", "alternativeRowScanning"):
                schema[key] = codes.get(handle, key)
            if schema["bitsPerValue"] != 16 or schema["packingType"] != "grid_simple":
                raise ValueError("native GRIB packing differs from frozen controls")
            expected_grid = {"gridType": "regular_ll", "edition": 2,
                "latitudeOfFirstGridPointInDegrees": 90.0,
                "latitudeOfLastGridPointInDegrees": 90.0 - (nj - 1) * .125,
                "longitudeOfFirstGridPointInDegrees": 0.0,
                "longitudeOfLastGridPointInDegrees": (ni - 1) * .125,
                "iDirectionIncrementInDegrees": .125, "jDirectionIncrementInDegrees": .125,
                "jScansPositively": 0, "iScansNegatively": 0,
                "jPointsAreConsecutive": 0, "alternativeRowScanning": 0}
            if any(schema[key] != value for key, value in expected_grid.items()):
                raise ValueError("native GRIB geometry differs from frozen controls")
            units.append({"offset": offset, "length": len(message), "attributes": attrs,
                          "schema": schema, "oracle": _oracle(values, valid, [nj, ni]),
                          "unit_id": hashlib.sha256(message).hexdigest()})
            offset += len(message)
    if len(units) != 4:
        raise ValueError("native GRIB inventory is not exactly four units")
    return units


def _check_spec(spec):
    if (spec["format"] not in ("grib", "hdf5") or spec["N"] not in (8, 128, 512, 1024)
            or spec["V_stored"] not in (1, 8, 32) or spec["size_profile"] not in SIDES
            or spec["D"] != 4 or spec["seed"] != SEED or spec["corpus_key"] != corpus_key(spec)):
        raise ValueError("spec is outside frozen matrix controls")
    if spec["V_stored"] == 1:
        if spec["history_scenario"] != "base":
            raise ValueError("V1 must use base history")
    elif (spec["history_scenario"] not in ("identical", "single-unit-change")
          or spec["N"] != 8 or spec["size_profile"] != "S0"):
        raise ValueError("historical corpus is outside frozen matrix")
    if spec["size_profile"] != "S0" and spec["N"] != 8:
        raise ValueError("large payload corpus is outside frozen matrix")


def prepare(spec, root):
    """Create an immutable full corpus once; return its complete raw manifest."""
    _check_spec(spec)
    root = Path(root)
    root.mkdir(parents=True, exist_ok=False)
    started = time.monotonic_ns()
    try:
        codes = _codes() if spec["format"] == "grib" else None
        native = ({"eccodes_api_version": int(codes.lib.codes_get_api_version()), "eccodes_library": codes.path}
                  if codes else _hdf_environment())
        entries, previous, changes = [], {}, []
        for ordinal in range(1, spec["V_stored"] + 1):
            for file_id in range(spec["N"]):
                logical = logical_file(spec["format"], file_id)
                relative = f"raw/v{ordinal:04d}/{logical}"
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                changed_slot = None
                if ordinal == 1:
                    if codes:
                        _grib_create(path, spec, file_id, codes)
                    else:
                        _hdf_create(path, spec, file_id)
                else:
                    shutil.copyfile(root / previous[file_id]["file"], path)
                    if spec["history_scenario"] == "single-unit-change":
                        changed_slot = (ordinal - 2) % 4
                        if codes:
                            _grib_change(path, changed_slot, codes)
                        else:
                            _hdf_change(path, changed_slot)
                units = _grib_inspect(path, codes) if codes else _hdf_inspect(path)
                for slot, unit in enumerate(units):
                    if unit["attributes"] != attributes(file_id, slot):
                        raise ValueError("native attributes differ from frozen generation")
                    side = _side(spec, file_id, slot)
                    if unit["oracle"]["elements"] != side * side:
                        raise ValueError("native unit shape differs from frozen generation")
                    unit["slot"] = slot
                metadata = path.stat()
                if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                    raise ValueError("raw version is not an independent regular file")
                entry = {"logical_file": logical, "file_id": file_id, "ordinal": ordinal,
                         "file": relative, "bytes": metadata.st_size, "sha256": _sha(path),
                         "units": units, "changed_slot": changed_slot}
                if ordinal > 1:
                    old = previous[file_id]
                    actual = [slot for slot in range(4) if units[slot]["unit_id"] != old["units"][slot]["unit_id"]]
                    if actual != ([] if changed_slot is None else [changed_slot]):
                        raise ValueError("version changed a different unit set from frozen history")
                    if changed_slot is None and entry["sha256"] != old["sha256"]:
                        raise ValueError("identical history changed full file bytes")
                    changes.append({"file_id": file_id, "ordinal": ordinal, "changed_slots": actual})
                previous[file_id] = entry
                entries.append(entry)
                path.chmod(0o444)
        for ordinal in range(1, spec["V_stored"] + 1):
            matches = sum(_matches(unit["attributes"]) for entry in entries if entry["ordinal"] == ordinal for unit in entry["units"])
            if matches != 2:
                raise ValueError("generation does not contain exactly two matched units")
        sizes = sorted(entry["bytes"] for entry in entries)
        manifest = {"protocol": PROTOCOL, "complete": True, "specification": spec,
                    "query": TARGET, "native_environment": native,
                    "controls": {"seed": SEED, "D": 4, "selected_side": 64,
                        "nonmatching_side": SIDES[spec["size_profile"]],
                        "path_rule": "opaque SHA256 seed/file_id; 16x16 buckets",
                        "scope_rule": "first C file_id values; nested, planted matches retained",
                        "target_rule": "file_id 0 and 1, one SHA256-selected slot each",
                        "grib_grid": {"first_latitude": 90.0, "first_longitude": 0.0,
                                      "step_degrees": .125, "jScansPositively": 0},
                        "grib_packing": {"packingType": "grid_simple", "bitsPerValue": 16},
                        "hdf5": {"dtype": "<f4", "chunks": [1, 1, 32, 32], "gzip_level": 1, "shuffle": True},
                        "history": "ordinal 2 changes slot 0 then cycles all four slots; first eight values += 1/128",
                        "physical_versions": "independent full regular files; byte copy, no links/reflinks/sparse"},
                    "entries": entries, "version_changes": changes,
                    "size_distribution": {"count": len(sizes), "min": min(sizes), "max": max(sizes),
                        "median": (sizes[(len(sizes) - 1) // 2] + sizes[len(sizes) // 2]) / 2,
                        "total_logical_bytes": sum(sizes), "sorted_bytes": sizes}}
        manifest["raw_manifest_id"] = _identity(manifest)
        _write(root / "manifest.json", manifest)
        _write(root / "preparation.json", {"protocol": "iotdbfs-fixed-matrix-raw-preparation/1", "complete": True,
            "elapsed_ns": time.monotonic_ns() - started, "manifest_sha256": _sha(root / "manifest.json"),
            "cost_boundary": "raw generation/copy, full native inventory/oracle and file hashes; not benchmark time"})
        (root / "manifest.json").chmod(0o444)
        (root / "preparation.json").chmod(0o444)
        return manifest
    except BaseException as error:
        _write(root / "FAILED.json", {"complete": False, "error_type": type(error).__name__,
            "error": str(error), "elapsed_ns": time.monotonic_ns() - started})
        raise
