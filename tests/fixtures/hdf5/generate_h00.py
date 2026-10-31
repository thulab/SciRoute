#!/usr/bin/env python3
"""Generate bounded, deterministic H00 HDF5 fixtures and independent oracles."""
from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
from pathlib import Path
import platform
import sys

import h5py
import numpy as np

SCHEMA = "iotdbfs-hdf5-h00-fixtures/1"
SHAPES = {1: (37,), 2: (19, 11), 3: (19, 11, 7)}
CHUNKS = {1: (8,), 2: (8, 6), 3: (8, 6, 4)}


def recipe(shape, *, fill=None, written=None):
    result = {"kind": "axis_affine", "offset": -3.25,
              "coefficients": [17 ** i / 8 for i in range(len(shape))]}
    if fill is not None:
        result["fill"] = fill
        result["written_boxes"] = written or []
    return result


def formula_array(shape, dtype, spec):
    """Writer formula: dense coordinate grid, evaluated in float64."""
    grid = np.indices(shape, dtype=np.float64)
    values = np.full(shape, spec["offset"], dtype=np.float64)
    for axis, coefficient in enumerate(spec["coefficients"]):
        values += grid[axis] * coefficient
    return values.astype(dtype)


def expected_selection(spec, start, count, dtype):
    """Independent selection oracle: sparse global axes, no HDF5 reads."""
    result = np.full(count, spec["offset"], dtype=np.float64)
    coordinates = []
    for axis, (begin, length, coefficient) in enumerate(
            zip(start, count, spec["coefficients"])):
        dimensions = [1] * len(count)
        dimensions[axis] = length
        coordinate = np.arange(begin, begin + length, dtype=np.float64).reshape(dimensions)
        coordinates.append(coordinate)
        result += coordinate * coefficient
    if "fill" in spec:
        written = np.zeros(count, dtype=bool)
        for box in spec["written_boxes"]:
            inside = np.ones(count, dtype=bool)
            for axis, coordinate in enumerate(coordinates):
                inside &= ((coordinate >= box["start"][axis]) &
                           (coordinate < box["start"][axis] + box["count"][axis]))
            written |= inside
        result[~written] = spec["fill"]
    return result.astype(np.dtype(dtype).newbyteorder("<"))


def digest(values):
    array = np.asarray(values)
    canonical = array.astype(array.dtype.newbyteorder("<"), copy=False)
    return hashlib.sha256(canonical.tobytes(order="C")).hexdigest()


def create_file(path, userblock=0):
    fcpl = h5py.h5p.create(h5py.h5p.FILE_CREATE)
    fcpl.set_obj_track_times(False)
    if userblock:
        fcpl.set_userblock(userblock)
    return h5py.File(h5py.h5f.create(bytes(path), h5py.h5f.ACC_TRUNC, fcpl=fcpl))


def undefined_fill(dcpl, datatype):
    # h5py's high-level setter cannot pass a NULL fill-value pointer. Resolve
    # this one public C API from the already loaded h5py extension, not from a
    # separately discovered HDF5 library with a potentially different ID table.
    library = ctypes.CDLL(h5py.h5p.__file__)
    setter = library.H5Pset_fill_value
    setter.argtypes = (ctypes.c_longlong, ctypes.c_longlong, ctypes.c_void_p)
    setter.restype = ctypes.c_int
    if setter(dcpl.id, datatype.id, None) < 0:
        raise RuntimeError("H5Pset_fill_value(NULL) failed")
    if dcpl.fill_value_defined() != h5py.h5d.FILL_VALUE_UNDEFINED:
        raise RuntimeError("undefined fill fixture was not created")


def lowlevel_dataset(file, shape, dtype, *, chunks=None, fill="default",
                     compact=False, null=False):
    dcpl = h5py.h5p.create(h5py.h5p.DATASET_CREATE)
    dcpl.set_obj_track_times(False)
    datatype = h5py.h5t.py_create(np.dtype(dtype))
    if chunks:
        dcpl.set_chunk(chunks)
    if compact:
        dcpl.set_layout(h5py.h5d.COMPACT)
    if fill == "undefined":
        undefined_fill(dcpl, datatype)
        dcpl.set_fill_time(h5py.h5d.FILL_TIME_NEVER)
    elif fill != "default":
        dcpl.set_fill_value(np.asarray(fill, dtype=dtype))
    space = h5py.h5s.create(h5py.h5s.NULL) if null else h5py.h5s.create_simple(shape)
    return h5py.Dataset(h5py.h5d.create(file.id, b"data", datatype, space, dcpl=dcpl))


def selections(shape):
    whole = ([0] * len(shape), list(shape))
    if not all(shape):
        return [("empty", *whole)]
    interior = ([min(3, size - 1) for size in shape],
                [min(9, max(1, size - 3)) for size in shape])
    edge = ([max(0, size - 3) for size in shape], [min(size, 3) for size in shape])
    return [("whole", *whole), ("interior", *interior), ("edge", *edge)]


def generate(output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    manifest = {"schema_version": SCHEMA, "digest_encoding": "C-order little-endian IEEE values",
                "environment": {"python": platform.python_version(), "numpy": np.__version__,
                                "h5py": h5py.__version__, "hdf5": h5py.version.hdf5_version,
                                "byteorder": sys.byteorder}, "files": []}

    def add(identity, shape, dtype, writer, *, supported=True, reason=None,
            spec=None, dataset="/data", userblock=0, profile=None):
        path = output / (identity + ".h5")
        with create_file(path, userblock) as file:
            writer(file)
        # Userblock bytes are deterministic and intentionally non-HDF5.
        if userblock:
            with path.open("r+b") as stream:
                header = b"IoTDB-FS H00 userblock\n"
                stream.write(header + bytes(userblock - len(header)))
        entry = {"id": identity, "path": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                 "size_bytes": path.stat().st_size, "support": supported, "reason": reason,
                 "profile": profile or identity, "datasets": [{"path": dataset,
                    "shape": list(shape) if shape is not None else None, "dtype": dtype,
                    "recipe": spec}], "selections": []}
        tests = selections(shape) if shape else [("unsupported", [], [])]
        for name, start, count in tests:
            expected = None
            if supported and spec is not None:
                values = expected_selection(spec, start, count, dtype)
                expected = {"shape": list(count), "dtype": np.dtype(dtype).newbyteorder("<").str,
                            "sha256": digest(values)}
            entry["selections"].append({"id": name, "dataset": dataset,
                "start": start, "count": count, "support": supported,
                "reason": reason, "expected": expected})
        manifest["files"].append(entry)
        return entry

    for rank, shape in SHAPES.items():
        spec = recipe(shape)
        for dtype in ("<f4", ">f4", "<f8", ">f8"):
            for layout in ("contiguous", "chunked"):
                options = {"chunks": CHUNKS[rank]} if layout == "chunked" else {}
                def write(file, s=shape, d=dtype, r=spec, opts=options):
                    file.create_dataset("data", data=formula_array(s, d, r),
                                        dtype=d, track_times=False, **opts)
                add(f"rank{rank}_{'le' if dtype[0] == '<' else 'be'}_f{np.dtype(dtype).itemsize * 8}_{layout}",
                    shape, dtype, write, spec=spec)

    shape, dtype, chunks = SHAPES[3], "<f4", CHUNKS[3]
    spec = recipe(shape)
    filters = {"deflate": {"compression": "gzip", "compression_opts": 6},
               "shuffle": {"shuffle": True}, "fletcher32": {"fletcher32": True},
               "deflate_shuffle": {"compression": "gzip", "shuffle": True},
               "deflate_fletcher32": {"compression": "gzip", "fletcher32": True},
               "shuffle_fletcher32": {"shuffle": True, "fletcher32": True},
               "all_filters": {"compression": "gzip", "shuffle": True, "fletcher32": True}}
    for name, options in filters.items():
        add(name, shape, dtype, lambda file, opts=options: file.create_dataset(
            "data", data=formula_array(shape, dtype, spec), chunks=chunks,
            track_times=False, **opts), spec=spec)

    def skipped(file):
        dset = file.create_dataset("data", data=formula_array(shape, dtype, spec),
            chunks=chunks, compression="gzip", shuffle=True, fletcher32=True, track_times=False)
        block = formula_array(chunks, dtype, spec)
        dset.id.write_direct_chunk((0, 0, 0), block.tobytes(), filter_mask=7)
    add("skipped_all_filters", shape, dtype, skipped, spec=spec)

    def corrupt_checksum(file):
        dset = file.create_dataset("data", data=formula_array((37,), "<f4", recipe((37,))),
                                  chunks=(37,), fletcher32=True, track_times=False)
        mask, raw = dset.id.read_direct_chunk((0,))
        damaged = bytearray(raw)
        damaged[0] ^= 1  # Change data while retaining the old Fletcher checksum.
        dset.id.write_direct_chunk((0,), bytes(damaged), filter_mask=mask)
    entry = add("corrupt_fletcher32", (37,), "<f4", corrupt_checksum,
                supported=False, reason="checksum_mismatch")
    entry["expected_status"] = "corrupt"
    for selection in entry["selections"]:
        selection["expected_status"] = "corrupt"

    for layout in ("chunked", "contiguous"):
        for fill in ("default", -7.25, "undefined"):
            shape2 = SHAPES[2]
            chunk2 = CHUNKS[2] if layout == "chunked" else None
            written = [{"start": [0, 0], "count": list(CHUNKS[2])}] if chunk2 else []
            spec2 = recipe(shape2, fill=0.0 if fill == "default" else fill, written=written)
            def write_fill(file, fill=fill, chunk2=chunk2, s=shape2):
                dset = lowlevel_dataset(file, s, "<f4", chunks=chunk2, fill=fill)
                if chunk2:
                    dset[:chunk2[0], :chunk2[1]] = formula_array(chunk2, "<f4", recipe(s))
            label = fill if isinstance(fill, str) else "explicit"
            entry = add(f"{layout}_unallocated_{label}_fill", shape2, "<f4", write_fill,
                spec=spec2 if fill != "undefined" else None, supported=fill != "undefined",
                reason="undefined_fill" if fill == "undefined" else None)
            if fill == "undefined" and chunk2:
                values = expected_selection(recipe(shape2), [0, 0], list(chunk2), "<f4")
                entry["selections"].append({"id": "allocated_chunk", "dataset": "/data",
                    "start": [0, 0], "count": list(chunk2), "support": True, "reason": None,
                    "expected": {"shape": list(chunk2), "dtype": "<f4", "sha256": digest(values)}})

    for block in (512, 4096):
        add(f"userblock_{block}", SHAPES[2], ">f8", lambda file: file.create_dataset(
            "data", data=formula_array(SHAPES[2], ">f8", recipe(SHAPES[2])), track_times=False),
            spec=recipe(SHAPES[2]), userblock=block)
        add(f"userblock_{block}_chunked_deflate_shuffle", SHAPES[3], ">f8",
            lambda file: file.create_dataset("data",
                data=formula_array(SHAPES[3], ">f8", recipe(SHAPES[3])),
                chunks=CHUNKS[3], compression="gzip", shuffle=True, track_times=False),
            spec=recipe(SHAPES[3]), userblock=block)

    shape_a, shape_b = (5, 7, 9), (11, 5)
    recipe_a, recipe_b = recipe(shape_a), recipe(shape_b)
    recipe_b["offset"] = 2048.5  # Distinguish datasets in both shape and values.
    def minimal_slice(file):
        file.create_dataset("a", data=formula_array(shape_a, "<f4", recipe_a),
                            chunks=(2, 3, 4), track_times=False)
        file.create_dataset("b", data=formula_array(shape_b, ">f8", recipe_b),
                            chunks=(3, 4), track_times=False)
    entry = add("minimal_slice", shape_a, "<f4", minimal_slice, spec=recipe_a, dataset="/a")
    entry["datasets"].append({"path": "/b", "shape": list(shape_b), "dtype": ">f8", "recipe": recipe_b})
    start, count = [1, 2, 3], [4, 5, 6]
    values = expected_selection(recipe_a, start, count, "<f4")
    entry["selections"].append({"id": "exact_minimal_slice", "dataset": "/a",
        "start": start, "count": count, "support": True, "reason": None,
        "expected": {"shape": count, "dtype": "<f4", "sha256": digest(values)}})
    for selection in entry["selections"]:
        selection["excluded_datasets"] = ["/b"]

    def links(file):
        dset = file.create_dataset("data", data=formula_array(SHAPES[2], "<f4", recipe(SHAPES[2])),
                                  chunks=CHUNKS[2], track_times=False)
        file["alias"] = dset
        gcpl = h5py.h5p.create(h5py.h5p.GROUP_CREATE)
        gcpl.set_obj_track_times(False)
        group = h5py.Group(h5py.h5g.create(file.id, b"group", gcpl=gcpl))
        group["self"] = group
        group["data_alias"] = dset
    entry = add("hardlink_alias_cycle", SHAPES[2], "<f4", links, spec=recipe(SHAPES[2]))
    for alias in ("/alias", "/group/data_alias", "/group/self/data_alias"):
        extra = dict(entry["selections"][1])
        extra.update(id="alias_" + alias.replace("/", "_"), dataset=alias)
        entry["selections"].append(extra)

    add("empty_extent", (0, 7), "<f4", lambda file: file.create_dataset(
        "data", shape=(0, 7), maxshape=(None, 7), chunks=(4, 7), dtype="<f4", track_times=False),
        spec=recipe((0, 7)))

    def reject(name, shape, dtype, writer, reason, dataset="/data"):
        add(name, shape, dtype, writer, supported=False, reason=reason, dataset=dataset)
    reject("scalar", (), "<f4", lambda file: file.create_dataset("data", data=np.float32(2),
        track_times=False), "unsupported_rank")
    reject("null_dataspace", None, "<f4", lambda file: lowlevel_dataset(file, (), "<f4", null=True),
           "null_dataspace")
    def compact(file):
        dset = lowlevel_dataset(file, (7,), "<f4", compact=True)
        dset[:] = np.arange(7, dtype="<f4")
    reject("compact", (7,), "<f4", compact, "compact_layout")
    reject("integer", (7,), "<i4", lambda file: file.create_dataset("data", data=np.arange(7, dtype="<i4"),
        track_times=False), "unsupported_datatype")
    def vlen(file):
        dset = file.create_dataset("data", (2,), dtype=h5py.vlen_dtype(np.dtype("<f4")), track_times=False)
        dset[0], dset[1] = np.arange(2, dtype="<f4"), np.arange(3, dtype="<f4")
    reject("vlen", (2,), "vlen_float32", vlen, "unsupported_datatype")
    reject("external_storage", (7,), "<f4", lambda file: file.create_dataset("data", (7,), dtype="<f4",
        external=[("not_present.raw", 0, h5py.h5f.UNLIMITED)], track_times=False), "external_storage")
    def soft(file):
        file.create_dataset("target", data=np.arange(7, dtype="<f4"), track_times=False)
        file["data"] = h5py.SoftLink("/target")
    reject("soft_link", (7,), "<f4", soft, "soft_link")
    def external_link(file):
        file["data"] = h5py.ExternalLink("not_present.h5", "/data")
    reject("external_link", (7,), "<f4", external_link, "external_link")
    def virtual(file):
        layout = h5py.VirtualLayout(shape=(7,), dtype="<f4")
        layout[:] = h5py.VirtualSource("not_present.h5", "/data", shape=(7,))
        layout.dcpl.set_obj_track_times(False)
        file.create_virtual_dataset("data", layout, fillvalue=-1)
    reject("virtual_dataset", (7,), "<f4", virtual, "virtual_layout")

    manifest["total_bytes"] = sum(item["size_bytes"] for item in manifest["files"])
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = generate(args.output)
    print(json.dumps({"manifest": str(args.output / "manifest.json"),
                      "files": len(result["files"]), "total_bytes": result["total_bytes"]}))
