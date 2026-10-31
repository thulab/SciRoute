"""Controlled dense HDF5 arrays and native physical-value verification."""
from __future__ import annotations

import hashlib
import itertools
import math

import h5py
import numpy as np

from common import TARGET, identity, generated_attributes

CONTROLLED = "iotdbfs-multifile-controlled-hdf5/1"


def environment():
    if h5py.version.hdf5_version != "1.14.4":
        raise ValueError(f"HDF5 1.14.4 required; found {h5py.version.hdf5_version}")
    return {"h5py": h5py.__version__, "hdf5": h5py.version.hdf5_version,
            "numpy": np.__version__}


def scalar_attributes(dataset):
    result = {}
    for name in dataset.attrs:
        attr = dataset.attrs.get_id(name)
        # Inspect types before H5Aread: no references, VLEN, compounds or large arrays.
        if (attr.shape != () or attr.dtype.kind not in "iufS" or
                attr.dtype.itemsize > 1024):
            continue
        value = dataset.attrs[name]
        if isinstance(value, (bytes, np.bytes_)):
            value = bytes(value).decode("utf-8")
        elif isinstance(value, np.generic):
            value = value.item()
        if type(value) in (str, int, float) and not (type(value) is float and not math.isfinite(value)):
            result[name] = value
    return result


def value_oracle(dataset):
    """Hash physical values in C order with at most 1 MiB of row data per read."""
    if dataset.dtype.kind not in "iuf" or dataset.dtype.itemsize > 8:
        raise ValueError("only fixed primitive numeric datasets are supported")
    if dataset.is_virtual or dataset.external:
        raise ValueError("external/VDS datasets are not standalone experiment inputs")
    dtype = dataset.dtype.newbyteorder("<")
    digest = hashlib.sha256()
    if not dataset.shape:
        digest.update(np.asarray(dataset[()], dtype=dtype).tobytes())
    elif dataset.ndim == 1:
        block = max(1, 1048576 // dtype.itemsize)
        for start in range(0, dataset.shape[0], block):
            digest.update(np.asarray(dataset[start:start + block], dtype=dtype).tobytes(order="C"))
    else:
        if dataset.shape[-1] * dtype.itemsize > 1048576:
            # Wide rows are streamed in column blocks, preserving C order.
            for prefix in np.ndindex(dataset.shape[:-1]):
                block = max(1, 1048576 // dtype.itemsize)
                for start in range(0, dataset.shape[-1], block):
                    digest.update(np.asarray(dataset[prefix + (slice(start, start + block),)], dtype=dtype).tobytes())
        else:
            rows = max(1, 1048576 // max(1, dataset.shape[-1] * dtype.itemsize))
            for prefix in np.ndindex(dataset.shape[:-2]):
                for start in range(0, dataset.shape[-2], rows):
                    part = dataset[prefix + (slice(start, start + rows), slice(None))]
                    digest.update(np.asarray(part, dtype=dtype).tobytes(order="C"))
    return {"physical_dtype": dtype.str, "elements": math.prod(dataset.shape),
            "value_sha256": digest.hexdigest(), "order": "C",
            "semantics": "physical values; no CF scaling or missing-value transform"}


def describe(dataset):
    attributes = scalar_attributes(dataset)
    oracle = value_oracle(dataset)
    schema = {"shape": list(dataset.shape), "dtype": dataset.dtype.str,
              "chunks": list(dataset.chunks) if dataset.chunks else None,
              "compression": dataset.compression, "compression_opts": dataset.compression_opts,
              "shuffle": dataset.shuffle, "fletcher32": dataset.fletcher32}
    return {"unit_id": identity({"schema": schema, "attributes": attributes, "oracle": oracle}),
            "dataset": dataset.name, "attributes": attributes, "schema": schema,
            "oracle": oracle,
            "allocated_chunks": dataset.id.get_num_chunks() if dataset.chunks else None,
            "stored_bytes": dataset.id.get_storage_size()}


def inspect(path, selected=None):
    environment()
    with h5py.File(path, "r") as file:
        controlled = file.attrs.get("controlled_protocol", b"") == CONTROLLED.encode()
        names = []
        file.visititems(lambda name, obj: names.append("/" + name) if isinstance(obj, h5py.Dataset) else None)
        if selected is None and not controlled:
            raise ValueError("public HDF5 inventory requires an explicit --dataset allowlist")
        chosen = sorted(names if selected is None else selected)
        if not chosen or len(set(chosen)) != len(chosen) or any(name not in names for name in chosen):
            raise ValueError("dataset allowlist is empty, duplicated or absent in source")
        return {"controlled": bool(controlled), "total_dataset_count": len(names),
                "units": [describe(file[name]) for name in chosen]}


def create_file(path, spec, file_index):
    environment()
    if spec["query"] != TARGET:
        raise ValueError("controlled generation uses the frozen target predicate; regroup may use other predicates")
    p = spec["payload"]
    shape, chunks = tuple(p["shape"]), tuple(p["chunks"])
    units = []
    with h5py.File(path, "x", rdcc_nbytes=2 * 1048576, rdcc_nslots=257) as file:
        file.attrs["controlled_protocol"] = np.bytes_(CONTROLLED)
        for slot in range(spec["D"]):
            dataset = file.create_dataset(f"/data/u{slot:04d}", shape=shape, chunks=chunks,
                dtype="<f4", compression="gzip", compression_opts=p["gzip_level"],
                shuffle=p["shuffle"], track_times=False)
            attributes = generated_attributes(file_index < spec["K"] and slot == 0, file_index * spec["D"] + slot)
            for name, value in attributes.items():
                dataset.attrs[name] = np.bytes_(value) if isinstance(value, str) else np.int64(value)
            seed = int(identity({"seed": spec["seed"], "file": file_index, "slot": slot})[:32], 16)
            rng = np.random.Generator(np.random.PCG64(seed))
            starts = [range(0, n, c) for n, c in zip(shape, chunks)]
            for origin in itertools.product(*starts):
                block_shape = tuple(min(c, n - o) for o, n, c in zip(origin, shape, chunks))
                words = rng.integers(0, 1 << 32, size=block_shape, dtype=np.uint32)
                special = (words & np.uint32(0x7F800000)) == np.uint32(0x7F800000)
                words[special] ^= np.uint32(0x00800000)
                values = words.astype("<u4", copy=False).view("<f4")
                dataset[tuple(slice(o, o + n) for o, n in zip(origin, block_shape))] = values
            expected = math.prod(math.ceil(n / c) for n, c in zip(shape, chunks))
            file.flush()
            if dataset.id.get_num_chunks() != expected:
                raise ValueError("generated dataset is not fully allocated")
            unit = describe(dataset)
            unit["lineage"] = {"kind": "generated", "file_index": file_index, "slot": slot,
                               "seed": spec["seed"]}
            units.append(unit)
    return {"controlled": True, "total_dataset_count": spec["D"], "units": units}


def copy_units(path, chosen, resolver):
    environment()
    with h5py.File(path, "x") as output:
        output.attrs["controlled_protocol"] = np.bytes_(CONTROLLED)
        group = output.create_group("data")
        for slot, (source, unit) in enumerate(chosen):
            if not source["controlled"]:
                raise ValueError("public HDF5 dataset regrouping is outside the first-version contract")
            with h5py.File(resolver(source), "r") as raw:
                if raw.attrs.get("controlled_protocol", b"") != CONTROLLED.encode():
                    raise ValueError("source is not tagged as controlled HDF5")
                dataset = raw[unit["dataset"]]
                for name in dataset.attrs:
                    dtype = dataset.attrs.get_id(name).dtype
                    if dtype.kind not in "iufS":
                        raise ValueError("dataset has references/VLEN or compound attributes; cannot regroup")
                raw.copy(dataset, group, name=f"u{slot:04d}")
    result = inspect(path)
    for actual, (_, old) in zip(result["units"], chosen):
        if actual["unit_id"] != old["unit_id"]:
            raise ValueError("HDF5 regrouping changed values, filters or attributes")
    return result
