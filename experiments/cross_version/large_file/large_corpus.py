#!/usr/bin/env python3
"""Large, fully allocated scientific files with measured fixed-block reuse.

This is a separate experiment, not an extension of the frozen small-file spec.
No padding, links, sparse datasets, or production-format changes are introduced.
"""
from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
from pathlib import Path
import shutil
import time

from experiments.cross_version.pilot import corpus
from common import TARGET, load_manifest, publication, seal, sha_file, write_json
import manager
import grib_data
import h5py
import numpy as np

PROTOCOL = "iotdbfs-large-version-data/1"
BLOCK = 1024 * 1024
SCENARIOS = ("identical", "single_unit")
FORMATS = ("hdf5", "grib")


def generate_grib(spec, root):
    """Global regular grid, with dimensions specified explicitly by the fixture."""
    from common import generated_attributes, identity, relative_file
    codes, sources = grib_data.Codes(), []
    ni, nj = spec["payload"]["Ni"], spec["payload"]["Nj"]
    with publication(root) as stage:
        for index in range(8):
            relative = relative_file(spec, index)
            path = stage / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("xb") as output:
                for slot in range(4):
                    attrs = generated_attributes(index < 2 and slot == 0, index * 4 + slot)
                    with codes.handle() as handle:
                        settings = {"Ni": ni, "Nj": nj,
                            "latitudeOfFirstGridPointInDegrees": -90.0,
                            "latitudeOfLastGridPointInDegrees": 90.0,
                            "longitudeOfFirstGridPointInDegrees": 0.0,
                            "longitudeOfLastGridPointInDegrees": 360.0 - 360.0 / ni,
                            "iDirectionIncrementInDegrees": 360.0 / ni, "jDirectionIncrementInDegrees": 180.0 / (nj - 1),
                            "jScansPositively": 1, "iScansNegatively": 0,
                            "jPointsAreConsecutive": 0, "alternativeRowScanning": 0,
                            "typeOfLevel": attrs["typeOfLevel"], "level": attrs["level"],
                            "shortName": attrs["shortName"], "dataDate": TARGET["validityDate"],
                            "dataTime": TARGET["validityTime"], "step": attrs["step"],
                            "packingType": "grid_simple", "bitsPerValue": 16}
                        for name, value in settings.items():
                            codes.set(handle, name, value)
                        seed = int(identity({"seed": spec["seed"], "file": index, "slot": slot})[:32], 16)
                        rng = np.random.Generator(np.random.PCG64(seed))
                        values = rng.integers(0, 65536, size=ni * nj).astype(np.float64) / 128 + 100
                        codes.check(codes.lib.codes_set_double_array(handle, b"values",
                            values.ctypes.data_as(ctypes.POINTER(ctypes.c_double)), len(values)), "large values")
                        output.write(codes.encoded(handle))
            inspected = grib_data.inspect(path)
            inspected["controlled"] = True
            sources.append(manager.file_record(path, inspected, relative, {"kind": "generated-global-grid"}))
        manifest = manager.new_manifest("generated", "grib", TARGET, sources, spec=spec,
            tree={k: spec[k] for k in ("depth", "seed", "tree_width")},
            provenance={"classification": "synthetic-controlled; global regular latitude/longitude grid"},
            timing={"generation_and_hash": 0})
        manager.finish(stage, manifest)


def require(value, message):
    if not value:
        raise RuntimeError(message)


def blocks(path):
    result = []
    with Path(path).open("rb") as stream:
        for value in iter(lambda: stream.read(BLOCK), b""):
            result.append((hashlib.sha256(value).hexdigest(), len(value)))
    return result


def resolve(root, manifest, entry):
    return corpus.resolve(root, manifest, entry)


def mutate(source, target, fmt, previous, ordinal):
    slot = (ordinal - 2) % 4
    if fmt == "hdf5":
        shutil.copyfile(source, target)
        with h5py.File(target, "r+") as file:
            dataset = file[previous[slot]["dataset"]]
            point = (0,) * dataset.ndim
            word = np.asarray([dataset[point]], dtype="<f4")
            word.view("<u4")[0] ^= np.uint32(ordinal - 1)
            dataset[point] = word[0]
    else:
        codes = grib_data.Codes()
        with Path(source).open("rb") as raw, Path(target).open("xb") as output:
            for index, unit in enumerate(previous):
                raw.seek(unit["offset"])
                encoded = raw.read(unit["length"])
                require(hashlib.sha256(encoded).hexdigest() == unit["unit_id"], "GRIB source changed")
                if index == slot:
                    with codes.handle(encoded) as handle:
                        values = codes.array(handle, "values")
                        # Preserve the minimum, maximum, binary scale and length.
                        candidates = np.flatnonzero((values > values.min() + 1) & (values < values.max() - 1))
                        require(len(candidates) > 0, "no interior grid value")
                        point = int(candidates[len(candidates) // 2])
                        values[point] += 1 / 128
                        values = np.ascontiguousarray(values, dtype=np.float64)
                        codes.check(codes.lib.codes_set_double_array(handle, b"values",
                            values.ctypes.data_as(ctypes.POINTER(ctypes.c_double)), len(values)), "one-point mutation")
                        changed = codes.encoded(handle)
                        require(len(changed) == len(encoded), "GRIB mutation shifted following messages")
                        encoded = changed
                output.write(encoded)
    current = corpus.inspect(target, fmt)
    corpus.check_transition(previous, current, fmt, slot)
    for old, new in zip(previous, current):
        if fmt == "grib":
            require((old["offset"], old["length"]) == (new["offset"], new["length"]), "GRIB layout moved")
    return current


def prepare(output):
    output = Path(output).resolve()
    require(not output.exists(), "new corpus path required")
    started = time.perf_counter_ns()
    with publication(output) as stage:
        bases, entries = {}, []
        for fmt in FORMATS:
            payload = ({"shape": [1, 1, 1024, 2048], "chunks": [1, 1, 256, 256],
                        "gzip_level": 1, "shuffle": False} if fmt == "hdf5" else
                       {"Ni": 1440, "Nj": 721, "bits_per_value": 16, "packing": "grid_simple"})
            spec = {"protocol": "iotdbfs-multifile-spec/1", "format": fmt, "N": 8, "K": 2,
                "D": 4, "depth": 2, "seed": 20261013, "tree_width": 4, "payload": payload,
                "query": TARGET, "max_output_bytes": 512 * BLOCK}
            root = stage / "bases" / (fmt + "-data")
            (manager.generate(spec, root) if fmt == "hdf5" else generate_grib(spec, root))
            base = load_manifest(root)
            bases[fmt] = {"root": str(output / "bases" / (fmt + "-data")),
                          "manifest_sha256": base["manifest_sha256"]}
            for scenario in SCENARIOS:
                for index, source in enumerate(base["sources"]):
                    previous = root / source["file"]
                    units = corpus.clean_units(source["units"])
                    for ordinal in range(1, 9):
                        locator = {"kind": "base", "file": source["file"]}
                        if scenario == "single_unit" and ordinal > 1:
                            relative = f"objects/{fmt}/v{ordinal:03d}/{source['file']}"
                            target = stage / relative
                            target.parent.mkdir(parents=True, exist_ok=True)
                            units = mutate(previous, target, fmt, units, ordinal)
                            previous = target
                            locator = {"kind": "local", "file": relative}
                        entries.append({"format": fmt, "scenario": scenario, "file_index": index,
                            "ordinal": ordinal, "logical_file": source["file"], "blob": locator,
                            "bytes": previous.stat().st_size, "sha256": sha_file(previous), "units": units,
                            "blocks": blocks(previous)})
        manifest = seal({"protocol": PROTOCOL, "bases": bases, "entries": entries,
            "controls": {"N": 8, "K": 2, "D": 4, "depth": 2, "seed": 20261013,
                         "tree_width": 4, "query": TARGET},
            "versions": 8, "block_bytes": BLOCK, "generation_ns": time.perf_counter_ns() - started,
            "mutation": "cumulative one-point change; slot=(ordinal-2)%4; HDF5 low mantissa XOR; GRIB interior value +1/128; no padding"})
        write_json(stage / "manifest.json", manifest)
    return verify(output)


def load(root, protocol=PROTOCOL):
    manifest = json.loads((Path(root) / "manifest.json").read_text())
    require(manifest.get("protocol") == protocol and seal(manifest) == manifest, "large manifest changed")
    return manifest


def verify(root, formats=FORMATS, protocol=PROTOCOL):
    root = Path(root).resolve()
    manifest = load(root, protocol)
    expected = {(fmt, scenario, index, ordinal) for fmt in formats for scenario in SCENARIOS
                for index in range(8) for ordinal in range(1, 9)}
    observed, checked, previous, layout = set(), {}, {}, {}
    for fmt in formats:
        base_root = Path(manifest["bases"][fmt]["root"])
        require(base_root == root / "bases" / (fmt + "-data"), "base locator differs")
        manager.verify(base_root)
        base = load_manifest(base_root)
        require(base["manifest_sha256"] == manifest["bases"][fmt]["manifest_sha256"], "base manifest changed")
        layout[fmt] = base
    for entry in manifest["entries"]:
        fmt, scenario, index, ordinal = [entry[k] for k in ("format", "scenario", "file_index", "ordinal")]
        key = fmt, scenario, index, ordinal
        require(key in expected and key not in observed, "unexpected/duplicate version")
        observed.add(key)
        source = layout[fmt]["sources"][index]
        require(entry["logical_file"] == source["file"], "logical file changed")
        local = scenario == "single_unit" and ordinal > 1
        wanted = {"kind": "local", "file": f"objects/{fmt}/v{ordinal:03d}/{source['file']}"} if local else {"kind": "base", "file": source["file"]}
        require(entry["blob"] == wanted, "unexpected locator")
        path = resolve(root, manifest, entry)
        require(not path.is_symlink() and path.stat().st_nlink == 1, "raw must be independent regular file")
        if path not in checked:
            checked[path] = (path.stat().st_size, sha_file(path), corpus.inspect(path, fmt), blocks(path))
        size, digest, units, chunks = checked[path]
        require([size, digest, units, chunks] == [entry["bytes"], entry["sha256"], entry["units"],
                [tuple(b) for b in entry["blocks"]]], "raw/native/block oracle differs")
        sequence = fmt, scenario, index
        if local:
            corpus.check_transition(previous[sequence], units, fmt, (ordinal - 2) % 4)
        else:
            require(digest == source["sha256"] and units == corpus.clean_units(source["units"]), "base reference differs")
        previous[sequence] = units
    require(observed == expected, "large version matrix incomplete")
    actual = {p for p in (root / "objects").rglob("*") if p.is_file()}
    require(actual == {p for p in checked if p.is_relative_to(root / "objects")}, "unregistered raw files")
    stats = []
    for fmt in formats:
        for scenario in SCENARIOS:
            for versions in (1, 8):
                entries = [e for e in manifest["entries"] if e["format"] == fmt and e["scenario"] == scenario and e["ordinal"] <= versions]
                unique = {tuple(b) for e in entries for b in e["blocks"]}
                logical = sum(e["bytes"] for e in entries)
                unique_bytes = sum(b[1] for b in unique)
                stats.append({"format": fmt, "scenario": scenario, "V": versions,
                    "logical_bytes": logical, "unique_fixed_block_bytes": unique_bytes,
                    "saving_fraction": 1 - unique_bytes / logical,
                    "file_min_bytes": min(e["bytes"] for e in entries), "file_max_bytes": max(e["bytes"] for e in entries)})
    return {"complete": True, "version_bindings": len(observed), "unique_raw_files": len(checked),
        "raw_file_bytes": sum(v[0] for v in checked.values()), "manifest_sha256": manifest["manifest_sha256"], "storage": stats}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "verify"))
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    print(json.dumps((prepare if args.command == "prepare" else verify)(args.output)), flush=True)
