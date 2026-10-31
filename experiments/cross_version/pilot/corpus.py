#!/usr/bin/env python3
"""Offline version sequences referencing the existing multi-file v1 corpus."""
from __future__ import annotations

import argparse
import ctypes
import json
from pathlib import Path
import shutil
import sys

MULTI = Path(__file__).resolve().parents[2] / "multi_file"
sys.path.insert(0, str(MULTI))
from common import (canonical, identity, integer, load_manifest, matches, publication,
                    safe_file, seal, sha_file, write_json)
import manager as multi
import grib_data
import hdf5_data
import h5py
import numpy as np

PROTOCOL = "iotdbfs-cross-version-data/1"
SCENARIOS = ["identical", "single_unit"]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_spec(path):
    return validate_spec(json.loads(Path(path).read_text()))


def validate_spec(spec):
    require(isinstance(spec, dict), "version spec must be an object")
    require(set(spec) == {"protocol", "versions", "scenarios", "max_new_raw_bytes"},
            "unknown/missing version spec fields")
    require(spec["protocol"] == "iotdbfs-cross-version-spec/1", "unsupported spec")
    integer(spec["versions"], "versions", 2, 32)
    integer(spec["max_new_raw_bytes"], "max_new_raw_bytes", 1, 1024**3)
    require(spec["scenarios"] == SCENARIOS, "pilot requires both frozen scenarios")
    return spec


def clean_units(units):
    return [{k: v for k, v in unit.items() if k != "lineage"} for unit in units]


def inspect(path, fmt):
    return clean_units((hdf5_data if fmt == "hdf5" else grib_data).inspect(path)["units"])


def mutate(source, output, fmt, units, ordinal):
    """Cumulative, rotating one-unit changes; never open a baseline for writing."""
    slot = (ordinal - 2) % len(units)
    if fmt == "hdf5":
        shutil.copyfile(source, output)
        with h5py.File(output, "r+") as file:
            dataset = file[units[slot]["dataset"]]
            index = (0,) * dataset.ndim
            value = np.asarray([dataset[index]], dtype="<f4")
            # Finite Float32 remains finite: only low mantissa bits change.
            value.view("<u4")[0] ^= np.uint32(ordinal - 1)
            dataset[index] = value[0]
    else:
        codes = grib_data.Codes()
        with Path(source).open("rb") as raw, Path(output).open("xb") as target:
            for i, unit in enumerate(units):
                raw.seek(unit["offset"])
                encoded = raw.read(unit["length"])
                require(len(encoded) == unit["length"] and
                        identity_bytes(encoded) == unit["unit_id"], "source message changed")
                if i == slot:
                    with codes.handle(encoded) as handle:
                        values = np.ascontiguousarray(-codes.array(handle, "values") + ordinal / 4,
                                                     dtype=np.float64)
                        codes.check(codes.lib.codes_set_double_array(handle, b"values",
                            values.ctypes.data_as(ctypes.POINTER(ctypes.c_double)),
                            len(values)), "mutate values")
                        encoded = codes.encoded(handle)
                target.write(encoded)
    actual = inspect(output, fmt)
    check_transition(units, actual, fmt, slot)
    return actual


def identity_bytes(value):
    import hashlib
    return hashlib.sha256(value).hexdigest()


def check_transition(previous, current, fmt, slot):
    require(len(previous) == len(current), "unit count changed")
    changed = [i for i, (old, new) in enumerate(zip(previous, current))
               if old["unit_id"] != new["unit_id"]]
    require(changed == [slot], f"expected only unit {slot} to change; found {changed}")
    for i, (old, new) in enumerate(zip(previous, current)):
        require(old["attributes"] == new["attributes"] and old["schema"] == new["schema"],
                "mutation changed attributes or physical schema")
        if i == slot:
            require(old["oracle"]["value_sha256"] != new["oracle"]["value_sha256"],
                    "mutation did not change native numerical values")
        else:
            require(old["oracle"] == new["oracle"], "unchanged unit oracle differs")
        if fmt == "grib":
            for key in ("latitudes_sha256", "longitudes_sha256", "mask_sha256"):
                require(old["oracle"][key] == new["oracle"][key], "GRIB grid/mask changed")


def load_bases(base_work):
    result = {}
    for fmt in ("hdf5", "grib"):
        root = Path(base_work).resolve() / (fmt + "-data")
        multi.verify(root)
        manifest = load_manifest(root)
        require(manifest["format"] == fmt and all(s["controlled"] for s in manifest["sources"]),
                "controlled, format-matched baseline required")
        require(all(manifest["spec"][k] == v for k, v in {"N": 8, "K": 2, "D": 4, "depth": 2}.items()),
                "pilot requires existing N8/K2/D4/depth2 baseline")
        result[fmt] = (root, manifest)
    require(all(result["grib"][1]["spec"][k] == result["hdf5"][1]["spec"][k]
                for k in ("N", "K", "D", "depth", "seed", "tree_width", "query")),
            "paired baseline controls differ")
    return result


def prepare(spec, base_work, output):
    spec = validate_spec(spec)
    bases = load_bases(base_work)
    new_bytes = 0
    entries = []
    with publication(output) as stage:
        for fmt, (base, manifest) in bases.items():
            for scenario in SCENARIOS:
                for index, source in enumerate(manifest["sources"]):
                    previous = safe_file(base, source["file"])
                    units = clean_units(source["units"])
                    for ordinal in range(1, spec["versions"] + 1):
                        locator = {"kind": "base", "file": source["file"]}
                        if scenario == "single_unit" and ordinal > 1:
                            relative = f"objects/{fmt}/v{ordinal:03d}/{source['file']}"
                            target = safe_file(stage, relative)
                            target.parent.mkdir(parents=True, exist_ok=True)
                            units = mutate(previous, target, fmt, units, ordinal)
                            previous = target
                            locator = {"kind": "local", "file": relative}
                            new_bytes += target.stat().st_size
                            require(new_bytes <= spec["max_new_raw_bytes"], "new raw byte budget exceeded")
                        entries.append({"format": fmt, "scenario": scenario, "file_index": index,
                            "ordinal": ordinal, "logical_file": source["file"], "blob": locator,
                            "bytes": previous.stat().st_size, "sha256": sha_file(previous),
                            "units": units})
        manifest = seal({"protocol": PROTOCOL, "spec": spec,
            "base_work": str(Path(base_work).resolve()),
            "bases": {fmt: {"root": str(base), "manifest_sha256": m["manifest_sha256"]}
                      for fmt, (base, m) in bases.items()},
            "controls": {k: bases["hdf5"][1]["spec"][k]
                         for k in ("N", "K", "D", "depth", "seed", "tree_width", "query")},
            "mutation": {"schedule": "cumulative; slot=(ordinal-2)%D",
                "hdf5": "XOR first Float32 low mantissa bits with ordinal-1",
                "grib": "native repack one message: -decoded_values + ordinal/4",
                "attributes_geometry_filters": "preserved; native re-read checked"},
            "new_raw_bytes": new_bytes, "logical_raw_bytes": sum(e["bytes"] for e in entries),
            "storage": "base files referenced without copies/links; only changed files stored locally",
            "entries": entries})
        write_json(stage / "manifest.json", manifest)
        verify(stage)
    return manifest


def load(root):
    path = Path(root) / "manifest.json"
    require(path.stat().st_size <= 64 * 1024**2, "version manifest exceeds bound")
    manifest = json.loads(path.read_text())
    require(manifest.get("protocol") == PROTOCOL and seal(manifest) == manifest,
            "version manifest fingerprint/protocol mismatch")
    return manifest


def resolve(root, manifest, entry):
    blob = entry["blob"]
    require(blob["kind"] in ("base", "local"), "unknown blob locator")
    base = manifest["bases"][entry["format"]]["root"] if blob["kind"] == "base" else root
    return safe_file(base, blob["file"])


def verify(root):
    root = Path(root).resolve()
    manifest = load(root)
    spec = validate_spec(manifest["spec"])
    bases = load_bases(manifest["base_work"])
    for fmt, (base, m) in bases.items():
        require(manifest["bases"][fmt] == {"root": str(base), "manifest_sha256": m["manifest_sha256"]},
                "baseline identity changed")
    expected = {(fmt, scenario, i, v) for fmt in bases for scenario in SCENARIOS
                for i in range(8) for v in range(1, spec["versions"] + 1)}
    entries = manifest["entries"]
    by_key = {(e["format"], e["scenario"], e["file_index"], e["ordinal"]): e for e in entries}
    require(len(by_key) == len(entries) and set(by_key) == expected, "version matrix differs")
    require(list(by_key) == [(fmt, scenario, i, v) for fmt in ("hdf5", "grib")
                            for scenario in SCENARIOS for i in range(8)
                            for v in range(1, spec["versions"] + 1)], "version publication order differs")
    require(manifest["controls"] == {k: bases["hdf5"][1]["spec"][k]
                                    for k in ("N", "K", "D", "depth", "seed", "tree_width", "query")},
            "version controls differ from baseline")
    inspected = {}
    locals_expected = set()
    for key in sorted(by_key):
        fmt, scenario, index, ordinal = key
        entry = by_key[key]
        base_source = bases[fmt][1]["sources"][index]
        require(entry["logical_file"] == base_source["file"], "logical path changed across versions")
        local = scenario == "single_unit" and ordinal > 1
        wanted_blob = {"kind": "local", "file": f"objects/{fmt}/v{ordinal:03d}/{base_source['file']}"} if local else {"kind": "base", "file": base_source["file"]}
        require(entry["blob"] == wanted_blob, "unexpected version storage locator")
        path = resolve(root, manifest, entry)
        if path not in inspected:
            inspected[path] = (path.stat().st_size, sha_file(path), inspect(path, fmt))
        size, digest, units = inspected[path]
        require((size, digest, units) == (entry["bytes"], entry["sha256"], entry["units"]),
                "version bytes/native oracle changed")
        if local:
            locals_expected.add(path.relative_to(root).as_posix())
            previous = by_key[(fmt, scenario, index, ordinal - 1)]
            check_transition(previous["units"], units, fmt, (ordinal - 2) % len(units))
        else:
            require(digest == base_source["sha256"] and units == clean_units(base_source["units"]),
                    "base reference differs")
        require(len(units) == 4 and sum(matches(u["attributes"], manifest["controls"]["query"])
                                      for u in units) == (1 if index < 2 else 0),
                "query match contract changed")
    actual_local = {p.relative_to(root).as_posix() for p in (root / "objects").rglob("*") if p.is_file()}
    require(actual_local == locals_expected, "extra or missing local objects")
    new_bytes = sum((root / p).stat().st_size for p in locals_expected)
    require(new_bytes == manifest["new_raw_bytes"] <= spec["max_new_raw_bytes"], "raw budget differs")
    require(sum(e["bytes"] for e in entries) == manifest["logical_raw_bytes"], "logical byte count differs")
    return {"verified": True, "version_bindings": len(entries), "local_files": len(locals_expected),
            "new_raw_bytes": new_bytes, "logical_raw_bytes": manifest["logical_raw_bytes"],
            "manifest_sha256": manifest["manifest_sha256"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("prepare")
    build.add_argument("--spec", type=Path, default=Path(__file__).parent / "specs/pilot.json")
    build.add_argument("--base-work", type=Path, required=True)
    build.add_argument("--output", type=Path, required=True)
    check = commands.add_parser("verify")
    check.add_argument("--input", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        manifest = prepare(read_spec(args.spec), args.base_work, args.output)
        result = {k: manifest[k] for k in ("manifest_sha256", "new_raw_bytes", "logical_raw_bytes")}
    else:
        result = verify(args.input)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
