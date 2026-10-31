#!/usr/bin/env python3
"""Offline controlled N/K/D/depth data manager. Never connects to IoTDB."""
from __future__ import annotations

import argparse
from collections import Counter
import importlib
import json
import os
from pathlib import Path
import platform
import shutil
import stat
import sys
import time
from urllib.parse import quote

from common import (MANIFEST_PROTOCOL, MAX_UNITS, TARGET, assert_dimensions,
    canonical, counts, estimated_payload_bytes, identity, integer, load_manifest,
    matches, publication, relative_file, seal, sha_file, source_path,
    validate_query, validate_spec, write_json)


def backend(format_id):
    if format_id not in ("hdf5", "grib"):
        raise ValueError("unsupported format")
    return importlib.import_module("hdf5_data" if format_id == "hdf5" else "grib_data")


def read_spec(path):
    return validate_spec(json.loads(Path(path).read_text()))


def query_template(format_id, query):
    names = {"shortName": "short_name", "typeOfLevel": "type_of_level",
             "level": "level", "step": "step", "validityDate": "validity_date",
             "validityTime": "validity_time"}
    predicates = []
    if format_id == "grib" and set(query) - set(names):
        raise ValueError("unsupported GRIB predicate key")
    for key, value in sorted(query.items()):
        field = ("hdf5.attr:" + quote(key, safe="") if format_id == "hdf5" else
                 "grib.message." + names[key])
        predicates.append({"kind": "compare", "field": field, "operator": "eq",
            "value": {"type": {str: "utf8", int: "int64", float: "float64"}[type(value)],
                      "value": value}})
    return {"kind": "dataset_catalog" if format_id == "hdf5" else "typed",
            "predicate": {"kind": "and", "operands": predicates},
            "source_binding": "bind uploaded manifest files to exact published versions before timing"}


def file_record(path, inspected, relative=None, origin=None):
    stat = Path(path).stat()
    record = {"bytes": stat.st_size, "sha256": sha_file(path), **inspected}
    record["file" if relative is not None else "external_path"] = relative if relative is not None else str(Path(path).resolve())
    if origin is not None:
        record["origin"] = origin
    return record


def check_budget(sources, maximum):
    size = sum(s["bytes"] for s in sources)
    if size > maximum:
        raise ValueError(f"actual raw output {size} exceeds max_output_bytes={maximum}")
    return size


def tree_stats(sources, tree):
    if tree is None:
        return None
    parents = Counter()
    directories = set()
    for source in sources:
        parts = Path(source["file"]).parts
        if parts[0] != "raw" or len(parts) - 2 != tree["depth"]:
            raise ValueError("actual path does not satisfy external depth")
        parents["/".join(parts[:-1])] += 1
        for end in range(2, len(parts)):
            directories.add("/".join(parts[:end]))
    return {**tree, "directory_count_below_raw": len(directories),
            "leaf_directories": len(parents), "max_leaf_files": max(parents.values()),
            "max_relative_path_bytes": max(len(s["file"].encode()) for s in sources),
            "organization": "opaque deterministic hash buckets; no semantic filename pruning claim"}


def new_manifest(kind, format_id, query, sources, *, spec=None, tree=None,
                 provenance=None, timing=None):
    actual = assert_dimensions(sources, spec) if spec else counts(sources, query)
    return {"protocol": MANIFEST_PROTOCOL, "kind": kind, "format": format_id,
            "spec": spec, "query": query, "query_template": query_template(format_id, query),
            "actual": actual, "tree": tree_stats(sources, tree),
            "raw_bytes": sum(s["bytes"] for s in sources), "sources": sources,
            "provenance": provenance or {}, "timings_ns": timing or {},
            "environment": {"python": platform.python_version(), **backend(format_id).environment()},
            "offline_only": True, "full_source_hashing_warms_os_pages": True,
            "timing_scope": "generation/inventory/regrouping and native verification only; exclude from online query timing"}


def validate_contents(root, manifest):
    b = backend(manifest["format"])
    sources = manifest["sources"]
    if not 1 <= len(sources) <= 4096 or sum(len(s["units"]) for s in sources) > MAX_UNITS:
        raise ValueError("manifest source/unit count exceeds bounds")
    paths = set()
    refreshed = []
    for source in sources:
        path = source_path(root, source)
        if not stat.S_ISREG(path.stat().st_mode):
            raise ValueError("source is not a regular file")
        resolved = str(path.resolve())
        if resolved in paths:
            raise ValueError("source path repeated in manifest")
        paths.add(resolved)
        if path.stat().st_size != source["bytes"] or sha_file(path) != source["sha256"]:
            raise ValueError(f"source bytes changed: {path}")
        selected = [u["dataset"] for u in source["units"]] if manifest["format"] == "hdf5" else None
        inspected = b.inspect(path, selected)
        if inspected["total_dataset_count"] != source["total_dataset_count"] or len(inspected["units"]) != len(source["units"]):
            raise ValueError("native unit count changed")
        for fresh, old in zip(inspected["units"], source["units"]):
            if any(fresh[key] != old[key] for key in fresh):
                raise ValueError(f"native unit/oracle changed: {path}")
        refreshed.append({**source, "units": inspected["units"]})
    if all("file" in s for s in sources):
        expected = {s["file"] for s in sources}
        found = set()
        raw_root = Path(root) / "raw"
        for directory, dirs, files in os.walk(raw_root, followlinks=False):
            for name in dirs + files:
                if (Path(directory) / name).is_symlink():
                    raise ValueError("raw tree contains a symlink")
            found.update((Path(directory) / name).relative_to(root).as_posix() for name in files)
        if expected != found:
            raise ValueError("raw tree differs from frozen candidate file set")
    actual = assert_dimensions(refreshed, manifest["spec"]) if manifest["spec"] else counts(refreshed, manifest["query"])
    if actual != manifest["actual"] or sum(s["bytes"] for s in sources) != manifest["raw_bytes"]:
        raise ValueError("manifest counts or aggregate size changed")
    tree = manifest["tree"]
    if tree is not None and tree_stats(sources, tree) != tree:
        raise ValueError("tree descriptor changed")
    return actual


def verify(root):
    root = Path(root)
    if not (root / "manifest.json").exists() and (root / "collection.json").exists():
        return verify_collection(root)
    manifest = load_manifest(root)
    if manifest.get("spec"):
        validate_spec(manifest["spec"])
    validate_query(manifest["query"])
    if manifest["query_template"] != query_template(manifest["format"], manifest["query"]):
        raise ValueError("query template differs from frozen query")
    actual = validate_contents(root, manifest)
    return {"manifest_sha256": manifest["manifest_sha256"], "actual": actual,
            "raw_bytes": manifest["raw_bytes"], "verified": True}


def verify_collection(root):
    from common import safe_file
    paths = [Path(root) / name for name in ("collection.json", "plan.json")]
    if any(path.stat().st_size > 1048576 for path in paths):
        raise ValueError("collection/plan exceeds bound")
    collection, plan = [json.loads(path.read_text()) for path in paths]
    if (collection.get("protocol") != "iotdbfs-multifile-collection/1" or seal(collection) != collection or
            plan.get("protocol") != "iotdbfs-multifile-gradient/1" or seal(plan) != plan or
            plan["manifest_sha256"] != collection["plan_sha256"]):
        raise ValueError("collection/plan fingerprint mismatch")
    if (not 1 <= len(collection["datasets"]) <= 32 or
            len(collection["datasets"]) != len(plan["jobs"]) or
            collection["axis"] != plan["axis"]):
        raise ValueError("collection differs from frozen gradient plan")
    total, actual = 0, []
    expected_directories = set()
    for item, job in zip(collection["datasets"], plan["jobs"]):
        spec = validate_spec(job["spec"])
        if (identity(spec) != job["spec_sha256"] or spec != {**plan["base"], plan["axis"]: job["value"]} or
                item["spec_sha256"] != job["spec_sha256"] or item["value"] != job["value"] or
                item["manifest"] != f"datasets/{job['spec_sha256']}/manifest.json"):
            raise ValueError("collection member is not the planned variant")
        child = safe_file(root, item["manifest"]).parent
        proof = verify(child)
        manifest = load_manifest(child)
        if (proof["manifest_sha256"] != item["manifest_sha256"] or
                manifest["format"] != spec["format"] or manifest["query"] != spec["query"] or
                assert_dimensions(manifest["sources"], spec) != item["actual"] or
                manifest["tree"] != item["tree"] or manifest["tree"]["depth"] != spec["depth"] or
                proof["raw_bytes"] != item["raw_bytes"]):
            raise ValueError("collection member differs from frozen descriptor")
        total += proof["raw_bytes"]
        actual.append(item["actual"])
        expected_directories.add(job["spec_sha256"])
    if {p.name for p in (Path(root) / "datasets").iterdir()} != expected_directories:
        raise ValueError("collection contains unregistered datasets")
    if total != collection["raw_bytes"]:
        raise ValueError("collection aggregate size differs")
    return {"collection_sha256": collection["manifest_sha256"], "datasets": len(actual),
            "actual": actual, "raw_bytes": total, "verified": True}


def finish(stage, manifest):
    started = time.perf_counter_ns()
    validate_contents(stage, manifest)
    manifest["timings_ns"]["native_verification"] = time.perf_counter_ns() - started
    manifest = seal(manifest)
    if len(canonical(manifest)) > 256 * 1024 * 1024:
        raise ValueError("output manifest exceeds bounded size")
    write_json(stage / "manifest.json", manifest)
    return manifest


def generate(spec, output, runtime_budget=None):
    spec = validate_spec(spec)
    b = backend(spec["format"])
    b.environment()  # Check native runtime before creating output.
    if spec["query"] != TARGET:
        raise ValueError("generation supports the frozen controlled target only")
    if spec["payload"] == {"policy": "preserve"}:
        raise ValueError("generation needs explicit payload geometry")
    started = time.perf_counter_ns()
    with publication(output) as stage:
        sources = []
        for index in range(spec["N"]):
            relative = relative_file(spec, index)
            path = stage / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            inspected = b.create_file(path, spec, index)
            sources.append(file_record(path, inspected, relative,
                                      {"kind": "generated", "spec_sha256": identity(spec)}))
            check_budget(sources, min(spec["max_output_bytes"], runtime_budget) if runtime_budget is not None else spec["max_output_bytes"])
        manifest = new_manifest("generated", spec["format"], spec["query"], sources,
            spec=spec, tree={k: spec[k] for k in ("depth", "seed", "tree_width")},
            provenance={"classification": "synthetic-controlled; not public observations"},
            timing={"generation_and_hash": time.perf_counter_ns() - started})
        return finish(stage, manifest)


def inventory(format_id, paths, query, output, datasets=None, max_source_bytes=8 * 1024**3):
    query = validate_query(query)
    query_template(format_id, query)  # Validate supported GRIB predicate keys.
    paths = [Path(p).resolve() for p in paths]
    integer(max_source_bytes, "max_source_bytes", 1, 4 * 1024**4)
    if not 1 <= len(paths) <= 4096 or len(set(paths)) != len(paths):
        raise ValueError("inventory requires 1..4096 distinct source paths")
    if sum(p.stat().st_size for p in paths) > max_source_bytes:
        raise ValueError("inventory source bytes exceed explicit budget")
    if any(not stat.S_ISREG(p.stat().st_mode) for p in paths):
        raise ValueError("inventory accepts regular files only")
    b = backend(format_id)
    b.environment()
    started = time.perf_counter_ns()
    with publication(output) as stage:
        sources = []
        for path in paths:
            before = path.stat()
            inspected = b.inspect(path, datasets)
            source = file_record(path, inspected)
            after = path.stat()
            if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
                raise ValueError("source changed during inventory")
            for position, unit in enumerate(source["units"]):
                unit["lineage"] = {"kind": "raw-inventory", "source_sha256": source["sha256"],
                                   "external_path": str(path), "unit_index": position}
            sources.append(source)
            if sum(len(s["units"]) for s in sources) > MAX_UNITS:
                raise ValueError("inventory exceeds unit count bound")
        manifest = new_manifest("inventory", format_id, query, sources,
            provenance={"classification": "external sources; do not assume distinct observations from paths alone",
                        "hdf5_data_dataset_allowlist": datasets},
            timing={"inventory_and_hash": time.perf_counter_ns() - started})
        return finish(stage, manifest)


def grouping(parent, spec):
    """No replacement; exactly one matched unit in each of K output files."""
    if parent["format"] != spec["format"]:
        raise ValueError("cannot regroup across formats")
    pool = [(s, u) for s in parent["sources"] for u in s["units"]]
    if spec["format"] == "hdf5" and any(not s["controlled"] for s in parent["sources"]):
        raise ValueError("public HDF5 supports external tree copying only")
    positives = [(s, u) for s, u in pool if matches(u["attributes"], spec["query"])]
    negatives = [(s, u) for s, u in pool if not matches(u["attributes"], spec["query"])]
    needed = spec["N"] * spec["D"] - spec["K"]
    if len(positives) < spec["K"] or len(negatives) < needed:
        raise ValueError(f"insufficient pool without replacement: need {spec['K']} matches/{needed} distractors, have {len(positives)}/{len(negatives)}")
    groups = []
    pos, neg = iter(positives), iter(negatives)
    for index in range(spec["N"]):
        group = [next(pos)] if index < spec["K"] else []
        group.extend(next(neg) for _ in range(spec["D"] - len(group)))
        groups.append(group)
    return groups, {"pool_units": len(pool), "used_units": spec["N"] * spec["D"],
                    "unused_units": len(pool) - spec["N"] * spec["D"],
                    "available_matches": len(positives), "available_distractors": len(negatives),
                    "selection": "source order, then unit order; no replacement"}


def regroup(input_root, spec, output, runtime_budget=None):
    preflight_started = time.perf_counter_ns()
    spec = validate_spec(spec)
    if spec["payload"] != {"policy": "preserve"}:
        raise ValueError("regroup requires payload={\"policy\":\"preserve\"}")
    parent = load_manifest(input_root)
    groups, usage = grouping(parent, spec)
    budget = min(spec["max_output_bytes"], runtime_budget) if runtime_budget is not None else spec["max_output_bytes"]
    stored = sum(u["length"] if spec["format"] == "grib" else u["stored_bytes"]
                 for group in groups for _, u in group)
    if stored > budget:
        raise ValueError("selected payload already exceeds raw-byte budget")
    grouping_ns = time.perf_counter_ns() - preflight_started
    verify_started = time.perf_counter_ns()
    verify(input_root)
    input_verification_ns = time.perf_counter_ns() - verify_started
    b = backend(spec["format"])
    started = time.perf_counter_ns()
    with publication(output) as stage:
        sources = []
        for index, group in enumerate(groups):
            relative = relative_file(spec, index)
            path = stage / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            inspected = b.copy_units(path, group, lambda s: source_path(input_root, s))
            for unit, (source, original) in zip(inspected["units"], group):
                unit["lineage"] = {"kind": "regrouped", "parent_manifest_sha256": parent["manifest_sha256"],
                    "source_sha256": source["sha256"], "unit_id": original["unit_id"],
                    "source_file": source.get("file", source.get("external_path")),
                    "source_selector": {k: original[k] for k in ("dataset", "offset", "length") if k in original},
                    "original_lineage": original.get("lineage")}
            sources.append(file_record(path, inspected, relative))
            check_budget(sources, budget)
        manifest = new_manifest("regrouped", spec["format"], spec["query"], sources,
            spec=spec, tree={k: spec[k] for k in ("depth", "seed", "tree_width")},
            provenance={"parent_manifest_sha256": parent["manifest_sha256"], "input_artifact": str(Path(input_root).resolve()), **usage},
            timing={"regroup_and_hash": time.perf_counter_ns() - started,
                    "input_manifest_and_grouping": grouping_ns,
                    "input_verification": input_verification_ns})
        return finish(stage, manifest)


def reorganize(input_root, output, depth, tree_width, seed, max_output_bytes):
    preflight_started = time.perf_counter_ns()
    integer(depth, "depth", 1, 8)
    integer(tree_width, "tree_width", 1, 256)
    integer(seed, "seed", 0, (1 << 63) - 1)
    integer(max_output_bytes, "max_output_bytes", 1, 4 * 1024**4)
    parent = load_manifest(input_root)
    check_budget(parent["sources"], max_output_bytes)
    manifest_load_ns = time.perf_counter_ns() - preflight_started
    verify_started = time.perf_counter_ns()
    verify(input_root)
    input_verification_ns = time.perf_counter_ns() - verify_started
    tree = {"depth": depth, "tree_width": tree_width, "seed": seed}
    started = time.perf_counter_ns()
    with publication(output) as stage:
        sources = []
        for index, source in enumerate(parent["sources"]):
            relative = relative_file({**tree, "format": parent["format"]}, index)
            path = stage / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source_path(input_root, source), path)
            if sha_file(path) != source["sha256"]:
                raise ValueError("tree copy changed source bytes")
            copied = {k: v for k, v in source.items() if k not in ("file", "external_path", "origin")}
            copied["file"] = relative
            copied["origin"] = {"kind": "byte-copy", "parent_manifest_sha256": parent["manifest_sha256"],
                                "source_file": source.get("file", source.get("external_path"))}
            sources.append(copied)
        spec = {**parent["spec"], "depth": depth, "tree_width": tree_width} if parent["spec"] else None
        manifest = new_manifest("tree-copy", parent["format"], parent["query"], sources,
            spec=spec, tree=tree, provenance={"parent_manifest_sha256": parent["manifest_sha256"],
                "input_artifact": str(Path(input_root).resolve()), "byte_preserving": True},
            timing={"copy_and_hash": time.perf_counter_ns() - started,
                    "input_manifest_load": manifest_load_ns,
                    "input_verification": input_verification_ns})
        return finish(stage, manifest)


def gradient(base, axis, values, output):
    base = validate_spec(base)
    if axis not in ("N", "K", "D", "depth") or not 1 <= len(values) <= 32 or len(set(values)) != len(values):
        raise ValueError("gradient needs one supported axis and 1..32 distinct values")
    jobs = []
    for value in values:
        spec = validate_spec({**base, axis: value})
        jobs.append({"value": value, "spec_sha256": identity(spec), "spec": spec,
                     "estimated_payload_bytes": estimated_payload_bytes(spec)})
    plan = seal({"protocol": "iotdbfs-multifile-gradient/1", "axis": axis,
                 "base": base, "jobs": jobs, "materialized": False})
    with publication(output) as stage:
        write_json(stage / "plan.json", plan)
        for job in jobs:
            write_json(stage / (job["spec_sha256"] + ".json"), job["spec"])
    return plan


def materialize(plan_root, output, mode, input_root=None, max_total_bytes=8 * 1024**3):
    integer(max_total_bytes, "max_total_bytes", 1, 4 * 1024**4)
    path = Path(plan_root) / "plan.json"
    if path.stat().st_size > 1048576:
        raise ValueError("gradient plan exceeds bound")
    plan = json.loads(path.read_text())
    if plan.get("protocol") != "iotdbfs-multifile-gradient/1" or seal(plan) != plan:
        raise ValueError("gradient plan fingerprint mismatch")
    if mode not in ("generate", "regroup", "reorganize") or (mode != "generate" and input_root is None):
        raise ValueError("regroup/reorganize require an input artifact")
    if mode == "reorganize" and plan["axis"] != "depth":
        raise ValueError("byte-preserving reorganization only varies depth")
    if not 1 <= len(plan["jobs"]) <= 32:
        raise ValueError("gradient job count exceeds bound")
    specs = []
    for job in plan["jobs"]:
        spec = validate_spec(job["spec"])
        if identity(spec) != job["spec_sha256"] or spec != {**plan["base"], plan["axis"]: job["value"]}:
            raise ValueError("gradient job differs from declared single-axis change")
        if json.loads((Path(plan_root) / (job["spec_sha256"] + ".json")).read_text()) != spec:
            raise ValueError("gradient spec file differs from frozen plan")
        specs.append(spec)
    if len({identity(s) for s in specs}) != len(specs):
        raise ValueError("duplicate gradient jobs")
    if mode != "generate":
        parent = load_manifest(input_root)
        for spec in specs:
            if mode == "regroup":
                grouping(parent, spec)
            elif parent["format"] != spec["format"] or parent["query"] != spec["query"]:
                raise ValueError("depth plan must preserve the input format/query")
            else:
                assert_dimensions(parent["sources"], spec)
    started = time.perf_counter_ns()
    with publication(output) as stage:
        results = []
        total = 0
        for job, spec in zip(plan["jobs"], specs):
            target = stage / "datasets" / job["spec_sha256"]
            remaining = max_total_bytes - total
            estimate = estimated_payload_bytes(spec)
            if remaining <= 0 or (estimate is not None and estimate > remaining):
                raise ValueError("gradient exceeds total raw-byte budget")
            if mode == "generate":
                result = generate(spec, target, remaining)
            elif mode == "regroup":
                result = regroup(input_root, spec, target, remaining)
            else:
                result = reorganize(input_root, target, spec["depth"], spec["tree_width"], spec["seed"],
                                    min(spec["max_output_bytes"], remaining))
            total += result["raw_bytes"]
            if total > max_total_bytes:
                raise ValueError("gradient exceeds total raw-byte budget")
            results.append({"value": job["value"], "spec_sha256": job["spec_sha256"],
                "manifest": target.relative_to(stage).as_posix() + "/manifest.json",
                "manifest_sha256": result["manifest_sha256"], "actual": result["actual"],
                "raw_bytes": result["raw_bytes"], "tree": result["tree"]})
        summary = seal({"protocol": "iotdbfs-multifile-collection/1", "mode": mode,
            "axis": plan["axis"], "plan_sha256": plan["manifest_sha256"], "datasets": results,
            "raw_bytes": total, "max_total_raw_bytes": max_total_bytes,
            "operation_before_collection_write_ns": time.perf_counter_ns() - started})
        write_json(stage / "plan.json", plan)
        write_json(stage / "collection.json", summary)
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("plan", "generate", "regroup"):
        sub = commands.add_parser(name)
        sub.add_argument("--spec", type=Path, required=True)
        if name != "plan":
            sub.add_argument("--output", type=Path, required=True)
        if name == "regroup":
            sub.add_argument("--input", type=Path, required=True)
    inv = commands.add_parser("inventory")
    inv.add_argument("--format", choices=("hdf5", "grib"), required=True)
    inv.add_argument("--source", type=Path, action="append", required=True)
    inv.add_argument("--dataset", action="append")
    inv.add_argument("--query-file", type=Path)
    inv.add_argument("--max-source-bytes", type=int, default=8 * 1024**3)
    inv.add_argument("--output", type=Path, required=True)
    reorg = commands.add_parser("reorganize")
    reorg.add_argument("--input", type=Path, required=True)
    reorg.add_argument("--output", type=Path, required=True)
    reorg.add_argument("--depth", type=int, required=True)
    reorg.add_argument("--tree-width", type=int, default=4)
    reorg.add_argument("--seed", type=int, default=20261013)
    reorg.add_argument("--max-output-bytes", type=int, default=8 * 1024**3)
    ver = commands.add_parser("verify")
    ver.add_argument("--input", type=Path, required=True)
    grad = commands.add_parser("gradient")
    grad.add_argument("--base-spec", type=Path, required=True)
    grad.add_argument("--axis", choices=("N", "K", "D", "depth"), required=True)
    grad.add_argument("--values", type=int, nargs="+", required=True)
    grad.add_argument("--output", type=Path, required=True)
    mat = commands.add_parser("materialize")
    mat.add_argument("--plan", type=Path, required=True)
    mat.add_argument("--mode", choices=("generate", "regroup", "reorganize"), required=True)
    mat.add_argument("--input", type=Path)
    mat.add_argument("--max-total-bytes", type=int, default=8 * 1024**3)
    mat.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            spec = read_spec(args.spec)
            result = {"spec": spec, "spec_sha256": identity(spec), "units": spec["N"] * spec["D"],
                      "estimated_payload_bytes": estimated_payload_bytes(spec),
                      "estimate_excludes": "file metadata, filesystem directories, manifests and publication; actual raw-byte budget also enforced",
                      "paths": {"first": relative_file(spec, 0), "last": relative_file(spec, spec["N"] - 1)}}
        elif args.command == "generate":
            result = generate(read_spec(args.spec), args.output)
        elif args.command == "regroup":
            result = regroup(args.input, read_spec(args.spec), args.output)
        elif args.command == "inventory":
            query = json.loads(args.query_file.read_text()) if args.query_file else TARGET
            result = inventory(args.format, args.source, query, args.output, args.dataset, args.max_source_bytes)
        elif args.command == "reorganize":
            result = reorganize(args.input, args.output, args.depth, args.tree_width, args.seed, args.max_output_bytes)
        elif args.command == "gradient":
            result = gradient(read_spec(args.base_spec), args.axis, args.values, args.output)
        elif args.command == "materialize":
            result = materialize(args.plan, args.output, args.mode, args.input, args.max_total_bytes)
        else:
            result = verify(args.input)
        if args.command == "gradient":
            result = {"plan": str(args.output.absolute() / "plan.json"), "jobs": len(result["jobs"]),
                      "plan_sha256": result["manifest_sha256"]}
        elif args.command == "materialize":
            result = {"collection": str(args.output.absolute() / "collection.json"),
                      "datasets": len(result["datasets"]), "raw_bytes": result["raw_bytes"],
                      "collection_sha256": result["manifest_sha256"]}
        elif args.command not in ("plan", "verify"):
            result = {"manifest": str(args.output.absolute() / "manifest.json"),
                      "manifest_sha256": result["manifest_sha256"], "actual": result["actual"],
                      "raw_bytes": result["raw_bytes"]}
        print(json.dumps(result, sort_keys=True))
        return 0
    except (ValueError, OSError, KeyError, TypeError) as error:
        print(f"{args.command}: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
