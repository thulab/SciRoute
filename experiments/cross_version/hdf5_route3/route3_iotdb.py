#!/usr/bin/env python3
"""Route-3 experiment glue for the unmodified V04 owner and HDF5 readers.

This module has no service/account operations or import-time database access.
The controller owns the original-instance SYNC window, journal lifecycle and
query clocks. ``mount`` delegates only to the established private FUSE helper.

Publication is real ordinary FUSE write/fsync/close, not metadata-only seeding.
``publish_round`` must be called in ordinal order across ALL files so its
half-open timestamp fence can address a whole round. Empty O_TRUNC versions
remain in the namespace and are not returned as scientific bindings.

Formal discovery returns (rows, stats, next_profile_offset). Typed reading
returns (owned_units, stats_per_request, next_profile_offset). Values/masks are
owned NumPy arrays; oracle hashing belongs outside the controller query clock.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import math
from pathlib import Path, PurePosixPath
import sys
import time
from urllib.parse import quote

V04_SHA = "83b90b15181f3311e5b70bf8dfbd334295b3e5422bf1ded388a9948ce2144305"
MAX_SOURCES = 1024
MAX_DATASETS = 1024
MAX_METADATA_BYTES = 64 * 1024 * 1024
MAX_EXACT_RECORDS = 65536


def require(value, message):
    if not value:
        raise RuntimeError(message)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _dependencies():
    # Keep cheap request construction/tests independent of scientific runtimes.
    root = Path(__file__).resolve().parents[2]
    for directory in (root / "multi_file", root / "single_file"):
        if str(directory) not in sys.path:
            sys.path.insert(0, str(directory))
    import small_runner
    from experiments.cross_version.pilot import publish
    return small_runner, publish


def _logical(path):
    value = PurePosixPath(path)
    require(isinstance(path, str) and path.startswith("/") and
            str(value) == path and "\0" not in path and
            all(part not in (".", "..") for part in value.parts),
            "logical path must be absolute and canonical")
    return path


def selector(prefix, selection="latest", fence=None, *, exact_path=None,
             exact_version=None):
    """Construct only existing production selectors; no ordinal reinterpretation."""
    _logical(prefix)
    if exact_path is not None:
        _logical(exact_path)
        require(exact_path == prefix or exact_path.startswith(prefix.rstrip("/") + "/"),
                "exact path escapes prefix")
    if selection == "latest":
        require(fence is None and exact_version is None, "latest has no version bounds")
        versions = {"kind": "latest"}
    elif selection == "between":
        require(fence is not None and len(fence) == 2 and
                all(type(v) is int and v >= 0 for v in fence) and
                fence[0] < fence[1] and exact_version is None,
                "between requires actual nonempty [lower, upper) timestamp fence")
        versions = {"kind": "between", "lower_inclusive": fence[0],
                    "upper_exclusive": fence[1]}
    else:
        require(selection == "exact" and fence is None and
                type(exact_version) is int and exact_version >= 0 and exact_path,
                "exact requires one actual path/version")
        versions = {"kind": "exact", "exact": exact_version}
    return {"paths": [{"kind": "exact" if exact_path else "subtree",
                       "path": exact_path or prefix}], "versions": versions,
            "formats": {"adapters": "listed", "adapter_ids": ["hdf5"],
                        "include_byte_passthrough": False}}


def discovery_request(prefix, predicate, selection="latest", fence=None,
                      *, exact_path=None, exact_version=None):
    request = {"sources": selector(prefix, selection, fence, exact_path=exact_path,
                                   exact_version=exact_version),
               "max_sources": MAX_SOURCES, "max_datasets": MAX_DATASETS,
               "max_catalog_bytes": MAX_METADATA_BYTES}
    if predicate is not None:
        request["predicate"] = predicate
    return {"kind": "dataset_catalog", "request": request}


def equality_predicate(attributes):
    """Same scalar equality encoding used by multi_file.manager.query_template."""
    require(isinstance(attributes, dict) and 1 <= len(attributes) <= 16,
            "one to sixteen equality attributes required")
    operands = []
    for name, value in sorted(attributes.items()):
        require(isinstance(name, str) and name and type(value) in (str, int, float),
                "unsupported equality attribute")
        require(type(value) is not float or math.isfinite(value), "nonfinite equality value")
        operands.append({"kind": "compare", "field": "hdf5.attr:" + quote(name, safe=""),
            "operator": "eq", "value": {"type": {str: "utf8", int: "int64", float: "float64"}[type(value)],
                                         "value": value}})
    return {"kind": "and", "operands": operands}


def roi_slices(origin, shape):
    require(len(origin) == len(shape) and 1 <= len(origin) <= 4 and
            all(type(v) is int and v >= 0 for v in origin) and
            all(type(v) is int and v > 0 for v in shape) and
            all(o + n <= (1 << 64) - 1 for o, n in zip(origin, shape)),
            "ROI requires one to four nonnegative axes and positive extents")
    return [{"dimension": "hdf5.axis.%d" % axis,
             "lower": {"kind": "inclusive", "value": {"type": "uint64", "value": o}},
             "upper": {"kind": "exclusive", "value": {"type": "uint64", "value": o + n}}}
            for axis, (o, n) in enumerate(zip(origin, shape))]


def typed_request(row, origin, shape):
    path = _logical(row["path"])
    require(type(row["version"]) is int and row["version"] >= 0 and
            isinstance(row["dataset"], str) and row["dataset"].startswith("/") and
            "\0" not in row["dataset"], "typed row identity is malformed")
    return {"kind": "typed", "request": {
        "sources": selector(path, "exact", exact_path=path, exact_version=row["version"]),
        "subset": {"projection": ["hdf5.value:" + quote(row["dataset"], safe="")],
                   "slices": roi_slices(origin, shape)}}}


def _call(context, request, offset, values, profile):
    paired, _ = _dependencies()
    if profile:
        if offset is None:
            offset = context.get("route3_profile_offset", 0)
        response, batches, stats, offset = paired.profile_call(context, request, offset, values)
        context["route3_profile_offset"] = offset
        return response, batches, stats, offset
    started = time.perf_counter_ns()
    response, control_ns = paired.query_control(context["socket"], request)
    batches, ipc_bytes = paired.ipc(context, response, values) if "path" in response else ([], 0)
    return response, batches, {"total_ns": time.perf_counter_ns() - started,
        "control_ns": control_ns, "ipc_bytes": ipc_bytes, "instrumentation_ns": 0}, offset


def reset_profile_offset(context):
    """Exclude publication/unprofiled probes before starting a formal query arm."""
    path = Path(context["root"]) / "queries.jsonl"
    context["route3_profile_offset"] = path.stat().st_size if path.exists() else 0
    return context["route3_profile_offset"]


def discover(context, prefix, predicate, selection="latest", fence=None,
             *, offset=None, profile=True, exact_path=None, exact_version=None):
    response, _, stats, offset = _call(context, discovery_request(prefix, predicate,
        selection, fence, exact_path=exact_path, exact_version=exact_version),
        offset, False, profile)
    rows = []
    seen = set()
    for item in response["datasets"]:
        row = {"path": item["path"], "version": item["version"], "dataset": item["dataset_path"]}
        _logical(row["path"])
        require(type(row["version"]) is int and row["version"] >= 0 and
                (row["path"] == prefix or row["path"].startswith(prefix.rstrip("/") + "/")),
                "production discovery source escapes scope or has invalid version")
        if exact_path is not None:
            require(row["path"] == exact_path, "discovery changed exact path")
        if exact_version is not None:
            require(row["version"] == exact_version, "discovery changed exact version")
        if fence is not None:
            require(fence[0] <= row["version"] < fence[1], "discovery escaped timestamp fence")
        identity = row["path"], row["version"], row["dataset"]
        require(identity not in seen, "production discovery duplicated a unit")
        seen.add(identity)
        rows.append(row)
    return rows, stats, offset


def typed_values(context, rows, origin, shape, *, offset=None, profile=True):
    """Read each selected exact HDF5 dataset ROI via current production typed path."""
    import numpy as np
    expected = math.prod(shape)
    owned, calls, seen = [], [], set()
    for row in rows:
        identity = row["path"], row["version"], row["dataset"]
        require(identity not in seen, "typed input duplicated a unit")
        seen.add(identity)
        _, parts, stats, offset = _call(context, typed_request(row, origin, shape),
                                       offset, True, profile)
        require(parts, "typed ROI returned no batches")
        for part in parts:
            meta = part["meta"]
            require(meta.get(b"iotdbfs.sources") == b"1" and
                    meta.get(b"iotdbfs.source.0.path") == row["path"].encode() and
                    meta.get(b"iotdbfs.source.0.version") == str(row["version"]).encode(),
                    "typed ROI provenance differs from selected exact source")
        values = np.concatenate([p["array"] for p in parts]).astype("<f4", copy=False)
        mask = np.concatenate([p["mask"] for p in parts]).astype("u1", copy=False)
        require(values.size == mask.size == expected, "typed ROI row count differs")
        owned.append({**row, "array": values, "mask": mask, "shape": list(shape)})
        calls.append(stats)
    return owned, calls, offset


def native_path(context, row):
    """Native H5Fopen path for exactly the production-discovered historical bytes."""
    _, publisher = _dependencies()
    return publisher.history_path(context["mount"], _logical(row["path"]), row["version"])


def publish_one(context, source, logical_path, ordinal, *, previous=None,
                expected_sha256=None, expected_datasets=4):
    """Real publish + latest binding/SHA; all costs are outside formal query clocks."""
    _, publisher = _dependencies()
    source, logical_path = Path(source).resolve(), _logical(logical_path)
    require(type(ordinal) is int and ordinal >= 1 and (ordinal == 1) == (previous is None),
            "publication ordinal/previous binding differs")
    require(source.is_file() and expected_datasets > 0, "publication source/dataset count missing")
    source_sha = sha(source)
    require(expected_sha256 is None or expected_sha256 == source_sha, "publication raw SHA differs")
    destination = Path(context["mount"]) / logical_path.lstrip("/")
    started = time.perf_counter_ns()
    publication_ns = publisher.write_version(source, destination, ordinal == 1)
    binding_started = time.perf_counter_ns()
    rows, stats, _ = discover(context, logical_path, None, profile=False,
                              exact_path=logical_path)
    versions = {row["version"] for row in rows}
    require(len(rows) == expected_datasets and len(versions) == 1,
            "publication latest scientific catalog is incomplete")
    version = versions.pop()
    require(previous is None or version > previous, "publication version failed to advance")
    require(sha(destination) == source_sha, "publication full-file SHA differs")
    return {"path": logical_path, "version": version, "ordinal": ordinal,
            "source": str(source), "sha256": source_sha, "bytes": source.stat().st_size,
            "publication_ns": publication_ns,
            "binding_sha_ns": time.perf_counter_ns() - binding_started,
            "total_ns": time.perf_counter_ns() - started, "datasets": rows,
            "binding_call": stats, "empty_truncate_snapshots_retained": True}


def publish_round(context, items, ordinal, previous, *, checkpoint=None):
    """Items: [{source,path,sha256?}]; previous maps logical path to actual version."""
    require(items and len({item["path"] for item in items}) == len(items),
            "round must contain distinct paths")
    paths = {item["path"] for item in items}
    require((ordinal == 1 and not previous) or
            (ordinal > 1 and set(previous) == paths),
            "round must cover the same complete path set in ordinal order")
    prior_max = max(previous.values(), default=-1)
    bindings = []
    for item in items:
        binding = publish_one(context, item["source"], item["path"], ordinal,
                              previous=previous.get(item["path"]),
                              expected_sha256=item.get("sha256"))
        previous[item["path"]] = binding["version"]
        bindings.append(binding)
        if checkpoint is not None:
            checkpoint(binding)
    versions = [binding["version"] for binding in bindings]
    require(min(versions) > prior_max, "new round timestamp overlaps prior round")
    return {"ordinal": ordinal, "bindings": bindings,
            "fence": [min(versions), max(versions) + 1],
            "previous_round_max_version": prior_max,
            "rounds_disjoint": True, "actual_timestamp_fence": True}


def create_launcher(path, daemon, prefix, journal, *, initialize=False,
                    max_registrations=8192):
    """Use an existing frontend capacity setting for the larger experiment.

    This bounds retained registration definitions, not request/source/output
    budgets. The original launcher source remains untouched; this new launcher
    includes the additional setting in its actual launch receipt and SHA.
    """
    require(sha(daemon) == V04_SHA, "route3 requires the frozen ordinary V04 r3 daemon")
    require(type(max_registrations) is int and 1 <= max_registrations <= 65536,
            "frontend registration capacity must be within 1..65536")
    from experiments.cross_version.version_directory import v03_n128
    path = Path(path)
    launch = v03_n128.launcher(path, Path(daemon), prefix, Path(journal), initialize)
    settings = dict(launch["settings"])
    original_line = "SETTINGS=" + repr(settings) + "\n"
    source = path.read_text()
    require(source.count(original_line) == 1, "established launcher settings representation changed")
    settings["query.frontend.max_registrations"] = str(max_registrations)
    path.write_text(source.replace(original_line, "SETTINGS=" + repr(settings) + "\n", 1))
    launch.update(settings=settings, sha256=sha(path))
    return launch


@contextmanager
def mount(launch, root, env, writer_lock, *, profile=True):
    """Original DB must already be in the caller's authorized SYNC window."""
    paired, _ = _dependencies()
    require(launch["binary_sha256"] == V04_SHA and sha(launch["binary"]) == V04_SHA,
            "mount daemon differs from frozen route3 V04 r3")
    with paired.sf.private_mount(Path(launch["path"]), Path(root), env,
                                 Path(writer_lock), profile=profile) as context:
        yield context
