#!/usr/bin/env python3
"""Publish real same-path versions, then verify history after a daemon restart."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from urllib.parse import quote
import uuid

from experiments.cross_version.pilot import corpus
from experiments.cross_version.pilot.corpus import require
from common import identity, load_manifest, matches, sha_file
import small_runner as paired
from environment import atomic_json, connection_environment
import runner as sf
import numpy as np

DATABASE = "h09probe1790939889dbfc1f"


def call(context, request, values=False):
    response, _ = paired.query_control(context["socket"], request)
    batches = paired.ipc(context, response, values)[0] if "path" in response else []
    return response, batches


def discovery(context, logical, fmt, template, version=None):
    request = paired.discovery_request(logical, template, False)
    request["request"]["sources"] = paired.selector(logical, fmt, version, logical)
    response, batches = call(context, request)
    return paired.discovered(response, batches, fmt, logical.rsplit("/", 1)[0])


def all_versions(context, logical, fmt, template):
    request = paired.discovery_request(logical, template, False)
    sources = paired.selector(logical, fmt, exact=logical)
    sources["versions"] = {"kind": "all"}
    request["request"]["sources"] = sources
    response, batches = call(context, request)
    return paired.discovered(response, batches, fmt, logical.rsplit("/", 1)[0])


def write_version(source, destination, first):
    destination.parent.mkdir(parents=True, exist_ok=True)
    require(destination.exists() != first, "publication destination presence differs")
    started = time.perf_counter_ns()
    with Path(source).open("rb", buffering=0) as raw, destination.open("xb" if first else "wb", buffering=0) as target:
        for block in iter(lambda: raw.read(1048576), b""):
            pending = memoryview(block)
            while pending:
                written = target.write(pending)
                require(written is not None and written > 0, "short publication write")
                pending = pending[written:]
        os.fsync(target.fileno())
    return time.perf_counter_ns() - started


def history_path(mount, logical, version):
    path = Path(logical)
    return mount / str(path.parent).lstrip("/") / f".{path.stem}_{version}{path.suffix}"


def validate_values(context, entry, logical, version):
    fmt = entry["format"]
    requests = []
    if fmt == "hdf5":
        for unit in entry["units"]:
            requests.append(([unit], {"projection": ["hdf5.value:" + quote(unit["dataset"], safe="")]}))
    else:
        requests.append((entry["units"], {"projection": ["grib.data.value"]}))
    checked = 0
    for units, subset in requests:
        _, batches = call(context, {"kind": "typed", "request": {
            "sources": paired.selector(logical, fmt, version, logical), "subset": subset}}, True)
        require(bool(batches), "empty typed version values")
        for batch in batches:
            meta = batch["meta"]
            require(meta.get(b"iotdbfs.sources") == b"1" and
                    meta.get(b"iotdbfs.source.0.path") == logical.encode() and
                    meta.get(b"iotdbfs.source.0.version") == str(version).encode(),
                    "typed exact source/version provenance differs")
        values = np.concatenate([b["array"] for b in batches])
        mask = np.concatenate([b["mask"] for b in batches])
        require(len(values) == sum(u["oracle"]["elements"] for u in units), "typed row count differs")
        start = 0
        for unit in units:
            end = start + unit["oracle"]["elements"]
            oracle = unit["oracle"]
            require(hashlib.sha256(values[start:end].tobytes()).hexdigest() == oracle["value_sha256"],
                    "typed historical values differ from independent native oracle")
            expected_mask = oracle.get("mask_sha256", hashlib.sha256(b"\1" * (end - start)).hexdigest())
            require(hashlib.sha256(mask[start:end].tobytes()).hexdigest() == expected_mask,
                    "typed historical validity mask differs")
            checked += 1
            start = end
    return checked


def audit(context, root, manifest, report, checkpoint):
    entries = manifest["entries"]
    require(len(report["bindings"]) == len(entries), "incomplete publication report")
    templates = {fmt: load_manifest(base["root"]) for fmt, base in manifest["bases"].items()}
    units_verified = 0
    for entry, binding in zip(entries, report["bindings"]):
        key = {k: entry[k] for k in ("format", "scenario", "file_index", "ordinal")}
        logical = report["logical_prefix"] + f"/{entry['scenario']}/{entry['format']}/{entry['logical_file']}"
        require(binding["key"] == key and binding["logical_path"] == logical and
                binding["sha256"] == entry["sha256"], "publication binding differs from frozen corpus")
        exact_path = history_path(context["mount"], logical, binding["version"])
        require(sha_file(exact_path) == entry["sha256"], "historical full-file bytes differ after restart")
        rows = discovery(context, logical, entry["format"], templates[entry["format"]], binding["version"])
        require(len(rows) == len(entry["units"]) and all(r["version"] == binding["version"] for r in rows),
                "historical exact catalog binding differs")
        units_verified += validate_values(context, entry, logical, binding["version"])
        binding["full_sha_verified_after_restart"] = True
        binding["typed_units_verified"] = len(entry["units"])
        if entry["ordinal"] == manifest["spec"]["versions"]:
            require(sha_file(context["mount"] / logical.lstrip("/")) == entry["sha256"], "latest bytes differ")
            latest = discovery(context, logical, entry["format"], templates[entry["format"]])
            require(all(r["version"] == binding["version"] for r in latest), "latest selected an older version")
            all_rows = all_versions(context, logical, entry["format"], templates[entry["format"]])
            wanted = {b["version"] for b in report["bindings"] if b["logical_path"] == logical}
            require(len(all_rows) == len(wanted) * 4 and
                    {r["version"] for r in all_rows} == wanted, "all-version native inventory differs")
            binding["scientific_history_count"] = len(wanted)
        if units_verified % 64 == 0:
            report["audit_progress_units"] = units_verified
            checkpoint()
    # Query the regular latest subtree and the original six-attribute predicate.
    checks = []
    for scenario in corpus.SCENARIOS:
        for fmt, template in templates.items():
            prefix = report["logical_prefix"] + f"/{scenario}/{fmt}/raw"
            for predicate in (False, True):
                response, batches = call(context, paired.discovery_request(prefix, template, predicate))
                rows = paired.discovered(response, batches, fmt, prefix)
                require(len(rows) == (2 if predicate else 32), "latest N/K/D query differs")
                checks.append({"scenario": scenario, "format": fmt, "predicate": predicate,
                               "rows": len(rows), "paths": len({r["path"] for r in rows})})
    report["acceptance"] = {"restart": True, "full_file_versions_verified": len(entries),
                            "typed_units_verified": units_verified, "latest_queries": checks}


def runtime(environment_path, source_pid, database):
    require(database == DATABASE, "use the existing idle multi-file experimental database")
    environment = json.loads(Path(environment_path).read_text())
    daemon = Path(environment["daemon"])
    require(sha_file(daemon) == environment["binaries"]["daemon"]["sha256"], "daemon identity changed")
    require(h5_runtime() == environment["python_versions"], "offline Python library identities differ")
    env = dict(os.environ, **connection_environment(source_pid, database))
    env["LD_LIBRARY_PATH"] = environment["runtime_library_path"]
    return environment, daemon, env


def h5_runtime():
    import h5py
    import pyarrow
    return {"h5py_extension": str(Path(h5py.h5.__file__).resolve()),
            "h5py_version": h5py.__version__, "hdf5_version": h5py.version.hdf5_version,
            "numpy_version": np.__version__, "pyarrow_extension": str(Path(pyarrow.lib.__file__).resolve()),
            "pyarrow_version": pyarrow.__version__}


def run(args):
    # IPC replies contain absolute paths; all owned-mount checks use that same form.
    for name in ("input", "environment", "work", "report", "writer_lock"):
        setattr(args, name, getattr(args, name).resolve())
    raw = corpus.verify(args.input)
    manifest = corpus.load(args.input)
    environment, daemon, env = runtime(args.environment, args.source_pid, args.database)
    args.work.mkdir(parents=True, exist_ok=False)
    if args.command == "publish":
        require(not args.report.exists(), "publication report already exists; use verify or resume")
        report = {"protocol": "iotdbfs-cross-version-publication/1", "complete": False,
            "database": args.database, "logical_prefix": "/cross-version-" + uuid.uuid4().hex[:16],
            "corpus": str(args.input.resolve()), "corpus_sha256": manifest["manifest_sha256"],
            "environment": str(args.environment.resolve()), "environment_sha256": sha_file(args.environment),
            "daemon_sha256": sha_file(daemon), "raw": raw, "bindings": [],
            "scope": "offline setup/acceptance; timings are not query performance or compression results",
            "storage": "existing base inputs/runtime reused; backend stores real published versions",
            "credentials_recorded": False}
        args.report.parent.mkdir(parents=True, exist_ok=True)
    else:
        report = json.loads(args.report.read_text())
        require(report.get("protocol") == "iotdbfs-cross-version-publication/1" and
                report["corpus_sha256"] == manifest["manifest_sha256"] and
                report["daemon_sha256"] == sha_file(daemon) and report["database"] == args.database and
                report["environment_sha256"] == sha_file(args.environment), "publication identities changed")
        if args.command == "resume":
            require(report["complete"] is False, "complete publication needs verification, not resumption")
        report["complete"] = False

    def checkpoint():
        atomic_json(args.report, report)

    checkpoint()
    if args.command in ("publish", "resume"):
        templates = {fmt: load_manifest(base["root"]) for fmt, base in manifest["bases"].items()}
        with sf.private_mount(daemon, args.work / "publisher", env, args.writer_lock, profile=False) as context:
            prior = {}
            committed = len(report["bindings"])
            require(committed <= len(manifest["entries"]), "too many recorded bindings")
            for position, entry in enumerate(manifest["entries"]):
                logical = report["logical_prefix"] + f"/{entry['scenario']}/{entry['format']}/{entry['logical_file']}"
                destination = context["mount"] / logical.lstrip("/")
                key = {k: entry[k] for k in ("format", "scenario", "file_index", "ordinal")}
                if position < committed:
                    binding = report["bindings"][position]
                    require(binding["key"] == key and binding["logical_path"] == logical and
                            binding["sha256"] == entry["sha256"] and
                            binding["version"] > prior.get(logical, 0), "recorded publication order differs")
                    require(sha_file(history_path(context["mount"], logical, binding["version"])) == entry["sha256"],
                            "recorded historical publication changed")
                    prior[logical] = binding["version"]
                    continue
                recovered = False
                if args.command == "resume" and destination.exists():
                    history = all_versions(context, logical, entry["format"], templates[entry["format"]])
                    known = {b["version"] for b in report["bindings"] if b["logical_path"] == logical}
                    actual = {r["version"] for r in history}
                    require(known <= actual and len(history) == len(actual) * 4,
                            "cannot reconcile recorded version history")
                    unknown = actual - known
                    require(len(unknown) <= 1, "more than one unrecorded publication; inspect before resuming")
                    if unknown:
                        version = unknown.pop()
                        require(version > prior.get(logical, 0) and version == max(actual) and
                                sha_file(history_path(context["mount"], logical, version)) == entry["sha256"],
                                "unrecorded publication differs from next planned version")
                        recovered = True
                    else:
                        require(entry["ordinal"] > 1 and actual == known,
                                "unexpected existing destination before first publication")
                elapsed = None if recovered else write_version(corpus.resolve(args.input, manifest, entry), destination, entry["ordinal"] == 1)
                rows = discovery(context, logical, entry["format"], templates[entry["format"]])
                versions = {row["version"] for row in rows}
                require(len(rows) == 4 and len(versions) == 1, "published native directory differs")
                version = versions.pop()
                require(type(version) is int and version > prior.get(logical, 0), "version did not advance")
                require(sha_file(destination) == entry["sha256"], "latest publication bytes differ")
                prior[logical] = version
                report["bindings"].append({"key": key,
                    "logical_path": logical, "version": version, "sha256": entry["sha256"],
                    "file_fsync_close_ns": elapsed, "recovered_unrecorded_publication": recovered})
                checkpoint()
    with sf.private_mount(daemon, args.work / "restart-audit", env, args.writer_lock, profile=False) as context:
        audit(context, args.input, manifest, report, checkpoint)
    # Re-read the external bases and local files after publication as well.
    report["raw_post_audit"] = corpus.verify(args.input)
    report["complete"] = True
    report["report_identity"] = identity({"corpus": report["corpus_sha256"], "bindings": report["bindings"]})
    checkpoint()
    print(json.dumps({"complete": True, "report": str(args.report.resolve()),
                      "logical_prefix": report["logical_prefix"], "bindings": len(report["bindings"])}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["publish", "resume", "verify"])
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--environment", type=Path, required=True)
    parser.add_argument("--writer-lock", type=Path, required=True)
    parser.add_argument("--source-pid", type=int, default=1001546)
    parser.add_argument("--database", default=DATABASE)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--work", type=Path, required=True)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
