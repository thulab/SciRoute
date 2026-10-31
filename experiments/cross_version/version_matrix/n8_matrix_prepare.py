#!/usr/bin/env python3
"""Prepare isolated N8 version cells using the caller's original-instance window.

This helper does not start or configure IoTDB. The caller verifies the frozen
binaries/raw corpus and supplies a private_mount wrapper with its startup limit.
Recovery and measured queries belong to the main runner, after all preparation.
"""
from __future__ import annotations

import json
import os
from pathlib import Path, PurePosixPath
import subprocess
import time
import uuid

from experiments.cross_version.pilot import corpus
from experiments.cross_version.pilot import publish
from experiments.cross_version.version_directory import v03_n128 as prior
from common import load_manifest, sha_file
from experiments.cross_version.pilot.corpus import require
from experiments.cross_version.version_directory.v04_n128 import preparation_config


VERSIONS = (1, 2, 4, 8)
SYSTEMS = ("V03", "V04")
FORMATS = ("hdf5", "grib")


def _private_output(path):
    return os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w")


def _probe(binary, command, prefix, env, directory):
    """Keep SDK diagnostics private; return only the probe's structured result."""
    stdout, stderr = directory / (command + ".stdout"), directory / (command + ".stderr")
    started = time.perf_counter_ns()
    with _private_output(stdout) as out, _private_output(stderr) as err:
        result = subprocess.run([str(binary), command, "--prefix", prefix], env=env,
            stdin=subprocess.DEVNULL, stdout=out, stderr=err, text=True, timeout=600)
    elapsed = time.perf_counter_ns() - started
    require(result.returncode == 0, "matrix native probe failed; inspect private probe logs")
    objects = [json.loads(line) for line in stdout.read_text().splitlines() if line.startswith("{")]
    require(len(objects) == 1 and (objects[0].get("completed") or command == "head"),
            "matrix native probe JSON missing/incomplete")
    return objects[0], {"wall_ns": elapsed, "binary": str(binary),
        "binary_sha256": sha_file(binary), "stdout": str(stdout),
        "stdout_sha256": sha_file(stdout), "stderr": str(stderr),
        "stderr_sha256": sha_file(stderr)}


def _selected(manifest, scenario, versions):
    entries = [entry for entry in manifest["entries"]
               if entry["scenario"] == scenario and entry["ordinal"] <= versions]
    expected = {(fmt, index, ordinal) for fmt in FORMATS
                for index in range(8) for ordinal in range(1, versions + 1)}
    observed = [(entry["format"], entry["file_index"], entry["ordinal"]) for entry in entries]
    require(len(observed) == len(expected) and set(observed) == expected,
            "matrix requires all eight paths and every ordinal for both formats")
    previous, names = {}, {}
    for entry in entries:
        key = entry["format"], entry["file_index"]
        name = PurePosixPath(entry["logical_file"])
        require(not name.is_absolute() and name.parts and
                all(part not in ("", ".", "..") for part in entry["logical_file"].split("/")),
                "unsafe matrix logical file")
        require(names.setdefault(key, str(name)) == str(name), "logical path changes across ordinals")
        require(entry["ordinal"] == previous.get(key, 0) + 1,
                "manifest order does not preserve each path's ordinal sequence")
        require(len(entry["units"]) == 4, "matrix requires four scientific units per version")
        previous[key] = entry["ordinal"]
    require(len(set((fmt, name) for (fmt, _), name in names.items())) == 16,
            "distinct file indexes alias a matrix path")
    return entries


def prepare_all(args, report, env, runner, manifest, checkpoint):
    """Append 16 cell records; checkpoint is a no-argument caller closure.

    Each record retains init/recover launchers and the actual scientific version
    returned by format-filtered discovery. Empty create/truncate snapshots never
    become ordinal bindings. This operation deliberately has no automatic resume:
    a partial report must be reconciled before any further writes.
    """
    require(env.get("IOTDB_DB") == prior.DATABASE, "matrix must use the existing experiment database")
    require(not report.get("namespaces"), "matrix preparation requires an empty namespace list")
    work = Path(args.work).resolve() / "preparation"
    require(not work.exists(), "matrix preparation directory already exists; refuse overwrite/resume")
    cells = [(scenario, versions, system) for scenario in corpus.SCENARIOS
             for versions in VERSIONS for system in SYSTEMS]
    selections = {(scenario, versions): _selected(manifest, scenario, versions)
                  for scenario in corpus.SCENARIOS for versions in VERSIONS}
    templates = {fmt: load_manifest(manifest["bases"][fmt]["root"]) for fmt in FORMATS}
    work.mkdir(mode=0o700)
    report["namespaces"] = []
    report["preparation_order"] = [{"system": system, "scenario": scenario, "V": versions}
                                    for scenario, versions, system in cells]
    report["preparation_scope"] = {"paths_per_format": 8, "formats": list(FORMATS),
        "namespaces": 16, "scientific_publications": 960, "raw_copies_created": False,
        "new_databases": False, "order": "scenario, V=1/2/4/8, alternating V03/V04",
        "recovery_audit": "caller must hash every exact scientific binding in a new process before queries"}
    checkpoint()
    identities = set()
    for scenario, versions, system in cells:
        started = time.perf_counter_ns()
        root = work / (system.lower() + "-" + scenario + "-v" + str(versions))
        root.mkdir(mode=0o700)
        journal = root / "owner"
        require(not journal.exists(), "matrix journal must be new")
        prefix = "vd_" + system.lower() + "_matrix_" + uuid.uuid4().hex[:16]
        daemon = Path(getattr(args, "daemon_" + system.lower())).resolve()
        probe = Path(getattr(args, "probe_" + system.lower())).resolve()
        init = prior.launcher(root / "initialize-launcher", daemon, prefix, journal, True)
        recover = prior.launcher(root / "recovery-launcher", daemon, prefix, journal, False)
        cell = {"system": system, "scenario": scenario, "V": versions, "prefix": prefix,
            "journal": str(journal), "logical_prefix": "/matrix", "bindings": [],
            "init_launch": init, "recover_launch": recover, "prepared": False,
            "root": str(root), "probe_receipts": {}}
        report["namespaces"].append(cell)
        checkpoint()
        with runner.sf.private_mount(Path(init["path"]), root / "initialize",
                                     env, args.writer_lock) as context:
            cell["initialization"] = {"startup_ns": context["startup_ns"],
                **preparation_config(context, init, root / "initialize.properties")}
            last = {}
            for entry in selections[scenario, versions]:
                logical = "/matrix/" + entry["format"] + "/" + entry["logical_file"]
                destination = context["mount"] / logical.lstrip("/")
                elapsed = publish.write_version(corpus.resolve(args.input, manifest, entry),
                                                destination, entry["ordinal"] == 1)
                binding_started = time.perf_counter_ns()
                rows = publish.discovery(context, logical, entry["format"], templates[entry["format"]])
                actual = {row["version"] for row in rows}
                require(len(rows) == 4 and len(actual) == 1 and
                        all(row["path"] == logical for row in rows), "matrix scientific directory differs")
                version = actual.pop()
                require(type(version) is int and version > last.get(logical, 0),
                        "matrix scientific version failed to advance")
                require(sha_file(destination) == entry["sha256"], "matrix latest publication bytes differ")
                last[logical] = version
                cell["bindings"].append({key: entry[key] for key in
                    ("format", "logical_file", "file_index", "ordinal", "sha256") } | {
                    "logical_path": logical, "version": version, "publication_ns": elapsed,
                    "binding_and_latest_sha_ns": time.perf_counter_ns() - binding_started})
                checkpoint()
            cell["head"], cell["probe_receipts"]["head"] = _probe(probe, "head", prefix, env, root)
            cell["objects"], cell["probe_receipts"]["stats"] = _probe(probe, "stats", prefix, env, root)
            identity = cell["head"]["namespace"], cell["head"]["incarnation"]
            require(identity not in identities, "matrix cells share a namespace identity")
            identities.add(identity)
            if system == "V04" and getattr(args, "payload_probe_v04", None):
                cell["payload_inspection"], cell["probe_receipts"]["inspect"] = _probe(
                    Path(args.payload_probe_v04), "inspect", prefix, env, root)
            checkpoint()
        cell["preparation_wall_ns"] = time.perf_counter_ns() - started
        cell["publication_ns"] = sum(binding["publication_ns"] for binding in cell["bindings"])
        cell["prepared"] = True
        checkpoint()
    require(len(report["namespaces"]) == 16 and
            sum(len(cell["bindings"]) for cell in report["namespaces"]) == 960,
            "matrix preparation is incomplete")
