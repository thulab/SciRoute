#!/usr/bin/env python3
"""Offline N128 owner preparation and frozen-runner three-arm V03 ablation.

Reuses raw/FS/SQLite files in place. Only a new schema2 owner prefix is written.
All clocks in small_runner/native remain frozen; migration/startup costs are
reported separately. Uses the authorized original instance and shared account.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import copy
import fcntl
import json
import os
from pathlib import Path
import re
import signal
import statistics
import subprocess
import sys
import time
from types import SimpleNamespace
import uuid

from experiments.cross_version.version_directory.v00_head_probe import ExistingInstance, save_json, public_failure
from experiments.cross_version.version_directory.v03_startup_wrapper import (RUNNER_SHA, NATIVE_SHA, sha, require,
                                 load_runner, adjust_mount)

DATABASE = "h09probe1790939889dbfc1f"
CONTROLS = {"N": 128, "K": 2, "D": 4, "depth": 2, "seed": 20261013, "tree_width": 4}
BEST_A = {"hdf5": 106.811351, "grib": 57.404444}
V02_SHA = "f61c283e16ff01eafcab6698714211b3b476c6380dc3d88d0e33bb41ae859bdc"
LEGACY_A_SHA = "e1460750fbee36070786801355d8e241137a69a239fa2940f34ede90f12e7db1"


def checkpoint(args, report):
    save_json(args.report, report)
    args.report.chmod(0o600)


@contextmanager
def sync_window(args, report):
    require(sys.platform == "linux" and os.geteuid() == 0, "Linux/root required")
    require(args.release == (args.runtime / "current").resolve(), "original release mismatch")
    control = args.work / "control"
    control.mkdir(mode=0o700)
    def event(kind, **fields):
        with (control / "operations.jsonl").open("a") as stream:
            stream.write(json.dumps({"time_ns": time.time_ns(), "event": kind, **fields}, sort_keys=True) + "\n")
            stream.flush(); os.fsync(stream.fileno())
        print(json.dumps({"event": kind}), flush=True)
    lock = (args.runtime / "run/v00-head-experiment.lock").open("a+b")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    instance = None
    old_handlers = {}
    try:
        instance = ExistingInstance(SimpleNamespace(runtime=args.runtime, release=args.release,
            work=control, datanode_pid=args.datanode_pid), event)
        def interrupted(signum, frame):
            raise RuntimeError("V03 interrupted; restoring original service")
        for signum in (signal.SIGTERM, signal.SIGHUP):
            old_handlers[signum] = signal.signal(signum, interrupted)
        instance.enable_sync()
        report["sync_datanode_pid"] = instance.pid
        checkpoint(args, report)
        yield
    finally:
        try:
            if instance:
                instance.restore()
                report["original_service_restored"] = True
                report["final_datanode_pid"] = instance.pid
                report["restored_config_sha256"] = sha(instance.config)
        finally:
            for signum, handler in old_handlers.items():
                signal.signal(signum, handler)
            lock.close()
            checkpoint(args, report)


def environment(args, runner):
    descriptor = json.loads(args.environment.read_text())
    require(sha(descriptor["daemon"]) == descriptor["binaries"]["daemon"]["sha256"],
            "reference runtime daemon SHA differs")
    require(sha(args.native) == NATIVE_SHA, "frozen native reader SHA differs")
    env = dict(os.environ, **runner.connection_environment(args.source_pid, DATABASE))
    require(env["IOTDB_PORT"] == "33667" and env["IOTDB_HOST"] in ("127.0.0.1", "localhost"),
            "original controlled endpoint differs")
    env["LD_LIBRARY_PATH"] = descriptor["runtime_library_path"]
    env.pop("IOTDBFS_EXPERIMENT_FROZEN_TYPED", None)
    return descriptor, env


def launcher(path, daemon, owner_prefix=None, journal=None, initialize=False):
    settings = {"query.hdf5.catalog_mode": "substructure-index"}
    if owner_prefix:
        settings.update({"storage.version_directory.mode": "owner",
            "storage.version_directory.table_prefix": owner_prefix,
            "storage.version_directory.owner_directory": str(journal),
            "storage.version_directory.initialize": str(initialize).lower()})
    # Configuration and exec run before mount readiness and before query clocks.
    source = '''#!/usr/bin/python3
from pathlib import Path
import hashlib,json,os,sys
exe=Path(DAEMON)
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
if sha(exe)!=DAEMON_SHA:raise RuntimeError("daemon SHA changed")
config=Path(os.environ["IOTDB_CONFIG_FILE"])
with config.open("a") as stream:
    for key,value in SETTINGS.items():stream.write(key+"="+value+"\\n")
config.with_suffix(".launch.json").write_text(json.dumps({"actual_binary":str(exe),"actual_binary_sha256":DAEMON_SHA,"settings":SETTINGS,"config_sha256":sha(config),"pid":os.getpid(),"before_query_clock":True},sort_keys=True)+"\\n")
os.execv(str(exe),[str(exe),*sys.argv[1:]])
'''
    source = "DAEMON=" + repr(str(daemon)) + "\nDAEMON_SHA=" + repr(sha(daemon)) + "\nSETTINGS=" + repr(settings) + "\n" + source.split("\n", 1)[1]
    path.write_text("#!/usr/bin/python3\n" + source)
    path.chmod(0o700)
    return {"path": str(path), "sha256": sha(path), "binary": str(daemon),
            "binary_sha256": sha(daemon), "settings": settings}


def descriptor_for(original, launch, path):
    result = copy.deepcopy(original)
    result["daemon"] = launch["path"]
    result["binaries"]["daemon"].update(binary=launch["path"], sha256=launch["sha256"])
    result["v03_real_daemon"] = launch
    save_json(path, result)
    return result


def probe(args, env, prefix, command):
    stem = args.work / "control" / ("probe-" + str(time.time_ns()))
    with stem.with_suffix(".stderr").open("xb") as log:
        result = subprocess.run([str(args.probe), command, "--prefix", prefix], env=env,
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=log, text=True, timeout=300)
    stem.with_suffix(".stdout").write_text(result.stdout)
    require(result.returncode == 0, "native stats/head probe failed; inspect private log")
    objects = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
    require(len(objects) == 1 and (objects[0].get("completed") or command == "head"), "native probe JSON missing")
    return objects[0]


def raw_inventory(runner, corpus, report, baseline_path):
    require(report.get("complete") and report.get("controls") == CONTROLS, "N128 baseline controls differ")
    for fmt in ("hdf5", "grib"):
        observations = [r["total_ns"] for r in report["records"] if r["format"] == fmt and r["arm"] == "iotdb" and r["phase"] == "discover-values" and r["repeat"] > 0]
        require(len(observations) == 12 and statistics.median(observations) / 1e6 == BEST_A[fmt], "baseline report is not the fixed historical best A")
    require(report["code_sha256"] == RUNNER_SHA and report["native_sha256"] == NATIVE_SHA,
            "baseline frozen code identities differ")
    require(Path(report["data_work"]).resolve() == corpus, "raw/FS/index must remain at original corpus location")
    manifests, files = {}, []
    for fmt in ("hdf5", "grib"):
        root = corpus / (fmt + "-data")
        runner.manager.verify(root)
        manifest = runner.load_manifest(root)
        frozen_manifest = baseline_path.parent / (fmt + "-data-manifest.json")
        require(manifest == json.loads(frozen_manifest.read_text()), "raw manifest differs from frozen A evidence")
        require(all(manifest["spec"][key] == value for key, value in CONTROLS.items()), "raw corpus controls differ")
        for source in manifest["sources"]:
            for path in (root / source["file"], corpus / (fmt + "-fs") / source["file"]):
                require(sha(path) == source["sha256"], "raw or original FS byte digest differs")
                files.append({"path": str(path), "sha256": source["sha256"], "bytes": path.stat().st_size})
        index = corpus / (fmt + "-index.sqlite")
        files.append({"path": str(index), "sha256": sha(index), "bytes": index.stat().st_size})
        manifests[fmt] = manifest
    return manifests, files


def mapping(runner, context, manifest, prefix):
    response, batches, stats, offset = runner.profile_call(context,
        runner.discovery_request(prefix, manifest, False), context.get("v03_profile_offset", 0))
    context["v03_profile_offset"] = offset
    rows = runner.discovered(response, batches, manifest["format"], prefix)
    versions = {row["path"]: row["version"] for row in rows}
    require(len(rows) == 512 and len(versions) == 128, "new owner N128 inventory differs")
    return versions, stats


def prepare(args, runner, descriptor, env, report):
    baseline = json.loads(args.baseline_report.read_text())
    manifests, inputs = raw_inventory(runner, args.corpus, baseline, args.baseline_report)
    require(not args.owner_journal.exists(), "new owner journal must not exist")
    require(re.fullmatch(r"vd_v03_[a-z0-9_]+", args.owner_prefix), "use a unique vd_v03_* table prefix")
    require(sha(args.daemon) == V02_SHA, "preparation requires the pinned verified V02 binary")
    logical = "/multi-file-v03-" + uuid.uuid4().hex[:16]
    report.update(logical_prefix=logical, owner_prefix=args.owner_prefix,
        owner_journal=str(args.owner_journal), original_corpus=str(args.corpus),
        inherited_baseline_report=str(args.baseline_report), inherited_baseline_sha256=sha(args.baseline_report),
        unchanged_inputs=inputs, new_raw_generation=False, raw_FS_copy=False, new_FS_index_build=False,
        initialization={}, publication={}, preparation=copy.deepcopy(baseline["preparation"]))
    init = launcher(args.work / "initialize-launcher", args.daemon, args.owner_prefix, args.owner_journal, True)
    recover = launcher(args.work / "recovery-launcher", args.daemon, args.owner_prefix, args.owner_journal, False)
    report["launchers"] = {"initialize": init, "recovery": recover}
    adjust_mount(runner.sf, args.startup_timeout, args.work / "startup-receipts.jsonl")
    checkpoint(args, report)
    with runner.sf.private_mount(Path(init["path"]), args.work / "initialize", env, args.writer_lock) as context:
        report["initialization"] = {"startup_ns": context["startup_ns"],
            "head": probe(args, env, args.owner_prefix, "head"),
            "objects": probe(args, env, args.owner_prefix, "stats")}
        for fmt, manifest in manifests.items():
            started = time.perf_counter_ns()
            writes = []
            for source in manifest["sources"]:
                raw = args.corpus / (fmt + "-data") / source["file"]
                destination = context["mount"] / logical.lstrip("/") / fmt / source["file"]
                destination.parent.mkdir(parents=True, exist_ok=True)
                require(not destination.exists(), "new owner destination already exists")
                writes.append(runner.sf.checked_copy(raw, destination))
            elapsed = time.perf_counter_ns() - started
            versions, stats = mapping(runner, context, manifest, logical + "/" + fmt + "/raw")
            report["publication"][fmt] = {"files": writes, "wall_ns_including_directory_creation": elapsed,
                "source_files": 128, "version_binding_outside_publication_clock": stats,
                "fs_costs": "inherited original absolute FS root/index; no copy/rebuild"}
            report["preparation"][fmt]["versions"] = versions
            report["preparation"][fmt]["publication"] = {"iotdb": report["publication"][fmt],
                "fs": baseline["preparation"][fmt]["publication"]["fs"]}
            checkpoint(args, report)
        report["published_head"] = probe(args, env, args.owner_prefix, "head")
        report["published_objects"] = probe(args, env, args.owner_prefix, "stats")
    with runner.sf.private_mount(Path(recover["path"]), args.work / "recovery", env, args.writer_lock) as context:
        report["recovery"] = {"startup_ns": context["startup_ns"], "full_file_hashes": 0}
        for fmt, manifest in manifests.items():
            versions, _ = mapping(runner, context, manifest, logical + "/" + fmt + "/raw")
            require(versions == report["preparation"][fmt]["versions"], "recovered exact version mapping changed")
            for source in manifest["sources"]:
                destination = context["mount"] / logical.lstrip("/") / fmt / source["file"]
                require(sha(destination) == source["sha256"], "recovered full file digest differs")
                report["recovery"]["full_file_hashes"] += 1
            oracle, offset = runner.iotdb_query(context, manifest, logical + "/" + fmt + "/raw",
                versions, context["v03_profile_offset"], True)
            context["v03_profile_offset"] = offset
            report["recovery"][fmt + "_oracle"] = oracle
        require(probe(args, env, args.owner_prefix, "head") == report["published_head"], "recovery/audit mutated head")
    report["data_work"] = str(args.corpus)
    report["native_sha256"] = NATIVE_SHA
    report["daemon_sha256"] = recover["sha256"]
    # This report is also a runner-compatible owner preparation locator.
    report["prepared"] = report["complete"] = True
    verify_inputs(report["unchanged_inputs"])


def verify_inputs(files):
    for item in files:
        require(Path(item["path"]).stat().st_size == item["bytes"] and sha(item["path"]) == item["sha256"],
                "an inherited raw/FS/SQLite input changed")


def arm_locator(base, path, launch, owner, prepared):
    locator = copy.deepcopy(prepared if owner else base)
    locator["daemon_sha256"] = launch["sha256"]
    locator["v03_locator_provenance"] = {"launcher_sha256": launch["sha256"],
        "actual_binary_sha256": launch["binary_sha256"], "owner": owner,
        "costs": "inherited preparation; no generation/publication/index build in timed arm"}
    save_json(path, locator)


def collect_arm(root, launch):
    receipts = []
    for path in sorted((root / "complete-query").rglob("profile.launch.json")):
        value = json.loads(path.read_text())
        require(value["actual_binary_sha256"] == launch["binary_sha256"] and
            value["actual_binary"] == launch["binary"] and value["settings"] == launch["settings"],
            "actual timed daemon/configuration differs")
        receipts.append({"path": str(path), **value})
    require(len(receipts) == 7, "expected publisher validation plus six fresh campaign mounts")
    source = root / "complete-results/report.json"
    source_sha = sha(source)
    observed = json.loads(source.read_text())
    require(observed["complete"] and observed["code_sha256"] == RUNNER_SHA and
        observed["controls"] == CONTROLS and len(observed["records"]) == 180, "incomplete frozen campaign")
    store_keys = ("version_store.sql_queries", "version_store.head_reads",
                  "version_store.object_reads", "version_store.blob_bytes")
    store_delta_keys = ("version_store_query_calls", "version_store_head_reads",
                        "version_store_object_reads", "version_store_blob_bytes")
    proof_keys = ("dataset_catalog_manifest_revalidation.calls", "dataset_catalog_marker_read.calls",
                  "dataset_catalog_verification_read.calls")

    def numeric(value):
        return isinstance(value, (int, float)) and not isinstance(value, bool)

    def summarize(values):
        present = [v for v in values if numeric(v)]
        return {"available_samples": len(present), "expected_samples": len(values),
            "median": statistics.median(present) if present else None,
            "minimum": min(present) if present else None, "maximum": max(present) if present else None}

    def call_profile(call):
        profile = call.get("server_profile", {})
        counters = profile.get("counters", {})
        admissions = sum(counters.get(key, 0) for key in
            ("version_context.admissions", "read_context.admissions"))
        context_instrumented = admissions > 0
        delta = profile.get("iotdb_delta", {})
        store_instrumented = all(numeric(delta.get(key)) for key in store_delta_keys)
        return {"tested_requests": 1, "context_instrumented": context_instrumented,
            "store_instrumented": store_instrumented,
            "admission_increments": admissions,
            "legacy_sdk_query_calls": delta.get("query_calls"),
            # Daemon-wide deltas include worker submissions without a thread-
            # local profile. Presence in this measured scope establishes
            # availability; older binaries' absent fields never mean zero.
            "store": {key: delta.get(field) if numeric(delta.get(field)) else None
                for key, field in zip(store_keys, store_delta_keys)},
            "thread_local_store_diagnostic": {key: counters.get(key) for key in store_keys},
            "independent_manifest_marker_calls": {
                key: counters.get(key, 0) if context_instrumented else counters.get(key) for key in proof_keys},
            "server_total_ns": profile.get("server_total_ns"),
            "stages_ns": profile.get("stages_ns", {})}

    def aggregate(calls):
        profiles = [call_profile(call) for call in calls]
        def summed(values):
            return sum(values) if values and all(numeric(value) for value in values) else None
        sdk = summed([p["legacy_sdk_query_calls"] for p in profiles])
        store = {key: summed([p["store"][key] for p in profiles]) for key in store_keys}
        stages = {}
        for key in sorted({key for p in profiles for key in p["stages_ns"]}):
            # Sum this stage within the request group before computing warm
            # medians; nested/overlapping stages are never added to each other.
            stages[key] = sum(p["stages_ns"].get(key, 0) for p in profiles)
        # These are disjoint client paths: IoTDBClient's legacy atomic counter
        # and IoTDBVersionStore's direct TableSession submission counter.
        total_sql = sdk + store[store_keys[0]] if sdk is not None and store[store_keys[0]] is not None else None
        if profiles and launch["binary_sha256"] == LEGACY_A_SHA and sdk is not None:
            total_sql = sdk  # Pinned historical binary contains no owner Store path.
        return {"tested_requests": len(profiles),
            "store_instrumentation": ("instrumented" if profiles and all(p["store_instrumented"] for p in profiles)
                else "not_requested" if not profiles else "unavailable"),
            "context_instrumented_requests": sum(p["context_instrumented"] for p in profiles),
            "admission_increments": sum(p["admission_increments"] for p in profiles),
            "legacy_sdk_query_calls": sdk, "store": store, "observed_total_query_submissions": total_sql,
            "thread_local_store_diagnostic": {key: summed([
                p["thread_local_store_diagnostic"][key] for p in profiles]) for key in store_keys},
            "independent_manifest_marker_calls": {
                key: summed([p["independent_manifest_marker_calls"][key] for p in profiles]) for key in proof_keys},
            "server_total_ns": summed([p["server_total_ns"] for p in profiles]), "stages_ns": stages}

    cells, medians = {}, {}
    for fmt in ("hdf5", "grib"):
        cells[fmt] = {}
        for phase in ("discover", "discover-values"):
            records = [r for r in observed["records"] if r["arm"] == "iotdb" and
                r["format"] == fmt and r["phase"] == phase and r["repeat"] > 0]
            require(len(records) == 12 and {(r["campaign"], r["repeat"]) for r in records} ==
                {(campaign, repeat) for campaign in range(3) for repeat in range(1, 5)},
                "warm metric must contain all twelve distinct observations")
            samples = []
            for row in records:
                discovery, values = [row["discovery"]], row.get("value_calls", [])
                samples.append({"campaign": row["campaign"], "repeat": row["repeat"],
                    "client_ns": {"discovery": row.get("discovery_client_ns"),
                        "value": row.get("selected_value_client_ns"), "total": row["total_ns"]},
                    "profile": {"discovery": aggregate(discovery), "value": aggregate(values),
                        "total": aggregate(discovery + values)}})
            summary = {"client_ns": {key: summarize([sample["client_ns"][key] for sample in samples])
                for key in ("discovery", "value", "total")}, "profile": {}}
            for scope in ("discovery", "value", "total"):
                profiles = [sample["profile"][scope] for sample in samples]
                stage_names = sorted({key for profile in profiles for key in profile["stages_ns"]})
                summary["profile"][scope] = {
                    "tested_requests": sum(p["tested_requests"] for p in profiles),
                    "store_instrumentation": ("instrumented" if all(p["store_instrumentation"] == "instrumented" for p in profiles)
                        else "not_requested" if all(p["tested_requests"] == 0 for p in profiles) else "unavailable"),
                    "legacy_sdk_query_calls": summarize([p["legacy_sdk_query_calls"] for p in profiles]),
                    "store": {key: summarize([p["store"][key] for p in profiles]) for key in store_keys},
                    "thread_local_store_diagnostic": {key: summarize([
                        p["thread_local_store_diagnostic"][key] for p in profiles]) for key in store_keys},
                    "observed_total_query_submissions": summarize([p["observed_total_query_submissions"] for p in profiles]),
                    "independent_manifest_marker_calls": {key: summarize([
                        p["independent_manifest_marker_calls"][key] for p in profiles]) for key in proof_keys},
                    "server_total_ns": summarize([p["server_total_ns"] for p in profiles]),
                    "stages_ns": {key: summarize([p["stages_ns"].get(key) for p in profiles]) for key in stage_names}}
            discovery_profiles = [sample["profile"]["discovery"] for sample in samples]
            tested = all(p["store_instrumentation"] == "instrumented" for p in discovery_profiles)
            markers_available = all(numeric(p["independent_manifest_marker_calls"][key])
                for p in discovery_profiles for key in proof_keys)
            summary["discovery_scope_evidence"] = {
                "requests_tested": 12, "context_instrumented_requests": sum(
                    p["context_instrumented_requests"] for p in discovery_profiles),
                "daemon_store_instrumented_requests": sum(
                    p["store_instrumentation"] == "instrumented" for p in discovery_profiles),
                "independent_manifest_marker_calls_all_zero": all(
                    p["independent_manifest_marker_calls"][key] == 0
                    for p in discovery_profiles for key in proof_keys) if markers_available else None,
                "all_observed_sdk_and_store_queries_zero": all(
                    p["observed_total_query_submissions"] == 0 for p in discovery_profiles)
                    if tested and all(numeric(p["observed_total_query_submissions"]) for p in discovery_profiles) else None,
                "scope": "discovery request profiles only; these statements do not assert zero payload/value SQL"}
            cells[fmt][phase] = {"observations": samples, "summary": summary}
            if phase == "discover-values":
                medians[fmt] = statistics.median(row["total_ns"] for row in records) / 1e6
    analysis = {"protocol": "iotdbfs-v03-frozen-warm-profile-analysis/1", "source_report": str(source),
        "source_report_sha256": source_sha, "fixed_A_ms": BEST_A, "raw_observations": 180,
        "warm_observations_per_format_phase": 12, "cells": cells,
        "semantics": {
            "timing": "unchanged client total_ns; all twelve repeat>0 observations; no runner or clock changes",
            "profile_stages": "per-observation same-stage sums then medians; stages overlap and cannot be added",
            "missing_stages": "missing stage observations remain unavailable; available_samples is explicit",
            "legacy_sdk": "IoTDBClient daemon-wide query_calls delta; includes value/payload SQL when issued",
            "store": "daemon-wide iotdb_delta version_store_query_calls/head_reads/object_reads/blob_bytes; includes worker submissions; scope field presence proves availability",
            "total_query_submissions": "legacy query_calls plus daemon-wide version_store_query_calls; disjoint SDK paths, including value/payload SQL when issued",
            "thread_local_store_diagnostic": "optional thread-local counters only; never the authoritative Store total or proof of zero",
            "unavailable": "V02/legacy lack V03 Store instrumentation: null with zero available_samples, never inferred zero",
            "payload": "value-request profiles remain separate; no global zero-payload-SQL claim"}}
    analysis_path = root / "warm-analysis.json"
    save_json(analysis_path, analysis)
    require(sha(source) == source_sha, "analysis modified the frozen raw report")
    return {"report_sha256": source_sha, "warm_discover_values_ms": medians, "launches": receipts,
        "warm_profile_analysis": {"path": str(analysis_path), "sha256": sha(analysis_path),
            "summary": {fmt: {phase: cells[fmt][phase]["summary"] for phase in cells[fmt]} for fmt in cells}},
        "raw_profile_evidence": "complete-results/report.json records.discovery/value_calls.server_profile"}


def ablate(args, runner, descriptor, env, report):
    prepared = json.loads(args.prepared.read_text())
    base = json.loads(args.baseline_report.read_text())
    require(prepared.get("complete") and prepared["controls"] == CONTROLS, "owner preparation incomplete")
    raw_inventory(runner, Path(prepared["data_work"]), base, args.baseline_report)
    verify_inputs(prepared["unchanged_inputs"])
    require(sha(args.v02_daemon) == V02_SHA, "V02 control binary is not pinned verified V02")
    require(sha(args.legacy_daemon) == LEGACY_A_SHA, "legacy control must be exact historical A binary")
    require(base["code_sha256"] == RUNNER_SHA and base["controls"] == CONTROLS,
            "historical A report identity differs")
    report.update(prepared_report=str(args.prepared), prepared_report_sha256=sha(args.prepared),
        baseline_report=str(args.baseline_report), baseline_report_sha256=sha(args.baseline_report),
        fixed_A_ms=BEST_A, arms={}, arm_order=["legacy-A-control", "pinnedV02-owner", "V03-owner"],
        comparison="V02/V03 share newly published owner exact versions; historical A uses legacy versions/layout")
    for label, daemon, owner in (("legacy-A-control", args.legacy_daemon, False),
        ("pinnedV02-owner", args.v02_daemon, True), ("V03-owner", args.daemon, True)):
        root = args.work / label; root.mkdir(mode=0o700)
        launch = launcher(root / "daemon-launcher", daemon,
            prepared["owner_prefix"] if owner else None,
            prepared["owner_journal"] if owner else None)
        descriptor_for(descriptor, launch, root / "environment.json")
        arm_locator(base, root / "execution-preparation-locator.json", launch, owner, prepared)
        command = [sys.executable, str(Path(__file__).with_name("v03_startup_wrapper.py")),
            "--frozen-runner", str(args.frozen_runner), "--startup-timeout", str(args.startup_timeout),
            "--receipt", str(root / "startup-receipts.jsonl"), "--",
            "--native", str(args.native), "--environment", str(root / "environment.json"),
            "--source-pid", str(args.source_pid), "--database", DATABASE,
            "--writer-lock", str(args.writer_lock),
            "--hdf5-spec", str(args.frozen_runner.parent / "specs/runner-hdf5-n128-d4.json"),
            "--grib-spec", str(args.frozen_runner.parent / "specs/runner-grib-n128-d4.json"),
            "--reuse-preparation", str(root / "execution-preparation-locator.json"),
            "--reuse-work", prepared["data_work"], "--campaigns", "3", "--repeats", "5",
            "--output", str(root / "complete-results"), "--work", str(root / "complete-query")]
        report["arms"][label] = {"command": command, "launcher": launch,
            "load_before": Path("/proc/loadavg").read_text().strip(), "complete": False}
        checkpoint(args, report)
        child_env = dict(os.environ)
        child_env.pop("LD_LIBRARY_PATH", None); child_env.pop("PYTHONPATH", None)
        before = probe(args, env, prepared["owner_prefix"], "head")
        started = time.perf_counter_ns()
        with (root / "runner.log").open("xb") as log:
            completed = subprocess.run(command, env=child_env, stdin=subprocess.DEVNULL,
                stdout=log, stderr=subprocess.STDOUT, timeout=14400)
        require(completed.returncode == 0, "frozen campaign failed; preserve all observations and inspect runner.log")
        require(probe(args, env, prepared["owner_prefix"], "head") == before, "read-only arm changed owner head")
        report["arms"][label].update(collect_arm(root, launch), complete=True,
            process_wall_ns=time.perf_counter_ns() - started,
            load_after=Path("/proc/loadavg").read_text().strip())
        verify_inputs(prepared["unchanged_inputs"])
        checkpoint(args, report)
    actual = report["arms"]["V03-owner"]["warm_discover_values_ms"]
    report["historical_best_gate"] = {fmt: {"A_ms": BEST_A[fmt], "V03_ms": actual[fmt],
        "passed": actual[fmt] <= BEST_A[fmt]} for fmt in ("hdf5", "grib")}
    report["performance_gate_passed"] = all(x["passed"] for x in report["historical_best_gate"].values())
    report["complete"] = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "ablate"))
    for name in ("daemon", "environment", "frozen-runner", "native", "baseline-report", "probe",
                 "runtime", "release", "writer-lock", "work", "report"):
        parser.add_argument(*(("--environment", "--env") if name == "environment" else ("--" + name,)), type=Path, required=True)
    
    parser.add_argument("--source-pid", type=int, default=1001546)
    parser.add_argument("--datanode-pid", type=int, required=True)
    parser.add_argument("--startup-timeout", type=int, default=900)
    parser.add_argument("--corpus", type=Path)
    parser.add_argument("--owner-prefix")
    parser.add_argument("--owner-journal", type=Path)
    parser.add_argument("--prepared", type=Path)
    parser.add_argument("--v02-daemon", type=Path)
    parser.add_argument("--legacy-daemon", type=Path)
    args = parser.parse_args()
    for name, value in vars(args).items():
        if isinstance(value, Path): setattr(args, name, value.resolve())
    required = ("corpus", "owner_prefix", "owner_journal") if args.command == "prepare" else (
        "prepared", "v02_daemon", "legacy_daemon")
    require(all(getattr(args, name) is not None for name in required), "command-specific arguments missing")
    require(not args.work.exists() and not args.report.exists(), "use fresh work/report paths")
    args.work.mkdir(parents=True, mode=0o700)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    report = {"protocol": "iotdbfs-v03-n128-" + args.command + "/1", "complete": False,
        "command": args.command, "database": DATABASE, "controls": CONTROLS,
        "runner_sha256": RUNNER_SHA, "native_sha256": NATIVE_SHA, "script_sha256": sha(__file__),
        "candidate_binary": str(args.daemon), "candidate_sha256": sha(args.daemon),
        "reference_environment": str(args.environment), "reference_environment_sha256": sha(args.environment),
        "probe": str(args.probe), "probe_sha256": sha(args.probe),
        "startup_receipts": str(args.work / "startup-receipts.jsonl"),
        "credentials_recorded": False, "scope": "fixed N128 V1; no payload sharing/GC/compression claim"}
    checkpoint(args, report)
    try:
        runner = load_runner(args.frozen_runner)
        descriptor, env = environment(args, runner)
        with sync_window(args, report):
            if args.command == "prepare": prepare(args, runner, descriptor, env, report)
            else: ablate(args, runner, descriptor, env, report)
    except BaseException as error:
        report["failure"] = public_failure(error)
        report["complete"] = False
        checkpoint(args, report)
        raise
    checkpoint(args, report)
    print(json.dumps({"complete": report["complete"], "report": str(args.report),
        "performance_gate_passed": report.get("performance_gate_passed")}))


if __name__ == "__main__":
    main()
