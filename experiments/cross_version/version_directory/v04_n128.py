#!/usr/bin/env python3
"""Prepare fresh V04 N128 data, then compare pinned V03 and V04 with frozen clocks.

Reuses the original raw/FS/SQLite corpus in place. V03 keeps its frozen report,
namespace-record/2 prefix and binary; V04 re-ingests into a new namespace-record/3
prefix. Preparation and startup costs are outside the unchanged client clocks.
Uses only the authorized original instance/account, restored by sync_window.
"""
from __future__ import annotations

import argparse
import copy
import json
import os
from pathlib import Path
import re
import signal
import statistics
import subprocess
import sys
import time
import uuid

from experiments.cross_version.version_directory import v03_n128 as prior
from experiments.cross_version.version_directory.v00_head_probe import public_failure, save_json
from experiments.cross_version.version_directory.v03_startup_wrapper import RUNNER_SHA, NATIVE_SHA, adjust_mount, load_runner, require, sha

DATABASE, CONTROLS, BEST_A = prior.DATABASE, prior.CONTROLS, prior.BEST_A
V03_SHA = "5e23a4a6a9b24e91d7d8b0f99227b752668d943d0e5f7c2ebf686130eea2e6f8"
V03_PREPARED_SHA = "9a50a75f8b3871263fd7ec30480c11c1643f3ecb8c6f25eca7a366f11032f504"
V03_PREFIX = "vd_v03_n128_20261016"
BATCH_FIELD = "version_store_batch_query_calls"
PREPARE_PROTOCOL = "iotdbfs-v04-n128-prepare/1"


def checkpoint(args, report):
    prior.checkpoint(args, report)


def old_preparation(path):
    require(sha(path) == V03_PREPARED_SHA, "V03 preparation report is not the frozen accepted report")
    value = json.loads(path.read_text())
    require(value.get("complete") and value.get("prepared") and value["controls"] == CONTROLS and
            value["owner_prefix"] == V03_PREFIX, "V03 preparation/prefix differs")
    return value


def journal_digest(path):
    root = Path(path)
    require(root.is_dir() and (root / "state").is_file(), "owner journal is missing")
    return {str(p.relative_to(root)): sha(p) for p in root.rglob("*") if p.is_file()}


def summarize(values):
    present = [v for v in values if isinstance(v, (int, float)) and not isinstance(v, bool)]
    return {"available_samples": len(present), "expected_samples": len(values),
            "median": statistics.median(present) if present else None,
            "minimum": min(present) if present else None, "maximum": max(present) if present else None}


def startup_evidence(path, expected, binary_sha):
    rows = [json.loads(line) for line in path.read_text().splitlines() if line]
    starts = [r for r in rows if r["kind"] == "mount"]
    ends = [r for r in rows if r["kind"] == "mount_before_stop"]
    require(len(starts) == len(ends) == expected, "startup/RSS receipts are incomplete")
    require({r["root"] for r in starts} == {r["root"] for r in ends} and
            len({r["root"] for r in starts}) == expected, "mount receipt roots differ")
    require(all(r["daemon_sha256"] == binary_sha for r in starts), "mounted binary differs from pinned arm")
    return {"path": str(path), "sha256": sha(path), "mounts": starts, "before_stop": ends,
            "startup_ns": summarize([r["startup_ns"] for r in starts]),
            "startup_rss_kib": summarize([r["memory_at_startup"].get("VmRSS_kib") for r in starts]),
            "postrun_rss_kib": summarize([r["memory"].get("VmRSS_kib") for r in ends]),
            "peak_rss_kib": summarize([r["memory"].get("VmHWM_kib") for r in ends])}


def archive_config(receipt, archive):
    source = Path(receipt["path"]).with_name("profile.properties")
    require(source.is_file() and sha(source) == receipt["config_sha256"], "actual daemon configuration changed")
    text = source.read_text()
    require(not re.search(r"(?im)^\s*[^#=\n]*(password|secret|token|credential)[^=\n]*=", text),
            "unexpected credential-like configuration field; do not archive it")
    archive.write_text(text)
    archive.chmod(0o600)
    return {"path": str(archive), "sha256": sha(archive), "original_path": str(source)}


def preparation_config(context, launch, archive):
    receipt_path = Path(context["root"]) / "profile.launch.json"
    receipt = json.loads(receipt_path.read_text())
    require(receipt["actual_binary_sha256"] == launch["binary_sha256"] and
            receipt["settings"] == launch["settings"], "preparation daemon/settings differ")
    return {"receipt": {"path": str(receipt_path), **receipt},
            "configuration": archive_config({"path": str(receipt_path), **receipt}, archive)}


def collect_arm(root, launch):
    # Reuse frozen V03 analysis for complete observations, timing and the two
    # disjoint SQL paths. Add only V04 batch availability and actual configs.
    result = prior.collect_arm(root, launch)
    analysis_path = Path(result["warm_profile_analysis"]["path"])
    analysis = json.loads(analysis_path.read_text())
    source = root / "complete-results/report.json"
    observed = json.loads(source.read_text())
    for fmt, phases in analysis["cells"].items():
        for phase, cell in phases.items():
            rows = {(r["campaign"], r["repeat"]): r for r in observed["records"]
                    if r["arm"] == "iotdb" and r["format"] == fmt and r["phase"] == phase and r["repeat"] > 0}
            for sample in cell["observations"]:
                row = rows[(sample["campaign"], sample["repeat"])]
                discovery, values = [row["discovery"]], row.get("value_calls", [])
                for scope, calls in (("discovery", discovery), ("value", values), ("total", discovery + values)):
                    fields = [c.get("server_profile", {}).get("iotdb_delta", {}).get(BATCH_FIELD) for c in calls]
                    numeric = lambda v: isinstance(v, (int, float)) and not isinstance(v, bool)
                    sample["profile"][scope][BATCH_FIELD] = sum(fields) if fields and all(map(numeric, fields)) else None
                    if launch["binary_sha256"] == V03_SHA:
                        require(all(v is None for v in fields), "pinned V03 unexpectedly reports V04 batch instrumentation")
                    elif fields:
                        require(all(map(numeric, fields)), "V04 batch instrumentation is missing")
            for scope in ("discovery", "value", "total"):
                cell["summary"]["profile"][scope][BATCH_FIELD] = summarize([
                    s["profile"][scope][BATCH_FIELD] for s in cell["observations"]])
    analysis["protocol"] = "iotdbfs-v04-frozen-warm-profile-analysis/1"
    analysis["semantics"]["batch_queries"] = (
        "daemon-wide version_store_batch_query_calls is a subset of Store query_calls; never add it again; "
        "pinned V03 has observable Store totals but absent batch fields remain null")
    analysis["semantics"]["unavailable"] = "missing measurements remain null; V03 Store totals are available, its V04 batch field is unavailable"
    save_json(analysis_path, analysis)
    result["warm_profile_analysis"] = {"path": str(analysis_path), "sha256": sha(analysis_path),
        "summary": {fmt: {phase: cell["summary"] for phase, cell in phases.items()}
                    for fmt, phases in analysis["cells"].items()}}
    configs = root / "actual-configurations"
    configs.mkdir(mode=0o700)
    result["actual_configurations"] = [archive_config(receipt, configs / ("mount-%02d.properties" % i))
        for i, receipt in enumerate(result["launches"])]
    result["startup_and_rss"] = startup_evidence(root / "startup-receipts.jsonl", 7, launch["binary_sha256"])
    require(sha(source) == result["report_sha256"], "analysis changed frozen raw observations")
    return result


def prepare(args, runner, descriptor, env, report):
    baseline = json.loads(args.baseline_report.read_text())
    manifests, inputs = prior.raw_inventory(runner, args.corpus, baseline, args.baseline_report)
    require(not args.owner_journal.exists(), "V04 owner journal must be new")
    require(re.fullmatch(r"vd_v04_[a-z0-9_]+", args.owner_prefix), "use a unique vd_v04_* prefix")
    require(sha(args.daemon) not in (V03_SHA, prior.V02_SHA, prior.LEGACY_A_SHA), "V04 preparation requires the candidate V04 binary")
    logical = "/multi-file-v04-" + uuid.uuid4().hex[:16]
    report.update(logical_prefix=logical, owner_prefix=args.owner_prefix, owner_journal=str(args.owner_journal),
        original_corpus=str(args.corpus), data_work=str(args.corpus), unchanged_inputs=inputs,
        inherited_baseline_report=str(args.baseline_report), inherited_baseline_sha256=sha(args.baseline_report),
        new_raw_generation=False, raw_FS_copy=False, new_FS_index_build=False,
        preparation=copy.deepcopy(baseline["preparation"]), publication={},
        persistent_organization="namespace-record/3, file-descriptor/2 and complete immutable payload maps/blocks; independent of V03 prefix")
    init = prior.launcher(args.work / "initialize-launcher", args.daemon, args.owner_prefix, args.owner_journal, True)
    recover = prior.launcher(args.work / "recovery-launcher", args.daemon, args.owner_prefix, args.owner_journal)
    report["launchers"] = {"initialize": init, "recovery": recover}
    adjust_mount(runner.sf, args.startup_timeout, args.work / "startup-receipts.jsonl")
    checkpoint(args, report)
    with runner.sf.private_mount(Path(init["path"]), args.work / "initialize", env, args.writer_lock) as context:
        report["initialization"] = {"startup_ns": context["startup_ns"],
            "head": prior.probe(args, env, args.owner_prefix, "head"),
            "objects": prior.probe(args, env, args.owner_prefix, "stats"),
            **preparation_config(context, init, args.work / "initialize.properties")}
        for fmt, manifest in manifests.items():
            started, writes = time.perf_counter_ns(), []
            for source in manifest["sources"]:
                raw = args.corpus / (fmt + "-data") / source["file"]
                destination = context["mount"] / logical.lstrip("/") / fmt / source["file"]
                destination.parent.mkdir(parents=True, exist_ok=True)
                require(not destination.exists(), "new V04 destination already exists")
                writes.append(runner.sf.checked_copy(raw, destination))
            elapsed = time.perf_counter_ns() - started
            versions, stats = prior.mapping(runner, context, manifest, logical + "/" + fmt + "/raw")
            report["publication"][fmt] = {"files": writes, "wall_ns_including_directory_creation": elapsed,
                "source_files": 128, "version_binding_outside_publication_clock": stats,
                "fs_costs": "original FS files and SQLite reused in place; no copy/index rebuild"}
            report["preparation"][fmt]["versions"] = versions
            report["preparation"][fmt]["publication"] = {"iotdb": report["publication"][fmt],
                "fs": baseline["preparation"][fmt]["publication"]["fs"]}
            checkpoint(args, report)
        report["published_head"] = prior.probe(args, env, args.owner_prefix, "head")
        report["published_objects"] = prior.probe(args, env, args.owner_prefix, "stats")
    with runner.sf.private_mount(Path(recover["path"]), args.work / "recovery", env, args.writer_lock) as context:
        report["recovery"] = {"startup_ns": context["startup_ns"], "full_file_hashes": 0,
            **preparation_config(context, recover, args.work / "recovery.properties")}
        for fmt, manifest in manifests.items():
            versions, _ = prior.mapping(runner, context, manifest, logical + "/" + fmt + "/raw")
            require(versions == report["preparation"][fmt]["versions"], "recovery changed exact version bindings")
            for source in manifest["sources"]:
                require(sha(context["mount"] / logical.lstrip("/") / fmt / source["file"]) == source["sha256"],
                        "recovered full-file hash differs")
                report["recovery"]["full_file_hashes"] += 1
            oracle, offset = runner.iotdb_query(context, manifest, logical + "/" + fmt + "/raw",
                versions, context["v03_profile_offset"], True)
            context["v03_profile_offset"] = offset
            report["recovery"][fmt + "_oracle"] = oracle
        require(prior.probe(args, env, args.owner_prefix, "head") == report["published_head"], "recovery changed head")
    report["startup_and_rss"] = startup_evidence(args.work / "startup-receipts.jsonl", 2, sha(args.daemon))
    report["daemon_sha256"] = recover["sha256"]  # Frozen runner expects the launcher's identity here.
    report["code_sha256"], report["native_sha256"] = RUNNER_SHA, NATIVE_SHA
    report["prepared"] = report["complete"] = True
    prior.verify_inputs(inputs)


def run_campaign(command, root, child_env):
    # An interrupted controller must not leave a private FUSE mount behind when
    # sync_window restores the original database service. No shared mount is touched.
    process = None
    with (root / "runner.log").open("xb") as log:
        try:
            process = subprocess.Popen(command, env=child_env, stdin=subprocess.DEVNULL,
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            return process.wait(timeout=14400)
        finally:
            if process is not None:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=30)
                for mount in (root / "complete-query").rglob("mount"):
                    if os.path.ismount(mount):
                        subprocess.run(["fusermount", "-u", str(mount)], check=True, timeout=30,
                            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def ablate(args, runner, descriptor, env, report):
    old = old_preparation(args.v03_prepared)
    prepared = json.loads(args.prepared.read_text())
    base = json.loads(args.baseline_report.read_text())
    require(prepared.get("complete") and prepared.get("prepared") and prepared["protocol"] == PREPARE_PROTOCOL and
            prepared["controls"] == CONTROLS, "V04 preparation is incomplete")
    require(sha(args.v03_daemon) == V03_SHA, "control daemon is not pinned V03")
    require(sha(args.daemon) == prepared["candidate_sha256"], "V04 binary differs from preparation")
    require(re.fullmatch(r"vd_v04_[a-z0-9_]+", prepared["owner_prefix"]) and
            prepared["owner_prefix"] != old["owner_prefix"] and
            Path(prepared["owner_journal"]).resolve() != Path(old["owner_journal"]).resolve(), "V03/V04 namespaces must be distinct")
    require(Path(prepared["data_work"]).resolve() == Path(old["data_work"]).resolve(), "arms must reuse the same raw/FS/index corpus")
    prior.raw_inventory(runner, Path(prepared["data_work"]), base, args.baseline_report)
    for value in (old, prepared):
        prior.verify_inputs(value["unchanged_inputs"])
    report.update(prepared_report=str(args.prepared), prepared_report_sha256=sha(args.prepared),
        v03_prepared_report=str(args.v03_prepared), v03_prepared_sha256=V03_PREPARED_SHA,
        fixed_A_ms=BEST_A, arm_order=["pinnedV03-owner", "V04-owner"], arms={},
        observations_per_arm=180, comparison="V03 namespace-record/2 independent payload OBJECTs versus fresh V04 namespace-record/3 content blocks; same raw/FS/SQLite and query clocks, different exact versions/storage organization",
        preparation_costs={"V03": {"report": str(args.v03_prepared), "sha256": V03_PREPARED_SHA},
                           "V04": {"report": str(args.prepared), "sha256": sha(args.prepared)}})
    prefixes = {v["owner_prefix"]: v for v in (old, prepared)}
    for label, daemon, selected in (("pinnedV03-owner", args.v03_daemon, old), ("V04-owner", args.daemon, prepared)):
        root = args.work / label
        root.mkdir(mode=0o700)
        launch = prior.launcher(root / "daemon-launcher", daemon, selected["owner_prefix"], selected["owner_journal"])
        prior.descriptor_for(descriptor, launch, root / "environment.json")
        prior.arm_locator(base, root / "execution-preparation-locator.json", launch, True, selected)
        command = [sys.executable, str(Path(__file__).with_name("v03_startup_wrapper.py")),
            "--frozen-runner", str(args.frozen_runner), "--startup-timeout", str(args.startup_timeout),
            "--receipt", str(root / "startup-receipts.jsonl"), "--", "--native", str(args.native),
            "--environment", str(root / "environment.json"), "--source-pid", str(args.source_pid),
            "--database", DATABASE, "--writer-lock", str(args.writer_lock),
            "--hdf5-spec", str(args.frozen_runner.parent / "specs/runner-hdf5-n128-d4.json"),
            "--grib-spec", str(args.frozen_runner.parent / "specs/runner-grib-n128-d4.json"),
            "--reuse-preparation", str(root / "execution-preparation-locator.json"), "--reuse-work", selected["data_work"],
            "--campaigns", "3", "--repeats", "5", "--output", str(root / "complete-results"),
            "--work", str(root / "complete-query")]
        before = {p: {"head": prior.probe(args, env, p, "head"), "journal": journal_digest(v["owner_journal"])}
                  for p, v in prefixes.items()}
        report["arms"][label] = {"command": command, "launcher": launch, "owner_prefix": selected["owner_prefix"],
            "prepared_sha256": V03_PREPARED_SHA if label == "pinnedV03-owner" else sha(args.prepared),
            "load_before": Path("/proc/loadavg").read_text().strip(), "complete": False}
        checkpoint(args, report)
        child_env = dict(os.environ)
        child_env.pop("LD_LIBRARY_PATH", None)
        child_env.pop("PYTHONPATH", None)
        started = time.perf_counter_ns()
        require(run_campaign(command, root, child_env) == 0, "frozen campaign failed; preserve observations and inspect runner.log")
        for prefix, value in prefixes.items():
            require(prior.probe(args, env, prefix, "head") == before[prefix]["head"] and
                    journal_digest(value["owner_journal"]) == before[prefix]["journal"], "read-only campaign changed an owner head/journal")
        report["arms"][label].update(collect_arm(root, launch), complete=True,
            process_wall_ns=time.perf_counter_ns() - started, load_after=Path("/proc/loadavg").read_text().strip(),
            preserved_heads_and_journals=before)
        prior.verify_inputs(prepared["unchanged_inputs"])
        checkpoint(args, report)
    report["historical_best_gate"] = {fmt: {"A_ms": BEST_A[fmt],
        "V03_ms": report["arms"]["pinnedV03-owner"]["warm_discover_values_ms"][fmt],
        "V04_ms": report["arms"]["V04-owner"]["warm_discover_values_ms"][fmt],
        "passed": report["arms"]["V04-owner"]["warm_discover_values_ms"][fmt] <= BEST_A[fmt]} for fmt in BEST_A}
    report["performance_gate_passed"] = all(v["passed"] for v in report["historical_best_gate"].values())
    report["complete"] = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "ablate"))
    for name in ("daemon", "environment", "frozen-runner", "native", "baseline-report", "probe", "runtime",
                 "release", "writer-lock", "work", "report", "v03-prepared"):
        parser.add_argument(*(("--environment", "--env") if name == "environment" else ("--" + name,)), type=Path, required=True)
    parser.add_argument("--source-pid", type=int, default=1001546)
    parser.add_argument("--datanode-pid", type=int, required=True)
    parser.add_argument("--startup-timeout", type=int, default=900)
    for name in ("corpus", "owner-journal", "prepared", "v03-daemon"):
        parser.add_argument("--" + name, type=Path)
    parser.add_argument("--owner-prefix")
    args = parser.parse_args()
    for name, value in vars(args).items():
        if isinstance(value, Path):
            setattr(args, name, value.resolve())
    required = ("corpus", "owner_prefix", "owner_journal") if args.command == "prepare" else ("prepared", "v03_daemon")
    require(all(getattr(args, name) is not None for name in required), "command-specific arguments missing")
    old = old_preparation(args.v03_prepared)
    if args.command == "prepare":
        require(args.corpus == Path(old["data_work"]).resolve(), "V04 must reuse the frozen V03 raw/FS/index corpus")
    require(not args.work.exists() and not args.report.exists(), "use fresh work/report paths")
    args.work.mkdir(parents=True, mode=0o700)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    report = {"protocol": "iotdbfs-v04-n128-" + args.command + "/1", "complete": False,
        "command": args.command, "database": DATABASE, "controls": CONTROLS,
        "runner_sha256": RUNNER_SHA, "native_sha256": NATIVE_SHA, "script_sha256": sha(__file__),
        "reused_helpers": {p: sha(Path(__file__).with_name(p)) for p in ("v03_n128.py", "v03_startup_wrapper.py", "v00_head_probe.py")},
        "candidate_binary": str(args.daemon), "candidate_sha256": sha(args.daemon),
        "reference_environment": str(args.environment), "reference_environment_sha256": sha(args.environment),
        "probe": str(args.probe), "probe_sha256": sha(args.probe),
        "baseline_report": str(args.baseline_report), "baseline_report_sha256": sha(args.baseline_report),
        "v03_prepared_report": str(args.v03_prepared), "v03_prepared_sha256": V03_PREPARED_SHA,
        "credentials_recorded": False, "scope": "fixed N128 V1 two-arm whole-system comparison; preparation outside clocks, no GC/disk compression inference"}
    checkpoint(args, report)
    try:
        runner = load_runner(args.frozen_runner)
        descriptor, env = prior.environment(args, runner)
        with prior.sync_window(args, report):
            old_head = prior.probe(args, env, V03_PREFIX, "head")
            old_journal = journal_digest(old["owner_journal"])
            try:
                if args.command == "prepare":
                    prepare(args, runner, descriptor, env, report)
                else:
                    ablate(args, runner, descriptor, env, report)
            finally:
                require(prior.probe(args, env, V03_PREFIX, "head") == old_head and
                        journal_digest(old["owner_journal"]) == old_journal, "frozen V03 head/journal changed")
                report["frozen_v03_preserved"] = {"head": old_head, "journal": old_journal}
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
