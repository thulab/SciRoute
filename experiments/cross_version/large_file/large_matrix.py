#!/usr/bin/env python3
"""Favorable large-file V1/V8 experiment using the frozen N8 query clocks.

Four cyclic arm orders cover every arm position per block. This is an explicit
best-case workload search, not a representative production benchmark.
"""
from __future__ import annotations

import argparse
import copy
import importlib
from collections import defaultdict
import json
import os
from pathlib import Path
import shutil
import statistics
import time
import uuid

from experiments.cross_version.large_file import large_corpus as data
from experiments.cross_version.version_matrix import n8_matrix as matrix
from experiments.cross_version.version_matrix import n8_matrix_prepare as prep
from experiments.cross_version.pilot import publish
from experiments.cross_version.version_directory import v03_n128 as prior
from common import load_manifest, sha_file
from experiments.cross_version.version_directory.v00_head_probe import save_json
from experiments.cross_version.version_directory.v03_startup_wrapper import adjust_mount, load_runner
from experiments.cross_version.version_directory.v04_n128 import journal_digest, preparation_config

VERSIONS = (1, 8)
SCENARIOS = data.SCENARIOS
FORMATS = data.FORMATS
SELECTIONS = ("latest", "all")
SYSTEMS = matrix.SYSTEMS
CAMPAIGNS = 4
REPEATS = 5


def require(value, message):
    if not value:
        raise RuntimeError(message)


def selected(manifest, scenario, versions):
    entries = [e for e in manifest["entries"] if e["scenario"] == scenario and e["ordinal"] <= versions]
    expected = {(fmt, index, ordinal) for fmt in FORMATS for index in range(8) for ordinal in range(1, versions + 1)}
    require(len(entries) == len(expected) and {(e["format"], e["file_index"], e["ordinal"]) for e in entries} == expected,
            "selected scientific sequence differs")
    require(all(len(e["units"]) == 4 for e in entries), "D4 scientific units required")
    return entries


def copy_fs(args, manifest, runner, env, report, checkpoint):
    if args.preparation_report:
        old = json.loads(args.preparation_report.read_text())
        require(old["corpus_sha256"] == manifest["manifest_sha256"] and len(old["fs"]) == 8,
                "prior FS preparation differs")
        for record in old["fs"]:
            require(sha_file(record["index"]) == record["index_sha256"] and
                all(sha_file(f["path"]) == f["sha256"] for f in record["files"]), "prior FS bytes changed")
        report["fs"] = copy.deepcopy(old["fs"])
        report["fs_preparation_reused_from"] = {"report": str(args.preparation_report),
            "sha256": sha_file(args.preparation_report), "no_new_copies_or_indexes": True}
        checkpoint()
        return
    report["fs"] = []
    inodes = set()
    for scenario in SCENARIOS:
        for versions in VERSIONS:
            for fmt in FORMATS:
                root = args.work / "fs" / scenario / ("V%d" % versions) / fmt
                root.mkdir(parents=True)
                files, started = [], time.perf_counter_ns()
                for entry in selected(manifest, scenario, versions):
                    if entry["format"] != fmt:
                        continue
                    source = data.resolve(args.input, manifest, entry)
                    target = root / ("v%03d" % entry["ordinal"]) / entry["logical_file"]
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with source.open("rb", buffering=0) as raw, target.open("xb", buffering=0) as output:
                        shutil.copyfileobj(raw, output, 1024 * 1024)
                        os.fsync(output.fileno())
                    stat, origin = target.stat(), source.stat()
                    identity = stat.st_dev, stat.st_ino
                    require(stat.st_nlink == 1 and identity not in inodes and
                            identity != (origin.st_dev, origin.st_ino) and not target.is_symlink(), "FS file aliases raw/history")
                    inodes.add(identity)
                    require(sha_file(target) == entry["sha256"], "FS copy changed")
                    files.append({"path": str(target), "sha256": entry["sha256"], "bytes": stat.st_size,
                        "allocated_bytes": stat.st_blocks * 512, "inode": stat.st_ino, "nlink": stat.st_nlink})
                copy_ns = time.perf_counter_ns() - started
                index = root.parent / (fmt + ".sqlite")
                worker = runner.NativeWorker(args.native, env)
                try:
                    result = worker.call({"op": "build", "root": str(root), "format": fmt,
                        "version_count": versions, "index": str(index)})
                finally:
                    worker.close()
                report["fs"].append({"scenario": scenario, "V": versions, "format": fmt,
                    "root": str(root), "index": str(index), "index_sha256": sha_file(index),
                    "files": files, "copy_fsync_verify_ns": copy_ns, "index_build": result})
                checkpoint()
                print(json.dumps({"event": "fs_prepared", "scenario": scenario, "V": versions, "format": fmt}), flush=True)


def prepare_owners(args, manifest, runner, env, report, checkpoint):
    root = args.work / "preparation"
    root.mkdir(mode=0o700)
    templates = {fmt: load_manifest(manifest["bases"][fmt]["root"]) for fmt in FORMATS}
    for scenario in SCENARIOS:
        for versions in VERSIONS:
            for system in ("V03", "V04"):
                if any((n["scenario"], n["V"], n["system"]) == (scenario, versions, system)
                       and n.get("prepared") for n in report["namespaces"]):
                    continue
                started = time.perf_counter_ns()
                cell_root = root / (system.lower() + "-" + scenario + "-v" + str(versions))
                cell_root.mkdir(mode=0o700)
                journal = cell_root / "owner"
                prefix = "vd_" + system.lower() + "_large_" + uuid.uuid4().hex[:16]
                daemon = getattr(args, "daemon_" + system.lower())
                init = prior.launcher(cell_root / "initialize-launcher", daemon, prefix, journal, True)
                recover = prior.launcher(cell_root / "recovery-launcher", daemon, prefix, journal, False)
                cell = {"system": system, "scenario": scenario, "V": versions, "prefix": prefix,
                    "journal": str(journal), "logical_prefix": "/large", "bindings": [],
                    "init_launch": init, "recover_launch": recover, "root": str(cell_root),
                    "prepared": False, "probe_receipts": {}}
                report["namespaces"].append(cell)
                checkpoint()
                with runner.sf.private_mount(Path(init["path"]), cell_root / "initialize", env, args.writer_lock) as context:
                    cell["initialization"] = {"startup_ns": context["startup_ns"],
                        **preparation_config(context, init, cell_root / "initialize.properties")}
                    last = {}
                    for entry in selected(manifest, scenario, versions):
                        logical = "/large/" + entry["format"] + "/" + entry["logical_file"]
                        target = context["mount"] / logical.lstrip("/")
                        elapsed = publish.write_version(data.resolve(args.input, manifest, entry), target, entry["ordinal"] == 1)
                        started_binding = time.perf_counter_ns()
                        rows = publish.discovery(context, logical, entry["format"], templates[entry["format"]])
                        actual = {row["version"] for row in rows}
                        require(len(rows) == 4 and len(actual) == 1 and all(row["path"] == logical for row in rows), "published directory differs")
                        version = actual.pop()
                        require(type(version) is int and version > last.get(logical, 0), "version did not advance")
                        require(sha_file(target) == entry["sha256"], "published raw bytes differ")
                        last[logical] = version
                        cell["bindings"].append({k: entry[k] for k in
                            ("format", "logical_file", "file_index", "ordinal", "sha256")} | {
                            "logical_path": logical, "version": version, "publication_ns": elapsed,
                            "binding_and_latest_sha_ns": time.perf_counter_ns() - started_binding})
                        checkpoint()
                    probe = getattr(args, "probe_" + system.lower())
                    cell["head"], cell["probe_receipts"]["head"] = prep._probe(probe, "head", prefix, env, cell_root)
                    cell["objects"], cell["probe_receipts"]["stats"] = prep._probe(probe, "stats", prefix, env, cell_root)
                    if system == "V04":
                        cell["payload_inspection"], cell["probe_receipts"]["inspect"] = prep._probe(
                            args.payload_probe_v04, "inspect", prefix, env, cell_root)
                cell["preparation_wall_ns"] = time.perf_counter_ns() - started
                cell["publication_ns"] = sum(b["publication_ns"] for b in cell["bindings"])
                cell["prepared"] = True
                checkpoint()
                print(json.dumps({"event": "namespace_prepared", "scenario": scenario, "V": versions, "system": system,
                    "scientific_versions": len(cell["bindings"])}), flush=True)
    require(len(report["namespaces"]) == 8 and sum(len(n["bindings"]) for n in report["namespaces"]) ==
        2 * 2 * 9 * 8 * len(FORMATS), "owner preparation incomplete")


def validate_preparation(old, manifest):
    """Accept only the precisely bounded, fully published V1 pre-query failure."""
    require(old.get("original_service_restored") is True and old.get("complete") is False and
        old.get("query_records") == 0 and old.get("corpus_sha256") == manifest["manifest_sha256"] and
        old.get("failure", {}).get("type") == "ValueError", "not the reconciled pre-query preparation")
    require(old.get("binaries") == matrix.DAEMON_SHAS, "prior daemon identities differ")
    require(len(old["namespaces"]) == 2 and
        {(n["system"], n["scenario"], n["V"]) for n in old["namespaces"]} ==
        {(s, "identical", 1) for s in ("V03", "V04")}, "unexpected prior namespace scope")
    for namespace in old["namespaces"]:
        require(namespace.get("head") and len(namespace["bindings"]) == 16 and
            {(b["format"], b["file_index"], b["ordinal"]) for b in namespace["bindings"]} ==
            {(fmt, i, 1) for fmt in FORMATS for i in range(8)}, "prior publication bindings incomplete")
    return old["namespaces"]


def reconcile_preparation(args, manifest, runner, env, report, checkpoint):
    if not args.preparation_report:
        return
    old = json.loads(args.preparation_report.read_text())
    cells = validate_preparation(old, manifest)
    report["preparation_continuation"] = {"source_report": str(args.preparation_report),
        "source_report_sha256": sha_file(args.preparation_report), "adopted_scientific_versions": 32,
        "reason": "read-only stats tool cumulative limit; no formal query was attempted", "verified": False}
    templates = {fmt: load_manifest(manifest["bases"][fmt]["root"]) for fmt in FORMATS}
    for original in cells:
        cell = copy.deepcopy(original)
        started = time.perf_counter_ns()
        root = args.work / "reconciliation" / cell["system"]
        root.mkdir(mode=0o700, parents=True)
        before = journal_digest(Path(cell["journal"]))
        typed_cases = []
        def typed_precheck(context, offset):
            for fmt in FORMATS:
                for selection in SELECTIONS:
                    row, offset = matrix.iotdb_query(runner, context, cell, manifest, templates[fmt], fmt, selection, offset)
                    typed_cases.append({"format": fmt, "selection": selection, **row})
            return offset
        if cell["system"] == "V03":
            with runner.sf.private_mount(Path(cell["recover_launch"]["path"]), root / "mount", env, args.writer_lock) as context:
                for binding in cell["bindings"]:
                    require(sha_file(publish.history_path(context["mount"], binding["logical_path"], binding["version"])) ==
                        binding["sha256"], "prior V03 exact bytes differ")
                offset = 0
                for fmt in FORMATS:
                    request = runner.discovery_request(cell["logical_prefix"] + "/" + fmt, templates[fmt], False)
                    request["request"]["sources"]["versions"] = {"kind": "all"}
                    response, batches, _, offset = runner.profile_call(context, request, offset)
                    rows = runner.discovered(response, batches, fmt, cell["logical_prefix"] + "/" + fmt)
                    require(len(rows) == 32 and {(r["path"], r["version"]) for r in rows} ==
                        {(b["logical_path"], b["version"]) for b in cell["bindings"] if b["format"] == fmt},
                        "prior V03 scientific set differs")
                typed_precheck(context, offset)
        else:
            with runner.sf.private_mount(Path(cell["recover_launch"]["path"]), root / "typed-mount", env, args.writer_lock) as context:
                typed_precheck(context, 0)
        save_json(root / "typed-precheck.json", {"complete": True, "cases": typed_cases,
            "scope": "preparation-time correctness only; excluded from formal timing observations"})
        probe = getattr(args, "probe_" + cell["system"].lower())
        head, head_receipt = prep._probe(probe, "head", cell["prefix"], env, root)
        require(head == cell["head"], "prior namespace head changed")
        cell["objects"], stats_receipt = prep._probe(probe, "stats", cell["prefix"], env, root)
        cell["continuation_probe_receipts"] = {"head": head_receipt, "stats": stats_receipt}
        if cell["system"] == "V04":
            graph, receipt = prep._probe(args.payload_probe_v04, "inspect", cell["prefix"], env, root)
            actual = {(f["path"], f["version"]): f for f in graph["files"]}
            wanted = {(b["logical_path"], b["version"]): b for b in cell["bindings"]}
            require(all(k in actual and actual[k]["sha256"] == b["sha256"] for k, b in wanted.items()) and
                all(f["size"] == 0 for k, f in actual.items() if k not in wanted) and graph["staging_payload_rows"] == 0,
                "prior V04 payload graph differs or unpublished staging remains")
            cell["payload_inspection"] = graph
            cell["continuation_probe_receipts"]["inspect"] = receipt
        require(journal_digest(Path(cell["journal"])) == before, "preparation reconciliation changed journal")
        cell["publication_ns"] = sum(b["publication_ns"] for b in cell["bindings"])
        cell["continuation_audit_wall_ns"] = time.perf_counter_ns() - started
        cell["prepared"] = True
        cell["adopted_from_report"] = str(args.preparation_report)
        report["namespaces"].append(cell)
        checkpoint()
    report["preparation_continuation"]["verified"] = True
    checkpoint()


def summarize(records):
    cells = defaultdict(list)
    keys = ("scenario", "V", "format", "selection", "system")
    for row in records:
        require(row["oracle_verified"], "unverified observation")
        cells[tuple(row[k] for k in keys)].append(row)
    cell_count = len(SCENARIOS) * len(VERSIONS) * len(FORMATS) * len(SELECTIONS) * len(SYSTEMS)
    require(len(cells) == cell_count and len(records) == cell_count * CAMPAIGNS * REPEATS, "incomplete large matrix")
    results = []
    for key, rows in sorted(cells.items()):
        require(len(rows) == 20 and {(r["campaign"], r["repeat"]) for r in rows} ==
                {(c, r) for c in range(4) for r in range(5)}, "replication differs")
        warm = [r for r in rows if r["repeat"] > 0]
        results.append(dict(zip(keys, key), warm_p50_ms=statistics.median(r["total_ns"] for r in warm) / 1e6,
            campaign_warm_p50_ms=[statistics.median(r["total_ns"] for r in warm if r["campaign"] == c) / 1e6 for c in range(4)],
            first_by_campaign_ms=[next(r["total_ns"] for r in rows if r["campaign"] == c and r["repeat"] == 0) / 1e6 for c in range(4)],
            warm_observations=16, independent_campaigns=4))
    lookup = {tuple(r[k] for k in keys): r for r in results}
    comparisons = []
    for scenario in SCENARIOS:
        for fmt in FORMATS:
            for selection in SELECTIONS:
                for control in ("V03", "FS-scan", "FS-index"):
                    a1, a8, b1, b8 = [lookup[(scenario, v, fmt, selection, system)] for v, system in
                        ((1, "V04"), (8, "V04"), (1, control), (8, control))]
                    t1, t8, f1, f8 = [r["warm_p50_ms"] for r in (a1, a8, b1, b8)]
                    rounds = []
                    for campaign in range(4):
                        x1, x8, y1, y8 = [r["campaign_warm_p50_ms"][campaign] for r in (a1, a8, b1, b8)]
                        rounds.append({"campaign": campaign, "V04_total_ms": x8, "control_total_ms": y8,
                            "V04_increment_ms": x8 - x1, "control_increment_ms": y8 - y1,
                            "speedup": y8 / x8, "relative_gain": (y8 / x8) / (y1 / x1),
                            "joint_pass": x8 < y8 and x8 - x1 < y8 - y1 and (y8 / x8) / (y1 / x1) > 1})
                    comparisons.append({"scenario": scenario, "format": fmt, "selection": selection, "control": control,
                        "V04_total_ms": t8, "control_total_ms": f8, "V04_growth": t8 / t1,
                        "control_growth": f8 / f1, "V04_increment_ms": t8 - t1, "control_increment_ms": f8 - f1,
                        "speedup": f8 / t8, "relative_gain": (f8 / t8) / (f1 / t1), "campaigns": rounds,
                        "all_campaigns_joint_pass": all(r["joint_pass"] for r in rounds)})
    return {"complete": True, "cells": results, "comparisons": comparisons,
        "inference": "selected favorable workload; four campaign units; pooled repetitions descriptive only; no theoretical maximum claim"}


def run(args):
    global FORMATS, data
    FORMATS = tuple(args.formats)
    data = importlib.import_module("." + args.corpus_module, __package__)
    require(FORMATS == tuple(data.FORMATS), "explicit corpus formats differ")
    for name, value in vars(args).items():
        if isinstance(value, Path):
            setattr(args, name, value.resolve())
    require(not args.work.exists() and not args.report.exists(), "new work/report required; no implicit retry")
    args.work.mkdir(mode=0o700, parents=True)
    report = {"protocol": "iotdbfs-large-version-matrix/1", "complete": False, "credentials_recorded": False,
        "namespaces": [], "query_records": 0, "controller_sha256": sha_file(__file__),
        "source_binding_sha256": sha_file(args.source_binding), "spec_sha256": sha_file(args.spec),
        "spec": json.loads(args.spec.read_text()), "records_file": str(args.work / "queries.jsonl")}
    def checkpoint():
        save_json(args.report, report)
        args.report.chmod(0o600)
    checkpoint()
    require(report["spec"]["version_counts"] == [1, 8] and report["spec"]["campaigns"] == 4 and
            report["spec"]["repeats"] == 5 and report["spec"]["selections"] == list(SELECTIONS) and
            report["spec"]["formats"] == list(FORMATS), "large spec differs")
    require(all(sha_file(getattr(args, "daemon_" + s.lower())) == digest for s, digest in matrix.DAEMON_SHAS.items()), "frozen daemon changed")
    runner = load_runner(args.frozen_runner)
    adjust_mount(runner.sf, 1800, args.work / "startup-receipts.jsonl")
    _, _, env = publish.runtime(args.environment, args.source_pid, prior.DATABASE)
    require(env["IOTDB_PORT"] == "33667" and env["IOTDB_HOST"] in ("127.0.0.1", "localhost"), "original endpoint differs")
    report["raw_pre_audit"] = data.verify(args.input)
    manifest = data.load(args.input)
    report["corpus_sha256"] = manifest["manifest_sha256"]
    report["binaries"] = {s: sha_file(getattr(args, "daemon_" + s.lower())) for s in ("V04", "V03")}
    report["native_sha256"] = sha_file(args.native)
    templates = {fmt: load_manifest(manifest["bases"][fmt]["root"]) for fmt in FORMATS}
    copy_fs(args, manifest, runner, env, report, checkpoint)
    precheck = []
    worker = runner.NativeWorker(args.native, env)
    try:
        for fs in report["fs"]:
            for selection in SELECTIONS:
                for system in ("FS-scan", "FS-index"):
                    row = worker.call({"op": "query", "root": fs["root"], "format": fs["format"],
                        "version_count": fs["V"], "mode": "scan" if system == "FS-scan" else "index",
                        "selection": selection, "exact_ordinal": 1, "index": fs["index"],
                        "predicate": manifest["controls"]["query"], "values": True})
                    matrix.check_native(row, matrix.oracle_for(manifest, fs["scenario"], fs["format"], fs["V"], selection),
                        fs["format"], fs["V"], selection, system)
                    precheck.append({"scenario": fs["scenario"], "V": fs["V"], "format": fs["format"],
                        "selection": selection, "system": system, "oracle_verified": True, **row})
    finally:
        worker.close()
    save_json(args.work / "native-precheck.json", {"complete": True, "observations": precheck,
        "scope": "untimed correctness precheck, excluded from formal query records"})
    report["native_precheck_sha256"] = sha_file(args.work / "native-precheck.json")
    checkpoint()
    records = []
    try:
        with (args.work / "queries.jsonl").open("x") as output:
            with prior.sync_window(args, report):
                reconcile_preparation(args, manifest, runner, env, report, checkpoint)
                prepare_owners(args, manifest, runner, env, report, checkpoint)
                groups = [(s, v) for s in SCENARIOS for v in VERSIONS]
                for campaign in range(CAMPAIGNS):
                    for group_index, (scenario, versions) in enumerate(groups[campaign:] + groups[:campaign]):
                        base_index = groups.index((scenario, versions))
                        shift = (campaign + base_index) % 4
                        systems = SYSTEMS[shift:] + SYSTEMS[:shift]
                        cell_order = [(fmt, selection) for fmt in FORMATS for selection in SELECTIONS]
                        rotation = (campaign + base_index) % len(cell_order)
                        cell_order = cell_order[rotation:] + cell_order[:rotation]
                        for position, system in enumerate(systems):
                            root = args.work / "queries" / f"campaign-{campaign}" / f"{scenario}-v{versions}-{system}"
                            root.mkdir(parents=True, mode=0o700)
                            def append(row, fmt, selection, repeat):
                                row.update(campaign=campaign, scenario=scenario, V=versions, format=fmt,
                                    selection=selection, system=system, repeat=repeat, arm_position=position)
                                output.write(json.dumps(row, sort_keys=True) + "\n")
                                output.flush()
                                records.append(row)
                                report["query_records"] = len(records)
                            if system in ("V04", "V03"):
                                namespace = next(n for n in report["namespaces"] if
                                    (n["scenario"], n["V"], n["system"]) == (scenario, versions, system))
                                before = journal_digest(Path(namespace["journal"]))
                                with runner.sf.private_mount(Path(namespace["recover_launch"]["path"]), root / "mount", env, args.writer_lock) as context:
                                    offset = 0
                                    namespace.setdefault("query_mounts", []).append({"campaign": campaign,
                                        "startup_ns": context["startup_ns"],
                                        **preparation_config(context, namespace["recover_launch"], root / "recovery.properties")})
                                    if campaign == 0:
                                        for binding in namespace["bindings"]:
                                            path = publish.history_path(context["mount"], binding["logical_path"], binding["version"])
                                            require(sha_file(path) == binding["sha256"], "recovered history bytes differ")
                                        for fmt in FORMATS:
                                            request = runner.discovery_request(namespace["logical_prefix"] + "/" + fmt, templates[fmt], False)
                                            request["request"]["sources"]["versions"] = {"kind": "all"}
                                            response, batches, _, offset = runner.profile_call(context, request, offset)
                                            rows = runner.discovered(response, batches, fmt, namespace["logical_prefix"] + "/" + fmt)
                                            wanted = {(b["logical_path"], b["version"]) for b in namespace["bindings"] if b["format"] == fmt}
                                            require(len(rows) == 32 * versions and {(r["path"], r["version"]) for r in rows} == wanted,
                                                "recovered full scientific directory differs")
                                        namespace["recovery_full_file_hashes"] = len(namespace["bindings"])
                                        namespace["recovery_scientific_units"] = 32 * len(FORMATS) * versions
                                    for fmt, selection in cell_order:
                                        for repeat in range(REPEATS):
                                            row, offset = matrix.iotdb_query(runner, context, namespace, manifest, templates[fmt], fmt, selection, offset)
                                            append(row, fmt, selection, repeat)
                                        os.fsync(output.fileno())
                                require(journal_digest(Path(namespace["journal"])) == before, "query modified owner journal")
                                head_root = root / "post-head"
                                head_root.mkdir(mode=0o700)
                                head, receipt = prep._probe(getattr(args, "probe_" + system.lower()), "head", namespace["prefix"], env, head_root)
                                require(head == namespace["head"], "query modified namespace head")
                                namespace["query_mounts"][-1]["head_receipt"] = receipt
                            else:
                                worker = runner.NativeWorker(args.native, env)
                                try:
                                    for fmt, selection in cell_order:
                                        fs = next(f for f in report["fs"] if (f["scenario"], f["V"], f["format"]) == (scenario, versions, fmt))
                                        expected = matrix.oracle_for(manifest, scenario, fmt, versions, selection)
                                        for repeat in range(REPEATS):
                                            row = worker.call({"op": "query", "root": fs["root"], "format": fmt,
                                                "version_count": versions, "mode": "scan" if system == "FS-scan" else "index",
                                                "selection": selection, "exact_ordinal": 1, "index": fs["index"],
                                                "predicate": manifest["controls"]["query"], "values": True})
                                            matrix.check_native(row, expected, fmt, versions, selection, system)
                                            row["oracle_verified"] = True
                                            append(row, fmt, selection, repeat)
                                        os.fsync(output.fileno())
                                finally:
                                    worker.close()
                            report["last_completed_block"] = {"campaign": campaign, "scenario": scenario, "V": versions, "system": system}
                            checkpoint()
                            print(json.dumps({"event": "query_block_complete", **report["last_completed_block"], "records": len(records)}), flush=True)
                report["raw_post_audit"] = data.verify(args.input)
                for fs in report["fs"]:
                    require(sha_file(fs["index"]) == fs["index_sha256"] and
                            all(sha_file(f["path"]) == f["sha256"] for f in fs["files"]), "FS inputs changed")
                analysis = summarize(records)
                save_json(args.work / "analysis.json", analysis)
                report["analysis_sha256"] = sha_file(args.work / "analysis.json")
                report["queries_sha256"] = sha_file(args.work / "queries.jsonl")
                report["complete"] = True
    except BaseException as error:
        report["complete"] = False
        report["failure"] = {"type": type(error).__name__, "message": "large matrix failed; inspect private logs; no automatic retry"}
        raise
    finally:
        checkpoint()
    print(json.dumps({"complete": report["complete"], "records": len(records),
        "original_service_restored": report.get("original_service_restored")}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("input", "work", "report", "spec", "source-binding", "environment", "frozen-runner", "native",
                 "daemon-v03", "daemon-v04", "probe-v03", "probe-v04", "payload-probe-v04", "writer-lock", "runtime", "release"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--source-pid", type=int, default=1001546)
    parser.add_argument("--datanode-pid", type=int, required=True)
    parser.add_argument("--preparation-report", type=Path)
    parser.add_argument("--formats", nargs="+", choices=("hdf5", "grib"), default=["hdf5", "grib"])
    parser.add_argument("--corpus-module", choices=("large_corpus", "cachefit_corpus"), default="large_corpus")
    run(parser.parse_args())
