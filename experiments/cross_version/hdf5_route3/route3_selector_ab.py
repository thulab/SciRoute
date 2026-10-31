#!/usr/bin/env python3
"""Reopen published route-3 namespaces for bounded selector old/new A/B.

No generation, publication, schema initialization, journal adoption or implicit
binary substitution. Each original journal is copied into the NEW run tree.
With both binaries, baseline mounts run first and candidate mounts second.
Single-binary execution never imports historical query rows or calls those
references a new paired A/B. Campaigns rotate inside each retained mount.
"""
from pathlib import Path
from contextlib import ExitStack
import argparse
import hashlib
import importlib
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
import uuid

PROTOCOL = "iotdbfs-route3-selector-ab/1"
CASES = {"n128r3": (128, 8), "n512r1": (512, 1), "n1024r1": (1024, 1)}
NATIVE_SHA = "d834c4c7a2a0a57f99eae39fcfbdd1d21b95990fbd6ad4f5a789d6f94c15c50a"
CONTEXT_CASES = {
    "store_counters_calibrated_against_actual_head_query",
    "predicate_miss_retained_in_all_candidate_context",
    "stable_admit_and_manifest_actual_sql_zero",
    "stable_discovery_actual_sql_zero",
    "warm_source_and_catalog_budgets_still_enforced",
    "unrelated_commit_preserves_frozen_binding",
    "latest_resolved_once_existing_context_stays_exact",
    "ring_gap_forces_source_and_ancestor_revalidation",
    "predicate_miss_source_permission_revocation_enforced",
    "current_ancestor_search_revocation_enforced",
    "exact_deleted_predicate_miss_rejects_new_admission",
}


def require(value, message):
    if not value:
        raise RuntimeError(message)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1048576), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load(path):
    return json.loads(Path(path).read_text())


def save(path, row):
    path = Path(path)
    pending = path.with_name(path.name + ".pending")
    with pending.open("w") as stream:
        json.dump(row, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(pending, path)


def append(path, row):
    with Path(path).open("a") as stream:
        stream.write(json.dumps(row, sort_keys=True, allow_nan=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def journal_snapshot(directory):
    directory = Path(directory)
    require(directory.is_dir() and not directory.is_symlink(), "owner journal missing or symlinked")
    observed = {}
    for path in directory.rglob("*"):
        require(not path.is_symlink() and path.is_file(), "unexpected owner journal entry")
        observed[path.relative_to(directory).as_posix()] = sha(path)
    require(set(observed) == {"owner.lock", "state"}, "completed owner journal must contain state and owner.lock only")
    return observed


def copy_journal(original, destination, expected):
    require(journal_snapshot(original) == expected and not Path(destination).exists(),
            "original journal changed or destination exists")
    shutil.copytree(original, destination)
    require(journal_snapshot(destination) == expected, "copied journal binding differs")


def parse_cases(value):
    names = value.split(",")
    require(names and len(names) == len(set(names)) and all(name in CASES for name in names),
            "cases must be a unique comma-separated subset of n128r3,n512r1,n1024r1")
    return names


def plan(mode, campaigns, repeats, binaries="both", cases=None, roi="both"):
    require(mode in ("probe", "full") and 1 <= campaigns <= 5 and 2 <= repeats <= 5,
            "invalid bounded A/B configuration")
    require(binaries in ("both", "candidate", "baseline"), "unknown binary selection")
    require(roi in ("both", "aligned"), "unknown ROI selection")
    cases = (["n128r3"] if mode == "probe" else list(CASES)) if cases is None else list(cases)
    require(cases and len(cases) == len(set(cases)) and all(name in CASES for name in cases),
            "cases must be a unique non-empty subset of published cases")
    require(mode != "probe" or cases == ["n128r3"], "probe is scoped to N128 stored-V8")
    binary_order = ["baseline", "candidate"] if binaries == "both" else [binaries]
    roi_count = 1 if mode == "probe" or roi == "aligned" else 2
    groups = sum((2 if mode == "probe" else (3 if CASES[name][1] == 8 else 1)) * roi_count
                 for name in cases)
    return {"cases": cases, "campaigns": campaigns, "repeats": repeats,
            "mode": mode, "binaries": binaries, "roi": "aligned" if mode == "probe" else roi,
            "mounts": len(cases) * len(binary_order),
            "expected_queries": groups * 5 * len(binary_order) * campaigns * repeats,
            "binary_order": binary_order, "inherited_query_rows": 0,
            "comparison_scope": "same-run blocked A/B" if binaries == "both" else
                binaries + "-only new observations; historical baselines and previous anchors are external references, not new queries",
            "order_limitation": "campaigns rotate inside each process; all baseline case mounts, when requested, precede all candidate case mounts; historical references are from an earlier execution/time period"}


def choose_groups(phase, mode, roi="both"):
    require(roi in ("both", "aligned"), "unknown ROI selection")
    selections = phase["selections"]
    if mode == "probe":
        selections = [s for s in selections if s["name"] in ("latest", "historical-one")]
        require(len(selections) == 2, "probe needs stored-V8 Latest and historical-one")
        origins = [[0, 0, 0, 0]]
    else:
        origins = [[0, 0, 0, 0]] if roi == "aligned" else [[0, 0, 0, 0], [0, 0, 16, 16]]
    require(all(s["kind"] in ("latest", "between") and s["rounds"] and
                len(s["rounds"]) <= 2 for s in selections), "unbounded or unknown selection")
    return [(s, origin) for s in selections for origin in origins]


def create_launcher(prior, path, binary, digest, prefix, journal):
    require(sha(binary) == digest, "daemon not in caller's explicit SHA allowlist")
    launch = prior.launcher(Path(path), Path(binary), prefix, Path(journal), False)
    require(launch["binary_sha256"] == digest and
            launch["settings"]["storage.version_directory.initialize"] == "false",
            "recovery launcher differs from approved binary/settings")
    settings = dict(launch["settings"])
    before = "SETTINGS=" + repr(settings) + "\n"
    source = Path(path).read_text()
    require(source.count(before) == 1, "frozen launcher representation changed")
    settings["query.frontend.max_registrations"] = "8192"
    Path(path).write_text(source.replace(before, "SETTINGS=" + repr(settings) + "\n", 1))
    launch.update(settings=settings, sha256=sha(path))
    return launch


def warm_sdk_queries(row):
    calls = row["discovery_calls"] + row["value_calls"]
    total = 0
    for call in calls:
        delta = call["server_profile"]["iotdb_delta"]
        require("query_calls" in delta and "version_store_query_calls" in delta,
                "SDK profile query counters unavailable")
        total += delta["query_calls"] + delta["version_store_query_calls"]
    return total if calls else None


def summarize(rows):
    grouped = {}
    for row in rows:
        key = (row["case"], row["stored_V"], row["selection"], tuple(row["origin"]), row["arm"], row["binary"])
        grouped.setdefault(key, []).append(row)
    cells = []
    for key, records in sorted(grouped.items()):
        warm = [r for r in records if r["repeat"] > 0]
        campaigns = sorted({r["campaign"] for r in records})
        cell = dict(zip(("case", "stored_V", "selection", "origin", "arm", "binary"), key))
        cell.update(samples=len(records), warm_samples=len(warm),
            warm_p50_ms=statistics.median(r["total_ns"] for r in warm) / 1e6,
            discovery_p50_ms=statistics.median(r["discovery_client_ns"] for r in warm) / 1e6,
            value_p50_ms=statistics.median(r["selected_value_client_ns"] for r in warm) / 1e6,
            campaign_warm_p50_ms=[statistics.median(r["total_ns"] for r in warm
                if r["campaign"] == c) / 1e6 for c in campaigns],
            first_ms=[r["total_ns"] / 1e6 for r in records if r["repeat"] == 0],
            profiled_warm_sdk_query_counts=[r["profiled_sdk_query_calls"] for r in warm])
        cells.append(cell)
    lookup = {(c["case"], c["stored_V"], c["selection"], tuple(c["origin"]), c["arm"], c["binary"]): c for c in cells}
    pairs = []
    for cell in cells:
        if cell["binary"] != "candidate":
            continue
        key = (cell["case"], cell["stored_V"], cell["selection"], tuple(cell["origin"]), cell["arm"], "baseline")
        if key not in lookup:
            continue
        baseline = lookup[key]
        pairs.append({k: cell[k] for k in ("case", "stored_V", "selection", "origin", "arm")} |
            {"baseline_ms": baseline["warm_p50_ms"], "candidate_ms": cell["warm_p50_ms"],
             "candidate_over_baseline": cell["warm_p50_ms"] / baseline["warm_p50_ms"],
             "campaign_ratios": [a / b for a, b in zip(cell["campaign_warm_p50_ms"], baseline["campaign_warm_p50_ms"])]})
    return {"complete": True, "actual_query_records": len(rows), "inherited_query_rows": 0,
        "observed_binaries": sorted({r["binary"] for r in rows}),
        "cells": cells, "pairs": pairs,
        "same_run_baseline_available": bool(pairs),
        "comparison_scope": "matched cells from blocked same-run baseline/candidate observations" if pairs else
            "no current paired A/B baseline; historical references or earlier anchors are external and are not current queries",
        "interpretation": "full-query descriptive observations; blocked order, historical time separation and temporal campaigns do not establish independent statistical replication; no inherited rows, automatic deployment or speedup threshold"}


def frozen_modules(stage, repo):
    cross = stage / "source-r2/experiments/cross_version"
    manifest = load(stage / "source-r2-manifest.json")
    require(sha(stage / "source-r2.tar.gz") == manifest["archive_sha256"], "frozen source archive changed")
    sys.path.insert(0, str(cross))
    names = ("v03_startup_wrapper", "route3_data", "route3_iotdb", "route3_native", "route3_run", "route3_control",
             "v03_n128", "n8_matrix_prepare")
    result = {}
    for name in names:
        path = cross / (name + ".py")
        relative = "experiments/cross_version/" + path.name
        require(sha(path) == manifest["files"][relative]["sha256"], "frozen helper changed: " + name)
        module = importlib.import_module(name)
        require(Path(module.__file__).resolve() == path.resolve(), "unexpected imported helper: " + name)
        result[name] = module
        if name == "v03_startup_wrapper":
            # Load the pinned runner/dependencies before corpus/publish can
            # populate Python's module cache with another helper snapshot.
            paired = module.load_runner(repo / "experiments/multi_file/work/hdf5-predicate-compiled-20261015/runner-source/experiments/multi_file/small_runner.py")
            sys.modules["small_runner"] = paired
            result["paired"] = paired
    return result


def context_probe(args, report):
    if args.context_probe is None:
        return
    binary = args.context_probe.resolve(strict=True)
    require(args.context_probe_sha and sha(binary) == args.context_probe_sha,
            "context probe explicit SHA differs")
    root = args.work / "context-probe"
    root.mkdir(mode=0o700)
    prefix = "vd_v03_select_" + uuid.uuid4().hex
    journal = root / "owner"
    command = [str(binary), "--prefix", prefix, "--journal", str(journal)]
    started = time.perf_counter_ns()
    stdout, stderr = root / "stdout.private.log", root / "stderr.private.log"
    with stdout.open("x") as out, stderr.open("x") as err:
        result = subprocess.run(command, env=args.env, stdin=subprocess.DEVNULL,
            stdout=out, stderr=err, text=True, timeout=args.context_probe_timeout)
    objects = [json.loads(line) for line in stdout.read_text().splitlines() if line.startswith("{")]
    receipt = {"command": command, "prefix": prefix, "journal": str(journal),
        "binary_sha256": args.context_probe_sha, "exit_code": result.returncode,
        "elapsed_ns": time.perf_counter_ns() - started,
        "stdout": str(stdout), "stdout_sha256": sha(stdout),
        "stderr": str(stderr), "stderr_sha256": sha(stderr),
        "scope": "new private probe prefix in original authorized database, existing context integration including 257-commit gap; outside all query/startup clocks"}
    save(root / "receipt.json", receipt)
    require(result.returncode == 0 and len(objects) == 1, "context probe failed; private stdout/stderr retained")
    output = objects[0]
    save(root / "result.json", output)
    require(output.get("completed") is True and output.get("case_count") == len(CONTEXT_CASES) and
            len(output.get("cases", [])) == len(CONTEXT_CASES) and set(output["cases"]) == CONTEXT_CASES and
            output.get("history_gap_commits") == 257 and
            output.get("prefix") == prefix and output.get("journal") == str(journal),
            "context probe completion/case/history-gap identity differs")
    receipt.update(complete=True, case_count=len(CONTEXT_CASES), history_gap_commits=257, result_sha256=sha(root / "result.json"))
    save(root / "receipt.json", receipt)
    report["context_probe"] = receipt
    save(args.report, report)


def prepare_case(source_stage, name, mode, data, roi="both"):
    root = source_stage / "cases" / name
    report = load(root / "report.json")
    N, V = CASES[name]
    require(report.get("complete") and report.get("controller_restoration_verified") and
            report["N"] == N and report["prefix"] == "vd_v04_rt3_" + name,
            "published case is incomplete or has unexpected scope")
    manifest = load(root / "raw/manifest.json")
    require(sha(root / "raw/manifest.json") == report["native_source_manifest_sha256"] and
            manifest["N"] == N and manifest["rounds"] == V, "input source manifest differs")
    require(sha(root / "bindings.jsonl") == report["bindings_sha256"] and
            sha(root / "queries.jsonl") == report["queries_sha256"], "input binding/query proof changed")
    bindings = [json.loads(line) for line in (root / "bindings.jsonl").read_text().splitlines()]
    require(len(bindings) == N * V, "scientific binding count differs")
    by_version = {(b["path"], b["version"]): b for b in bindings}
    by_round = {(b["path"], b["ordinal"]): b for b in bindings}
    require(len(by_version) == len(by_round) == len(bindings), "duplicate input exact version/round")
    phase = next(p for p in report["phases"] if p["V"] == V)
    directory = root / ("phase-v%d" % V)
    require(phase.get("complete") and phase.get("head_verified") and phase.get("journal_unchanged"),
            "published phase was not read-only verified")
    require(sha(directory / "catalog.json") == phase["catalog_sha256"] and
            sha(directory / "catalog.sqlite") == phase["index_sha256_before"] == phase["index_sha256_after"],
            "original native catalog/index changed")
    started = time.perf_counter_ns()
    for entry in manifest["entries"]:
        path = root / "raw" / entry["file"]
        require(not path.is_symlink() and path.is_file() and path.stat().st_nlink == 1 and
                sha(path) == entry["sha256"], "published raw source changed")
        logical = report["logical_prefix"] + "/" + entry["logical_file"]
        matching = by_round.get((logical, entry["ordinal"]))
        require(matching is not None and Path(matching["source"]).resolve() == path.resolve() and
                matching["sha256"] == entry["sha256"], "raw/binding identity differs")
    groups = choose_groups(phase, mode, roi)
    expected = {(s["name"], tuple(origin)): data.expected(manifest, s["rounds"], origin)
                for s, origin in groups}
    return {"name": name, "N": N, "stored_V": V, "root": root, "report": report,
        "manifest": manifest, "bindings": by_version, "phase": phase, "groups": groups,
        "expected": expected, "owner": root / "owner", "journal_digest": journal_snapshot(root / "owner"),
        "native_config": {"catalog_path": str(directory / "catalog.json"), "index_path": str(directory / "catalog.sqlite"),
            "cache_files": 8, "cache_datasets": 8, "metadata_cache_bytes": 2097152, "raw_chunk_cache_bytes": 262144},
        "input_report_sha256": sha(root / "report.json"), "input_manifest_sha256": sha(root / "raw/manifest.json"),
        "full_raw_validation_ns": time.perf_counter_ns() - started}


def run(args):
    args.repo = args.repo.resolve(strict=True)
    source_stage = args.source_stage.resolve(strict=True)
    args.work = args.work.resolve()
    require(not args.work.exists(), "new A/B work tree required; no automatic resume/overwrite")
    require(source_stage != args.work and not args.work.is_relative_to(source_stage),
            "A/B output must be outside the original frozen stage")
    require(source_stage.parents[3] == args.repo, "source stage belongs to another repo")
    configuration = plan(args.mode, args.campaigns, args.repeats, args.binaries, args.cases, args.roi)
    require((args.baseline_daemon is None) == (args.baseline_sha is None),
            "baseline path and SHA must be supplied together")
    require((args.candidate_daemon is None) == (args.candidate_sha is None),
            "candidate path and SHA must be supplied together")
    require(args.binaries == "candidate" or args.baseline_daemon is not None,
            "requested baseline requires an explicit path and SHA")
    require(args.binaries == "baseline" or args.candidate_daemon is not None,
            "requested candidate requires an explicit path and SHA")
    binary = {}
    if args.binaries != "baseline":
        binary["candidate"] = (args.candidate_daemon.resolve(strict=True), args.candidate_sha)
    baseline_reference = None
    if args.baseline_daemon is not None:
        baseline_path = args.baseline_daemon.resolve(strict=True)
        require(sha(baseline_path) == args.baseline_sha, "baseline explicit SHA differs")
        if args.binaries != "candidate":
            binary["baseline"] = (baseline_path, args.baseline_sha)
        else:
            baseline_reference = {"path": str(baseline_path), "sha256": args.baseline_sha,
                                  "executed_in_this_run": False, "query_rows_inherited": 0}
    require(all(len(s) == 64 and all(c in "0123456789abcdef" for c in s) and sha(p) == s
                for p, s in binary.values()), "baseline/candidate explicit binary allowlist mismatch")
    require(args.binaries != "both" or args.baseline_sha != args.candidate_sha,
            "baseline and candidate must be distinct binaries")
    library = args.native_library.resolve(strict=True)
    require(args.native_sha == NATIVE_SHA and sha(library) == args.native_sha, "frozen native helper differs")
    require((args.context_probe is None) == (args.context_probe_sha is None),
            "context probe path and SHA must be supplied together")
    modules = frozen_modules(source_stage, args.repo)
    data, remote, frozen = (modules[n] for n in ("route3_data", "route3_iotdb", "route3_run"))
    cases = [prepare_case(source_stage, n, args.mode, data, args.roi) for n in configuration["cases"]]
    args.work.mkdir(mode=0o700, parents=True)
    args.report = args.work / "report.json"
    args.runtime = args.repo.parent / "runtime"
    args.release = (args.runtime / "current").resolve(strict=True)
    args.datanode_pid = int((args.runtime / "run/datanode.pid").read_text())
    report = {"protocol": PROTOCOL, "complete": False, "configuration": configuration,
        "source_stage": str(source_stage), "source_manifest_sha256": sha(source_stage / "source-r2-manifest.json"),
        "binaries": {k: {"path": str(p), "sha256": s} for k, (p, s) in binary.items()},
        "unexecuted_baseline_binary_reference": baseline_reference,
        "inherited_query_rows": 0,
        "historical_baseline_scope": "original sealed matrices and earlier anchor observations may be compared externally; their rows are never appended here or counted as this run's queries",
        "native_library": str(library), "native_sha256": args.native_sha,
        "runner_sha256": sha(__file__), "cases": [{k: c[k] for k in ("name", "N", "stored_V", "journal_digest", "input_report_sha256", "input_manifest_sha256", "full_raw_validation_ns")} for c in cases],
        "mounts": [], "query_records": 0,
        "reuse_scope": "same final immutable namespace heads, exact historical bindings, raw bytes and native indexes; N128 remains stored-V8; no republish or V1 namespace reconstruction",
        "startup_scope": "recovery validates all history and may issue existing idempotent staging cleanup DELETE/INSERT statements; startup outside query clocks; no claim that mount is free of database writes",
        "SDK_zero_scope": "warm query socket discovery/typed profile query_calls+version_store_query_calls; native FUSE operations outside socket profiles are not a global SDK-zero proof",
        "first_repeat_scope": "first request per selection/ROI group, possibly prewarmed by prior groups; no cold-disk claim"}
    save(args.report, report)
    startup = modules["v03_startup_wrapper"]
    paired = modules["paired"]
    descriptor_path = args.repo / "experiments/multi_file/work/runner-20261013/environment.json"
    _, publisher = remote._dependencies()
    descriptor, _, args.env = publisher.runtime(descriptor_path, 1001546, publisher.DATABASE)
    require(args.env["IOTDB_HOST"] in ("localhost", "127.0.0.1") and args.env["IOTDB_PORT"] == "33667",
            "original authorized database endpoint differs")
    report["runtime_descriptor_sha256"] = sha(descriptor_path)
    report["python_library_versions"] = descriptor["python_versions"]
    startup.adjust_mount(paired.sf, 1800, args.work / "startup-receipts.jsonl")
    writer_lock = args.repo.parent / "public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock"
    require(writer_lock.is_file(), "existing experiment writer lock missing")
    inspector = args.repo / "experiments/cross_version/work/large-file-upper-r3-20261017/store-inspector/large_store_probe"
    probe = modules["n8_matrix_prepare"]._probe
    rows = []
    started = time.monotonic()
    try:
        with modules["route3_control"].sync_window(args, report):
            context_probe(args, report)
            for label in configuration["binary_order"]:
                daemon, digest = binary[label]
                for case in cases:
                    root = args.work / label / case["name"]
                    root.mkdir(parents=True, mode=0o700)
                    journal = root / "owner"
                    copy_journal(case["owner"], journal, case["journal_digest"])
                    prefix = case["report"]["prefix"]
                    for name in ("head-before", "head-after"):
                        (root / name).mkdir()
                    before_head, before_receipt = probe(inspector, "head", prefix, args.env, root / "head-before")
                    require(before_head == case["phase"]["head"], "published namespace head changed")
                    launch = create_launcher(modules["v03_n128"], root / "launcher", daemon, digest, prefix, journal)
                    mount_row = {"binary": label, "case": case["name"], "stored_V": case["stored_V"],
                        "launcher": launch, "head_before": before_head, "head_before_receipt": before_receipt,
                        "journal_digest_before": case["journal_digest"]}
                    report["mounts"].append(mount_row)
                    save(args.report, report)
                    mount_started = time.perf_counter_ns()
                    # Direct established private_mount, with this runner's
                    # explicit SHA allowlist; frozen remote.mount is untouched.
                    with paired.sf.private_mount(Path(launch["path"]), root / "mount", args.env,
                                                 writer_lock, profile=True) as context:
                        mount_row.update(startup_wall_ns=time.perf_counter_ns() - mount_started,
                                         startup_ns=context["startup_ns"], daemon_pid=context["pid"])
                        time.sleep(.05)
                        context["route3_profile_offset"] = remote.reset_profile_offset(context)
                        native_started = time.perf_counter_ns()
                        with ExitStack() as stack:
                            native = {arm: stack.enter_context(modules["route3_native"].NativeContext(library, case["native_config"]))
                                      for arm in frozen.ARMS if arm != "typed"}
                            mount_row["native_context_setup_ns"] = time.perf_counter_ns() - native_started
                            for campaign in range(args.campaigns):
                                groups = case["groups"]
                                shift = campaign % len(groups)
                                arm_order = frozen.ARMS[campaign % len(frozen.ARMS):] + frozen.ARMS[:campaign % len(frozen.ARMS)]
                                for selection, origin in groups[shift:] + groups[:shift]:
                                    for arm in arm_order:
                                        for repeat in range(args.repeats):
                                            row = frozen.timed_query(arm, native.get(arm), context,
                                                case["report"]["logical_prefix"], remote.equality_predicate(data.PREDICATE),
                                                selection, origin, case["bindings"],
                                                case["expected"][selection["name"], tuple(origin)])
                                            row["profiled_sdk_query_calls"] = warm_sdk_queries(row)
                                            if repeat > 0:
                                                require(row["profiled_sdk_query_calls"] in (None, 0), "warm profiled query unexpectedly submitted SDK SQL")
                                            row.update(binary=label, case=case["name"], N=case["N"], V=case["stored_V"],
                                                stored_V=case["stored_V"], campaign=campaign, repeat=repeat)
                                            append(args.work / "queries.jsonl", row)
                                            rows.append(row)
                                            report["query_records"] = len(rows)
                                    report["last_completed_group"] = {"binary": label, "case": case["name"], "campaign": campaign,
                                        "selection": selection["name"], "origin": origin}
                                    save(args.report, report)
                                    print(json.dumps({"event": "query_group", "records": len(rows), **report["last_completed_group"]}), flush=True)
                    mount_row["mount_and_queries_wall_ns"] = time.perf_counter_ns() - mount_started
                    after_head, after_receipt = probe(inspector, "head", prefix, args.env, root / "head-after")
                    require(after_head == before_head and journal_snapshot(journal) == case["journal_digest"] and
                            journal_snapshot(case["owner"]) == case["journal_digest"], "query changed head/original/copied journal")
                    directory = case["root"] / ("phase-v%d" % case["stored_V"])
                    require(sha(directory / "catalog.json") == case["phase"]["catalog_sha256"] and
                            sha(directory / "catalog.sqlite") == case["phase"]["index_sha256_after"], "query changed original native catalog/index")
                    require(sha(case["root"] / "report.json") == case["input_report_sha256"] and
                            sha(case["root"] / "raw/manifest.json") == case["input_manifest_sha256"], "original evidence changed")
                    mount_row.update(head_after=after_head, head_after_receipt=after_receipt,
                        head_unchanged=True, copied_journal_unchanged=True, original_journal_unchanged=True,
                        native_index_unchanged=True, complete=True)
                    save(args.report, report)
            require(len(rows) == configuration["expected_queries"] and all(r["oracle_verified"] for r in rows),
                    "bounded same-matrix A/B incomplete")
            save(args.work / "analysis.json", summarize(rows))
            report.update(complete=True, queries_sha256=sha(args.work / "queries.jsonl"),
                          analysis_sha256=sha(args.work / "analysis.json"))
    except BaseException as error:
        report.update(complete=False, failure={"type": type(error).__name__,
            "message": "A/B stopped; original-service safety window restores; private logs retained; no automatic retry"})
        raise
    finally:
        report["wall_seconds"] = time.monotonic() - started
        save(args.report, report)
    print(json.dumps({"complete": report["complete"], "queries": len(rows),
        "original_service_restored": report.get("original_service_restored")}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("repo", "source-stage", "work", "native-library"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--baseline-daemon", type=Path)
    parser.add_argument("--baseline-sha")
    parser.add_argument("--candidate-daemon", type=Path)
    parser.add_argument("--candidate-sha")
    parser.add_argument("--binaries", choices=("both", "candidate", "baseline"), default="both")
    parser.add_argument("--roi", choices=("both", "aligned"), default="both",
                        help="full mode ROI subset; probe always uses aligned origin zero")
    parser.add_argument("--cases", type=parse_cases,
                        help="comma-separated published case names; defaults to original mode scope")
    parser.add_argument("--native-sha", default=NATIVE_SHA)
    parser.add_argument("--mode", choices=("probe", "full"), default="probe")
    parser.add_argument("--context-probe", type=Path)
    parser.add_argument("--context-probe-sha")
    parser.add_argument("--context-probe-timeout", type=int, default=1800)
    parser.add_argument("--campaigns", type=int)
    parser.add_argument("--repeats", type=int)
    args = parser.parse_args()
    args.campaigns = args.campaigns if args.campaigns is not None else (2 if args.mode == "probe" else 3)
    args.repeats = args.repeats if args.repeats is not None else (3 if args.mode == "probe" else 5)
    run(args)


if __name__ == "__main__":
    main()
