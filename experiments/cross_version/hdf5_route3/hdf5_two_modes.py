#!/usr/bin/env python3
"""Owned end-to-end HDF5 queries: retained R3, indexed, and native HDF5.

Reopen the existing N512/V1 publication; select four complete buckets (C128)
containing both matching datasets. No corpus publication or inherited timings.
This controller must only be launched after the final complete build is bound.
"""
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
import argparse
import json
import statistics
import time

from experiments.cross_version.hdf5_route3 import route3_global_hot_ab as shared

ab = shared.ab
SHARED_SHA = "43b00122aa80d8468d1febed47abf55ebc471d12b4b9113e3fd9a589a69c6016"
BASELINE_SHA = "df9036003417e66bbf8e21e0eb832a01338db986c2b16a7780780b81b362fd56"
MODES = ("baseline-r3", "iotdb-indexed", "native-hdf5")


def plan(fs_scan=False):
    arms = ["FS-index", "typed"] + (["FS-scan"] if fs_scan else [])
    return dict(mount_order=list(MODES), arms=arms, campaigns=2, repeats=3,
                expected_queries=3 * len(arms) * 2 * 3, backing_N=512,
                candidate_sources=128, V=1, selection="latest", K=2,
                origin=[0, 0, 0, 0], shape=[1, 1, 64, 64], inherited_query_rows=0)


def create_launcher(prior, path, binary, digest, prefix, journal, mode):
    ab.require(mode in MODES, "unknown execution mode")
    launch = ab.create_launcher(prior, path, binary, digest, prefix, journal)
    settings = dict(launch["settings"])
    if mode != "baseline-r3":
        before = "SETTINGS=" + repr(settings) + "\n"
        source = Path(path).read_text()
        ab.require(source.count(before) == 1, "generated settings binding changed")
        ab.require(settings.pop("query.hdf5.catalog_mode", None) == "substructure-index",
                   "old frozen launcher catalog configuration differs")
        settings["query.hdf5.mode"] = mode
        Path(path).write_text(source.replace(before, "SETTINGS=" + repr(settings) + "\n", 1))
        launch.update(settings=settings, sha256=ab.sha(path))
    ab.require(settings.get("storage.version_directory.initialize") == "false",
               "this runner must never initialize a namespace")
    return launch


def verify_mode(row, mode):
    calls = row["discovery_calls"] + row["value_calls"]
    if mode == "baseline-r3":
        return
    for call in calls:
        ab.require(call["server_profile"]["server_hdf5_execution"]["mode"] == mode,
                   "actual daemon execution mode differs from requested mode")
    if row["arm"] == "typed":
        ab.require(calls and row["value_calls"], "typed result has no actual value calls")
        for call in row["value_calls"]:
            counters = call["server_profile"]["counters"]
            slabs = counters.get("hdf5.native.slabs", 0)
            reads = counters.get("hdf5.native.source.calls", 0)
            if mode == "native-hdf5":
                ab.require(slabs > 0 and reads > 0 and
                           counters.get("hdf5.selection.decoder_compiles", 0) == 0,
                           "native query did not execute libhdf5 slabs through the source driver")
            else:
                ab.require(slabs == 0 and reads == 0,
                           "indexed query executed the native HDF5 value engine")


def summarize(rows, configuration):
    ab.require(len(rows) == configuration["expected_queries"], "incomplete query matrix")
    cells = []
    for mode in MODES:
        for arm in configuration["arms"]:
            samples = [r for r in rows if (r["mode"], r["arm"]) == (mode, arm)]
            ab.require(len(samples) == 6, "incomplete mode/arm cell")
            warm = [r for r in samples if r["repeat"] > 0]
            cells.append(dict(mode=mode, arm=arm, samples=6, warm_samples=4,
                warm_p50_ms=statistics.median(r["total_ns"] for r in warm) / 1e6,
                first_ms=[r["total_ns"] / 1e6 for r in samples if r["repeat"] == 0],
                campaign_warm_p50_ms=[statistics.median(r["total_ns"] for r in warm
                    if r["campaign"] == c) / 1e6 for c in range(2)],
                discovery_p50_ms=statistics.median(r["discovery_client_ns"] for r in warm) / 1e6,
                value_p50_ms=statistics.median(r["selected_value_client_ns"] for r in warm) / 1e6))
    typed = {r["mode"]: r["warm_p50_ms"] for r in cells if r["arm"] == "typed"}
    controls = {r["mode"]: r["warm_p50_ms"] for r in cells if r["arm"] == "FS-index"}
    return dict(cells=cells, comparisons=[dict(mode=mode,
        typed_over_r3=typed[mode] / typed["baseline-r3"],
        fs_index_over_r3=controls[mode] / controls["baseline-r3"],
        typed_over_same_mount_fs_index=typed[mode] / controls[mode]) for mode in MODES],
        scope="fresh complete client-owned values and mask; C128 is a four-bucket selective subset of N512/V1; mount blocks are baseline then indexed then native; two temporal campaigns are not independent replication; nested stage medians are not additive")


def verify_candidate_binding(args):
    binding = ab.load(args.build_binding)
    ab.require(binding.get("complete") is True, "final build binding is incomplete")
    binaries = binding["binaries"]
    for name, path in (("iotdb_fs", args.candidate), ("v03_context_probe", args.context_probe),
                       ("global_hot_cache_probe", args.global_probe)):
        ab.require(ab.sha(path) == binaries[name], "final build binary binding differs: " + name)
    source = args.candidate_source.resolve(strict=True)
    files = binding["source_files"]
    for name, digest in files.items():
        ab.require(ab.sha(source / name) == digest, "final complete source binding differs: " + name)
    ab.require("include/iotdbfs/formats/hdf5/Hdf5QueryMode.h" in files,
               "final source graph omits execution mode contract")
    args.context_probe_sha = binaries["v03_context_probe"]
    args.global_probe_sha = binaries["global_hot_cache_probe"]
    args.global_probe_source = source / "tests/benchmarks/versioning/global_hot_cache_probe.cpp"
    return binaries["iotdb_fs"]


def run(args):
    ab.require(ab.sha(shared.__file__) == SHARED_SHA and ab.sha(ab.__file__) == shared.HELPER_SHA,
               "frozen shared experiment helper changed")
    args.repo = args.repo.resolve(strict=True)
    args.source_stage = args.source_stage.resolve(strict=True)
    args.work = args.work.resolve()
    ab.require(not args.work.exists() and not args.work.is_relative_to(args.source_stage),
               "fresh output outside original stage required")
    args.baseline = args.baseline.resolve(strict=True)
    args.candidate = args.candidate.resolve(strict=True)
    args.context_probe = args.context_probe.resolve(strict=True)
    args.global_probe = args.global_probe.resolve(strict=True)
    candidate_sha = verify_candidate_binding(args)
    ab.require(ab.sha(args.baseline) == BASELINE_SHA and candidate_sha != BASELINE_SHA,
               "explicit old R3/new binary identities differ")
    library = args.native_library.resolve(strict=True)
    ab.require(ab.sha(library) == ab.NATIVE_SHA, "native FS helper changed")
    modules = ab.frozen_modules(args.source_stage, args.repo)
    data, remote, frozen = (modules[n] for n in ("route3_data", "route3_iotdb", "route3_run"))
    case = ab.prepare_case(args.source_stage, "n512r1", "full", data, roi="aligned")
    configuration = plan(args.fs_scan)
    ab.require(list(data.ROI) == configuration["shape"] and
               len(case["expected"]["latest", (0, 0, 0, 0)]) == 2, "frozen K2 ROI differs")
    buckets = shared.pilot_buckets(case["manifest"], data.PREDICATE)
    prefix = case["report"]["logical_prefix"]
    scope = dict(name="selective128", N=128,
        paths=[dict(kind="subtree", path=prefix + "/" + b) for b in buckets])
    args.work.mkdir(mode=0o700, parents=True)
    args.report = args.work / "report.json"
    pilot = args.work / "native-selective128"
    pilot.mkdir()
    catalog = ab.load(case["root"] / "phase-v1/catalog.json")
    catalog["sources"] = [r for r in catalog["sources"] if Path(r["logical_path"]).parent.name in buckets]
    ab.require(len(catalog["sources"]) == 128, "FS-index candidate population differs")
    ab.save(pilot / "catalog.json", catalog)
    native_config = dict(case["native_config"], catalog_path=str(pilot / "catalog.json"),
                         index_path=str(pilot / "catalog.sqlite"))
    started_setup = time.perf_counter_ns()
    with modules["route3_native"].NativeContext(library, dict(native_config, build_index=True)) as builder:
        index_stats = builder.stats()
    original_files = [case["root"] / p for p in ("phase-v1/catalog.json", "phase-v1/catalog.sqlite", "report.json", "raw/manifest.json")]
    frozen_inputs = {str(p): ab.sha(p) for p in original_files}
    native_hashes = {str(p): ab.sha(p) for p in (pilot / "catalog.json", pilot / "catalog.sqlite")}
    report = dict(protocol="iotdbfs-hdf5-two-modes/1", complete=False, query_records=0,
        plan=configuration, selector_paths=scope["paths"], mounts=[], runner_sha256=ab.sha(__file__),
        build_binding_sha256=ab.sha(args.build_binding), source_case={k:case[k] for k in
            ("name", "N", "stored_V", "journal_digest", "input_report_sha256", "input_manifest_sha256", "full_raw_validation_ns")},
        baseline_sha256=BASELINE_SHA, candidate_sha256=candidate_sha, native_helper_sha256=ab.NATIVE_SHA,
        native_index_build_ns=time.perf_counter_ns()-started_setup, native_index_build_stats=index_stats,
        frozen_inputs=frozen_inputs, native_subset_artifacts=native_hashes,
        timing="discovery, authorization, typed execution, IPC and materialized owned float32/mask inside total; profile-wait and independent oracle excluded; setup, recovery and probes separate",
        limits="existing 1024-source/64MiB request budgets; no scientific data publication or namespace initialization")
    args.runtime = args.repo.parent / "runtime"
    args.release = (args.runtime / "current").resolve(strict=True)
    args.datanode_pid = int((args.runtime / "run/datanode.pid").read_text())
    _, publisher = remote._dependencies()
    _, _, args.env = publisher.runtime(args.repo / "experiments/multi_file/work/runner-20261013/environment.json", 1001546, publisher.DATABASE)
    ab.require(args.env["IOTDB_HOST"] in ("localhost", "127.0.0.1") and args.env["IOTDB_PORT"] == "33667", "original database endpoint differs")
    paired = modules["paired"]
    modules["v03_startup_wrapper"].adjust_mount(paired.sf, 1800, args.work / "startup-receipts.jsonl")
    writer_lock = args.repo.parent / "public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock"
    inspector = args.repo / "experiments/cross_version/work/large-file-upper-r3-20261017/store-inspector/large_store_probe"
    head_probe = modules["n8_matrix_prepare"]._probe
    rows = []
    started = time.monotonic()
    ab.save(args.report, report)
    try:
        with modules["route3_control"].sync_window(args, report):
            shared.run_probe(args, report, args.context_probe, args.context_probe_sha, "v03-context", True)
            shared.run_probe(args, report, args.global_probe, args.global_probe_sha, "global-hot")
            for mode in MODES:
                binary, digest = (args.baseline, BASELINE_SHA) if mode == "baseline-r3" else (args.candidate, candidate_sha)
                root = args.work / mode
                root.mkdir()
                journal = root / "owner"
                ab.copy_journal(case["owner"], journal, case["journal_digest"])
                for name in ("head-before", "head-after"):
                    (root / name).mkdir()
                before, receipt = head_probe(inspector, "head", case["report"]["prefix"], args.env, root / "head-before")
                ab.require(before == case["phase"]["head"], "scientific head changed")
                launch = create_launcher(modules["v03_n128"], root / "launcher", binary, digest, case["report"]["prefix"], journal, mode)
                mount = dict(mode=mode, binary_sha256=digest, launcher=launch, head_before=before, head_before_receipt=receipt)
                report["mounts"].append(mount)
                ab.save(args.report, report)
                tick = time.perf_counter_ns()
                with paired.sf.private_mount(Path(launch["path"]), root / "mount", args.env, writer_lock, profile=True) as context:
                    mount.update(startup_ns=context["startup_ns"], startup_wall_ns=time.perf_counter_ns()-tick, daemon_pid=context["pid"])
                    context["route3_profile_offset"] = remote.reset_profile_offset(context)
                    with ExitStack() as stack:
                        native = {arm:stack.enter_context(modules["route3_native"].NativeContext(library, native_config))
                                  for arm in configuration["arms"] if arm != "typed"}
                        for campaign in range(2):
                            arms = configuration["arms"]
                            order = arms[campaign % len(arms):] + arms[:campaign % len(arms)]
                            for arm in order:
                                for repeat in range(3):
                                    row = shared.timed_query(arm, native.get(arm), context, case, scope, modules)
                                    row.update(mode=mode, binary=mode, N=128, backing_N=512, V=1, campaign=campaign, repeat=repeat)
                                    ab.require(row["oracle_verified"] and row["selected_units"] == 2, "K2 owned result oracle differs")
                                    verify_mode(row, mode)
                                    row["profiled_sdk_query_calls"] = ab.warm_sdk_queries(row)
                                    if repeat > 0 and mode != "native-hdf5":
                                        ab.require(row["profiled_sdk_query_calls"] in (None, 0), "warm query unexpectedly issued SQL")
                                    ab.append(args.work / "queries.jsonl", row)
                                    rows.append(row)
                                    report["query_records"] = len(rows)
                            report["last_group"] = dict(mode=mode, campaign=campaign)
                            ab.save(args.report, report)
                            print(json.dumps(dict(event="group", rows=len(rows), **report["last_group"])), flush=True)
                after, receipt = head_probe(inspector, "head", case["report"]["prefix"], args.env, root / "head-after")
                ab.require(after == before and ab.journal_snapshot(journal) == ab.journal_snapshot(case["owner"]) == case["journal_digest"], "scientific head or owner journal changed")
                mount.update(complete=True, head_after=after, head_after_receipt=receipt, head_unchanged=True, journals_unchanged=True)
                ab.save(args.report, report)
            for path, digest in {**frozen_inputs, **native_hashes}.items():
                ab.require(ab.sha(path) == digest, "scientific/native input evidence changed")
            analysis = summarize(rows, configuration)
            ab.save(args.work / "analysis.json", analysis)
            report.update(complete=True, queries_sha256=ab.sha(args.work / "queries.jsonl"), analysis_sha256=ab.sha(args.work / "analysis.json"))
    except BaseException as error:
        report.update(complete=False, failure=dict(type=type(error).__name__, message="stopped; original service restoration attempted by cancel-safe control; no automatic retry"))
        raise
    finally:
        report["wall_seconds"] = time.monotonic()-started
        ab.save(args.report, report)
    print(json.dumps(dict(complete=report["complete"], queries=len(rows), restored=report.get("original_service_restored"))), flush=True)


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("repo", "source-stage", "work", "baseline", "candidate", "candidate-source", "build-binding", "native-library", "context-probe", "global-probe"):
        p.add_argument("--"+name, type=Path, required=True)
    p.add_argument("--fs-scan", action="store_true")
    return p


if __name__ == "__main__":
    run(parser().parse_args())
