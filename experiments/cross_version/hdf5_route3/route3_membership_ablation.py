#!/usr/bin/env python3
"""Bounded four-binary membership ablation over already-published route-3 data.

The default is a fresh 2x2 ablation at N512/V1, Latest and aligned ROI. Each
variant retains one mount for all temporal campaigns. All four binaries,
including the relinked A baseline, have explicit caller-provided identities.
No generation/publication, journal adoption, implicit binaries or inherited rows.
"""
from contextlib import ExitStack
from pathlib import Path
import argparse
import json
import random
import time
from types import SimpleNamespace

from experiments.cross_version.hdf5_route3 import route3_selector_ab as ab

BASE_RUNNER_SHA = "7788de55a7c5cfd6c2be6ef6b17ced22e0792621ac9317372322cea7afce8d6f"
R4_REFERENCE_SHA = "cfe530cb8c293e8d7ff131e27a3fe0208aeadf8ade6a567b274ed06a36235089"
FACTORS = {"A": (False, False), "B": (True, False),
           "C": (False, True), "D": (True, True)}
MEMBERSHIP_CHECKS = {"reverse_source_order_preserved", "valid_source_manifest_reads",
    "unknown_ancestor_malformed_keys_rejected", "omitted_snapshot_source_rejected",
    "unselected_context_rejected", "duplicate_revision_and_reselection_rejected",
    "residual_file_predicate_candidates_retained", "same_path_other_published_versions_rejected"}
METADATA_OBSERVATIONS = {"membership_fixture_with_rejections_bytes"}


def variants(path):
    """Caller explicitly binds all four separately-built binaries and factors."""
    value = ab.load(path)
    ab.require(set(value) == {"variants"} and isinstance(value["variants"], list),
               "variants config must contain only the four variants list")
    result = {}
    for item in value["variants"]:
        ab.require(set(item) == {"label", "record_membership_set", "manifest_membership_set", "daemon", "sha256"},
                   "variant needs explicit label, factors, daemon and SHA")
        label = item["label"]
        ab.require(label in FACTORS and label not in result, "duplicate/unknown variant")
        factors = (item["record_membership_set"], item["manifest_membership_set"])
        ab.require(all(type(flag) is bool for flag in factors) and factors == FACTORS[label],
                   "variant factors do not match the frozen 2x2 design")
        digest = item["sha256"]
        ab.require(isinstance(digest, str) and len(digest) == 64 and
                   all(c in "0123456789abcdef" for c in digest), "invalid variant SHA")
        binary = Path(item["daemon"])
        ab.require(binary.is_absolute() and binary.is_file() and not binary.is_symlink(),
                   "variant binary must be an existing absolute regular file")
        ab.require(ab.sha(binary) == digest, "variant binary SHA mismatch")
        result[label] = dict(item, daemon=str(binary.resolve(strict=True)))
    ab.require(set(result) == set(FACTORS), "all four variants A/B/C/D are required")
    ab.require(len({item["sha256"] for item in result.values()}) == 4,
               "ablation requires four distinct compiled binaries")
    return result


def plan(seed, campaigns, repeats):
    ab.require(type(seed) is int and 0 <= seed < 2**32 and
               1 <= campaigns <= 5 and 2 <= repeats <= 5, "invalid bounded ablation configuration")
    order = list(FACTORS)
    random.Random(seed).shuffle(order)
    tasks = [{"variant": label, "case": "n512r1", "scope": "four-way-factorial"} for label in order]
    return {"seed": seed, "variant_order": order, "tasks": tasks,
        "campaigns": campaigns, "repeats": repeats, "roi": "aligned", "arms": 5,
        "expected_queries": 4 * 5 * campaigns * repeats,
        "mounts": len(tasks), "inherited_query_rows": 0,
        "order_scope": "seeded single random permutation of four retained mount blocks; campaigns rotate query groups and arms within a mount; this is not a counterbalanced or independently replicated variant-order experiment",
        "historical_scope": "earlier N128/other matrices are external background only, never new ablation observations"}


def probe_configs(path):
    if path is None:
        return []
    value = ab.load(path)
    ab.require(set(value) == {"probes"} and isinstance(value["probes"], list), "probe config must contain only a probes list")
    probes = {}
    for item in value["probes"]:
        ab.require(set(item) == {"variant", "binary", "sha256"} and item["variant"] in ("A", "D") and item["variant"] not in probes,
                   "context probe needs unique A/D variant, binary and SHA")
        binary = Path(item["binary"])
        digest = item["sha256"]
        ab.require(binary.is_absolute() and binary.is_file() and not binary.is_symlink() and
                   isinstance(digest, str) and len(digest) == 64 and ab.sha(binary) == digest,
                   "context probe binary identity differs")
        probes[item["variant"]] = dict(item, binary=str(binary.resolve(strict=True)))
    ab.require(set(probes) == {"A", "D"}, "context probe list requires both A and D")
    return [probes[label] for label in ("A", "D")]


def run_context_probes(args, report, probes):
    ab.require({item["variant"] for item in probes} == {"A", "D"} and len(probes) == 2,
               "A/D context probes are required before mounting")
    outputs = {}
    for item in probes:
        work = args.work / "context-probes" / item["variant"]
        work.mkdir(mode=0o700, parents=True)
        result = {}
        ab.context_probe(SimpleNamespace(context_probe=Path(item["binary"]),
            context_probe_sha=item["sha256"], context_probe_timeout=args.context_probe_timeout,
            work=work, report=work / "report.json", env=args.env), result)
        result_path = work / "context-probe/result.json"
        ab.require(ab.sha(result_path) == result["context_probe"]["result_sha256"],
                   "context probe result identity changed")
        outputs[item["variant"]] = ab.load(result_path)
        report.setdefault("context_probes", []).append({"variant": item["variant"], **result["context_probe"]})
        ab.save(args.report, report)
    checks = {}
    for name, expected_keys in (("membership_checks", MEMBERSHIP_CHECKS),
                                ("metadata_budget_observations", METADATA_OBSERVATIONS)):
        values = {label: outputs[label].get(name) for label in ("A", "D")}
        checks[name + "_complete"] = all(isinstance(value, dict) and set(value) == expected_keys and
            all(type(count) is int and count > 0 for count in value.values()) for value in values.values())
        checks[name + "_equal"] = values["A"] == values["D"]
    comparison = {"complete": all(checks.values()), "checks": checks,
        "membership_checks_by_variant": {label: outputs[label].get("membership_checks") for label in ("A", "D")},
        "metadata_budget_observations_by_variant": {label: outputs[label].get("metadata_budget_observations") for label in ("A", "D")},
        "scope": "complete named fixture observations must be positive integers and equal across A/D; observed metadata bytes are compared directly, never hard-coded; gate precedes every formal mount"}
    ab.save(args.work / "context-probes/comparison.json", comparison)
    report["context_probe_consistency"] = comparison
    ab.save(args.report, report)
    ab.require(comparison["complete"], "A/D membership/budget observations differ or are incomplete; no mount permitted")


def summarize(rows):
    report = ab.summarize(rows)
    report["comparison_scope"] = "fresh four-variant N512 cells; one mount block per variant; no inherited rows"
    report["factorial_cells"] = []
    lookup = {}
    for cell in report["cells"]:
        key = (cell["case"], cell["stored_V"], cell["selection"], tuple(cell["origin"]), cell["arm"])
        lookup.setdefault(key, {})[cell["binary"]] = cell
    for key, cells in sorted(lookup.items()):
        if key[0] != "n512r1":
            continue
        ab.require(set(cells) == set(FACTORS), "incomplete four-way factorial cell")
        med = {label: cell["warm_p50_ms"] for label, cell in cells.items()}
        ab.require(all(value > 0 for value in med.values()), "nonpositive complete-query median")
        ratios = {label: [value / baseline for value, baseline in
                   zip(cells[label]["campaign_warm_p50_ms"], cells["A"]["campaign_warm_p50_ms"])]
                  for label in ("B", "C", "D")}
        report["factorial_cells"].append(dict(zip(("case", "stored_V", "selection", "origin", "arm"), key)) |
            {"warm_p50_ms_by_variant": med, "B_over_A": med["B"] / med["A"],
             "C_over_A": med["C"] / med["A"], "D_over_A": med["D"] / med["A"],
             "multiplicative_interaction_DA_over_BC": med["D"] * med["A"] / (med["B"] * med["C"]),
             "campaign_ratios_over_A": ratios})
    report["pairs"] = []
    report["same_run_baseline_available"] = bool(report["factorial_cells"])
    report["interpretation"] = "complete-query descriptive factorial ratios; first requests and warm medians retained per variant; FS controls expose drift; temporal campaigns inside one mount are not independent variant replications; historical matrices are external background only"
    return report


def run(args):
    ab.require(ab.sha(ab.__file__) == BASE_RUNNER_SHA, "shared runner helper is not the frozen allowed revision")
    args.repo = args.repo.resolve(strict=True)
    source_stage = args.source_stage.resolve(strict=True)
    args.work = args.work.resolve()
    ab.require(not args.work.exists() and not args.work.is_relative_to(source_stage),
               "fresh output outside original frozen stage required")
    ab.require(source_stage.parents[3] == args.repo, "source stage belongs to another repo")
    binaries = variants(args.variants)
    configuration = plan(args.seed, args.campaigns, args.repeats)
    probes = probe_configs(args.context_probe_list)
    ab.require(probes, "A/D context probe config is required before any mount")
    library = args.native_library.resolve(strict=True)
    ab.require(ab.sha(library) == ab.NATIVE_SHA, "frozen native R2 helper differs")
    modules = ab.frozen_modules(source_stage, args.repo)
    data, remote, frozen = (modules[n] for n in ("route3_data", "route3_iotdb", "route3_run"))
    ab.require(tuple(frozen.ARMS) == ("FS-scan", "FS-index", "catalog-native-local", "catalog-native-fuse", "typed"),
               "frozen five-path workload differs")
    names = list(dict.fromkeys(task["case"] for task in configuration["tasks"]))
    cases = {name: ab.prepare_case(source_stage, name, "full", data, roi="aligned") for name in names}
    for case in cases.values():
        ab.require(len(case["groups"]) == 1 and case["groups"][0][0]["name"] == "latest" and
                   case["groups"][0][0]["kind"] == "latest" and case["groups"][0][0]["rounds"] == [1] and
                   case["groups"][0][1] == [0, 0, 0, 0], "N512/V1 Latest/aligned scope differs")
    args.work.mkdir(mode=0o700, parents=True)
    args.report = args.work / "report.json"
    args.runtime = args.repo.parent / "runtime"
    args.release = (args.runtime / "current").resolve(strict=True)
    args.datanode_pid = int((args.runtime / "run/datanode.pid").read_text())
    report = {"protocol": "iotdbfs-route3-membership-ablation/1", "complete": False,
        "configuration": configuration, "variants": binaries,
        "baseline_scope": "A is a fresh explicitly SHA-bound relink of R4 source, not reuse of the historical R4 executable or query rows; independent source/build audit must verify factor isolation",
        "historical_R4_reference_sha256": R4_REFERENCE_SHA,
        "variants_config_sha256": ab.sha(args.variants), "runner_sha256": ab.sha(__file__),
        "shared_runner_sha256": BASE_RUNNER_SHA, "source_stage": str(source_stage),
        "source_manifest_sha256": ab.sha(source_stage / "source-r2-manifest.json"),
        "native_sha256": ab.NATIVE_SHA, "inherited_query_rows": 0, "query_records": 0, "mounts": [],
        "cases": [{k: cases[name][k] for k in ("name", "N", "stored_V", "journal_digest", "input_report_sha256", "input_manifest_sha256", "full_raw_validation_ns")} for name in names],
        "reuse_scope": "same completed immutable namespace heads, exact original timestamp bindings, raw bytes and native indexes; no republish or head rewind",
        "startup_scope": "original recovery and idempotent staging cleanup may issue SQL; recovery/setup are separately timed outside complete-query clocks",
        "SDK_zero_scope": "warm socket discovery/typed query_calls plus version_store_query_calls; native FUSE operations outside socket profiles are not a global SQL-zero proof",
        "first_repeat_scope": "first request in a group can already be warmed by previous groups; no cold-disk claim",
        "factor_label_scope": "factor labels are caller-supplied build claims bound to four distinct binary SHAs; source-difference correctness requires the independent build audit"}
    if args.context_probe_list is not None:
        report["context_probe_config_sha256"] = ab.sha(args.context_probe_list)
        report["context_probe_binaries"] = probes
    ab.save(args.report, report)
    startup, paired = modules["v03_startup_wrapper"], modules["paired"]
    descriptor_path = args.repo / "experiments/multi_file/work/runner-20261013/environment.json"
    _, publisher = remote._dependencies()
    descriptor, _, args.env = publisher.runtime(descriptor_path, 1001546, publisher.DATABASE)
    ab.require(args.env["IOTDB_HOST"] in ("localhost", "127.0.0.1") and args.env["IOTDB_PORT"] == "33667",
               "original authorized endpoint differs")
    report["runtime_descriptor_sha256"] = ab.sha(descriptor_path)
    report["python_library_versions"] = descriptor["python_versions"]
    startup.adjust_mount(paired.sf, 1800, args.work / "startup-receipts.jsonl")
    writer_lock = args.repo.parent / "public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock"
    ab.require(writer_lock.is_file(), "existing experiment writer lock missing")
    inspector = args.repo / "experiments/cross_version/work/large-file-upper-r3-20261017/store-inspector/large_store_probe"
    probe = modules["n8_matrix_prepare"]._probe
    rows = []
    started = time.monotonic()
    try:
        with modules["route3_control"].sync_window(args, report):
            run_context_probes(args, report, probes)
            for task in configuration["tasks"]:
                label, name = task["variant"], task["case"]
                variant, case = binaries[label], cases[name]
                root = args.work / label / name
                root.mkdir(parents=True, mode=0o700)
                journal = root / "owner"
                ab.copy_journal(case["owner"], journal, case["journal_digest"])
                for item in ("head-before", "head-after"):
                    (root / item).mkdir()
                prefix = case["report"]["prefix"]
                before_head, before_receipt = probe(inspector, "head", prefix, args.env, root / "head-before")
                ab.require(before_head == case["phase"]["head"], "published namespace head changed")
                launch = ab.create_launcher(modules["v03_n128"], root / "launcher", Path(variant["daemon"]), variant["sha256"], prefix, journal)
                mount_row = {**task, "stored_V": case["stored_V"], "launcher": launch,
                    "head_before": before_head, "head_before_receipt": before_receipt,
                    "journal_digest_before": case["journal_digest"]}
                report["mounts"].append(mount_row)
                ab.save(args.report, report)
                mount_started = time.perf_counter_ns()
                with paired.sf.private_mount(Path(launch["path"]), root / "mount", args.env,
                                             writer_lock, profile=True) as context:
                    mount_row.update(startup_wall_ns=time.perf_counter_ns() - mount_started,
                                     startup_ns=context["startup_ns"], daemon_pid=context["pid"])
                    time.sleep(.05)
                    context["route3_profile_offset"] = remote.reset_profile_offset(context)
                    native_started = time.perf_counter_ns()
                    with ExitStack() as stack:
                        native = {arm: stack.enter_context(modules["route3_native"].NativeContext(library, case["native_config"])) for arm in frozen.ARMS if arm != "typed"}
                        mount_row["native_context_setup_ns"] = time.perf_counter_ns() - native_started
                        for campaign in range(args.campaigns):
                            groups = case["groups"]
                            shift = campaign % len(groups)
                            order = frozen.ARMS[campaign % len(frozen.ARMS):] + frozen.ARMS[:campaign % len(frozen.ARMS)]
                            for selection, origin in groups[shift:] + groups[:shift]:
                                for arm in order:
                                    for repeat in range(args.repeats):
                                        row = frozen.timed_query(arm, native.get(arm), context,
                                            case["report"]["logical_prefix"], remote.equality_predicate(data.PREDICATE),
                                            selection, origin, case["bindings"], case["expected"][selection["name"], tuple(origin)])
                                        row["profiled_sdk_query_calls"] = ab.warm_sdk_queries(row)
                                        if repeat > 0:
                                            ab.require(row["profiled_sdk_query_calls"] in (None, 0), "warm profiled query issued SDK SQL")
                                        row.update(binary=label, variant=label, factor_scope=task["scope"],
                                            record_membership_set=variant["record_membership_set"],
                                            manifest_membership_set=variant["manifest_membership_set"],
                                            case=name, N=case["N"], V=case["stored_V"], stored_V=case["stored_V"],
                                            campaign=campaign, repeat=repeat)
                                        ab.append(args.work / "queries.jsonl", row)
                                        rows.append(row)
                                        report["query_records"] = len(rows)
                                report["last_completed_group"] = {**task, "campaign": campaign, "selection": selection["name"], "origin": origin}
                                ab.save(args.report, report)
                                print(json.dumps({"event": "query_group", "records": len(rows), **report["last_completed_group"]}), flush=True)
                mount_row["mount_and_queries_wall_ns"] = time.perf_counter_ns() - mount_started
                after_head, after_receipt = probe(inspector, "head", prefix, args.env, root / "head-after")
                ab.require(after_head == before_head and ab.journal_snapshot(journal) == case["journal_digest"] and ab.journal_snapshot(case["owner"]) == case["journal_digest"], "query changed head/original/copied journal")
                directory = case["root"] / ("phase-v%d" % case["stored_V"])
                ab.require(ab.sha(directory / "catalog.json") == case["phase"]["catalog_sha256"] and
                    ab.sha(directory / "catalog.sqlite") == case["phase"]["index_sha256_after"], "query changed original native catalog/index")
                ab.require(ab.sha(case["root"] / "report.json") == case["input_report_sha256"] and ab.sha(case["root"] / "raw/manifest.json") == case["input_manifest_sha256"], "original evidence changed")
                mount_row.update(head_after=after_head, head_after_receipt=after_receipt,
                    head_unchanged=True, copied_journal_unchanged=True, original_journal_unchanged=True,
                    native_index_unchanged=True, complete=True)
                ab.save(args.report, report)
            ab.require(len(rows) == configuration["expected_queries"] and all(r["oracle_verified"] for r in rows), "bounded membership ablation incomplete")
            ab.save(args.work / "analysis.json", summarize(rows))
            report.update(complete=True, queries_sha256=ab.sha(args.work / "queries.jsonl"),
                          analysis_sha256=ab.sha(args.work / "analysis.json"))
    except BaseException as error:
        report.update(complete=False, failure={"type": type(error).__name__, "message": "ablation stopped; safety window restores original service; private logs retained; no automatic retry"})
        raise
    finally:
        report["wall_seconds"] = time.monotonic() - started
        ab.save(args.report, report)
    print(json.dumps({"complete": report["complete"], "queries": len(rows), "original_service_restored": report.get("original_service_restored")}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("repo", "source-stage", "work", "variants", "native-library"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--seed", type=int, default=20261018)
    parser.add_argument("--campaigns", type=int, default=5)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--context-probe-list", type=Path, required=True)
    parser.add_argument("--context-probe-timeout", type=int, default=1800)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
