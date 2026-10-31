#!/usr/bin/env python3
"""Validate and summarize the paired independent-session Q=1/10/100 campaign."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from pathlib import Path
import statistics

import runner as base
import session_runner as experiment


def median(values):
    return statistics.median(list(values))


def validate_formal(data):
    base.require(data.get("protocol") == experiment.PROTOCOL and data.get("complete") is True and
                 data.get("smoke_only") is False, "formal session campaign incomplete or smoke-only")
    base.require(data["q_values"] == [1, 10, 100] and len(data["campaigns"]) == 3,
                 "three paired campaigns/Q=1/10/100 required")
    base.require(data["profiling"], "diagnostic summary requires profiles")
    expected = {(tier, case) for tier in ("small", "medium", "large")
                for case in ("point", "small_roi", "cross_chunk")}
    for campaign in data["campaigns"]:
        base.require(set(campaign["sources"]) == {"small", "medium", "large"} and
                     all(s.get("full_copy_sha_verified") for s in campaign["sources"].values()),
                     "source SHA verification incomplete")
        for key in ("unbound_cases", "bound_cases"):
            cases = campaign[key]
            base.require(len(cases) == 9 and {(c["tier"], c["case"]) for c in cases} == expected,
                         "missing or duplicate scenario")
        for case in campaign["unbound_cases"]:
            source = campaign["sources"][case["tier"]]
            for arm in ("fs", "iotdb"):
                rows = case["observations"][arm]
                base.require(len(rows) == 100, "100 actual unbound reads required")
                calculated = base.summary(source["writes"][arm]["total_ns"],
                    source["source_locator_ns"] if arm == "iotdb" else 0, rows, data["q_values"])
                base.require(calculated == case["summary"][arm], "unbound accounting differs")
        for case in campaign["bound_cases"]:
            source = campaign["sources"][case["tier"]]
            base.require(len(case["sessions"]) == 3 and [s["q"] for s in case["sessions"]] == [1, 10, 100],
                         "each Q requires an independent actual session")
            prepared_ids, result_paths = set(), set()
            for session in case["sessions"]:
                q = session["q"]
                for arm in ("fs", "iotdb"):
                    info = session["arms"][arm]
                    rows = info["observations"]
                    calculated = experiment.session_summary(source["writes"][arm]["total_ns"],
                        source["source_locator_ns"] if arm == "iotdb" else 0,
                        info["binding"]["total_ns"], info["release"]["total_ns"], rows, q)
                    base.require(calculated == session["summary"][arm], "bound accounting differs")
                    if arm == "fs":
                        base.require(all(r["mode"] == "bound" and
                            r["session_bind_ns"] == info["binding"]["total_ns"] for r in rows),
                            "native session binding must be counted once")
                        base.require(all(r["session_close_ns"] == 0 for r in rows[:-1]) and
                            rows[-1]["session_close_ns"] == info["release"]["total_ns"],
                            "native session close differs")
                    else:
                        prepared_id = info["binding"]["prepared_id"]
                        base.require(prepared_id not in prepared_ids, "Q reused an earlier prepared session")
                        prepared_ids.add(prepared_id)
                        base.require(info["release"].get("released") is True and
                                     info["unique_result_paths"] == q, "binding/results not released or unique")
                        for index, row in enumerate(rows):
                            path = row["result_path"]
                            base.require(path not in result_paths, "bound read reused an IPC result")
                            result_paths.add(path)
                            experiment.validate_bound_profile(row["server_profile"], index, case["selection"]["rows"])


def profile_metrics(profiles):
    result = {"server_total_ns": median(p["server_total_ns"] for p in profiles)}
    for category in ("stages_ns", "counters", "iotdb_delta", "runtime_delta"):
        keys = set().union(*(p.get(category, {}).keys() for p in profiles))
        result[category] = {key: median(p.get(category, {}).get(key, 0) for p in profiles)
                            for key in sorted(keys)}
    return result


def summarize(data):
    validate_formal(data)
    campaigns = data["campaigns"]
    report = {"protocol": "iotdbfs-single-file-session-summary/1", "complete": True,
        "campaigns": 3, "observations_per_arm": 5697, "unbound_observations_per_arm": 2700,
        "bound_observations_per_arm": 2997, "q_values": [1, 10, 100],
        "aggregation": "Medians of three campaign statistics; ratios are medians of paired ratios. Every bound Q is its own real bind/Q-read/release session. Unbound Q totals are prefixes of 100 reads. Nested profiles overlap and are not summed.",
        "cache_semantics": data["cache_semantics"], "fixed_session_scope": data["fixed_session_scope"],
        "durability_semantics": data["durability_semantics"], "writes": {}, "cases": []}
    for tier in ("small", "medium", "large"):
        sources = [c["sources"][tier] for c in campaigns]
        report["writes"][tier] = {"bytes": sources[0]["bytes"],
            "fs_total_ns_median": median(s["writes"]["fs"]["total_ns"] for s in sources),
            "iotdb_total_ns_median": median(s["writes"]["iotdb"]["total_ns"] for s in sources),
            "source_locator_ns_median": median(s["source_locator_ns"] for s in sources),
            "source_versions": [s["version"] for s in sources]}
        for name in ("point", "small_roi", "cross_chunk"):
            groups = {mode: [next(x for x in c[mode + "_cases"]
                                if x["tier"] == tier and x["case"] == name) for c in campaigns]
                      for mode in ("unbound", "bound")}
            entry = {"tier": tier, "case": name, "unbound": {}, "bound": {}, "q": {}, "profiles": {}}
            for arm in ("fs", "iotdb"):
                entry["unbound"][arm] = {key: median(c["summary"][arm][key] for c in groups["unbound"])
                    for key in ("first_query_ns", "all_p50_ns", "all_p95_ns", "warm_p50_ns", "warm_p95_ns")}
            for q in data["q_values"]:
                key = str(q)
                bound_sessions = [next(s for s in c["sessions"] if s["q"] == q) for c in groups["bound"]]
                row = {"unbound": {}, "bound": {}}
                for arm in ("fs", "iotdb"):
                    row["unbound"][arm] = {
                        "query_sum_ns": median(c["summary"][arm]["query_sum_ns_by_q"][key] for c in groups["unbound"]),
                        "lifecycle_total_ns": median(c["summary"][arm]["lifecycle_ns_by_q"][key] for c in groups["unbound"])}
                    row["bound"][arm] = {metric: median(s["summary"][arm][metric] for s in bound_sessions)
                        for metric in ("bind_ns", "release_ns", "query_sum_ns", "session_total_ns",
                                       "lifecycle_total_ns", "first_query_ns", "all_p50_ns", "all_p95_ns")}
                    row["bound"][arm].update({metric: median(s["summary"][arm][metric] for s in bound_sessions)
                        if q > 1 else None for metric in ("warm_p50_ns", "warm_p95_ns")})
                    row[arm + "_bound_vs_unbound_ratio"] = median(
                        s["summary"][arm]["session_total_ns"] /
                        c["summary"][arm]["query_sum_ns_by_q"][key]
                        for s, c in zip(bound_sessions, groups["unbound"]))
                row["bound_paired_session_ba_ratio"] = median(
                    s["summary"]["iotdb"]["session_total_ns"] / s["summary"]["fs"]["session_total_ns"]
                    for s in bound_sessions)
                row["bound_paired_lifecycle_ba_ratio"] = median(
                    s["summary"]["iotdb"]["lifecycle_total_ns"] / s["summary"]["fs"]["lifecycle_total_ns"]
                    for s in bound_sessions)
                entry["q"][key] = row
                if q == 100:
                    entry["bound"] = row["bound"]
                    for label, profiles in (
                        ("bind", [s["arms"]["iotdb"]["binding"]["server_profile"] for s in bound_sessions]),
                        ("first_read", [s["arms"]["iotdb"]["observations"][0]["server_profile"] for s in bound_sessions]),
                        ("warm_read", [r["server_profile"] for s in bound_sessions
                                       for r in s["arms"]["iotdb"]["observations"][1:]]),
                        ("release", [s["arms"]["iotdb"]["release"]["server_profile"] for s in bound_sessions]),
                        ("unbound_warm", [r["server_profile"] for c in groups["unbound"]
                                          for r in c["observations"]["iotdb"][1:]])):
                        entry["profiles"][label] = profile_metrics(profiles)
            entry["bound_paired_warm_ba_ratio"] = median(
                next(s for s in c["sessions"] if s["q"] == 100)["summary"]["iotdb"]["warm_p50_ns"] /
                next(s for s in c["sessions"] if s["q"] == 100)["summary"]["fs"]["warm_p50_ns"]
                for c in groups["bound"])
            report["cases"].append(entry)
    return report


def ms(value):
    return f"{value / 1e6:.3f}"


def markdown(report):
    lines = ["# Single-file paired bind-once experiment", "",
        "Three paired campaigns, three size tiers and three physical selections: **5,697 actual reads per arm** (2,700 reopen/re-register and 2,997 bound). Every Q=1/10/100 bound group has a fresh native session and a fresh IoTDB-FS daemon; its bind and close/release are measured once. All full-file SHA, exact versions, selections, values and masks passed.", "",
        "Times below are milliseconds; figures are medians across three campaigns. Ratios are paired medians and need not equal ratios of displayed medians. First means a fresh reader/runtime, not cold OS pages or cold database storage. Profiling is enabled in both IoTDB modes.", "",
        "## Complete warm read comparison (Q=100 sessions)", "",
        "| Tier / case | FS reopen p50 / p95 | FS bound p50 / p95 | IoTDB re-register p50 / p95 | IoTDB bound p50 / p95 | Bound warm B/A |",
        "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for case in report["cases"]:
        values = [case[mode][arm] for mode, arm in
                  (("unbound", "fs"), ("bound", "fs"), ("unbound", "iotdb"), ("bound", "iotdb"))]
        cells = " | ".join(ms(v["warm_p50_ns"]) + " / " + ms(v["warm_p95_ns"]) for v in values)
        lines.append(f"| {case['tier']} / {case['case']} | {cells} | {case['bound_paired_warm_ba_ratio']:.2f}x |")
    lines += ["", "## Actual query/session totals", "",
        "Reopen/re-register totals are actual Q-prefix sums. Bound totals include one bind + exactly Q reads + its own close/release, including the Q=1 close. Reads always materialize new IPC results and consume/copy them before release; repeated reads of an old IPC result are excluded.", "",
        "| Tier / case | Q | FS reopen | FS bound session | IoTDB re-register | IoTDB bound session | FS bound / reopen | IoTDB bound / re-register | Bound B/A |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for case in report["cases"]:
        for q, row in case["q"].items():
            lines.append(f"| {case['tier']} / {case['case']} | {q} | {ms(row['unbound']['fs']['query_sum_ns'])} | {ms(row['bound']['fs']['session_total_ns'])} | {ms(row['unbound']['iotdb']['query_sum_ns'])} | {ms(row['bound']['iotdb']['session_total_ns'])} | {row['fs_bound_vs_unbound_ratio']:.3f} | {row['iotdb_bound_vs_unbound_ratio']:.3f} | {row['bound_paired_session_ba_ratio']:.2f}x |")
    lines += ["", "## Publication and lifecycle", "",
        "Both modes share the same one-time publication and exact source locator per campaign. Each row is a separate scenario: lifecycle = shared write + IoTDB source locator (FS: zero) + the selected Q query/session total. Do not sum scenario rows as one execution.", "",
        "| Tier | Bytes | FS write | IoTDB publish | IoTDB source locator |",
        "| --- | ---: | ---: | ---: | ---: |"]
    for tier, row in report["writes"].items():
        lines.append(f"| {tier} | {row['bytes']:,} | {ms(row['fs_total_ns_median'])} | {ms(row['iotdb_total_ns_median'])} | {ms(row['source_locator_ns_median'])} |")
    lines += ["", "| Tier / case | Q | FS reopen lifecycle | FS bound lifecycle | IoTDB re-register lifecycle | IoTDB bound lifecycle | Bound lifecycle B/A |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for case in report["cases"]:
        for q, row in case["q"].items():
            cells = " | ".join(ms(row[mode][arm]["lifecycle_total_ns"])
                for mode, arm in (("unbound", "fs"), ("bound", "fs"), ("unbound", "iotdb"), ("bound", "iotdb")))
            lines.append(f"| {case['tier']} / {case['case']} | {q} | {cells} | {row['bound_paired_lifecycle_ba_ratio']:.2f}x |")
    lines += ["", "## Q=100 bound setup and warm diagnostics", "",
        "Inclusive server stages overlap; do not add them. First reads may perform real payload SQL/decode; all warm reads are checked for zero SQL/payload reads/decode, and every read for zero catalog and decoder compilation plus prepared reuse. Bind-time SQL is retained in the setup cost.", "",
        "| Tier / case | Bind | Release | Warm server | Warm IPC encode | Warm SQL | Warm catalog calls | Warm prepare calls | Warm payload calls | Warm decode count |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for case in report["cases"]:
        row, profile = case["bound"]["iotdb"], case["profiles"]["warm_read"]
        counters, io, runtime = profile["counters"], profile["iotdb_delta"], profile["runtime_delta"]
        lines.append(f"| {case['tier']} / {case['case']} | {ms(row['bind_ns'])} | {ms(row['release_ns'])} | {ms(profile['server_total_ns'])} | {ms(profile['stages_ns'].get('typed_ipc_encode', 0))} | {io.get('query_calls', 0):g} | {counters.get('registration_catalog_calls', 0):g} | {counters.get('typed_prepare_calls', 0):g} | {io.get('payload_calls', 0):g} | {runtime.get('decode_count', 0):g} |")
    lines += ["", "## Scope and operational limits", "",
        report["fixed_session_scope"], "",
        "FS uses native HDF5 1.14.4 with its recorded native adaptive metadata/raw chunk cache and no external index. Bound mode retains H5F/H5D handles; each read still selects a slab, creates a memory space, runs H5Dread and makes an owned copy. IoTDB-FS uses the same HDF5 core, shares verified layout/decoded fragments and freezes one exact source/version/plan; every read constructs a cursor and publishes a new bounded IPC stream, then mmap/read/copy/close through FUSE. All mappings close before session release.", "",
        "The source namespace must remain read-only during fixed sessions. This is an explicitly gated experimental upper bound on amortization, not a production cache of changing permissions or latest-version metadata. OS and database caches are not reset. Daemons are private and sequential; existing mounts are untouched. Native process startup/hash/JSON output, daemon startup, generation and correctness hashes are excluded from query timers. Alternating campaign arm/mode order is recorded; no significance test or cold-disk claim is made.", "",
        report["durability_semantics"], "",
        "Machine-readable aggregates are in `summary.json`; complete observations are in `campaign.json` or `campaign.json.gz`. Raw logs, copies and settings remain under the ignored server `work/` directory. Controlled synthetic data establishes overhead for this fixed layout, not scientific indexing benefits.", ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw = (gzip.decompress(args.campaign.read_bytes()) if args.campaign.suffix == ".gz"
           else args.campaign.read_bytes())
    report = summarize(json.loads(raw))
    report["campaign_sha256"] = hashlib.sha256(raw).hexdigest()
    args.output.mkdir(parents=True, exist_ok=True)
    base.save(args.output / "summary.json", report)
    (args.output / "RESULTS.md").write_text(markdown(report))
    print(json.dumps({"complete": True, "observations_per_arm": report["observations_per_arm"]}))


if __name__ == "__main__":
    main()
