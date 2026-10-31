#!/usr/bin/env python3
"""Validate and aggregate independent Q=1/10/100 GRIB paired sessions."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
from pathlib import Path
import statistics


PROTOCOL = "iotdbfs-grib-single-file-paired/1"
TIERS = ("small", "medium")
CASES = ("binary_one", "point", "roi")
MODES = ("unbound", "bound")
QS = (1, 10, 100)
HASH_KEYS = ("value_sha256", "latitude_sha256", "longitude_sha256", "mask_sha256")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value, label):
    require(type(value) is int and value >= 0, f"invalid nonnegative integer: {label}")
    return value


def fingerprint(value, label):
    require(isinstance(value, str) and len(value) == 64 and
            all(c in "0123456789abcdef" for c in value), f"invalid SHA-256: {label}")


def percentile(values, fraction):
    ordered = sorted(values)
    require(bool(ordered), "empty latency sample")
    position = (len(ordered) - 1) * fraction
    lo, hi = math.floor(position), math.ceil(position)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (position - lo)


def latency(times):
    require(bool(times), "empty session")
    warm = times[1:]
    return {"first_query_ns": times[0], "all_p50_ns": statistics.median(times),
            "all_p95_ns": percentile(times, .95),
            "warm_p50_ns": statistics.median(warm) if warm else None,
            "warm_p95_ns": percentile(warm, .95) if warm else None}


def accounting(source, arm, info, q):
    rows = info["observations"]
    require(len(rows) == q, "session is not exactly Q actual observations")
    times = [integer(r["total_ns"], "read.total_ns") for r in rows]
    binding = integer(info["binding_ns"], "binding_ns")
    release = integer(info["release_ns"], "release_ns")
    write = integer(source["writes"][arm]["total_ns"], "write.total_ns")
    locator = integer(source["source_locator_ns"], "source_locator_ns") if arm == "iotdb" else 0
    queries = sum(times)
    session = binding + queries + release
    return {"queries": q, "bind_ns": binding, "release_ns": release,
            "query_sum_ns": queries, "session_total_ns": session,
            "write_ns": write, "source_locator_ns": locator,
            "lifecycle_total_ns": write + locator + session, **latency(times)}


def check_profile(profile, mode, rows):
    require(profile.get("ok") is True, "failed materialization profile")
    counters = profile.get("counters", {})
    require(counters.get("typed_cursor_next_calls", 0) > 0 and
            counters.get("typed_ipc_batches", 0) > 0 and
            counters.get("typed_ipc_rows") == rows,
            "read did not execute a fresh cursor and encode the expected rows")
    expected_operation = "experiment_typed_read" if mode == "bound" else "typed"
    require(profile.get("operation") == expected_operation, "materialization operation differs")
    if mode == "bound":
        require(counters.get("typed_prepared_reuse", 0) > 0 and
                all(counters.get(k, 0) == 0 for k in
                    ("registration_catalog_calls", "catalog_scan_calls", "typed_prepare_calls")),
                "bound read repeated catalog/planning or did not reuse the fixed plan")


def validate_formal(data):
    require(data.get("protocol") == PROTOCOL and data.get("complete") is True and
            data.get("smoke_only") is False, "incomplete, partial or smoke-only campaign")
    require(data.get("q_values") == list(QS) and data.get("tiers") == list(TIERS),
            "formal Q values or GFS tiers differ")
    for key in ("data_manifest_sha256", "daemon_sha256", "native_sha256"):
        fingerprint(data[key], key)
    campaigns = data["campaigns"]
    require(len(campaigns) == 3 and len({c["index"] for c in campaigns}) == 3,
            "three distinct paired campaigns required")
    expected = {(t, c, m) for t in TIERS for c in CASES for m in MODES}
    source_hashes, oracles = {}, {}
    result_paths, binding_ids = set(), set()
    observations = {arm: 0 for arm in ("fs", "iotdb")}
    for campaign in campaigns:
        require(set(campaign["sources"]) == set(TIERS), "missing source tier")
        require(set(campaign["query_order"]) == {"fs", "iotdb"} and
                len(campaign["query_order"]) == 2 and set(campaign["mode_order"]) == set(MODES) and
                len(campaign["mode_order"]) == 2, "invalid paired execution order")
        for tier, source in campaign["sources"].items():
            require(source.get("full_copy_sha_verified") is True, "full source SHA verification missing")
            fingerprint(source["sha256"], "source.sha256")
            integer(source["version"], "exact source version")
            require(integer(source["bytes"], "source bytes") > 0, "empty GRIB source")
            identity = (source["sha256"], source["bytes"])
            require(source_hashes.setdefault(tier, identity) == identity,
                    "paired campaigns use different raw bytes for a tier")
            require(isinstance(source["logical_path"], str) and source["logical_path"].startswith("/"),
                    "missing exact source path")
        cases = campaign["cases"]
        require(len(cases) == len(expected) and
                {(c["tier"], c["case"], c["mode"]) for c in cases} == expected,
                "missing or duplicate tier/case/mode")
        for case in cases:
            tier, name, mode = case["tier"], case["case"], case["mode"]
            selection = case["selection"]
            require(selection["view"] == ("binary" if name == "binary_one" else "values"),
                    "selection view does not match scenario")
            require(bool(selection["filter"]), "message predicate missing")
            oracle = selection["oracle"]
            count = integer(oracle["candidate_messages"], "candidate messages")
            selected = integer(oracle["selected_messages"], "selected messages")
            require(count > selected and selected == 1, "one-message physical pruning is not demonstrated")
            require(integer(oracle["rows"], "oracle rows") > 0, "empty selection")
            require(len(oracle["message_offsets"]) == len(oracle["message_lengths"]) == selected and
                    len(oracle["messages"]) == selected, "incomplete message oracle")
            for offset, length in zip(oracle["message_offsets"], oracle["message_lengths"]):
                require(integer(length, "message length") > 0 and
                        integer(offset, "message offset") + length <= campaign["sources"][tier]["bytes"],
                        "message extends beyond source")
            for key in HASH_KEYS:
                fingerprint(oracle[key], "oracle." + key)
            signature = json.dumps(selection, sort_keys=True, separators=(",", ":"))
            require(oracles.setdefault((tier, name), signature) == signature,
                    "selection/oracle changed across modes or campaigns")
            sessions = case["sessions"]
            require(len(sessions) == 3 and [s["q"] for s in sessions] == list(QS),
                    "each Q must be an independent actual session")
            for session in sessions:
                q = session["q"]
                require(set(session["arms"]) == {"fs", "iotdb"}, "unpaired session")
                for arm in ("fs", "iotdb"):
                    info = session["arms"][arm]
                    rows = info["observations"]
                    metrics = accounting(campaign["sources"][tier], arm, info, q)
                    provided = session["summary"][arm]
                    for key in ("session_total_ns", "lifecycle_total_ns", "query_sum_ns"):
                        require(provided.get(key) == metrics[key], "session accounting differs: " + key)
                    observations[arm] += q
                    if mode == "unbound":
                        require(info["binding_ns"] == info["release_ns"] == 0,
                                "unbound session has unexplained once-only costs")
                    for index, row in enumerate(rows):
                        require(row.get("iteration" if arm == "fs" else "index") == index,
                                "observation ordering differs")
                        for key in ("rows", *HASH_KEYS):
                            require(row[key] == oracle[key], "result differs from raw oracle: " + key)
                        if arm == "fs":
                            require(row["mode"] == mode and row["view"] == selection["view"],
                                    "native mode/view differs")
                            for key in ("candidate_messages", "selected_messages", "message_offsets", "message_lengths", "messages"):
                                require(row[key] == oracle[key], "native message retrieval differs: " + key)
                            require(row["source_bytes"] == campaign["sources"][tier]["bytes"],
                                    "native source size differs")
                            require(row["session_bind_ns"] == (info["binding_ns"] if index == 0 else 0),
                                    "native bind must be recorded once for the session")
                            require(row["session_close_ns"] == (info["release_ns"] if index == q - 1 else 0),
                                    "native session close missing or repeated")
                        else:
                            path = row["result_path"]
                            require(path not in result_paths, "read reused an earlier IPC route")
                            result_paths.add(path)
                            check_profile(row["server_profile"], mode, oracle["rows"])
                    if mode == "bound" and arm == "iotdb":
                        require(info.get("released") is True, "fixed binding not released")
                        binding = info.get("prepared_id", info.get("binding_id"))
                        if binding is not None:
                            require(binding not in binding_ids, "Q reused another session's binding")
                            binding_ids.add(binding)
    require(observations == {"fs": 3996, "iotdb": 3996}, "actual observation total differs")
    return observations


def profile_metrics(profiles):
    result = {}
    for scalar in ("server_total_ns",):
        values = [p[scalar] for p in profiles if scalar in p]
        if values:
            result[scalar] = {"p50": statistics.median(values), "p95": percentile(values, .95)}
    for category in ("stages_ns", "counters", "iotdb_delta", "runtime_delta"):
        keys = set().union(*(p.get(category, {}) for p in profiles))
        result[category] = {key: statistics.median(p.get(category, {}).get(key, 0) for p in profiles)
                            for key in sorted(keys)}
    return result


def summarize(data):
    counts = validate_formal(data)
    campaigns = data["campaigns"]
    report = {"protocol": "iotdbfs-grib-single-file-summary/1", "complete": True,
              "campaigns": 3, "observations_per_arm": counts["fs"], "q_values": list(QS),
              "aggregation": "Warm p50/p95 pool actual Q=100 session reads after the first. Q totals are medians of three independent sessions. Ratios are medians of campaign-paired ratios, not ratios of aggregate medians.",
              "writes": {}, "cases": []}
    for key in ("cache_semantics", "fixed_session_scope", "durability_semantics", "environment",
                "frozen_session_scope", "publication_scope", "timing_scope",
                "data_manifest_sha256", "daemon_sha256", "native_sha256"):
        if key in data:
            report[key] = data[key]
    for tier in TIERS:
        sources = [c["sources"][tier] for c in campaigns]
        report["writes"][tier] = {"bytes": sources[0]["bytes"], "sha256": sources[0]["sha256"],
            "versions": [s["version"] for s in sources],
            "source_locator_ns": statistics.median(s["source_locator_ns"] for s in sources),
            **{arm + "_total_ns": statistics.median(s["writes"][arm]["total_ns"] for s in sources)
               for arm in ("fs", "iotdb")}}
        for name in CASES:
            item = {"tier": tier, "case": name, "modes": {}}
            for mode in MODES:
                cases = [next(c for c in camp["cases"] if
                         (c["tier"], c["case"], c["mode"]) == (tier, name, mode)) for camp in campaigns]
                item["selection"] = cases[0]["selection"]
                group = {"q": {}, "warm": {}, "profiles": {}}
                for q in QS:
                    sessions = [next(s for s in c["sessions"] if s["q"] == q) for c in cases]
                    metrics = {arm: [accounting(camp["sources"][tier], arm, s["arms"][arm], q)
                                     for camp, s in zip(campaigns, sessions)] for arm in ("fs", "iotdb")}
                    row = {arm: {key: statistics.median(v[key] for v in values)
                                for key in values[0] if values[0][key] is not None}
                           for arm, values in metrics.items()}
                    for metric in ("query_sum_ns", "session_total_ns", "lifecycle_total_ns"):
                        row["paired_" + metric + "_ba_ratio"] = statistics.median(
                            b[metric] / a[metric] for a, b in zip(metrics["fs"], metrics["iotdb"]))
                    group["q"][str(q)] = row
                    if q == 100:
                        for arm in ("fs", "iotdb"):
                            warm = [r for s in sessions for r in s["arms"][arm]["observations"][1:]]
                            group["warm"][arm] = {"observations": len(warm),
                                "p50_ns": statistics.median(r["total_ns"] for r in warm),
                                "p95_ns": percentile([r["total_ns"] for r in warm], .95)}
                            if arm == "fs":
                                for key in ("discovery_ns", "payload_read_ns", "decode_ns", "selection_setup_ns",
                                            "owned_copy_ns", "actual_payload_read_bytes", "payload_read_calls",
                                            "decoded_messages", "cache_hits", "cache_misses", "cache_resident_bytes"):
                                    group["warm"][arm][key] = statistics.median(r[key] for r in warm)
                            else:
                                for key in ("registration_ns", "mmap_open_ns", "ipc_decode_ns", "owned_copy_close_ns"):
                                    values = [r[key] for r in warm]
                                    group["warm"][arm][key] = {"p50": statistics.median(values),
                                                              "p95": percentile(values, .95)}
                                group["profiles"]["warm"] = profile_metrics([r["server_profile"] for r in warm])
                        group["profiles"]["first"] = profile_metrics([s["arms"]["iotdb"]["observations"][0]["server_profile"] for s in sessions])
                        for label in ("binding", "release"):
                            profiles = [s["arms"]["iotdb"].get(label + "_profile") for s in sessions]
                            if all(p is not None for p in profiles):
                                group["profiles"][label] = profile_metrics(profiles)
                item["modes"][mode] = group
            for q in QS:
                for arm in ("fs", "iotdb"):
                    ratios = []
                    for campaign in campaigns:
                        totals = {}
                        for mode in MODES:
                            case = next(c for c in campaign["cases"] if
                                (c["tier"], c["case"], c["mode"]) == (tier, name, mode))
                            session = next(s for s in case["sessions"] if s["q"] == q)
                            totals[mode] = accounting(campaign["sources"][tier], arm, session["arms"][arm], q)["session_total_ns"]
                        ratios.append(totals["bound"] / totals["unbound"])
                    item["modes"]["bound"]["q"][str(q)][arm + "_bound_vs_unbound_ratio"] = statistics.median(ratios)
            report["cases"].append(item)
    return report


def ms(value):
    return f"{value / 1e6:.3f}"


def markdown(report):
    lines = ["# GRIB single-file paired experiment", "",
        "Three paired campaigns use two real GFS file tiers, three selections, two reader modes and independent Q=1/10/100 sessions: **3,996 actual reads per arm**. Every output is checked against the raw-file oracle, with exact source versions, full-copy SHA verification and successful fixed-session release. Partial and smoke campaigns are rejected by this summary.", "",
        "Times are milliseconds. Warm p50/p95 pool 297 Q=100 reads per tier/case/mode/arm, excluding each session's first read. Q totals are medians of three actual sessions; B/A ratios are campaign-paired medians. A fresh reader/daemon is not a cold OS or database cache.", "",
        "## Warm reads", "",
        "| Tier / selection | Mode | FS p50 / p95 | IoTDB-FS p50 / p95 |",
        "| --- | --- | ---: | ---: |"]
    for case in report["cases"]:
        for mode, group in case["modes"].items():
            a, b = group["warm"]["fs"], group["warm"]["iotdb"]
            lines.append(f"| {case['tier']} / {case['case']} | {mode} | {ms(a['p50_ns'])} / {ms(a['p95_ns'])} | {ms(b['p50_ns'])} / {ms(b['p95_ns'])} |")
    lines += ["", "## Actual Q query and session totals", "",
        "Query total sums exactly Q reads. Session total adds one metadata bind and the actual close/release. Each Q has its own session; no Q=100 prefix substitutes for a Q=1/10 session.", "",
        "| Tier / selection | Mode | Q | FS queries | B queries | FS session | B session | Paired session B/A |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for case in report["cases"]:
        for mode, group in case["modes"].items():
            for q, row in group["q"].items():
                a, b = row["fs"], row["iotdb"]
                lines.append(f"| {case['tier']} / {case['case']} | {mode} | {q} | {ms(a['query_sum_ns'])} | {ms(b['query_sum_ns'])} | {ms(a['session_total_ns'])} | {ms(b['session_total_ns'])} | {row['paired_session_total_ns_ba_ratio']:.3f}x |")
    lines += ["", "## Publication and complete lifecycle", "",
        "Publication is shared by the scenarios within each campaign. Lifecycle = one publication + exact source locator (B only) + the chosen session. Separate scenario totals must not be summed as if they were one execution.", "",
        "| Tier | Source bytes | FS publication | B publication | B exact-version locator |",
        "| --- | ---: | ---: | ---: | ---: |"]
    for tier, row in report["writes"].items():
        lines.append(f"| {tier} | {row['bytes']:,} | {ms(row['fs_total_ns'])} | {ms(row['iotdb_total_ns'])} | {ms(row['source_locator_ns'])} |")
    lines += ["", "| Tier / selection | Mode | Q | FS lifecycle | B lifecycle | Paired lifecycle B/A |",
        "| --- | --- | ---: | ---: | ---: | ---: |"]
    for case in report["cases"]:
        for mode, group in case["modes"].items():
            for q, row in group["q"].items():
                lines.append(f"| {case['tier']} / {case['case']} | {mode} | {q} | {ms(row['fs']['lifecycle_total_ns'])} | {ms(row['iotdb']['lifecycle_total_ns'])} | {row['paired_lifecycle_total_ns_ba_ratio']:.3f}x |")
    lines += ["", "## Retrieval and warm diagnostics", "",
        "Point and ROI selection still read/decode the complete selected GRIB message; crop selectivity does not imply intra-message physical pruning. Source payload bytes below measure transfer, not physical disk reads. Native metadata bytes are unavailable from the headers-only API and are not reported as zero. Shared-runtime decode counters do not cover the GRIB adapter's private decoder.", "",
        "| Tier / selection | Selected / candidates | Selected encoded bytes | Mode | Warm FS payload bytes / decodes | Warm B SQL / payload calls | Warm B transferred payload bytes |",
        "| --- | ---: | ---: | --- | ---: | ---: | ---: |"]
    for case in report["cases"]:
        oracle = case["selection"]["oracle"]
        for mode, group in case["modes"].items():
            a = group["warm"]["fs"]
            io = group["profiles"]["warm"].get("iotdb_delta", {})
            lines.append(f"| {case['tier']} / {case['case']} | {oracle['selected_messages']} / {oracle['candidate_messages']} | {sum(oracle['message_lengths']):,} | {mode} | {a['actual_payload_read_bytes']:g} / {a['decoded_messages']:g} | {io.get('query_calls', 0):g} / {io.get('payload_calls', 0):g} | {io.get('payload_returned_bytes', 0):g} |")
    lines += ["", "## Q=100 setup and client stages", "",
        "Bind/release figures are campaign medians. Client stage figures are pooled warm medians; stage medians are not additive. Server stages are inclusive and may overlap.", "",
        "| Tier / selection | Mode | FS bind / close | B bind / release | B registration | B mmap/open | B IPC conversion | B owned copy/close |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for case in report["cases"]:
        for mode, group in case["modes"].items():
            setup, stages = group["q"]["100"], group["warm"]["iotdb"]
            a, b = setup["fs"], setup["iotdb"]
            stage_cells = " | ".join(ms(stages[key]["p50"]) for key in
                ("registration_ns", "mmap_open_ns", "ipc_decode_ns", "owned_copy_close_ns"))
            lines.append(f"| {case['tier']} / {case['case']} | {mode} | {ms(a['bind_ns'])} / {ms(a['release_ns'])} | {ms(b['bind_ns'])} / {ms(b['release_ns'])} | {stage_cells} |")
    lines += ["", "## Scope and operational limits", "",
        "FS uses native ecCodes headers-only scans with no sidecar or dataset-wide external index. Unbound reads rescan messages and retain no payload/decoded cache. Bound sessions scan metadata once, then lazily retain bounded encoded message bytes and native decoded arrays; every read recrops and makes an owned output copy. This FS bound raw-message cache may be stronger than IoTDB-FS BinaryView reuse; the recorded read counters expose this difference rather than assuming equivalent cache hits.", "",
        "IoTDB-FS ingestion builds a persistent message layout and pays its cost in publication. Unbound queries perform normal registration/materialization. Fixed bound sessions hold one exact-version prepared selection, construct a fresh cursor and encode/publish an IPC result for every read. Client registration, mmap/open, IPC conversion, owned copy and close are included. Knowing offsets beforehand is confined to the raw correctness oracle, not the measured FS discovery operation.", "",
        "The experimental binding requires an unchanged, read-only source and permissions for its lifetime. OS/database caches are not reset. Native process startup, hash checks and JSON printing are excluded from read timers and reported separately in raw observations; source locator and publication are excluded from query/session totals but included in lifecycle totals. Profiling is enabled on B; inclusive server stages overlap and must not be added. The binary-message case and decoded-value cases have different output work and are interpreted separately.", "",
        "Complete observations are retained in `campaign.json` or `campaign.json.gz`; `summary.json` contains the checked aggregate. This experiment evaluates single-file retrieval and repeated-query amortization, and does not establish cross-file, multi-version or distributed indexing benefits.", ""]
    for key in ("frozen_session_scope", "publication_scope", "timing_scope"):
        if key in report:
            lines += [key.replace("_", " ").capitalize() + ": " + report[key], ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw = args.campaign.read_bytes()
    if args.campaign.suffix == ".gz":
        raw = gzip.decompress(raw)
    report = summarize(json.loads(raw))
    report["campaign_sha256"] = hashlib.sha256(raw).hexdigest()
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "summary.json").write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n")
    (args.output / "RESULTS.md").write_text(markdown(report))
    print(json.dumps({"complete": True, "observations_per_arm": report["observations_per_arm"]}))


if __name__ == "__main__":
    main()
