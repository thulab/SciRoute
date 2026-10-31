#!/usr/bin/env python3
"""Summarize an accepted three-tier Q=1/10/100 campaign without extrapolation."""
import argparse
import gzip
import hashlib
import json
import statistics
from pathlib import Path

from runner import require, save


def median(values):
    return statistics.median(list(values))


def ms(value):
    return f"{value / 1e6:.3f}"


def summarize(data):
    require(data["protocol"] == "iotdbfs-single-file-campaign/1" and
            data["complete"] and not data["smoke_only"], "formal campaign incomplete")
    require(data["q_values"] == [1, 10, 100], "Q values differ")
    require(data["profiling"], "this diagnostic summary requires server profiles")
    campaigns = data["campaigns"]
    require(len(campaigns) == 3, "three paired campaigns required")
    tiers, cases = ["small", "medium", "large"], ["point", "small_roi", "cross_chunk"]
    expected = {(tier, case) for tier in tiers for case in cases}
    for campaign in campaigns:
        require(set(campaign["sources"]) == set(tiers), "missing source tier")
        require(all(s["full_copy_sha_verified"] for s in campaign["sources"].values()),
                "full source verification incomplete")
        require(len(campaign["cases"]) == 9 and
                {(c["tier"], c["case"]) for c in campaign["cases"]} == expected,
                "missing or duplicate case")
        for case in campaign["cases"]:
            for arm in ("fs", "iotdb"):
                rows = case["observations"][arm]
                require(len(rows) == 100, "100 actual queries required")
                for q in data["q_values"]:
                    require(sum(r["total_ns"] for r in rows[:q]) ==
                            case["summary"][arm]["query_sum_ns_by_q"][str(q)],
                            "query accounting differs")
                    require(case["summary"][arm]["lifecycle_ns_by_q"][str(q)] ==
                            case["summary"][arm]["write_ns"] +
                            case["summary"][arm]["exact_source_binding_ns"] +
                            sum(r["total_ns"] for r in rows[:q]), "lifecycle accounting differs")
    report = {"protocol": "iotdbfs-single-file-summary/1", "complete": True,
              "campaigns": 3, "observations_per_arm": 2700, "queries_per_case_per_arm": 300,
              "warm_observations_per_case_per_arm": 297, "profiling": data["profiling"],
              "aggregation": "First/p50/p95 are medians of three campaign statistics; B/A is median of three paired ratios. Q totals are actual prefix sums, summarized by median across campaigns. Nested stages overlap and are not added to wall time.",
              "writes": {}, "cases": [], "cache_semantics": data["cache_semantics"],
              "durability_semantics": data["durability_semantics"]}
    for tier in tiers:
        sources = [c["sources"][tier] for c in campaigns]
        report["writes"][tier] = {
            "bytes": sources[0]["bytes"],
            "fs_total_ns_median": median(s["writes"]["fs"]["total_ns"] for s in sources),
            "iotdb_total_ns_median": median(s["writes"]["iotdb"]["total_ns"] for s in sources),
            "binding_ns_median": median(s["exact_source_binding_ns"] for s in sources),
            "source_versions": [s["version"] for s in sources]}
    for tier in tiers:
        for name in cases:
            group = [next(x for x in c["cases"] if x["tier"] == tier and x["case"] == name)
                     for c in campaigns]
            entry = {"tier": tier, "case": name, "arms": {}, "q": {},
                     "paired_p50_ratio_median": median(x["iotdb_fs_p50_ratio"] for x in group)}
            entry["under_10x_target"] = entry["paired_p50_ratio_median"] < 10
            for arm in ("fs", "iotdb"):
                entry["arms"][arm] = {key: median(x["summary"][arm][key] for x in group)
                    for key in ("first_query_ns", "all_p50_ns", "all_p95_ns", "warm_p50_ns", "warm_p95_ns")}
            for q in data["q_values"]:
                key = str(q)
                entry["q"][key] = {arm: {measure: median(x["summary"][arm][measure][key] for x in group)
                    for measure in ("query_sum_ns_by_q", "lifecycle_ns_by_q")} for arm in ("fs", "iotdb")}
                entry["q"][key]["paired_query_ratio_median"] = median(
                    x["summary"]["iotdb"]["query_sum_ns_by_q"][key] /
                    x["summary"]["fs"]["query_sum_ns_by_q"][key] for x in group)
                entry["q"][key]["paired_lifecycle_ratio_median"] = median(
                    x["summary"]["iotdb"]["lifecycle_ns_by_q"][key] /
                    x["summary"]["fs"]["lifecycle_ns_by_q"][key] for x in group)
            cold, warm = [], []
            for item in group:
                rows = item["observations"]["iotdb"]
                cold.append(rows[0]["server_profile"])
                warm.extend(row["server_profile"] for row in rows[1:])
            entry["profile"] = {}
            for label, profiles in (("first", cold), ("warm", warm)):
                metrics = {"server_total_ns": median(p["server_total_ns"] for p in profiles)}
                for category in ("stages_ns", "iotdb_delta", "runtime_delta"):
                    keys = set().union(*(p.get(category, {}).keys() for p in profiles))
                    metrics[category] = {key: median(p.get(category, {}).get(key, 0) for p in profiles)
                                         for key in sorted(keys)}
                entry["profile"][label] = metrics
            report["cases"].append(entry)
    report["under_10x_cases"] = sum(x["under_10x_target"] for x in report["cases"])
    return report


def markdown(report):
    lines = ["# Single-file FS / IoTDB-FS — Q=1/10/100", "",
        "Three paired campaigns; small/medium/large controlled synthetic HDF5 files, three physical subset cases. **2,700 complete queries per arm**. All full-file identities, exact versions, subset values, masks and shapes passed the runner's checks.", "",
        "All time tables use milliseconds. First/p50/p95 are medians of three campaign statistics. Ratios are medians of paired campaign ratios and therefore need not equal ratios of the displayed medians. The 297 warm observations per case are distributed across three fresh runtimes. First means a fresh reader/runtime, not cold OS pages or cold IoTDB storage.", "",
        "## Write, publication and exact-version binding", "",
        "| Tier | Bytes | FS write+fsync+close | IoTDB-FS write+fsync+close | IoTDB exact-version binding |",
        "| --- | ---: | ---: | ---: | ---: |"]
    for tier, row in report["writes"].items():
        lines.append(f"| {tier} | {row['bytes']:,} | {ms(row['fs_total_ns_median'])} | {ms(row['iotdb_total_ns_median'])} | {ms(row['binding_ns_median'])} |")
    lines += ["", "## Complete query latency", "",
        "| Tier / case | FS first | IoTDB first | FS warm p50 / p95 | IoTDB warm p50 / p95 | Paired all-query p50 B/A | <10x target |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |"]
    for row in report["cases"]:
        a, b = row["arms"]["fs"], row["arms"]["iotdb"]
        lines.append(f"| {row['tier']} / {row['case']} | {ms(a['first_query_ns'])} | {ms(b['first_query_ns'])} | {ms(a['warm_p50_ns'])} / {ms(a['warm_p95_ns'])} | {ms(b['warm_p50_ns'])} / {ms(b['warm_p95_ns'])} | {row['paired_p50_ratio_median']:.2f}x | {'pass' if row['under_10x_target'] else 'fail'} |")
    lines += ["", "## Actual Q prefix totals and lifecycle totals", "",
        "Lifecycle = one write+file-fsync+close + one IoTDB exact-version binding + Q complete queries. The FS binding term is zero. Each case is an independent scenario; summing cases would count the write more than once.", "",
        "| Tier / case | Q | FS query sum | IoTDB query sum | FS lifecycle | IoTDB lifecycle | Paired query B/A | Paired lifecycle B/A |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for row in report["cases"]:
        for q, item in row["q"].items():
            a, b = item["fs"], item["iotdb"]
            lines.append(f"| {row['tier']} / {row['case']} | {q} | {ms(a['query_sum_ns_by_q'])} | {ms(b['query_sum_ns_by_q'])} | {ms(a['lifecycle_ns_by_q'])} | {ms(b['lifecycle_ns_by_q'])} | {item['paired_query_ratio_median']:.2f}x | {item['paired_lifecycle_ratio_median']:.2f}x |")
    lines += ["", "## Warm query diagnostics", "",
        "Pooled median of 297 warm server profiles per case. Stage times are inclusive and overlap; they are not added. SQL count covers daemon client executeQuery calls; payload bytes mean backend returned bytes, not physical disk I/O.", "",
        "| Tier / case | Server total | Catalog | Prepare | Cursor | IPC encode | Client SQL calls | Payload calls / bytes | Decode count |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for row in report["cases"]:
        p = row["profile"]["warm"]
        s, i, r = p["stages_ns"], p["iotdb_delta"], p["runtime_delta"]
        lines.append(f"| {row['tier']} / {row['case']} | {ms(p['server_total_ns'])} | {ms(s.get('registration_catalog', 0))} | {ms(s.get('typed_prepare', 0))} | {ms(s.get('typed_cursor_execute', 0))} | {ms(s.get('typed_ipc_encode', 0))} | {i.get('query_calls', 0):g} | {i.get('payload_calls', 0):g} / {i.get('payload_returned_bytes', 0):g} | {r.get('decode_count', 0):g} |")
    lines += ["", "## Scope", "",
        f"The prespecified <10x paired median latency target is met by **{report['under_10x_cases']}/9 cases**. Correctness completion does not imply performance acceptance; all failures are retained.", "",
        "The FS arm uses native HDF5 1.14.4 without an external index. Each FS query opens/dataset-selects/reads/copies/closes; CLI startup, hashing and JSON output are outside query timing and reported separately. IoTDB-FS also uses HDF5 1.14.4 and performs a new registration, materialization, IPC mmap/read/copy and close per query. Its daemon startup is outside query timing. Nested server profiles exclude socket framing/profile sink, which remain within the client wall timer.", "",
        "OS pages and the IoTDB server cache are not reset. Raw sources were fully hashed before timing; decoded fragments and verified layout are reused within each case. IoTDB-FS decoded cache: 256 MiB/65,536 entries, read parallelism 1, FUSE attribute/entry TTL 1 s; ordinary raw-file cache and full-prefetch caps are 1 byte. FS native handles close on every query and use recorded native HDF5 cache defaults. Profiling is enabled. Three campaigns use FS-first, IoTDB-first, FS-first order; no significance test or cold-disk claim is made. Input schema/chunks/filters and query coordinates remain frozen across tiers.", "",
        "Both writes include file fsync and close; directory setup, environment build, data generation, daemon startup and correctness hashes are outside lifecycle totals. Backend WAL/sync policy is unchanged, so equivalent power-loss durability is not claimed. Synthetic values measure overhead under this fixed schema and do not establish scientific-workload indexing benefits.", "",
        "See `summary.json` for machine-readable aggregates and profile counters, `campaign.json.gz` for all observations, and `run-manifest.json` for source/environment identity, cleanup and evidence hashes. Raw server logs and file copies remain in ignored `work/`.", ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw = (gzip.decompress(args.campaign.read_bytes()) if args.campaign.suffix == ".gz"
           else args.campaign.read_bytes())
    data = json.loads(raw)
    report = summarize(data)
    report["campaign_sha256"] = hashlib.sha256(raw).hexdigest()
    args.output.mkdir(parents=True, exist_ok=True)
    save(args.output / "summary.json", report)
    (args.output / "RESULTS.md").write_text(markdown(report))
    print(json.dumps({"complete": True, "cases_under_10x": report["under_10x_cases"],
                      "observations_per_arm": report["observations_per_arm"]}))


if __name__ == "__main__":
    main()
