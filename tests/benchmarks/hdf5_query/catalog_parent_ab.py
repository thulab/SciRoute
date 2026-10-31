#!/usr/bin/env python3
"""Compare sequential parents, bulk parents, and exact-path combined catalog."""
import argparse
import hashlib
import json
import statistics
import subprocess
from pathlib import Path

from grib_hdf5_boundary_probe import (
    connection_environment, grib_request, hdf5_request, run_campaign,
)


def percentile(values, fraction):
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lo = int(position)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (position - lo)


def summarize(observations):
    output = {}
    for variant in ("before", "bulk", "after"):
        output[variant] = {}
        for format_name in ("grib", "hdf5"):
            rows = [r for r in observations if r["variant"] == variant and
                    r["format"] == format_name and r["repetition"] >= 0]
            metrics = {
                "server_total_ms": [r["server_total_ns"] / 1e6 for r in rows],
                "client_control_ms": [r["control_ns"] / 1e6 for r in rows],
                "catalog_ms": [r["stages_ns"]["registration_catalog"] / 1e6 for r in rows],
                "authorize_ms": [r["stages_ns"]["registration_authorize"] / 1e6 for r in rows],
                "metadata_sql_ms": [r["iotdb_delta"]["metadata_query_nanoseconds"] / 1e6 for r in rows],
                "complete_client_ms": [(r["control_ns"] + r["stat_ns"] + r["read_ns"]) / 1e6 for r in rows],
            }
            output[variant][format_name] = {
                "samples": len(rows),
                "metrics": {name: {"p50": statistics.median(values),
                                   "p95": percentile(values, .95)}
                            for name, values in metrics.items()},
                "metadata_sql_calls": sorted({r["iotdb_delta"]["metadata_query_calls"] for r in rows}),
                "layout_sql_calls": sorted({r["iotdb_delta"]["layout_calls"] for r in rows}),
                "payload_sql_calls": sorted({r["iotdb_delta"]["payload_calls"] for r in rows}),
                "catalog_rows": sorted({r["counters"].get("published_catalog_manifest_rows")
                                        for r in rows}, key=lambda x: -1 if x is None else x),
                "campaign_catalog_p50_ms": [statistics.median(
                    r["stages_ns"]["registration_catalog"] / 1e6
                    for r in rows if r["pair"] == pair) for pair in sorted({r["pair"] for r in rows})],
                "ipc_sha256": sorted({r["ipc_sha256"] for r in rows}),
                "ipc_bytes": sorted({r["ipc_bytes"] for r in rows}),
            }
    for format_name in ("grib", "hdf5"):
        before = output["before"][format_name]
        after = output["after"][format_name]
        if before["ipc_sha256"] != after["ipc_sha256"] or before["ipc_sha256"] != output["bulk"][format_name]["ipc_sha256"] or len(before["ipc_sha256"]) != 1:
            raise RuntimeError("result digest changed between variants or campaigns")
    return output


def summarize_permissions(observations):
    output = summarize(observations)
    for variant in ("before", "bulk", "after"):
        for format_name in ("grib", "hdf5"):
            rows = [r for r in observations if r["variant"] == variant and
                    r["format"] == format_name and r["repetition"] >= 0]
            item = output[variant][format_name]
            for counter in ("authorization_source_metadata_calls",
                            "authorization_source_catalog_reuse",
                            "authorization_parent_metadata_calls",
                            "authorization_parent_catalog_reuse"):
                item[counter] = sorted({r["counters"].get(counter, 0) for r in rows})
            item["campaign_authorize_p50_ms"] = [statistics.median(
                r["stages_ns"]["registration_authorize"] / 1e6
                for r in rows if r["pair"] == pair)
                for pair in sorted({r["pair"] for r in rows})]
            if item["metadata_sql_calls"] != [{"before": 3, "bulk": 2, "after": 1}[variant]]:
                raise RuntimeError("unexpected catalog/source/parent SQL count")
            if item["authorization_source_metadata_calls"] != [0]:
                raise RuntimeError("unexpected source reread count")
            if item["authorization_source_catalog_reuse"] != [1]:
                raise RuntimeError("unexpected permission reuse count")
            if item["authorization_parent_metadata_calls"] != [{"before": 2, "bulk": 1, "after": 0}[variant]]:
                raise RuntimeError("unexpected parent SQL count")
            if item["authorization_parent_catalog_reuse"] != ([2] if variant == "after" else [0]):
                raise RuntimeError("unexpected parent catalog reuse count")
    return output


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--bulk", type=Path, required=True)
    parser.add_argument("--catalog-probe", type=Path, required=True)
    parser.add_argument("--pilot-manifest", type=Path, required=True)
    parser.add_argument("--profile-config", type=Path, required=True)
    parser.add_argument("--source-pid", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=20)
    args = parser.parse_args()
    if not 1 <= args.pairs <= 5 or not 1 <= args.repeats <= 20:
        raise ValueError("campaign or repeat bound exceeded")
    # sockaddr_un has a small path budget; fail before mounting anything.
    if len(str(args.output / "after" / "campaign-00" / "query.sock").encode()) >= 108:
        raise ValueError("use a shorter output directory for Unix socket paths")
    args.output.mkdir(parents=True, exist_ok=False)
    database = "h09probe1790939889dbfc1f"
    env = connection_environment(args.source_pid, database)
    env["IOTDB_CONFIG_FILE"] = str(args.profile_config)
    env["IOTDBFS_NAMESPACE_LOCK"] = str(args.output / "namespace.lock")
    source = json.loads(args.pilot_manifest.read_text())["sources"][0]
    requests = {"hdf5": hdf5_request(source),
                "grib": grib_request("/geos-pilot-9856a092a812/grib-control.grib2")}

    def inventory():
        result = subprocess.run([str(args.catalog_probe)], env=env,
                                capture_output=True, text=True, timeout=120)
        (args.output / "catalog-probe.log").write_text(result.stderr)
        if result.returncode:
            raise RuntimeError("catalog equivalence probe failed; see catalog-probe.log")
        lines = [line for line in result.stdout.splitlines() if line.startswith('{"namespace_published_versions"')]
        if len(lines) != 1:
            raise RuntimeError("missing catalog equivalence report")
        return json.loads(lines[0])

    initial_inventory = inventory()
    observations = []
    for variant in ("before", "bulk", "after"):
        (args.output / variant).mkdir()
    for pair in range(args.pairs):
        order = (("before", "bulk", "after"), ("after", "before", "bulk"), ("bulk", "after", "before"))[pair % 3]
        for variant in order:
            rows = run_campaign(args.output / variant, pair, getattr(args, variant),
                                env, requests, args.repeats)
            for row in rows:
                row.update(variant=variant, pair=pair)
            observations.extend(rows)
    final_inventory = inventory()
    def identity(census):
        return census["namespace_published_versions"], [
            (r["name"], r["sql_rows"], r["selected"]) for r in census["cases"]]
    if identity(initial_inventory) != identity(final_inventory):
        raise RuntimeError("catalog changed during experiment")
    report = {
        "protocol": "iotdbfs-parent-permission-ab/1", "database": database,
        "before_sha256": hashlib.sha256(args.before.read_bytes()).hexdigest(),
        "after_sha256": hashlib.sha256(args.after.read_bytes()).hexdigest(),
        "bulk_sha256": hashlib.sha256(args.bulk.read_bytes()).hexdigest(),
        "requests": requests, "catalog_equivalence": initial_inventory,
        "summary": summarize_permissions(observations), "observations": observations,
        "ablation": "Before is the previously validated source-catalog-reuse binary with sequential parent reads. Bulk is current code with only exact-catalog ancestor combination disabled. After is production combined code. Every parent X_OK check remains enforced.",
        "semantics": "Interleaved independent mounts, one warmup each format per mount; OS/IoTDB caches not cleared. server_total includes catalog/authorization/execution/IPC/publication; SQL timers are nested, not additive.",
    }
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"report": str(args.output / "report.json"),
                      "summary": report["summary"]}, indent=2))


if __name__ == "__main__":
    main()
