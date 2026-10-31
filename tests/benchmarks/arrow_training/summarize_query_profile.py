#!/usr/bin/env python3
"""Join client window timings to server profiles by immutable result path.

Runtime deltas belong to the whole daemon. Requests overlapping another logged
request in the same process are retained but excluded from attribution summaries.
Use a complete log covering the run; absence of logged overlap cannot establish
isolation from unlogged background work. No client/server clock alignment is used.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import statistics

PROTOCOL = "iotdbfs-query-profile/1"
PREPARE = ("json_parse", "snapshot_parse", "view_parse", "request_parse",
           "request_validate", "snapshot_prepare", "snapshot_authorize",
           "coverage_index_prepare", "module_prepare", "route_register")
EXECUTE = ("window_planner_create", "window_assembler_create", "scheduler_admit",
           "window_plan", "scheduler_wait", "window_prefetch", "window_assemble",
           "window_transform", "window_batch_finalize")
# Nested detail stages: report separately, never add to prepare_total.
PREPARE_DETAIL = ("snapshot_fingerprint", "snapshot_catalog_scan",
                  "snapshot_source_open", "snapshot_verify_sources",
                  "snapshot_channel_declarations", "coverage_source_open",
                  "coverage_layout_read", "coverage_layout_parse",
                  "coverage_span_build", "coverage_ranges_build",
                  "snapshot_view_validate")
STAGES = PREPARE + ("publish_route", "window_execute", "ipc_encode", "result_publish",
                    "query_authorize") + EXECUTE + PREPARE_DETAIL
COUNTERS = ("hits", "misses", "read_bytes", "read_ranges", "payload_read_calls",
            "payload_read_nanoseconds", "fragment_load_nanoseconds", "decode_count",
            "decoded_bytes", "evictions", "redecodes", "copy_bytes", "producer_count",
            "waits", "decompressions")
CLIENT_SECONDS = ("registration_seconds", "stat_seconds", "ipc_read_seconds",
                  "ipc_decode_seconds", "request_encode_seconds",
                  "socket_roundtrip_seconds", "response_decode_seconds",
                  "ipc_validation_seconds", "ipc_array_seconds")


def distribution(values: list[float]) -> dict:
    if not values:
        return {"count": 0, "sum": 0, "mean": None, "p50": None, "p95": None}
    ordered = sorted(values)
    def percentile(p):
        rank = (len(ordered) - 1) * p
        lo = math.floor(rank)
        hi = math.ceil(rank)
        return ordered[lo] + (ordered[hi] - ordered[lo]) * (rank - lo)
    return {"count": len(values), "sum": sum(values),
            "mean": statistics.mean(values), "p50": percentile(.5),
            "p95": percentile(.95)}


def read_server_log(path: Path) -> list[dict]:
    records = []
    for line_number, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
            if row.get("protocol") != PROTOCOL:
                raise ValueError("unexpected protocol")
            for name in ("pid", "sequence", "steady_start_ns", "steady_end_ns", "server_total_ns"):
                if type(row[name]) is not int or row[name] < 0:
                    raise ValueError(f"invalid {name}")
            if row["steady_end_ns"] < row["steady_start_ns"]:
                raise ValueError("reversed server interval")
            for values in (row.get("stages_ns", {}), row.get("runtime_delta", {})):
                if not isinstance(values, dict) or any(type(v) is not int or v < 0 for v in values.values()):
                    raise ValueError("invalid stage/counter value")
            shape = row.get("payload_read_shape", [])
            if not isinstance(shape, list):
                raise ValueError("invalid payload shape")
            seen = set()
            for cell in shape:
                if not isinstance(cell, dict) or set(cell) != {
                        "range_bin", "byte_bin", "calls", "nanoseconds", "bytes", "ranges"}:
                    raise ValueError("invalid payload shape cell")
                if any(type(value) is not int or value < 0 for value in cell.values()):
                    raise ValueError("invalid payload shape value")
                key = (cell["range_bin"], cell["byte_bin"])
                if key in seen or any(index > 4 for index in key) or cell["calls"] == 0:
                    raise ValueError("invalid payload shape bucket")
                seen.add(key)
            if "payload_read_shape" in row:
                delta = row.get("runtime_delta", {})
                for shape_name, total_name in (("calls", "payload_read_calls"),
                                               ("nanoseconds", "payload_read_nanoseconds")):
                    if total_name in delta and sum(cell[shape_name] for cell in shape) != delta[total_name]:
                        raise ValueError("payload shape does not match runtime total")
            records.append(row)
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f"{path}:{line_number}: {error}") from error
    identities = [(row["pid"], row["sequence"]) for row in records]
    if len(set(identities)) != len(identities):
        raise ValueError("duplicate server pid/sequence; use a log from one daemon lifetime per pid")
    return records


def client_requests(report: dict, split: str, probe: bool = False) -> list[dict]:
    if not probe:
        metrics = report["backend_metrics"][split]
        if "request_timings" not in metrics:
            raise ValueError("client report has no request_timings; rerun with per-request profiling")
        return list(metrics["request_timings"])
    requests = []
    for case_index, case in enumerate(report["cases"]):
        paths, seconds = case.get("paths"), case["seconds"]
        if paths is None or len(paths) != len(seconds):
            raise ValueError("probe paths and seconds must exist and have equal lengths")
        for repeat, (path, elapsed) in enumerate(zip(paths, seconds)):
            requests.append({"path": path, "registration_seconds": elapsed,
                             "anchors": report.get("anchors", []),
                             "case_index": case_index, "repeat": repeat,
                             "channels": case["channels"],
                             "valid_coordinate": case["valid_coordinate"]})
    return requests


def summarize(records: list[dict], requests: list[dict]) -> dict:
    by_path = {}
    for row in records:
        if row.get("path"):
            by_path.setdefault(row["path"], []).append(row)
    joined = []
    for client in requests:
        item = {"client": client, "attributable": False}
        matches = by_path.get(client.get("path"), [])
        if not client.get("path"):
            item["reason"] = "client_returned_no_path"
        elif len(matches) != 1:
            item["reason"] = "unmatched_path" if not matches else "ambiguous_path"
        else:
            server = matches[0]
            item["server"] = server
            conflicts = [other for other in records if other is not server and
                         other["pid"] == server["pid"] and
                         other["steady_start_ns"] < server["steady_end_ns"] and
                         server["steady_start_ns"] < other["steady_end_ns"]]
            item["overlapping_requests"] = [
                {"pid": other["pid"], "sequence": other["sequence"], "path": other.get("path")}
                for other in conflicts]
            item["attributable"] = bool(server.get("ok")) and not conflicts
            if not server.get("ok"):
                item["reason"] = "server_failed"
            elif conflicts:
                item["reason"] = "overlapping_daemon_requests"
        joined.append(item)
    usable = [item for item in joined if item["attributable"]]
    def seconds(name):
        return [item["server"].get("stages_ns", {}).get(name, 0) / 1e9 for item in usable]
    server_summary = {name: distribution(seconds(name)) for name in STAGES}
    server_summary["server_total"] = distribution([
        item["server"]["server_total_ns"] / 1e9 for item in usable])
    server_summary["prepare_total"] = distribution([
        sum(item["server"].get("stages_ns", {}).get(name, 0) for name in PREPARE) / 1e9
        for item in usable])
    shape = {(r, b): {"calls": 0, "nanoseconds": 0, "bytes": 0, "ranges": 0}
             for r in range(5) for b in range(5)}
    for item in usable:
        for cell in item["server"].get("payload_read_shape", []):
            total = shape[cell["range_bin"], cell["byte_bin"]]
            for field in total:
                total[field] += cell[field]
    shape_summary = [dict(range_bin=r, byte_bin=b, **values,
                          mean_milliseconds_per_call=(values["nanoseconds"] / values["calls"] / 1e6
                                                      if values["calls"] else None))
                     for (r, b), values in sorted(shape.items()) if values["calls"]]
    return {
        "protocol": "iotdbfs-query-profile-summary/1",
        "request_count": len(joined), "attributable_count": len(usable),
        "excluded_count": len(joined) - len(usable),
        "boundaries": {
            "server_total": "Outer server request wall time; includes preparation and publish_route.",
            "prepare_total": {"sum_of_disjoint_stages": list(PREPARE)},
            "prepare_detail": "Nested timers across preparation AND execution: fingerprint can run repeatedly; coverage_span_build includes source open/layout read/layout parse; snapshot_view_validate includes fingerprint/channel declarations. Never sum with parent stages.",
            "publish_route": "Parent of materialization, including window_execute, query_authorize, ipc_encode, result_publish and bridge overhead.",
            "window_execute": {"parent_of": list(EXECUTE)},
            "ipc_encode": "Server IPC serialization; separate from client IPC read/decode.",
            "client_registration_seconds": "Client socket round trip including synchronous server materialization.",
            "runtime_nanoseconds": "Aggregate load/read worker durations; nested and potentially concurrent, never additive to server wall time.",
            "rule": "Do not add parent stages to their children or worker durations to wall time.",
            "missing_stage": "Zero, as required by the server profile protocol.",
            "isolation": "Same-pid half-open intervals are checked against ALL supplied log records, including failed/unselected requests. Complete logging and otherwise idle runtime are required.",
            "percentiles": "Linear interpolation between sorted samples.",
            "summary_scope": "Only successfully matched, nonoverlapping requests; excluded records remain below.",
        },
        "server_seconds": server_summary,
        "client_seconds": {name: distribution([item["client"][name] for item in usable
                                               if name in item["client"]])
                           for name in CLIENT_SECONDS},
        "runtime_delta": {name: distribution([item["server"].get("runtime_delta", {}).get(name, 0)
                                               for item in usable]) for name in COUNTERS},
        "payload_read_shape": {
            "range_upper": [1, 4, 16, 32, None],
            "byte_upper": [16384, 65536, 262144, 1048576, None],
            "cells": shape_summary,
            "scope": "Actual source read attempts. Cell time includes SQL, session wait, response conversion and close; cells do not identify causal byte or range cost.",
        },
        "ipc_bytes": distribution([item["client"]["ipc_bytes"] for item in usable
                                   if "ipc_bytes" in item["client"]]),
        # Runtime gauges are snapshots, not counters; retain per request only.
        "requests": joined,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-log", type=Path, required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--client-report", type=Path)
    source.add_argument("--probe-report", type=Path)
    parser.add_argument("--split", default="train")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = json.loads((args.client_report or args.probe_report).read_text())
    result = summarize(read_server_log(args.server_log),
                       client_requests(report, args.split, bool(args.probe_report)))
    result["inputs"] = {"server_log": str(args.server_log),
                        "client_report": str(args.client_report or args.probe_report),
                        "split": None if args.probe_report else args.split}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: result[key] for key in
                      ("request_count", "attributable_count", "excluded_count")}, indent=2))


if __name__ == "__main__":
    main()
