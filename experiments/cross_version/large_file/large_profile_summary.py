#!/usr/bin/env python3
"""Describe sequential request counters without adding overlapping stages."""
from collections import defaultdict
import json
from pathlib import Path
import statistics
import sys


def counter_sum(profiles, section, name):
    values = [p.get(section, {}).get(name) for p in profiles]
    return sum(values) if values and all(v is not None for v in values) else None


def summarize(path):
    groups = defaultdict(list)
    for line in Path(path).open():
        row = json.loads(line)
        if row["system"] not in ("V03", "V04"):
            continue
        discovery = [c["server_profile"] for c in row["discovery_calls"]]
        values = [c["server_profile"] for c in row["value_calls"]]
        metrics = {"complete_ms": row["total_ns"] / 1e6,
            "discovery_client_ms": row["discovery_client_ns"] / 1e6,
            "selected_values_client_ms": row["selected_value_client_ns"] / 1e6,
            "values_control_ms": sum(c["control_ns"] for c in row["value_calls"]) / 1e6,
            "values_server_total_ms": sum(p["server_total_ns"] for p in values) / 1e6,
            "values_ipc_bytes": sum(c["ipc_bytes"] for c in row["value_calls"]),
            "values_cache_owned_bytes_max": max(p.get("runtime_after", {}).get("cache_owned_bytes", 0) for p in values)}
        for name in ("typed_ipc_array_build", "typed_ipc_encode", "typed_cursor_execute"):
            duration = counter_sum(values, "stages_ns", name)
            metrics["values_" + name + "_ms"] = duration / 1e6 if duration is not None else None
        for name in ("hits", "misses", "decode_count", "decompressions", "read_bytes", "payload_read_bytes", "evictions", "redecodes"):
            metrics["values_" + name] = counter_sum(values, "runtime_delta", name)
        for name in ("payload_map.root_requests", "version_directory.head_control_calls"):
            metrics["discovery_" + name] = counter_sum(discovery, "counters", name)
        for name in ("query_calls", "version_store_query_calls", "version_store_batch_query_calls", "version_store_blob_bytes"):
            metrics["whole_" + name] = counter_sum(discovery + values, "iotdb_delta", name)
        key = tuple(row[k] for k in ("scenario", "V", "format", "selection", "system"))
        groups[key].append({"campaign": row["campaign"], "repeat": row["repeat"], "metrics": metrics})
    result = []
    for key, rows in sorted(groups.items()):
        def median(selected, name):
            values = [r["metrics"][name] for r in selected]
            return statistics.median(values) if values and all(v is not None for v in values) else None
        warm, first = [r for r in rows if r["repeat"] > 0], [r for r in rows if r["repeat"] == 0]
        result.append(dict(zip(("scenario", "V", "format", "selection", "system"), key),
            warm_p50={name: median(warm, name) for name in rows[0]["metrics"]},
            first_by_campaign=[{"campaign": r["campaign"], **r["metrics"]} for r in sorted(first, key=lambda r:r["campaign"])],
            note="per-cell medians; cannot sum stage medians to reconstruct complete median; first request is not necessarily cold"))
    formats = {r["format"] for r in result}
    return {"complete": bool(formats) and len(result) == 16 * len(formats), "cells": result,
        "scope": "sequential request profile counters; SDK submissions are not wire RPCs; nullable fields remain unavailable"}


if __name__ == "__main__":
    result = summarize(sys.argv[1])
    with Path(sys.argv[2]).open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps({"complete": result["complete"], "cells": len(result["cells"])}))
