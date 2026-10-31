"""Summarize completed pilot records; inclusive stage clocks are not additive."""
import argparse
from collections import Counter
import json
from pathlib import Path
import statistics


def analyze(report):
    if report.get("complete") is not True:
        raise ValueError("incomplete campaign")
    count = Counter((r["format"], r["phase"], r["arm"]) for r in report["records"])
    wanted = {(f, p, a): report["campaigns"] * report["repeats"]
              for f in ("hdf5", "grib") for p in ("discover", "discover-values")
              for a in ("fs-scan", "fs-index", "iotdb")}
    expected_k = report["preparation"]["hdf5"]["actual"]["K"]
    if count != wanted or any(r["selected_units"] != expected_k for r in report["records"]):
        raise ValueError("unbalanced campaign or unexpected pilot matches")
    summaries = []
    for fmt in ("hdf5", "grib"):
        for phase in ("discover", "discover-values"):
            rows = [r for r in report["records"] if r["format"] == fmt and
                    r["phase"] == phase and r["arm"] == "iotdb" and r["repeat"] > 0]
            calls = [[r["discovery"], *r["value_calls"]] for r in rows]

            def median_sum(section, key):
                return statistics.median(sum(c["server_profile"].get(section, {}).get(key, 0)
                                             for c in group) for group in calls)

            stage_keys = sorted({k for group in calls for c in group for k in c["server_profile"]["stages_ns"]})
            sql = median_sum("iotdb_delta", "query_calls")
            if sql != statistics.median(r["iotdb_sql_queries"] for r in rows) or sql <= 0:
                raise ValueError("SQL counter does not agree with raw profiles")
            summaries.append({"format": fmt, "phase": phase,
                "warm_observations": len(rows),
                "registrations": statistics.median(r["registration_requests"] for r in rows),
                "client_p50_ms": statistics.median(r["total_ns"] for r in rows) / 1e6,
                "server_p50_ms": statistics.median(r["server_total_ns"] for r in rows) / 1e6,
                "ipc_owned_read_p50_ms": statistics.median(sum(c["ipc_read_owned_copy_ns"] for c in group) for group in calls) / 1e6,
                "sql_calls": sql,
                "execute_query_p50_ms": median_sum("iotdb_delta", "query_nanoseconds") / 1e6,
                "layout_calls": median_sum("iotdb_delta", "layout_calls"),
                "payload_calls": median_sum("iotdb_delta", "payload_calls"),
                "payload_returned_bytes": median_sum("iotdb_delta", "payload_returned_bytes"),
                "decode_count": median_sum("runtime_delta", "decode_count"),
                "ipc_bytes": statistics.median(sum(c["ipc_bytes"] for c in group) for group in calls),
                "inclusive_stages_p50_ms": {k: median_sum("stages_ns", k) / 1e6 for k in stage_keys}})
    return {"protocol": "iotdbfs-multifile-pilot-analysis/1", "balanced_observations": sum(count.values()),
            "summary": report["summary"], "iotdb_diagnostics": summaries,
            "note": "Stage clocks nest and overlap. Medians of components do not sum to a median total."}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = analyze(json.loads(args.report.read_text()))
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
