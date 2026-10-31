#!/usr/bin/env python3
"""Validate complete serial multi-view cohorts and summarize paired repetitions."""
import argparse
from collections import defaultdict
import json
import math
from pathlib import Path
import statistics

CASES = (("original", "overlap"), ("sm128", "overlap"), ("sm128", "disjoint"))
ARMS = (("fs", "isolated"), ("fs", "shared"), ("typed", "isolated"), ("typed", "shared"))


def number(value, label):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError(f"invalid nonnegative finite number: {label}")
    return value


def distribution(values):
    return dict(count=len(values), mean=statistics.mean(values), min=min(values),
                max=max(values), by_repetition=values)


def declared_cases(method):
    cases = method.get("cases", [list(case) for case in CASES])
    if not isinstance(cases, list) or not cases:
        raise ValueError("cases must be a nonempty list")
    normalized = []
    for case in cases:
        if (not isinstance(case, list) or len(case) != 2 or
                any(not isinstance(value, str) for value in case) or tuple(case) not in CASES):
            raise ValueError("unexpected declared case")
        normalized.append(tuple(case))
    if len(set(normalized)) != len(normalized):
        raise ValueError("duplicate declared case")
    return normalized


def aggregate(method, observations):
    if method.get("protocol") != "iotdbfs-shared-views-experiment/1":
        raise ValueError("unexpected experiment protocol")
    reps, requests = method.get("repetitions"), method.get("requests_per_experiment")
    if type(reps) is not int or reps <= 0 or type(requests) is not int or requests <= 0:
        raise ValueError("invalid repetitions or request count")
    if method.get("experiments") != 4 or method.get("passes") != 2:
        raise ValueError("expected four experiments and two passes")
    cases = declared_cases(method)
    warm_zero_reads = method.get("warm_zero_reads_expected", True)
    if type(warm_zero_reads) is not bool:
        raise ValueError("warm_zero_reads_expected must be a boolean")
    expected = {(rep, layout, pattern, backend, sharing, repeat)
                for rep in range(reps) for layout, pattern in cases
                for backend, sharing in ARMS for repeat in range(2)}
    groups = defaultdict(list)
    for row in observations:
        try:
            key = tuple(row[name] for name in
                        ("repetition", "layout", "pattern", "backend", "sharing", "pass_index"))
            if type(key[0]) is not int or type(key[-1]) is not int or key not in expected:
                raise ValueError("unexpected observation key")
            if row.get("passed") is not True:
                raise ValueError("failed observation")
            cohort = row["cohort"]
            if not isinstance(cohort, list) or any(type(i) is not int for i in cohort):
                raise ValueError("invalid cohort")
            wanted = ([list(range(4))] if key[3:5] != ("typed", "isolated")
                      else [[i] for i in range(4)])
            if cohort not in wanted:
                raise ValueError("unexpected cohort")
            n = len(cohort) * requests
            if row["requests"] != n or row["samples"] != n * 2:
                raise ValueError("incorrect request/sample count")
            times = row["request_seconds"]
            if set(times) != {str(i) for i in cohort}:
                raise ValueError("request timings do not cover cohort")
            for values in times.values():
                if not isinstance(values, list) or len(values) != requests:
                    raise ValueError("incomplete request timings")
                for value in values:
                    number(value, "request_seconds")
            for name in ("execution_seconds", "setup_seconds", "prepare_seconds", "cache_owned_bytes"):
                number(row[name], name)
            if not math.isclose(row["execution_seconds"], sum(map(sum, times.values())),
                                rel_tol=1e-9, abs_tol=1e-9):
                raise ValueError("execution time disagrees with request timings")
            for name in ("runtime_delta", "server_seconds"):
                if not isinstance(row[name], dict):
                    raise ValueError("invalid metric dictionary")
                for field, value in row[name].items():
                    number(value, f"{name}.{field}")
            decode_field = "decode_count" if key[3] == "typed" else "batch_decodes"
            for name in ("read_bytes", decode_field):
                if name not in row["runtime_delta"]:
                    raise ValueError(f"missing counter: {name}")
            if key[-1] == 1 and ((warm_zero_reads and row["runtime_delta"]["read_bytes"] != 0) or
                                (warm_zero_reads and row["runtime_delta"][decode_field] != 0) or
                                row["setup_seconds"] != 0 or row["prepare_seconds"] != 0):
                raise ValueError("warm pass read bytes, decode count or preparation were nonzero")
            groups[key].append(row)
        except (KeyError, TypeError) as error:
            raise ValueError(f"malformed observation: {error}") from error
    if set(groups) != expected:
        raise ValueError("incomplete experiment arm coverage")
    aggregated = []
    for key in sorted(expected):
        rows = groups[key]
        wanted = ({(i,) for i in range(4)} if key[3:5] == ("typed", "isolated")
                  else {(0, 1, 2, 3)})
        actual = [tuple(row["cohort"]) for row in rows]
        if set(actual) != wanted or len(actual) != len(wanted):
            raise ValueError("duplicate or missing cohort")
        entry = dict(zip(("repetition", "layout", "pattern", "backend", "sharing", "pass_index"), key))
        for name in ("execution_seconds", "setup_seconds", "prepare_seconds", "cache_owned_bytes", "requests", "samples"):
            entry[name] = sum(row[name] for row in rows)
        entry["cold_total_seconds"] = entry["execution_seconds"] + entry["setup_seconds"] + entry["prepare_seconds"]
        for name in ("runtime_delta", "server_seconds"):
            fields = set(rows[0][name])
            if any(set(row[name]) != fields for row in rows):
                raise ValueError("inconsistent cohort metric fields")
            entry[name] = {field: sum(row[name][field] for row in rows) for field in sorted(fields)}
        entry["decode_count"] = entry["runtime_delta"]["decode_count" if key[3] == "typed" else "batch_decodes"]
        entry["read_bytes"] = entry["runtime_delta"]["read_bytes"]
        if warm_zero_reads and key[-1] == 0 and "demand_units" in method:
            backend = key[3]
            scope = "private" if key[4] == "isolated" else "shared"
            unit = "batch" if backend == "fs" else "fragment"
            field = f"{backend}_{scope}_{unit}_units"
            try:
                expected_decodes = method["demand_units"][f"{key[1]}-{key[2]}"][field]
            except (KeyError, TypeError) as error:
                raise ValueError("incomplete physical demand oracle") from error
            if type(expected_decodes) is not int or expected_decodes < 0:
                raise ValueError("invalid physical demand oracle count")
            if entry["decode_count"] != expected_decodes:
                raise ValueError(f"cold decode count disagrees with physical demand oracle: {key}, "
                                 f"observed {entry['decode_count']}, expected {expected_decodes}")
            entry["expected_decode_count"] = expected_decodes
        # Shared FS client_metrics repeats the shared reader once per view.
        # runtime_delta was deduplicated by reader identity in the runner.
        aggregated.append(entry)
    return aggregated


def summarize(method, observations):
    rows = aggregate(method, observations)
    cases = declared_cases(method)
    metrics = ("execution_seconds", "setup_seconds", "prepare_seconds", "cold_total_seconds",
               "read_bytes", "decode_count", "cache_owned_bytes")
    groups = []
    for layout, pattern in cases:
        for backend, sharing in ARMS:
            for repeat in range(2):
                selected = sorted((r for r in rows if
                    (r["layout"], r["pattern"], r["backend"], r["sharing"], r["pass_index"]) ==
                    (layout, pattern, backend, sharing, repeat)), key=lambda r: r["repetition"])
                groups.append(dict(layout=layout, pattern=pattern, backend=backend,
                    sharing=sharing, pass_index=repeat,
                    metrics={name: distribution([r[name] for r in selected]) for name in metrics}))
    pairs = []
    by_key = {(r["repetition"], r["layout"], r["pattern"], r["backend"], r["sharing"], r["pass_index"]): r for r in rows}
    for layout, pattern in cases:
        for backend in ("fs", "typed"):
            for repeat in range(2):
                values = defaultdict(list)
                for rep in range(method["repetitions"]):
                    isolated = by_key[rep, layout, pattern, backend, "isolated", repeat]
                    shared = by_key[rep, layout, pattern, backend, "shared", repeat]
                    for name in metrics:
                        values[name + "_saved"].append(isolated[name] - shared[name])
                        # 0/0, including warm reads, is undefined, not a speedup.
                        if shared[name] > 0:
                            values[name + "_isolated_over_shared"].append(isolated[name] / shared[name])
                    primary = "cold_total_seconds" if repeat == 0 else "execution_seconds"
                    values["primary_speedup"].append(isolated[primary] / shared[primary]
                                                     if shared[primary] else None)
                pairs.append(dict(layout=layout, pattern=pattern, backend=backend, pass_index=repeat,
                    primary_metric="cold_total_seconds" if repeat == 0 else "execution_seconds",
                    metrics={name: distribution(v) if len(v) == method["repetitions"] and None not in v else None
                             for name, v in values.items()}))
    return dict(protocol="iotdbfs-shared-views-summary/1", method=method,
        observation_count=len(observations), complete_arm_count=len(rows),
        requests=sum(r["requests"] for r in rows), samples=sum(r["samples"] for r in rows),
        boundaries={
            "execution": "Sum of serial request times; typed isolated sums four fresh-daemon references, not simultaneous makespan.",
            "cold": "Execution plus all view client construction and preparation; daemon startup excluded.",
            "warm": "Second pass execution with retained prepared IDs and decoded caches; finite-cache evictions may require reads when warm_zero_reads_expected is false.",
            "cache": "Shared runtime cache-owned bytes; isolated sums four caches. Excludes prepared context, result store and process RSS.",
            "statistics": "One complete four-view arm per independent repetition; ratios paired within repetition, then summarized.",
            "decode": "FS physical IPC batch decodes versus typed adapter fragment decodes; compare within backend.",
            "demand_oracle": "When supplied and warm_zero_reads_expected is true, cold decode counts must equal independently enumerated unique demanded physical units; warm decode counts must be zero. Pressure campaigns permit redecoding.",
            "runtime": "Runner runtime_delta is authoritative; client_metrics may alias shared reader and is not summed.",
            "scope": "Serial sharing only; no concurrent throughput, scheduler fairness or OS-cold claims."},
        groups=groups, paired_contrasts=pairs, arms=rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    method = json.loads((args.input / "method.json").read_text())
    observations = json.loads((args.input / "results.json").read_text())
    complete = json.loads((args.input / "complete.json").read_text())
    if complete.get("passed") is not True or complete.get("observations") != len(observations):
        raise ValueError("run did not finish successfully")
    result = summarize(method, observations)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(json.dumps({k: result[k] for k in ("observation_count", "complete_arm_count", "requests", "samples")}))


if __name__ == "__main__":
    main()
