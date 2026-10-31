#!/usr/bin/env python3
"""Independently validate and summarize the matched OISST A--E campaign."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import statistics


def need(value, message):
    if not value:
        raise ValueError(message)


def read(path):
    return json.loads(path.read_text())


def rows(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def median(values):
    return statistics.median(values) if values else None


def summary(root):
    campaign = read(root / "campaign.json")
    need(campaign["protocol"] == "iotdbfs-hdf5-sc-perf-campaign/1" and campaign["complete"],
         "campaign incomplete or unknown protocol")
    need(len(campaign["runs"]) == campaign["campaigns"], "run count mismatch")
    source_ids = {source["id"] for source in campaign["sources"]}
    sources_by_id = {source["id"]: source for source in campaign["sources"]}
    scenarios = set(campaign["cases"])
    expected_cases = {f"{source}:{scenario}" for source in source_ids for scenario in scenarios}
    paths = set(campaign["paths"])
    expected = {(p, n, case) for p in paths for n in range(campaign["passes"])
                for case in expected_cases}
    by_path_scenario_pass = defaultdict(list)
    metric_samples = defaultdict(list)
    catalog = defaultdict(list)
    campaign_ratios = defaultdict(list)
    matched = 0
    for identity, run in enumerate(campaign["runs"]):
        need(run["campaign"] == identity and run["validated"], "run ordering or validation failure")
        work = root / f"campaign-{identity}"
        observations = read(work / "observations.json")
        lookup = {}
        for entry in observations:
            key = (entry["path"], entry["pass"], entry["case_id"])
            need(key not in lookup, f"duplicate observation {key}")
            lookup[key] = entry
        need(set(lookup) == expected, f"observation matrix mismatch in campaign {identity}")
        for case in expected_cases:
            oracle = lookup[("A", 0, case)]
            for path in paths:
                for n in range(campaign["passes"]):
                    item = lookup[(path, n, case)]
                    need((item["rows"], item["masked"], item["digest_fnv64"]) ==
                         (oracle["rows"], oracle["masked"], oracle["digest_fnv64"]),
                         f"value/mask mismatch: {identity}/{path}/{n}/{case}")
                    matched += 1
                    scenario = case.split(":", 1)[1]
                    phase = "first" if n == 0 else "warm"
                    by_path_scenario_pass[(path, scenario, phase)].append(item["latency_ns"])
        for key in ("native_scan_ns", "fs_prebuilt_filter_ns", "iotdb_client_ns",
                    "iotdb_candidate_datasets", "iotdb_sql_queries", "iotdb_catalog_read_bytes"):
            catalog[key].append(run["catalog"][key])
        for path in ("B", "D"):
            probe = rows(work / f"{path}-raw.jsonl")
            need(len(probe) == 1 + campaign["passes"] * len(expected_cases),
                 f"direct raw cardinality mismatch: {path}")
            setup = probe[0]
            need(setup["kind"] == "setup" and setup["path"] == path,
                 "direct setup record missing")
            for key in ("wall_ns", "index_build_ns", "index_read_calls", "index_read_bytes"):
                metric_samples[(path, "setup", key)].append(setup[key])
            for item in probe[1:]:
                key = (path, item["pass"], item["case_id"])
                need(item["kind"] == "query" and key in lookup, "unmatched direct sample")
                need(item["latency_ns"] == lookup[key]["latency_ns"] and
                     item["fnv1a64"] == lookup[key]["digest_fnv64"],
                     "direct raw sample does not match observation")
                phase = "first" if item["pass"] == 0 else "warm"
                scenario = item["case_id"].split(":", 1)[1]
                for name in ("payload_calls", "payload_ranges", "payload_bytes",
                             "layout_calls", "layout_bytes", "manifest_calls"):
                    value = item["backend_delta"].get(name, 0)
                    metric_samples[(path, phase, name)].append(value)
                    metric_samples[(path, scenario, phase, name)].append(value)
                for name in ("hits", "misses", "decode_count", "decoded_bytes",
                             "payload_read_calls", "payload_read_nanoseconds"):
                    value = item["runtime_delta"].get(name, 0)
                    metric_samples[(path, phase, name)].append(value)
                    metric_samples[(path, scenario, phase, name)].append(value)
                for name in ("prepare", "execute_setup", "cursor_next", "owned_copy"):
                    value = item["timings_ns"].get(name, 0)
                    metric_samples[(path, phase, name + "_ns")].append(value)
                    metric_samples[(path, scenario, phase, name + "_ns")].append(value)
                for name in ("hdf5.layout.directory", "hdf5.assemble", "hdf5.dataset.cf_binding"):
                    value = item["stages_ns"].get(name, 0)
                    metric_samples[(path, phase, name + "_ns")].append(value)
                    metric_samples[(path, scenario, phase, name + "_ns")].append(value)
        for path in ("A", "C"):
            for source in source_ids:
                native = read(work / f"{path}-{source}.json")
                need(len(native) == campaign["passes"] * len(scenarios),
                     "native raw cardinality")
                for item in native:
                    scenario = item["scenario"]
                    key = (path, item["pass"], f"{source}:{scenario}")
                    need(key in lookup and item["digest_fnv64"] == lookup[key]["digest_fnv64"],
                         "native raw digest mismatch")
                    phase = "first" if item["pass"] == 0 else "warm"
                    for name in ("h5dread_ns", "transform_ns", "coordinate_resolve_ns", "cpu_user_ns",
                                 "cpu_system_ns", "minor_faults", "major_faults"):
                        metric_samples[(path, phase, name)].append(item[name])
                        metric_samples[(path, scenario, phase, name)].append(item[name])
                    if item["pass"] == 0 and scenario == "point":
                        metric_samples[(path, "setup", "setup_ns")].append(item["setup_ns"])
                    for name in ("source_bytes", "dataset_storage_bytes", "logical_raw_bytes",
                                 "owned_value_bytes"):
                        metric_samples[(path, scenario, name)].append(item[name])
        e = read(work / "E-raw.json")
        profiles = rows(work / "server-profile.jsonl")
        typed = {p["path"]: p for p in profiles if p["operation"] == "typed" and p["ok"]}
        need(len(e) == len(typed) == campaign["passes"] * len(expected_cases),
             "E route/profile cardinality mismatch")
        for item in e:
            key = ("E", item["pass"], item["case_id"])
            source = sources_by_id[item["case_id"].split(":", 1)[0]]
            need((item["logical_path"], item["version"]) ==
                 (source["logical_path"], source["version"]), "E source/version drift")
            need(item["result_path"] in typed and item["digest_fnv64"] == lookup[key]["digest_fnv64"],
                 "E route/profile mismatch")
            profile = typed[item["result_path"]]
            phase = "first" if item["pass"] == 0 else "warm"
            scenario = item["case_id"].split(":", 1)[1]
            for name in ("registration_ns", "ipc_owned_ns", "validation_cleanup_ns", "ipc_file_bytes",
                         "owned_value_bytes", "request_bytes", "response_bytes"):
                metric_samples[("E", phase, name)].append(item[name])
                metric_samples[("E", scenario, phase, name)].append(item[name])
            metric_samples[("E", phase, "server_total_ns")].append(profile["server_total_ns"])
            metric_samples[("E", scenario, phase, "server_total_ns")].append(profile["server_total_ns"])
            for name in ("registration_total", "typed_prepare", "typed_execute_setup",
                         "typed_cursor_execute", "typed_ipc_encode", "query_authorize",
                         "result_publish"):
                value = profile["stages_ns"].get(name, 0)
                metric_samples[("E", phase, name + "_ns")].append(value)
                metric_samples[("E", scenario, phase, name + "_ns")].append(value)
            for name in ("query_calls", "metadata_query_calls", "payload_calls", "payload_ranges",
                         "payload_returned_bytes", "layout_calls", "layout_returned_bytes"):
                value = profile["iotdb_delta"].get(name, 0)
                metric_samples[("E", phase, "iotdb_" + name)].append(value)
                metric_samples[("E", scenario, phase, "iotdb_" + name)].append(value)
        for numerator, denominator in (("B", "A"), ("C", "A"), ("D", "B"), ("E", "D")):
            for scenario in scenarios:
                for phase, passes in (("first", [0]), ("warm", range(1, campaign["passes"]))):
                    lhs = [lookup[(numerator, p, case)]["latency_ns"] for p in passes
                           for case in expected_cases if case.endswith(":" + scenario)]
                    rhs = [lookup[(denominator, p, case)]["latency_ns"] for p in passes
                           for case in expected_cases if case.endswith(":" + scenario)]
                    need(sum(rhs) > 0, "zero timing denominator")
                    campaign_ratios[(f"{numerator}/{denominator}", scenario, phase)].append(sum(lhs)/sum(rhs))
    return {"protocol": "iotdbfs-hdf5-sc-perf-summary/1", "complete": True,
            "source_count": len(source_ids), "campaigns": campaign["campaigns"],
            "passes": campaign["passes"], "validated_observations": matched,
            "latency_ns": {"/".join(key): {"median": median(value), "mean": statistics.mean(value),
                                      "count": len(value)}
                           for key, value in sorted(by_path_scenario_pass.items())},
            "metrics": {"/".join(key): {"sum": sum(value), "mean": statistics.mean(value),
                                   "count": len(value)}
                        for key, value in sorted(metric_samples.items())},
            "catalog": {key: {"values": value, "median": median(value)}
                        for key, value in sorted(catalog.items())},
            "paired_campaign_ratios": {"/".join(key): {"values": value, "mean": statistics.mean(value)}
                                       for key, value in sorted(campaign_ratios.items())}}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    arguments = parser.parse_args()
    output = summary(arguments.root)
    destination = arguments.root / "summary.json"
    destination.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"validated_observations": output["validated_observations"],
                      "summary": str(destination)}))
