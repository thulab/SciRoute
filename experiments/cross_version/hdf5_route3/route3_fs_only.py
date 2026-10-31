#!/usr/bin/env python3
"""Selective native-FS mechanism study, explicitly separate from real IoTDB.

One N=1024/V=32 independent-file history supplies declared file-ID scopes N=512
and N=1024. These are catalog scopes within the immutable master directory, not
exclusive physical subdirectories. This program has no database/service API.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
import os
from pathlib import Path
import statistics
import time

from experiments.cross_version.hdf5_route3 import route3_data as data

PROTOCOL = "iotdbfs-route3-fs-only/1"
NS = (512, 1024)
VERSIONS = (1, 8, 32)
ARMS = ("FS-scan", "FS-index")
CAMPAIGNS, REPEATS = 5, 5
LOGICAL_PREFIX = "/route3-fs-only/"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def append(path, row):
    with Path(path).open("a") as out:
        out.write(json.dumps(row, sort_keys=True, allow_nan=False) + "\n")
        out.flush()


def selections(V):
    require(V in VERSIONS, "unsupported version scope")
    result = [("latest", {"kind": "latest"}, [V])]
    if V > 1:
        middle = V // 2
        result += [("historical_one", {"kind": "ordinal", "lower": middle,
                    "upper": middle + 1}, [middle]),
                   ("historical_two", {"kind": "ordinal", "lower": middle,
                    "upper": middle + 2}, [middle, middle + 1])]
    return result


def scoped_entries(manifest, N, V):
    require(N in NS and V in VERSIONS, "unsupported file/version scope")
    entries = [entry for entry in manifest["entries"]
               if entry["file_id"] < N and entry["ordinal"] <= V]
    identities = {(entry["file_id"], entry["ordinal"]) for entry in entries}
    require(len(entries) == N * V and len(identities) == len(entries),
            "catalog scope lacks complete unique file/version universe")
    require(identities == {(n, v) for n in range(N) for v in range(1, V + 1)},
            "catalog scope file IDs/rounds differ")
    return entries


def catalog_for(raw, entries):
    # No attributes, match identities, ROI values or oracle data enter selection.
    return {"protocol": "iotdbfs-route3-publication/1", "sources": [
        {"logical_path": LOGICAL_PREFIX + entry["logical_file"],
         "ordinal": entry["ordinal"], "version": entry["ordinal"],
         "path": str((raw / entry["file"]).resolve(strict=True)),
         "size": entry["bytes"], "mtime_ns": entry["mtime_ns"]}
        for entry in entries]}


def case_order(campaign):
    cases = [(N, V) for N in NS for V in VERSIONS]
    shift = campaign % len(cases)
    return cases[shift:] + cases[:shift]


def arm_order(campaign, N, V):
    return ARMS if (campaign + NS.index(N) + VERSIONS.index(V)) % 2 == 0 else tuple(reversed(ARMS))


def memory_sample():
    status = Path("/proc/self/status")
    if not status.exists():
        return {"scope": "process memory supplied by external launcher on this platform"}
    wanted = {"VmRSS", "VmHWM", "VmSize"}
    return {line.split(":", 1)[0]: line.split(":", 1)[1].strip()
            for line in status.read_text().splitlines() if line.split(":", 1)[0] in wanted}


def index_plans(path, V):
    import sqlite3
    prefix = "SELECT source,dataset FROM units WHERE name=? AND type=? AND level=? AND step=? AND date=? AND time=?"
    parameters = [data.PREDICATE[key] for key in ("shortName", "typeOfLevel", "level", "step", "validityDate", "validityTime")]
    result = {"sqlite_version": sqlite3.sqlite_version, "selections": {}}
    with sqlite3.connect("file:" + str(path) + "?mode=ro", uri=True) as connection:
        for label, selection, _ in selections(V):
            if selection["kind"] == "latest":
                suffix = " AND version=(SELECT MAX(s.version) FROM sources s WHERE s.logical=units.logical)"
                params = parameters
            else:
                suffix = " AND ordinal>=? AND ordinal<?"
                params = parameters + [selection["lower"], selection["upper"]]
            result["selections"][label] = [list(row) for row in connection.execute(
                "EXPLAIN QUERY PLAN " + prefix + suffix + " ORDER BY logical,version,dataset", params)]
    return result


def query(context, arm, selection, origin, expected):
    import numpy as np
    from experiments.cross_version.hdf5_route3.route3_native import counter_delta
    before = context.stats()
    started = time.perf_counter_ns()
    selected = context.select("scan" if arm == "FS-scan" else "index",
                              selection, data.PREDICATE)
    owned = [(unit, *context.read(unit["path"], unit["dataset"],
                                  list(origin), list(data.ROI)))
             for unit in selected["units"]]
    total_ns = time.perf_counter_ns() - started
    delta = counter_delta(before, context.stats())
    # All provenance, independent physical-value hashes and correctness checks
    # happen after the complete owned-array query timer has stopped.
    actual = {}
    for unit, values, mask in owned:
        require(unit["logical_path"].startswith(LOGICAL_PREFIX), "foreign source")
        key = (unit["logical_path"][len(LOGICAL_PREFIX):], unit["ordinal"], unit["dataset"])
        require(key not in actual and key in expected, "duplicate or unexpected result")
        oracle = expected[key]
        require(values.flags.owndata and mask.flags.owndata and
                values.flags.c_contiguous and mask.flags.c_contiguous,
                "query must return actual client-owned contiguous arrays")
        require(values.dtype == np.dtype("float32") and mask.dtype == np.dtype("uint8") and
                tuple(values.shape) == data.ROI and tuple(mask.shape) == data.ROI,
                "result dtype/shape differs")
        require(values.size == oracle["elements"] and
                hashlib.sha256(values.tobytes(order="C")).hexdigest() == oracle["value_sha256"] and
                hashlib.sha256(mask.tobytes(order="C")).hexdigest() == oracle["mask_sha256"],
                "native result differs from independent source ROI oracle")
        actual[key] = True
    require(set(actual) == set(expected), "query omitted selected scientific results")
    return {"total_ns": total_ns, "stats": delta,
            "selected_sources": selected["selected_sources"],
            "candidate_datasets": selected["candidate_datasets"],
            "selected_units": len(owned), "owned_elements": sum(o["elements"] for o in expected.values()),
            "owned_bytes": sum(o["elements"] * 5 for o in expected.values()),
            "oracle_verified": True, "owned_client_arrays": True}


def summarize(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[tuple(row[k] for k in ("N", "V", "selection", "roi", "arm"))].append(row)
    cells = []
    for key, records in sorted(grouped.items()):
        warm = [r for r in records if r["repeat"] > 0]
        first = [r for r in records if r["repeat"] == 0]
        require(len(warm) == CAMPAIGNS * (REPEATS - 1) and len(first) == CAMPAIGNS,
                "incomplete query cell")
        cell = dict(zip(("N", "V", "selection", "roi", "arm"), key))
        cell.update(samples=len(records), warm_samples=len(warm), first_samples=len(first),
            warm_median_ms=statistics.median(r["total_ns"] for r in warm) / 1e6,
            first_median_ms=statistics.median(r["total_ns"] for r in first) / 1e6,
            campaign_warm_medians_ms=[statistics.median(r["total_ns"] for r in warm
                if r["campaign"] == campaign) / 1e6 for campaign in range(CAMPAIGNS)],
            selected_sources=sorted({r["selected_sources"] for r in records}),
            candidate_datasets=sorted({r["candidate_datasets"] for r in records}),
            selected_units=sorted({r["selected_units"] for r in records}),
            owned_bytes=sorted({r["owned_bytes"] for r in records}))
        fields = set().union(*(r["stats"] for r in warm))
        cell["warm_counter_medians"] = {field: statistics.median(r["stats"].get(field, 0)
                for r in warm) for field in sorted(fields)}
        cells.append(cell)
    lookup = {(c["N"], c["V"], c["selection"], c["roi"], c["arm"]): c for c in cells}
    ratios = []
    for c in cells:
        alternatives = []
        if c["N"] == 1024:
            alternatives.append(("N1024/N512", (512, c["V"], c["selection"], c["roi"], c["arm"])))
        if c["selection"] == "latest" and c["V"] > 1:
            alternatives.append(("V%d/V1" % c["V"], (c["N"], 1, c["selection"], c["roi"], c["arm"])))
        elif c["selection"] != "latest" and c["V"] == 32:
            alternatives.append(("V32/V8", (c["N"], 8, c["selection"], c["roi"], c["arm"])))
        for name, key in alternatives:
            baseline = lookup[key]
            ratios.append({k: c[k] for k in ("N", "V", "selection", "roi", "arm")} |
                          {"comparison": name, "warm_median_ratio": c["warm_median_ms"] / baseline["warm_median_ms"]})
    return {"protocol": PROTOCOL, "query_records": len(rows), "cells": cells, "growth_ratios": ratios,
            "interpretation": "FS-only native mechanism study; no measured IoTDB result at these N/V combinations; five temporal campaigns are not independent statistical replications"}


def audit_raw(raw, manifest):
    started = time.perf_counter_ns()
    identities = set()
    bindings = hashlib.sha256()
    total, allocated = 0, 0
    for entry in manifest["entries"]:
        path = raw / entry["file"]
        stat = path.stat()
        require(not path.is_symlink() and stat.st_nlink == 1 and
                (stat.st_dev, stat.st_ino) not in identities, "raw files are not independent")
        identities.add((stat.st_dev, stat.st_ino))
        require(stat.st_size == entry["bytes"] and stat.st_mtime_ns == entry["mtime_ns"] and
                data.sha_file(path) == entry["sha256"], "raw input changed")
        total += stat.st_size
        allocated += stat.st_blocks * 512
        bindings.update(json.dumps({k: entry[k] for k in
            ("file", "file_id", "ordinal", "bytes", "sha256")}, sort_keys=True).encode() + b"\n")
    return {"complete": True, "files": len(identities), "physical_bytes": total,
            "allocated_bytes": allocated, "source_binding_sha256": bindings.hexdigest(),
            "full_source_sha_verification_ns": time.perf_counter_ns() - started,
            "allocation_verified_during_generation": "every dataset has all sixteen chunks allocated; no sparse payloads, links or reflinks"}


def run(args):
    from experiments.cross_version.hdf5_route3.route3_native import NativeContext
    work = args.work.resolve()
    library = args.native_library.resolve(strict=True)
    require(data.sha_file(library) == args.native_sha, "native library SHA mismatch")
    require(not work.exists(), "work directory must be new; no implicit resume/retry")
    work.mkdir(parents=True)
    raw, catalogs = work / "master-raw", work / "catalogs"
    catalogs.mkdir()
    started = time.time()
    report = {"protocol": PROTOCOL, "complete": False, "fs_only": True,
        "database_operations": 0, "service_operations": 0,
        "native_library": str(library), "native_sha256": args.native_sha,
        "N": list(NS), "V": list(VERSIONS), "D": 4, "K": 2,
        "campaigns": CAMPAIGNS, "repeats": REPEATS, "exclude_repeat": 0,
        "master_N": 1024, "master_V": 32, "roi": data.ROI, "origins": data.ORIGINS,
        "source_scope": "N512 and N1024 are declared file_id<N catalog scopes within one immutable full N1024 master history, not exclusive physical subdirectories",
        "versions": "native FS version=ordinal; no IoTDB timestamp binding or large-V IoTDB result is asserted",
        "query_boundary": "client JSON metadata selection plus native hyperslab writes into actual client-owned Float32 values and uint8 masks; independent SHA/provenance oracle validation outside clock",
        "cache_scope": "fresh bounded native context per campaign/case/arm; up to 8 file and 8 dataset handles; 2MiB metadata/file and 256KiB raw chunks/dataset; no cold-disk claim",
        "first_repeat_semantics": "repeat 0 is the first request of that selection/ROI group; previous groups may already have warmed caches; not a cold-query measurement",
        "order": "five campaigns rotate cases and alternate arm order; odd campaign count gives one arm an extra first position per case; all per-campaign results retained",
        "software": {name: data.sha_file(Path(__file__).with_name(name)) for name in
                     ("route3_fs_only.py", "route3_data.py", "route3_native.py")},
        "indexes": [], "query_records": 0, "context_setup_records": 0,
        "context_setup_total_ns": 0}
    def progress(phase, **extra):
        data.save(work / "progress.json", {"protocol": PROTOCOL, "phase": phase,
            "elapsed_seconds": time.time() - started, "query_records": report["query_records"],
            "process_memory": memory_sample(), **extra})
        data.save(work / "report.json", report)
    progress("generation")
    try:
        generation_started = time.perf_counter_ns()
        for ordinal in range(1, 33):
            mark = time.perf_counter_ns()
            manifest = data.prepare_round(raw, 1024, ordinal)
            append(work / "generation.jsonl", {"ordinal": ordinal, "round_ns": time.perf_counter_ns() - mark,
                "files": len(manifest["entries"]), "physical_bytes": manifest["physical_bytes"],
                "allocated_bytes": manifest["allocated_bytes"]})
            progress("generation", ordinal=ordinal, physical_bytes=manifest["physical_bytes"])
        report["manifest_sha256"] = data.sha_file(raw / "manifest.json")
        report["generation_complete_ns"] = time.perf_counter_ns() - generation_started
        report["generation_worker_ns"] = manifest["preparation_ns"]
        report["generation_process_memory"] = memory_sample()
        cases, expected = {}, {}
        for N in NS:
            for V in VERSIONS:
                entries = scoped_entries(manifest, N, V)
                catalog = catalogs / ("N%d-V%d.json" % (N, V))
                index = catalogs / ("N%d-V%d.sqlite" % (N, V))
                data.save(catalog, catalog_for(raw, entries))
                config = {"catalog_path": str(catalog), "index_path": str(index)}
                mark = time.perf_counter_ns()
                with NativeContext(library, {**config, "build_index": True}) as context:
                    stats = context.stats()
                setup = {"N": N, "V": V, "catalog": str(catalog), "catalog_sha256": data.sha_file(catalog),
                    "index": str(index), "index_sha256": data.sha_file(index),
                    "setup_client_ns": time.perf_counter_ns() - mark, "stats": stats,
                    "query_plans": index_plans(index, V), "process_memory": memory_sample()}
                append(work / "indexsetup.jsonl", setup)
                report["indexes"].append(setup)
                cases[N, V] = config
                subset = {**manifest, "entries": entries}
                for label, _, ordinals in selections(V):
                    for roi, origin in enumerate(data.ORIGINS):
                        expected[N, V, label, roi] = data.expected(subset, ordinals, origin)
                progress("index_setup", N=N, V=V)
        rows = []
        for campaign in range(CAMPAIGNS):
            for N, V in case_order(campaign):
                arms = arm_order(campaign, N, V)
                groups = [(label, selection, ordinals, roi, origin)
                          for label, selection, ordinals in selections(V)
                          for roi, origin in enumerate(data.ORIGINS)]
                shift = campaign % len(groups)
                groups = groups[shift:] + groups[:shift]
                for arm in arms:
                    context_started = time.perf_counter_ns()
                    with NativeContext(library, cases[N, V]) as context:
                        context_ns = time.perf_counter_ns() - context_started
                        append(work / "contextsetup.jsonl", {"campaign": campaign, "N": N, "V": V,
                            "arm": arm, "context_open_ns": context_ns,
                            "initial_stats": context.stats(), "process_memory": memory_sample()})
                        report["context_setup_records"] += 1
                        report["context_setup_total_ns"] += context_ns
                        for label, selection, ordinals, roi, origin in groups:
                            for repeat in range(REPEATS):
                                row = query(context, arm, selection, origin, expected[N, V, label, roi])
                                require(row["selected_sources"] == N * len(ordinals), "source pruning cardinality differs")
                                require(row["selected_units"] == 2 * len(ordinals), "K2 result cardinality differs")
                                if arm == "FS-scan":
                                    require(row["candidate_datasets"] == N * len(ordinals) * 4 and
                                            row["stats"]["scan_source_visits"] == N * len(ordinals),
                                            "scan did not inspect exactly the caller-selected history")
                                row.update(campaign=campaign, N=N, V=V, arm=arm, selection=label,
                                           roi=roi, origin=list(origin), repeat=repeat, ordinals=ordinals)
                                rows.append(row)
                                append(work / "queries.jsonl", row)
                                report["query_records"] += 1
                            with (work / "queries.jsonl").open("rb") as committed:
                                os.fsync(committed.fileno())
                            progress("queries", campaign=campaign, N=N, V=V, arm=arm, selection=label, roi=roi)
        require(len(rows) == 1400, "FS-only query matrix incomplete")
        for setup in report["indexes"]:
            require(data.sha_file(setup["index"]) == setup["index_sha256"] and
                    data.sha_file(setup["catalog"]) == setup["catalog_sha256"], "native catalog/index changed")
        report["indexes_unchanged"] = True
        progress("source_audit")
        audit = audit_raw(raw, manifest)
        data.save(work / "source-audit.json", audit)
        require(data.sha_file(raw / "manifest.json") == report["manifest_sha256"], "raw manifest changed")
        analysis = summarize(rows)
        data.save(work / "analysis.json", analysis)
        report.update(complete=True, elapsed_seconds=time.time() - started,
                      source_audit=audit, analysis_sha256=data.sha_file(work / "analysis.json"),
                      final_process_memory=memory_sample())
        progress("complete")
        return report
    except BaseException as exc:
        report["failure"] = {"type": type(exc).__name__, "message": str(exc)}
        progress("failed")
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-library", type=Path, required=True)
    parser.add_argument("--native-sha", required=True)
    args = parser.parse_args()
    print(json.dumps(run(args), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
