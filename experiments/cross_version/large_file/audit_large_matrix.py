#!/usr/bin/env python3
"""Independent standard-library audit of raw observations and payload graphs.

Does not import the runner, connect to a database, or infer database disk size.
IoTDB numeric hashes were checked during execution; arrays are not retained.
"""
import argparse
from collections import defaultdict
import hashlib
import itertools
import json
import math
from pathlib import Path
import statistics

BLOCK = 1024 * 1024
FORMATS = ("hdf5", "grib")
SCENARIOS = ("identical", "single_unit")
SYSTEMS = ("V04", "V03", "FS-scan", "FS-index")
KEYS = ("scenario", "V", "format", "selection", "system")


def require(value, message):
    if not value:
        raise ValueError(message)


def read(path):
    return json.loads(Path(path).read_text())


def sha(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for value in iter(lambda: stream.read(BLOCK), b""):
            result.update(value)
    return result.hexdigest()


def raw_blocks(path):
    result, offset = [], 0
    with path.open("rb") as stream:
        for value in iter(lambda: stream.read(BLOCK), b""):
            digest = hashlib.sha256(b"iotdbfs-content-id/1\0")
            for text in (b"payload-block", b"iotdbfs-payload-block/1"):
                digest.update(len(text).to_bytes(4, "big"))
                digest.update(text)
            digest.update(len(value).to_bytes(8, "big"))
            digest.update(value)
            result.append({"logical_offset": offset, "length": len(value), "content_offset": 0,
                "content_size": len(value), "content_id": digest.hexdigest()})
            offset += len(value)
    return result


def audit(stage):
    global FORMATS
    stage = stage.resolve()
    command = read(stage / "command.json")
    option = lambda name: Path(command[command.index("--" + name) + 1])
    report = read(option("report"))
    FORMATS = tuple(report["spec"]["formats"])
    require(FORMATS in (("hdf5", "grib"), ("grib",)), "unsupported audit format matrix")
    format_count = len(FORMATS)
    require(report.get("complete") and report.get("original_service_restored"), "run/restoration incomplete")
    require(read(stage / "execution-status.json")["exit_code"] == 0, "controller failed")
    binding = read(option("source-binding"))
    require(sha(option("source-binding")) == report["source_binding_sha256"], "source binding changed")
    for name, digest in binding["source"].items():
        require(not Path(name).is_absolute() and ".." not in Path(name).parts and
                sha(stage / "source" / name) == digest, "source changed")
    require(sha(option("native")) == report["native_sha256"] == binding["native_sha256"], "native changed")
    require(sha(option("spec")) == report["spec_sha256"] and sha(command[2]) == report["controller_sha256"], "controller/spec changed")
    for system in ("V03", "V04"):
        require(sha(option("daemon-" + system.lower())) == report["binaries"][system], "daemon changed")
    manifest = read(option("input") / "manifest.json")
    body = {k: v for k, v in manifest.items() if k != "manifest_sha256"}
    digest = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()).hexdigest()
    require(digest == manifest["manifest_sha256"] == report["corpus_sha256"], "corpus changed")
    entries = {(e["scenario"], e["format"], e["file_index"], e["ordinal"]): e for e in manifest["entries"]}
    require(len(entries) == len(manifest["entries"]) == 128 * format_count and set(entries) ==
        set(itertools.product(SCENARIOS, FORMATS, range(8), range(1, 9))), "corpus version matrix incomplete")
    raw = {}
    for entry in entries.values():
        blob = entry["blob"]
        root = Path(manifest["bases"][entry["format"]]["root"]) if blob["kind"] == "base" else option("input")
        path = root / blob["file"]
        require(path.resolve().is_relative_to(root.resolve()) and not path.is_symlink(), "unsafe raw locator")
        if path not in raw:
            raw[path] = {"sha256": sha(path), "size": path.stat().st_size, "extents": raw_blocks(path)}
        require(raw[path]["sha256"] == entry["sha256"] and raw[path]["size"] == entry["bytes"], "raw bytes changed")
        entry["_oracle"] = raw[path]
    rows = [json.loads(line) for line in (option("work") / "queries.jsonl").read_text().splitlines()]
    require(len(rows) == report["query_records"] == 640 * format_count and sha(option("work") / "queries.jsonl") == report["queries_sha256"], "query file incomplete/changed")
    cells = defaultdict(list)
    for row in rows:
        require(row["oracle_verified"] is True and row["total_ns"] > 0, "unverified/invalid observation")
        ordinals = range(1, row["V"] + 1) if row["selection"] == "all" else (row["V"],)
        expected = {}
        for entry in entries.values():
            if (entry["scenario"], entry["format"]) != (row["scenario"], row["format"]) or entry["ordinal"] not in ordinals:
                continue
            for unit in entry["units"]:
                if all(unit["attributes"].get(k) == v for k, v in manifest["controls"]["query"].items()):
                    expected[(entry["logical_file"], entry["ordinal"], unit["dataset"] if row["format"] == "hdf5" else unit["offset"])] = unit["oracle"]
        require(len(expected) == row["selected_units"] == 2 * len(ordinals), "selected version count differs")
        if row["system"] in ("V03", "V04"):
            require(row["selected_owned_rows"] == sum(u["elements"] for u in expected.values()) and
                row["discovery_requests"] == 1 and row["value_requests"] == (len(expected) if row["format"] == "hdf5" else 1), "IoTDB request/output count differs")
        else:
            observed = {(u["logical_file"], u["ordinal"], u["dataset"] if row["format"] == "hdf5" else u["offset"]): u for u in row["units"]}
            require(len(observed) == len(row["units"]) and set(observed) == set(expected), "native identity set differs")
            for key, oracle in expected.items():
                unit = observed[key]
                mask = oracle.get("mask_sha256", hashlib.sha256(b"\1" * oracle["elements"]).hexdigest())
                require(unit["rows"] == oracle["elements"] and unit["value_sha256"] == oracle["value_sha256"] and
                    unit["mask_sha256"] == mask and unit["path"] == "v%03d/%s" % (key[1], key[0]), "native numeric oracle differs")
        cells[tuple(row[k] for k in KEYS)].append(row)
    expected_cells = set(itertools.product(SCENARIOS, (1, 8), FORMATS, ("latest", "all"), SYSTEMS))
    require(set(cells) == expected_cells, "cell matrix differs")
    analysis = read(option("work") / "analysis.json")
    require(sha(option("work") / "analysis.json") == report["analysis_sha256"], "analysis changed")
    stored = {tuple(c[k] for k in KEYS): c for c in analysis["cells"]}
    verified = {}
    for key, group in cells.items():
        require(len(group) == 20 and {(r["campaign"], r["repeat"]) for r in group} ==
            set(itertools.product(range(4), range(5))), "cell replication differs")
        require(sorted({r["arm_position"] for r in group if r["repeat"] == 1}) == [0, 1, 2, 3], "arm positions unbalanced")
        warm = [r for r in group if r["repeat"] > 0]
        result = {"warm_p50_ms": statistics.median(r["total_ns"] for r in warm) / 1e6,
            "campaign_warm_p50_ms": [statistics.median(r["total_ns"] for r in warm if r["campaign"] == c) / 1e6 for c in range(4)],
            "first_by_campaign_ms": [next(r["total_ns"] for r in group if r["campaign"] == c and r["repeat"] == 0) / 1e6 for c in range(4)]}
        require(all(stored[key][k] == v for k, v in result.items()), "cell statistics differ")
        require(stored[key]["warm_observations"] == 16 and stored[key]["independent_campaigns"] == 4,
                "replication labels differ")
        verified[key] = dict(zip(KEYS, key), **result)
    comparisons = []
    for comparison in analysis["comparisons"]:
        scenario, fmt, selection, control = [comparison[k] for k in ("scenario", "format", "selection", "control")]
        selected = [verified[(scenario, v, fmt, selection, s)] for v, s in ((1, "V04"), (8, "V04"), (1, control), (8, control))]
        t1, t8, f1, f8 = [r["warm_p50_ms"] for r in selected]
        expected = {"V04_total_ms": t8, "control_total_ms": f8, "V04_growth": t8 / t1, "control_growth": f8 / f1,
            "V04_increment_ms": t8 - t1, "control_increment_ms": f8 - f1, "speedup": f8 / t8,
            "relative_gain": (f8 / t8) / (f1 / t1)}
        require(all(math.isclose(comparison[k], v, rel_tol=1e-12, abs_tol=1e-12) for k, v in expected.items()), "comparison differs")
        passes = []
        require(len(comparison["campaigns"]) == 4, "comparison campaigns incomplete")
        for c in range(4):
            x1, x8, y1, y8 = [r["campaign_warm_p50_ms"][c] for r in selected]
            passed = x8 < y8 and x8 - x1 < y8 - y1 and (y8 / x8) / (y1 / x1) > 1
            campaign = comparison["campaigns"][c]
            require(campaign["campaign"] == c and campaign["joint_pass"] == passed, "campaign decision differs")
            measures = {"V04_total_ms": x8, "control_total_ms": y8, "V04_increment_ms": x8 - x1,
                "control_increment_ms": y8 - y1, "speedup": y8 / x8, "relative_gain": (y8 / x8) / (y1 / x1)}
            require(all(math.isclose(campaign[k], v, rel_tol=1e-12, abs_tol=1e-12) for k, v in measures.items()),
                "campaign statistics differ")
            passes.append(passed)
        require(comparison["all_campaigns_joint_pass"] == all(passes), "joint gate differs")
        comparisons.append(comparison)
    require(len(comparisons) == 12 * format_count and len({(c["scenario"], c["format"], c["selection"], c["control"]) for c in comparisons}) == 12 * format_count, "comparison matrix differs")
    namespaces = report["namespaces"]
    require(len(namespaces) == 8 and len({n["prefix"] for n in namespaces}) == 8 and
        len({(n["head"]["namespace"], n["head"]["incarnation"]) for n in namespaces}) == 8 and
        {(n["system"], n["scenario"], n["V"]) for n in namespaces} ==
        set(itertools.product(("V03", "V04"), SCENARIOS, (1, 8))), "namespace isolation differs")
    storage = []
    for namespace in namespaces:
        require(namespace["prepared"] and namespace["recovery_full_file_hashes"] == len(namespace["bindings"]) == 8 * format_count * namespace["V"] and
            namespace["recovery_scientific_units"] == 32 * format_count * namespace["V"] and len(namespace["query_mounts"]) == 4, "namespace recovery audit incomplete")
        ledger = namespace["objects"]
        require(sum(t["rows"] for t in ledger["by_type"].values()) == ledger["total_rows"] and
            sum(t["value_bytes"] for t in ledger["by_type"].values()) == ledger["total_value_bytes"], "object ledger differs")
        if namespace["system"] != "V04":
            continue
        graph = namespace["payload_inspection"]
        require(graph["completed"] and graph["head_unchanged"] and graph["staging_payload_rows"] == 0 and
            graph["prefix"] == namespace["prefix"] and
            (graph["namespace"], graph["incarnation"], graph["head_sequence"]) ==
            (namespace["head"]["namespace"], namespace["head"]["incarnation"], namespace["head"]["seq"]), "payload graph incomplete")
        files = {(f["path"], f["version"]): f for f in graph["files"]}
        require(len(files) == len(graph["files"]), "duplicate graph file")
        bound, all_blocks, seals, root_by_sha, sha_by_root = set(), {}, set(), {}, {}
        for fmt in FORMATS:
            unique, logical = {}, 0
            for b in namespace["bindings"]:
                if b["format"] != fmt:
                    continue
                entry = entries[namespace["scenario"], fmt, b["file_index"], b["ordinal"]]
                require(b["logical_file"] == entry["logical_file"] and b["sha256"] == entry["sha256"], "binding differs")
                identity = b["logical_path"], b["version"]
                require(identity not in bound, "duplicate scientific binding")
                bound.add(identity)
                actual, oracle = files[identity], entry["_oracle"]
                require(all(actual[k] == oracle[k] for k in ("sha256", "size", "extents")), "persisted payload graph differs from raw blocks")
                require(actual["seal_id"] not in seals, "scientific versions share a seal")
                seals.add(actual["seal_id"])
                require(root_by_sha.setdefault(actual["sha256"], actual["payload_root"]) == actual["payload_root"] and
                    sha_by_root.setdefault(actual["payload_root"], actual["sha256"]) == actual["sha256"], "payload roots do not follow content identity")
                logical += actual["size"]
                for extent in actual["extents"]:
                    unique[extent["content_id"]] = extent["content_size"]
            all_blocks.update(unique)
            storage.append({"scenario": namespace["scenario"], "V": namespace["V"], "format": fmt,
                "logical_bytes": logical, "unique_referenced_payload_bytes": sum(unique.values()),
                "payload_saving_fraction": 1 - sum(unique.values()) / logical})
        extras = [f for k, f in files.items() if k not in bound]
        require(all(f["size"] == 0 and f["extents"] == [] for f in extras), "unbound nonempty file")
        require(sum(all_blocks.values()) == graph["unique_payload_block_bytes"] and len(all_blocks) == graph["unique_payload_blocks"], "graph block aggregate differs")
        require(sum(f["size"] for f in files.values()) == graph["logical_payload_bytes"] and
            len({f["payload_root"] for f in files.values()}) == graph["unique_payload_maps"], "graph totals differ")
        storage.append({"scenario": namespace["scenario"], "V": namespace["V"], "format": "combined",
            "all_objects_blob_bytes": ledger["total_value_bytes"], "by_type": ledger["by_type"],
            "unique_referenced_payload_bytes": sum(all_blocks.values()), "empty_published_versions": len(extras),
            "scope": "BLOB value lengths, not physical database disk size; V03 independent payload not included in its object ledger"})
    inodes, copies = set(), 0
    for fs in report["fs"]:
        require(sha(fs["index"]) == fs["index_sha256"], "SQLite index changed")
        for f in fs["files"]:
            path, stat = Path(f["path"]), Path(f["path"]).stat()
            inode = stat.st_dev, stat.st_ino
            require(inode not in inodes and stat.st_nlink == 1 and not path.is_symlink() and
                sha(path) == f["sha256"] and stat.st_size == f["bytes"], "FS copy differs/aliases history")
            inodes.add(inode)
            copies += 1
    require(copies == 144 * format_count, "FS copy count differs")
    operations = [json.loads(line) for line in (option("work") / "control/operations.jsonl").read_text().splitlines()]
    require([o["signal"] for o in operations if o["event"] == "signal_datanode"] == ["SIGTERM", "SIGTERM"] and
        operations[-1]["event"] == "original_service_restored" and operations[-1]["pid"] == report["final_datanode_pid"], "service control history differs")
    return {"complete": True, "query_records": len(rows), "cells_verified": len(cells), "comparisons_verified": len(comparisons),
        "scientific_bindings_verified": sum(len(n["bindings"]) for n in namespaces), "fs_copies_verified": copies,
        "cells": list(verified.values()), "comparisons": comparisons, "storage": storage,
        "inputs": {"report_sha256": sha(option("report")), "queries_sha256": report["queries_sha256"], "audit_script_sha256": sha(__file__)},
        "limits": "selected favorable workload; IoTDB numeric hashes checked at execution, arrays not retained; physical database disk compression unmeasured"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", type=Path)
    args = parser.parse_args()
    result = audit(args.stage)
    target = args.stage / "audit.json"
    with target.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps({k: result[k] for k in ("complete", "query_records", "cells_verified", "comparisons_verified", "scientific_bindings_verified", "fs_copies_verified")}))
