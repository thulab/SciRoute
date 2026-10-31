"""Pure controller checks: no format libraries, database, subprocess or mount."""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import itertools
import json
from pathlib import Path
import struct
import sys
import types
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("n8_matrix_under_test", HERE / "n8_matrix.py")
matrix = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(matrix)


def digest(value):
    return hashlib.sha256(value).hexdigest()


def fixture():
    entries = []
    for scenario, fmt, index in itertools.product(matrix.SCENARIOS, matrix.FORMATS, range(8)):
        values = [float(index * 10 + unit) for unit in range(4)]
        for ordinal in range(1, 9):
            if scenario == "single_unit" and ordinal > 1:
                values[(ordinal - 2) % 4] += ordinal
            units = []
            for unit, value in enumerate(values):
                encoded = struct.pack("<ff", value, value + 0.5)
                units.append({"dataset": "/T%d" % unit, "offset": unit * 100,
                    "attributes": {"match": index < 2 and unit == 0},
                    "oracle": {"elements": 2, "value_sha256": digest(encoded)},
                    "fixture_values": encoded})
            entries.append({"scenario": scenario, "format": fmt, "ordinal": ordinal,
                "file_index": index, "logical_file": "raw/d0/file_%d.%s" % (index, fmt),
                "units": units})
    return {"controls": {"query": {"match": True}}, "entries": entries}


def namespace(manifest, scenario="identical", V=8):
    bindings = []
    for e in manifest["entries"]:
        if e["scenario"] == scenario and e["ordinal"] <= V:
            bindings.append({k: e[k] for k in ("format", "ordinal", "logical_file", "file_index")} | {
                "logical_path": "/matrix/" + e["format"] + "/" + e["logical_file"],
                # Each path's ordinal 1 has a different actual row_time.
                "version": 10000 + 1000 * matrix.FORMATS.index(e["format"]) +
                    100 * e["file_index"] + 7 * e["ordinal"]})
    return {"scenario": scenario, "V": V, "logical_prefix": "/matrix",
            "bindings": list(reversed(bindings))}


class Array:
    def __init__(self, data, width):
        self.data, self.width = data, width

    def __len__(self):
        return len(self.data) // self.width

    def tobytes(self):
        return self.data


def concatenate(parts):
    return Array(b"".join(p.data for p in parts), parts[0].width)


class Paired:
    """Fake socket/IPC transport; requests still choose real exact bindings."""
    def __init__(self, manifest, ns, fault=None):
        self.manifest, self.ns, self.fault = manifest, ns, fault
        self.requests = []
        self.entries = {(e["format"], e["logical_file"], e["ordinal"]): e
                        for e in manifest["entries"] if e["scenario"] == ns["scenario"]}

    @staticmethod
    def selector(prefix, fmt, version=None, exact=None):
        return {"paths": [{"kind": "exact" if exact else "subtree", "path": exact or prefix}],
            "versions": {"kind": "latest"} if version is None else {"kind": "exact", "exact": version},
            "formats": {"adapters": "listed", "adapter_ids": [fmt], "include_byte_passthrough": False}}

    def discovery_request(self, prefix, template, predicate=True):
        return {"kind": "discovery", "request": {"sources": self.selector(prefix, template["format"]),
                "predicate": {"frozen": predicate}}}

    @staticmethod
    def discovered(response, batches, fmt, prefix):
        return response["rows"]

    def profile_call(self, context, request, offset, values=False):
        self.requests.append((copy.deepcopy(request), values))
        sources = request["request"]["sources"]
        fmt = sources["formats"]["adapter_ids"][0]
        candidates = [b for b in self.ns["bindings"] if b["format"] == fmt and
            any(b["logical_path"] == p["path"] if p["kind"] == "exact" else
                b["logical_path"].startswith(p["path"] + "/") for p in sources["paths"])]
        version = sources["versions"]
        if version["kind"] == "exact":
            candidates = [b for b in candidates if b["version"] == version["exact"]]
        elif version["kind"] == "latest":
            candidates = [b for b in candidates if b["ordinal"] == self.ns["V"]]
        else:
            assert version["kind"] == "all"
        if self.fault == "collapse_versions":
            candidates = [b for b in candidates if b["ordinal"] == 1]
        rows, batches = [], []
        for b in candidates:
            entry = self.entries[fmt, b["logical_file"], b["ordinal"]]
            for unit in entry["units"]:
                if not unit["attributes"]["match"]:
                    continue
                rows.append({"path": b["logical_path"], "version": b["version"],
                             "dataset": unit["dataset"], "offset": unit["offset"]})
                if values:
                    actual_version = b["version"] + (1 if self.fault == "provenance" else 0)
                    data = b"\0" * 8 if self.fault == "values" else unit["fixture_values"]
                    batches.append({"meta": {b"iotdbfs.sources": b"1",
                        b"iotdbfs.source.0.path": b["logical_path"].encode(),
                        b"iotdbfs.source.0.version": str(actual_version).encode()},
                        "array": Array(data, 4), "mask": Array(b"\1\1", 1)})
        return {"rows": rows}, batches, {"instrumentation_ns": 10}, offset + 1


def records():
    result = []
    for scenario, V, fmt, selection, system, campaign, repeat in itertools.product(
            matrix.SCENARIOS, matrix.VERSIONS, matrix.FORMATS, matrix.SELECTIONS,
            matrix.SYSTEMS, range(3), range(5)):
        base, increment = (10, 1) if system == "V04" else (20, 4)
        ns = round((base + increment * (V - 1)) * (1 + campaign / 4) * 1e6)
        result.append(dict(scenario=scenario, V=V, format=fmt, selection=selection,
            system=system, campaign=campaign, repeat=repeat,
            total_ns=ns if repeat else 9999999999))
    return result


class MatrixTests(unittest.TestCase):
    def setUp(self):
        self.manifest = fixture()

    def test_spec_rejects_changed_matrix_or_clock_replication(self):
        spec = json.loads((HERE / "specs/n8-version-matrix.json").read_text())
        self.assertIs(matrix.validate_spec(spec), spec)
        for field, value in [("N", 9), ("version_counts", [1, 8]), ("exact_ordinal", 2),
                             ("campaigns", 1), ("repeats", 6), ("exclude_repeat", 1)]:
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                matrix.validate_spec(spec | {field: value})

    def test_import_does_not_load_native_or_database_dependencies(self):
        import builtins
        original = builtins.__import__
        def guarded(name, *args, **kwargs):
            if name.split(".")[0] in {"numpy", "h5py", "pyarrow", "corpus", "publish", "n8_matrix_prepare"}:
                raise AssertionError("heavy import: " + name)
            return original(name, *args, **kwargs)
        spec = importlib.util.spec_from_file_location("n8_matrix_import_check", HERE / "n8_matrix.py")
        with patch("builtins.__import__", guarded):
            spec.loader.exec_module(importlib.util.module_from_spec(spec))

    def test_oracle_keeps_each_scientific_ordinal_even_for_identical_bytes(self):
        for scenario, fmt, V, selection in itertools.product(
                matrix.SCENARIOS, matrix.FORMATS, matrix.VERSIONS, matrix.SELECTIONS):
            with self.subTest(scenario=scenario, fmt=fmt, V=V, selection=selection):
                ords = matrix.ordinals(V, selection)
                self.assertEqual(len(matrix.selected_entries(self.manifest, scenario, fmt, V, selection)), 8 * len(ords))
                expected = matrix.oracle_for(self.manifest, scenario, fmt, V, selection)
                self.assertEqual(len(expected), 2 * len(ords))
                self.assertEqual({key[1] for key in expected}, set(ords))
        expected = matrix.oracle_for(self.manifest, "identical", "hdf5", 8, "all")
        self.assertEqual(len(expected), 16)
        self.assertEqual(len({o["value_sha256"] for o in expected.values()}), 2)

    def test_missing_corpus_and_duplicate_matching_identity_are_rejected(self):
        malformed = copy.deepcopy(self.manifest)
        malformed["entries"].pop()
        with self.assertRaises(RuntimeError):
            matrix.oracle_for(malformed, "single_unit", "grib", 8, "all")
        duplicate = copy.deepcopy(self.manifest)
        first = duplicate["entries"][0]
        first["units"][1] = copy.deepcopy(first["units"][0])
        with self.assertRaises(RuntimeError):
            matrix.oracle_for(duplicate, "identical", "hdf5", 1, "latest")

    def test_exact_uses_eight_path_specific_ordinal_one_row_times(self):
        ns = namespace(self.manifest)
        paired = Paired(self.manifest, ns)
        for fmt in matrix.FORMATS:
            requests = matrix.discovery_requests(paired, {"format": fmt}, ns, fmt, "exact")
            self.assertEqual(len(requests), 8)
            bindings = {b["logical_path"]: b for b in ns["bindings"]
                        if b["format"] == fmt and b["ordinal"] == 1}
            actual = {}
            for request in requests:
                source = request["request"]["sources"]
                self.assertEqual(source["paths"][0]["kind"], "exact")
                self.assertFalse(source["formats"]["include_byte_passthrough"])
                path = source["paths"][0]["path"]
                self.assertEqual(source["versions"], {"kind": "exact", "exact": bindings[path]["version"]})
                actual[path] = source["versions"]["exact"]
            self.assertEqual(set(actual), set(bindings))
            self.assertEqual(len(set(actual.values())), 8)

    def test_latest_and_all_use_one_format_filtered_subtree_discovery(self):
        ns = namespace(self.manifest)
        paired = Paired(self.manifest, ns)
        for fmt, selection in itertools.product(matrix.FORMATS, ("latest", "all")):
            requests = matrix.discovery_requests(paired, {"format": fmt}, ns, fmt, selection)
            self.assertEqual(len(requests), 1)
            source = requests[0]["request"]["sources"]
            self.assertEqual(source["versions"], {"kind": selection})
            self.assertEqual(source["paths"], [{"kind": "subtree", "path": "/matrix/" + fmt}])
            self.assertEqual(source["formats"]["adapter_ids"], [fmt])

    def query(self, fmt, selection, V=8, fault=None):
        ns = namespace(self.manifest, "single_unit", V)
        paired = Paired(self.manifest, ns, fault)
        # This test intentionally never imports NumPy or any format library.
        fake_numpy = types.SimpleNamespace(concatenate=concatenate)
        with patch.dict(sys.modules, {"numpy": fake_numpy}), patch.object(
                matrix.time, "perf_counter_ns", side_effect=[1000, 2000, 3000]):
            value, offset = matrix.iotdb_query(paired, {}, ns, self.manifest,
                {"format": fmt, "query_template": {"predicate": {"match": True}}}, fmt, selection, 17)
        return paired, value, offset

    def test_complete_queries_have_exact_rpc_counts_owned_rows_and_excluded_profile_waits(self):
        for fmt, selection in itertools.product(matrix.FORMATS, matrix.SELECTIONS):
            with self.subTest(fmt=fmt, selection=selection):
                paired, row, offset = self.query(fmt, selection)
                discoveries = 8 if selection == "exact" else 1
                units = 16 if selection == "all" else 2
                reads = units if fmt == "hdf5" else (2 if selection == "exact" else 1)
                self.assertTrue(row["oracle_verified"])
                self.assertEqual((row["discovery_requests"], row["value_requests"]), (discoveries, reads))
                self.assertEqual(row["selected_units"], units)
                self.assertEqual(row["selected_owned_rows"], 2 * units)
                self.assertEqual(offset, 17 + discoveries + reads)
                self.assertEqual(row["total_ns"], 2000 - 10 * (discoveries + reads))
                self.assertEqual(row["discovery_client_ns"], 1000 - 10 * discoveries)
                self.assertEqual(row["selected_value_client_ns"], 1000 - 10 * reads)
                if fmt == "grib" and selection == "all":
                    sources = paired.requests[-1][0]["request"]["sources"]
                    self.assertEqual(sources["versions"], {"kind": "all"})
                    self.assertEqual(len(sources["paths"]), 2)

    def test_all_query_rejects_collapsed_history_corrupt_values_or_wrong_provenance(self):
        for fmt, fault in itertools.product(matrix.FORMATS, ("collapse_versions", "values", "provenance")):
            with self.subTest(fmt=fmt, fault=fault), self.assertRaises(RuntimeError):
                self.query(fmt, "all", fault=fault)

    def test_native_checks_version_multiset_mask_hash_and_selected_scan_size(self):
        expected = matrix.oracle_for(self.manifest, "identical", "grib", 8, "all")
        units = [dict(logical_file=k[0], ordinal=k[1], offset=k[2], path="v%03d/%s" % (k[1], k[0]),
            rows=o["elements"], value_sha256=o["value_sha256"], mask_sha256=digest(b"\1\1"))
            for k, o in expected.items()]
        row = {"units": units, "selected_units": 16, "enumerated_files": 64, "candidate_units": 256}
        matrix.check_native(row, expected, "grib", 8, "all", "FS-scan")
        for corrupt in ("duplicate", "path", "mask", "scan_size"):
            bad = copy.deepcopy(row)
            if corrupt == "duplicate": bad["units"][-1] = copy.deepcopy(bad["units"][0])
            if corrupt == "path": bad["units"][0]["path"] = "v001/wrong.grib"
            if corrupt == "mask": bad["units"][0]["mask_sha256"] = digest(b"\0\0")
            if corrupt == "scan_size": bad["enumerated_files"] = 8
            with self.subTest(corrupt=corrupt), self.assertRaises(RuntimeError):
                matrix.check_native(bad, expected, "grib", 8, "all", "FS-scan")

    def test_summary_requires_192_cells_2880_queries_and_12_warm_samples(self):
        rows = records()
        summary = matrix.summarize(rows)
        self.assertEqual(len(summary["cells"]), 192)
        self.assertEqual(len(summary["comparisons"]), 108)
        for cell in summary["cells"]:
            self.assertEqual((cell["observations"], cell["warm_observations"]), (15, 12))
            self.assertEqual(len(cell["campaign_warm_p50_ms"]), 3)
            self.assertEqual(cell["first_by_campaign_ms"], [9999.999999] * 3)
            self.assertLess(cell["warm_max_ms"], 1000)
        for bad in (rows[:-1], rows + [rows[0]], [rows[1], *rows[1:]]):
            with self.assertRaises(RuntimeError):
                matrix.summarize(bad)

    def test_summary_delta_growth_speedup_and_gate_use_campaign_totals(self):
        rows = records()
        def target(summary):
            return next(r for r in summary["comparisons"] if (r["scenario"], r["format"],
                r["selection"], r["V"], r["control"]) == ("identical", "hdf5", "all", 2, "FS-index"))
        result = target(matrix.summarize(rows))
        self.assertAlmostEqual(result["V04_increment_ms"], 1.25)
        self.assertAlmostEqual(result["control_increment_ms"], 5)
        self.assertAlmostEqual(result["V04_growth"], 1.1)
        self.assertAlmostEqual(result["control_growth"], 1.2)
        self.assertAlmostEqual(result["speedup_control_over_V04"], 24 / 11)
        self.assertAlmostEqual(result["relative_advantage_change"], 12 / 11)
        self.assertTrue(result["pilot_all_campaigns_faster_and_relative_gain"])
        # A favorable pooled median must not hide one campaign with no relative gain.
        for r in rows:
            if (r["scenario"], r["format"], r["selection"], r["V"], r["system"], r["campaign"]) == (
                    "identical", "hdf5", "all", 2, "V04", 2):
                r["total_ns"] = 20_000_000
        result = target(matrix.summarize(rows))
        self.assertLess(result["V04_total_ms"], result["control_total_ms"])
        self.assertTrue(result["campaigns"][2]["V04_faster"])
        self.assertLess(result["campaigns"][2]["relative_advantage_change"], 1)
        self.assertFalse(result["pilot_all_campaigns_faster_and_relative_gain"])

    def test_each_campaign_contains_all_eight_isolated_scenario_version_groups(self):
        wanted = set(itertools.product(matrix.SCENARIOS, matrix.VERSIONS))
        orders = [matrix.campaign_order(c) for c in range(3)]
        for order in orders:
            self.assertEqual(len(order), 8)
            self.assertEqual(set(order), wanted)
        self.assertEqual(len({tuple(order) for order in orders}), 3)


if __name__ == "__main__":
    unittest.main()
