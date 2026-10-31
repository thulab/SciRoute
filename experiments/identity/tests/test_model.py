"""Synthetic JSON identity tests; no scientific data or benchmark execution."""
import copy
import hashlib
import random
import unittest

from experiments.identity.model import (
    IdentityError, build_case, build_corpus, build_run, require_comparable,
    validate_result, verify,
)


def fingerprint(label):
    return hashlib.sha256(label.encode()).hexdigest()


def imported_population(n=3, versions=3, units=2, format_id="grib", sizes=None):
    """Every file/version is explicitly publication-bound; hashes are test labels."""
    namespace = "synthetic-json-only-N%d-V%d-%s" % (n, versions, format_id)
    records = []
    for file_id in range(n):
        logical = "bucket%04d/file%04d.%s" % (
            file_id // 32, file_id, "grib2" if format_id == "grib" else "h5")
        for ordinal in range(1, versions + 1):
            source_units = []
            for unit_id in range(units):
                unit = {"attributes": {"kind": "target" if file_id < 2 and
                        (file_id == 0 or unit_id == 0) else "other"},
                    "oracle": {"origin": [0, 0], "shape": [2, 2],
                        "elements": 4, "dtype": "float32",
                        "value_sha256": fingerprint("value-%d-%d-%d" % (file_id, ordinal, unit_id)),
                        "mask_sha256": fingerprint("mask-4-valid")}}
                if format_id == "grib":
                    unit.update(offset=unit_id * 8, length=8, schema={"Ni": 2, "Nj": 2})
                else:
                    unit.update(dataset="/dataset%d" % unit_id,
                                schema={"shape": [2, 2], "chunks": [1, 1]})
                source_units.append(unit)
            size = sizes[len(records)] if sizes is not None else max(32, units * 8)
            records.append({"logical_file": logical, "ordinal": ordinal,
                "bytes": size, "sha256": fingerprint("source-%d-%d-%d-%d" % (n, file_id, ordinal, size)),
                "binding": {"path": "/" + namespace + "/" + logical,
                    "version": 1700000000000 + file_id * 100 + ordinal * 7},
                "units": source_units})
    bindings = [{"logical_file": r["logical_file"], "ordinal": r["ordinal"],
        "bytes": r["bytes"], "sha256": r["sha256"], **r["binding"]} for r in records]
    geometry = {"Ni": 2, "Nj": 2} if format_id == "grib" else {"shape": [2, 2], "chunks": [1, 1]}
    return {"bound": True, "warnings": [], "format": format_id, "N_total": n,
        "generation": {"kind": "synthetic-json-unit-test-only", "seed": 7,
            "format": format_id, "N": n, "D": units, "rounds": versions,
            "payload": geometry}, "inventory": records,
        "source_evidence": {"manifest_sha256": fingerprint("manifest-" + namespace)},
        "publication_evidence": {"receipt_sha256": fingerprint("publication-" + namespace),
            "namespace": namespace, "bindings": bindings}}


def request_for(corpus, selection=None, expected=None, scope=None):
    paths = sorted({r["logical_file"] for r in corpus["inventory"]})
    return {"scope": scope or {"kind": "all", "logical_files": paths,
        "buckets": [], "selection_basis": "fixed prospectively by synthetic test"},
        "selection": selection or {"kind": "latest"},
        "predicate": {"kind": "target"}, "operation": "discover-values",
        "roi": {"kind": "whole", "origin": [0, 0], "shape": [2, 2]},
        "output": {"projection": ["value", "mask"], "dtype": "float32"},
        "expected_K": expected or {"K_files": 2, "K_file_versions": 2, "K_units": 3}}


def run_for(case, **changes):
    arguments = {"execution": "synthetic-execution-a", "arm": "IoTDB-FS",
        "implementation": {"binary_sha256": fingerprint("binary-a"),
            "runner_sha256": fingerprint("runner"), "engine": "indexed"},
        "clock": {"protocol": "owned-values/1", "unit": "ns"},
        "cache": {"protocol": "warm-resident/1", "warmups": 1},
        "environment": {"host": "synthetic-host", "configuration_sha256": fingerprint("configuration")},
        "replication": {"campaigns": 1, "measured_repeats": 5, "ordering": "interleaved"}}
    arguments.update(changes)
    return build_run(case, **arguments)


class CorpusIdentityTests(unittest.TestCase):
    def test_complete_population_drives_n_v_d_and_actual_size(self):
        corpus = build_corpus(imported_population(sizes=[32, 40, 48] * 3))
        p = corpus["parameters"]
        self.assertEqual((p["N"], p["V_stored"], p["D"]), (3, 3, 2))
        self.assertEqual(p["S"], {"unit": "encoded_bytes_per_file_version", "count": 9,
            "min": 32, "max": 48, "constant": None, "total_logical_version_bytes": 360,
            "histogram": [[32, 3], [40, 3], [48, 3]]})
        self.assertEqual(verify(corpus, "corpus"), corpus["corpus_id"])

    def test_constant_and_varying_s_have_distinct_identity(self):
        constant = build_corpus(imported_population())
        varying = build_corpus(imported_population(sizes=[33] * 9))
        self.assertEqual(constant["parameters"]["S"]["constant"], 32)
        self.assertEqual(varying["parameters"]["S"]["constant"], 33)
        self.assertNotEqual(constant["corpus_id"], varying["corpus_id"])

    def test_inventory_order_is_canonical_and_inputs_are_not_mutated(self):
        original = imported_population()
        before = copy.deepcopy(original)
        shuffled = copy.deepcopy(original)
        random.Random(71).shuffle(shuffled["inventory"])
        self.assertEqual(build_corpus(original), build_corpus(shuffled))
        self.assertEqual(original, before)

    def test_n_cannot_relabel_an_existing_complete_population(self):
        imported = imported_population(n=512, versions=1)
        imported["N_total"] = 128
        with self.assertRaises(IdentityError):
            build_corpus(imported)

    def test_missing_or_incomplete_publication_cannot_be_bound(self):
        for change in ("missing", "unbound", "partial"):
            with self.subTest(change=change):
                imported = imported_population()
                if change == "missing": imported.pop("publication_evidence")
                if change == "unbound": imported["bound"] = False
                if change == "partial": imported["publication_evidence"]["bindings"].pop()
                with self.assertRaises(IdentityError): build_corpus(imported)

    def test_benign_import_evidence_notes_are_preserved(self):
        imported = imported_population()
        imported["warnings"] = ["raw source hashes were not freshly reread"]
        corpus = build_corpus(imported)
        self.assertEqual(corpus["evidence_notes"], imported["warnings"])
        imported["warnings"].append("later caller mutation")
        self.assertEqual(corpus["evidence_notes"], ["raw source hashes were not freshly reread"])

    def test_publication_inventory_hash_size_and_exact_binding_must_agree(self):
        for field, value in (("sha256", fingerprint("different")), ("bytes", 33),
                             ("path", "/different/file.grib2"), ("version", 5)):
            with self.subTest(field=field):
                imported = imported_population()
                imported["publication_evidence"]["bindings"][0][field] = value
                with self.assertRaises(IdentityError): build_corpus(imported)

    def test_duplicate_and_gapped_scientific_versions_fail(self):
        for change in ("duplicate", "gap", "shared_exact", "rebound"):
            with self.subTest(change=change):
                imported = imported_population()
                records = imported["inventory"]
                if change == "duplicate": records.append(copy.deepcopy(records[0]))
                if change == "gap": records[1]["ordinal"] = 4
                if change == "shared_exact": records[1]["binding"] = copy.deepcopy(records[0]["binding"])
                if change == "rebound": records[1]["binding"]["path"] = "/rebound/source.grib2"
                with self.assertRaises(IdentityError): build_corpus(imported)

    def test_database_latest_order_cannot_disagree_with_scientific_ordinals(self):
        imported = imported_population()
        new_version = imported["inventory"][2]["binding"]["version"] + 10
        imported["inventory"][0]["binding"]["version"] = new_version
        imported["publication_evidence"]["bindings"][0]["version"] = new_version
        with self.assertRaises(IdentityError): build_corpus(imported)

    def test_hdf5_and_grib_equal_counts_are_distinct_corpora_and_cases(self):
        grib = build_corpus(imported_population())
        hdf5 = build_corpus(imported_population(format_id="hdf5"))
        grib_case = build_case(grib, request_for(grib))
        hdf5_case = build_case(hdf5, request_for(hdf5))
        self.assertEqual(grib_case["parameters"]["expected_K"], hdf5_case["parameters"]["expected_K"])
        self.assertNotEqual(grib["corpus_id"], hdf5["corpus_id"])
        self.assertNotEqual(grib_case["case_id"], hdf5_case["case_id"])
        with self.assertRaises(IdentityError):
            require_comparable(run_for(grib_case), run_for(hdf5_case))

    def test_unit_locator_must_match_format_and_encoded_source_bounds(self):
        for change in ("grib-dataset", "hdf5-message", "range"):
            with self.subTest(change=change):
                imported = imported_population(format_id="hdf5" if change == "hdf5-message" else "grib")
                unit = imported["inventory"][2]["units"][0]
                if change == "grib-dataset": unit.pop("offset"); unit.pop("length"); unit["dataset"] = "/fake"
                if change == "hdf5-message": unit.pop("dataset"); unit.update(offset=0, length=8)
                if change == "range": unit.update(offset=31, length=8)
                with self.assertRaises(IdentityError): build_corpus(imported)

    def test_no_native_oracle_or_missing_hash_is_rejected(self):
        for change in ("missing", "value", "mask", "elements"):
            with self.subTest(change=change):
                imported = imported_population()
                unit = imported["inventory"][2]["units"][0]
                if change == "missing": unit.pop("oracle")
                else: unit["oracle"].pop({"value": "value_sha256", "mask": "mask_sha256",
                    "elements": "elements"}[change])
                with self.assertRaises(IdentityError):
                    corpus = build_corpus(imported)
                    build_case(corpus, request_for(corpus))


class CaseIdentityTests(unittest.TestCase):
    def setUp(self):
        self.corpus = build_corpus(imported_population())

    def test_version_modes_keep_separate_file_version_and_unit_cardinalities(self):
        modes = [({"kind": "latest"}, 2, 3, 3), ({"kind": "all"}, 6, 9, 9),
            ({"kind": "exact", "ordinals": [1]}, 2, 3, 3),
            ({"kind": "between", "ordinals": [1, 2]}, 4, 6, 6)]
        identities = set()
        for selection, version_hits, unit_hits, candidate_versions in modes:
            with self.subTest(selection=selection):
                expected = {"K_files": 2, "K_file_versions": version_hits, "K_units": unit_hits}
                case = build_case(self.corpus, request_for(self.corpus, selection, expected))
                self.assertEqual(case["parameters"]["expected_K"], expected)
                self.assertEqual(case["parameters"]["C_versions"], candidate_versions)
                self.assertEqual(len(case["expected_matches"]), unit_hits)
                identities.add(case["case_id"])
        self.assertEqual(len(identities), 4)

    def test_selection_rejects_hidden_filters_absent_and_noncontiguous_ordinals(self):
        invalid = [{"kind": "latest", "ordinals": [1]}, {"kind": "all", "ordinals": [1]},
            {"kind": "exact", "ordinals": [1, 2]}, {"kind": "exact", "ordinals": [4]},
            {"kind": "between", "ordinals": [1, 3]}, {"kind": "between", "ordinals": [2, 1]}]
        for selection in invalid:
            with self.subTest(selection=selection), self.assertRaises(IdentityError):
                build_case(self.corpus, request_for(self.corpus, selection))

    def test_ordinal_never_substitutes_for_actual_published_database_version(self):
        request = request_for(self.corpus, {"kind": "exact", "ordinals": [2]})
        case = build_case(self.corpus, request)
        for match in case["expected_matches"]:
            self.assertEqual(match["ordinal"], 2)
            original = next(r for r in self.corpus["inventory"] if
                r["logical_file"] == match["logical_file"] and r["ordinal"] == 2)
            self.assertEqual(match["binding"], original["binding"])
            self.assertGreater(match["binding"]["version"], 1700000000000)
            self.assertNotEqual(match["binding"]["version"], match["ordinal"])

    def test_full_n_remains_512_when_four_complete_buckets_select_128_paths(self):
        independent = build_corpus(imported_population(n=128, versions=1))
        backing = build_corpus(imported_population(n=512, versions=1))
        paths = sorted({r["logical_file"] for r in backing["inventory"]})[:128]
        scope = {"kind": "buckets", "logical_files": paths,
            "buckets": ["bucket%04d" % i for i in range(4)],
            "selection_basis": "fixed four complete buckets before execution"}
        small = build_case(independent, request_for(independent))
        subset = build_case(backing, request_for(backing, scope=scope))
        self.assertEqual(small["parameters"]["C_paths"], subset["parameters"]["C_paths"])
        self.assertEqual(small["parameters"]["expected_K"], subset["parameters"]["expected_K"])
        self.assertEqual(subset["parameters"]["N"], 512)
        self.assertEqual(subset["parameters"]["N_label"], "N512/B4/C128")
        self.assertEqual(small["parameters"]["N_label"], "N128")
        self.assertNotEqual(small["case_id"], subset["case_id"])
        with self.assertRaises(IdentityError): require_comparable(run_for(small), run_for(subset))

    def test_bucket_scope_must_contain_exact_complete_nonempty_buckets(self):
        corpus = build_corpus(imported_population(n=64, versions=1))
        first = sorted({r["logical_file"] for r in corpus["inventory"]})[:32]
        for change in ("partial", "empty-extra", "overlap", "hidden-all"):
            with self.subTest(change=change):
                scope = {"kind": "buckets", "logical_files": first,
                    "buckets": ["bucket0000"], "selection_basis": "fixed before execution"}
                if change == "partial": scope["logical_files"] = first[:-1]
                if change == "empty-extra": scope["buckets"].append("nonexistent")
                if change == "overlap": scope["buckets"].append("bucket0000/child")
                if change == "hidden-all": scope["kind"] = "all"; scope["buckets"] = []
                with self.assertRaises(IdentityError): build_case(corpus, request_for(corpus, scope=scope))

    def test_k_is_prospective_and_each_cardinality_is_enforced(self):
        for field in ("K_files", "K_file_versions", "K_units"):
            with self.subTest(field=field):
                request = request_for(self.corpus)
                request["expected_K"][field] += 1
                with self.assertRaises(IdentityError): build_case(self.corpus, request)
        request = request_for(self.corpus)
        case = build_case(self.corpus, request)
        request["expected_K"]["K_units"] = 999
        self.assertEqual(case["parameters"]["expected_K"]["K_units"], 3)
        with self.assertRaises(IdentityError): build_case(self.corpus, request)

    def test_phase_predicate_dtype_and_projection_change_identity_even_same_k(self):
        original = request_for(self.corpus)
        baseline = build_case(self.corpus, original)
        changes = []
        changed = copy.deepcopy(original); changed["operation"] = "discover"
        changed["output"] = {"projection": ["locators"], "dtype": "none"}; changes.append(changed)
        changed = copy.deepcopy(original); changed["predicate"] = {"kind": "other"}; changes.append(changed)
        changed = copy.deepcopy(original); changed["output"]["dtype"] = "float64"; changes.append(changed)
        changed = copy.deepcopy(original); changed["output"]["projection"] = ["mask", "value"]; changes.append(changed)
        ids = {baseline["case_id"]}
        for changed in changes: ids.add(build_case(self.corpus, changed)["case_id"])
        self.assertEqual(len(ids), 5)

    def test_discovery_locators_do_not_require_or_synthesize_value_oracles(self):
        imported = imported_population()
        for record in imported["inventory"]:
            for unit in record["units"]: unit.pop("oracle")
        corpus = build_corpus(imported)
        request = request_for(corpus)
        request["operation"] = "discover"
        request["output"] = {"projection": ["locators"], "dtype": "none"}
        case = build_case(corpus, request)
        self.assertEqual(len(case["expected_matches"]), 3)
        wrong = copy.deepcopy(request)
        wrong["output"] = {"projection": ["value", "mask"], "dtype": "float32"}
        with self.assertRaises(IdentityError): build_case(corpus, wrong)
        with self.assertRaises(IdentityError): build_case(corpus, request_for(corpus))

    def test_value_only_oracle_never_synthesizes_a_missing_mask(self):
        imported = imported_population()
        imported["inventory"][2]["units"][0]["oracle"].pop("mask_sha256")
        corpus = build_corpus(imported)
        request = request_for(corpus)
        request["output"]["projection"] = ["value"]
        case = build_case(corpus, request)
        self.assertNotIn("mask_sha256", case["expected_matches"][0]["native_oracle"])

    def test_expected_k_requires_exactly_three_integer_cardinalities(self):
        for change in ("extra", "missing", "boolean"):
            with self.subTest(change=change):
                request = request_for(self.corpus)
                if change == "extra": request["expected_K"]["K"] = 3
                if change == "missing": request["expected_K"].pop("K_units")
                if change == "boolean":
                    request["predicate"] = {"kind": "absent"}
                    request["expected_K"] = {"K_files": 0, "K_file_versions": 0, "K_units": False}
                with self.assertRaises(IdentityError): build_case(self.corpus, request)

    def test_roi_window_requires_exact_native_oracle_and_changes_identity(self):
        imported = imported_population()
        for record in imported["inventory"]:
            for unit in record["units"]:
                original = unit.pop("oracle")
                unit["oracles"] = [original, {**original, "origin": [0, 0], "shape": [1, 1],
                    "elements": 1, "value_sha256": fingerprint("window00"), "mask_sha256": fingerprint("mask1")},
                    {**original, "origin": [1, 1], "shape": [1, 1], "elements": 1,
                    "value_sha256": fingerprint("window11"), "mask_sha256": fingerprint("mask1")}]
        corpus = build_corpus(imported)
        ids = set()
        for origin in ([0, 0], [1, 1]):
            request = request_for(corpus); request["roi"] = {"kind": "window", "origin": origin, "shape": [1, 1]}
            case = build_case(corpus, request); ids.add(case["case_id"])
            self.assertTrue(all(m["native_oracle"]["elements"] == 1 for m in case["expected_matches"]))
        self.assertEqual(len(ids), 2)
        request["roi"]["origin"] = [0, 1]
        with self.assertRaises(IdentityError): build_case(corpus, request)

    def test_whole_roi_cannot_relabel_different_origin_shape_or_oracle_count(self):
        for change in ("origin", "shape", "oracle-elements"):
            with self.subTest(change=change):
                imported = imported_population()
                request = request_for(self.corpus)
                if change == "origin": request["roi"]["origin"] = [1, 0]
                if change == "shape": request["roi"]["shape"] = [1, 1]
                if change == "oracle-elements": imported["inventory"][2]["units"][0]["oracle"]["elements"] = 99
                with self.assertRaises(IdentityError):
                    corpus = build_corpus(imported)
                    build_case(corpus, request)


class RunAndResultIdentityTests(unittest.TestCase):
    def setUp(self):
        self.corpus = build_corpus(imported_population())
        self.case = build_case(self.corpus, request_for(self.corpus))

    def test_implementation_ab_changes_run_id_but_preserves_comparability(self):
        original = run_for(self.case)
        replacement = run_for(self.case, execution="synthetic-execution-b",
            implementation={"binary_sha256": fingerprint("binary-b"),
                "runner_sha256": fingerprint("runner"), "engine": "indexed"})
        self.assertNotEqual(original["run_id"], replacement["run_id"])
        self.assertTrue(require_comparable(original, replacement))

    def test_clock_cache_environment_and_replication_cannot_silently_change(self):
        original = run_for(self.case)
        variations = {"clock": {"protocol": "server-only/1", "unit": "ns"},
            "cache": {"protocol": "cold-process/1", "warmups": 0},
            "environment": {"host": "another-host", "configuration_sha256": fingerprint("configuration")},
            "replication": {"campaigns": 2, "measured_repeats": 5, "ordering": "blocked"},
            "arm": "FS-index"}
        for field, value in variations.items():
            with self.subTest(field=field), self.assertRaises(IdentityError):
                require_comparable(original, run_for(self.case, **{field: value}))

    def test_sealing_detects_payload_protocol_and_identity_tampering(self):
        for kind, original in (("corpus", self.corpus), ("case", self.case),
                               ("run", run_for(self.case))):
            for mutation in ("payload", "protocol", "identity"):
                with self.subTest(kind=kind, mutation=mutation):
                    document = copy.deepcopy(original)
                    if mutation == "payload": document["unexpected"] = "tampered"
                    if mutation == "protocol": document["protocol"] += "-wrong"
                    if mutation == "identity": document[kind + "_id"] = fingerprint("wrong-id")
                    with self.assertRaises(IdentityError): verify(document, kind)

    def test_result_must_keep_frozen_case_corpus_binding_and_native_oracles(self):
        result = {"case_id": self.case["case_id"], "corpus_id": self.corpus["corpus_id"],
                  "actual_matches": copy.deepcopy(self.case["expected_matches"])}
        self.assertTrue(validate_result(self.case, result))
        for change in ("case", "corpus", "binding", "oracle", "missing-unit", "ordinal-as-version"):
            with self.subTest(change=change):
                wrong = copy.deepcopy(result)
                if change in ("case", "corpus"): wrong[change + "_id"] = fingerprint("wrong")
                if change == "binding": wrong["actual_matches"][0]["binding"]["version"] += 1
                if change == "oracle": wrong["actual_matches"][0]["native_oracle"]["value_sha256"] = fingerprint("answer")
                if change == "missing-unit": wrong["actual_matches"].pop()
                if change == "ordinal-as-version": wrong["actual_matches"][0]["binding"]["version"] = wrong["actual_matches"][0]["ordinal"]
                with self.assertRaises(IdentityError): validate_result(self.case, wrong)


if __name__ == "__main__":
    unittest.main()
