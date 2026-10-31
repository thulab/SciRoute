"""Tiny synthetic JSON evidence tests; no scientific files or benchmark runs."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from experiments.cross_version.resource_limited import large_scene
from experiments.identity.model import IdentityError, digest
from experiments.identity.registry import Registry


def fingerprint(label):
    return hashlib.sha256(label.encode()).hexdigest()


def seal(document):
    body = deepcopy(document)
    body.pop("manifest_sha256", None)
    return {**body, "manifest_sha256": digest(body)}


def fixture():
    query = {"shortName": "t", "typeOfLevel": "isobaricInhPa", "level": 500,
             "step": 0, "validityDate": 20261013, "validityTime": 0}
    controls = {"N": 8, "D": 4, "K": 2, "depth": 2, "seed": 7, "tree_width": 4, "query": query}
    spec = {**controls, "payload": {"Ni": 3, "Nj": 2, "bits_per_value": 16, "packing": "grid_simple"}}
    raw_root = "/synthetic/work/large/corpus/bases/grib-data"
    fs_root = "/synthetic/work/large-r2/run/fs/identical/V8/grib"
    sources, entries, bindings, copies = [], [], [], []
    for file_id in range(8):
        logical = "raw/b%02d/f%02d.grib" % (file_id // 2, file_id)
        units = []
        for unit_id in range(4):
            attrs = dict(query)
            if file_id >= 2 or unit_id:
                attrs["step"] = file_id * 4 + unit_id + 1
            units.append({"offset": unit_id * 8, "length": 8,
                "unit_id": fingerprint("encoded-message-%d-%d" % (file_id, unit_id)),
                "attributes": attrs, "schema": {"Ni": 3, "Nj": 2},
                "oracle": {"elements": 6, "physical_dtype": "<f4",
                    **{key: fingerprint("%s-%d-%d" % (key, file_id, unit_id)) for key in
                       ("value_sha256", "mask_sha256", "latitudes_sha256", "longitudes_sha256")}}})
        source = {"file": logical, "bytes": 32 + file_id, "sha256": fingerprint("source-%d" % file_id), "units": units}
        sources.append(source)
        for ordinal in range(1, 9):
            entries.append({"format": "grib", "scenario": "identical", "logical_file": logical,
                "ordinal": ordinal, "bytes": source["bytes"], "sha256": source["sha256"],
                "units": deepcopy(units), "blob": {"kind": "base", "file": logical}})
            bindings.append({"format": "grib", "logical_file": logical, "ordinal": ordinal,
                "logical_path": "/large/grib/" + logical, "version": 1700000000000 + file_id * 100 + ordinal * 3,
                "sha256": source["sha256"]})
            copies.append({"path": fs_root + "/v%03d/" % ordinal + logical,
                           "bytes": source["bytes"], "sha256": source["sha256"], "nlink": 1})
    template = seal({"protocol": "iotdbfs-multifile-data/1", "format": "grib", "actual": {"N": 8},
                     "spec": spec, "query": query, "sources": sources})
    manifest = seal({"protocol": "iotdbfs-large-version-data/1", "controls": controls, "versions": 8,
        "bases": {"grib": {"root": raw_root, "manifest_sha256": template["manifest_sha256"]}},
        "entries": entries, "mutation": "synthetic JSON fixture only; no generated scientific bytes"})
    namespace = {"system": "V04", "scenario": "identical", "V": 8, "prepared": True,
        "bindings": bindings, "prefix": "synthetic_v04", "logical_prefix": "/large",
        "journal": "/synthetic/work/large-r3/run/preparation/v04-identical-v8/owner",
        "head": {"namespace": "synthetic-ns", "incarnation": "synthetic-incarnation",
                 "commit": fingerprint("commit"), "root": fingerprint("root"), "seq": 140}}
    report = {"complete": True, "original_service_restored": True, "corpus_sha256": manifest["manifest_sha256"],
        "namespaces": [namespace], "native_sha256": fingerprint("native-binary"),
        "fs": [{"format": "grib", "scenario": "identical", "V": 8, "root": fs_root,
                "index": fs_root + ".sqlite", "index_sha256": fingerprint("index"), "files": copies,
                "index_build": {"files": 64, "units": 256, "version_count": 8, "total_ns": 123}}]}
    return manifest, template, report


def evidence_for(manifest, template, report):
    return {key: {"path": key + ".json", "sha256": digest(value)} for key, value in
            (("manifest", manifest), ("template", template), ("publication", report))}


def normalise(documents):
    return large_scene._normalise(*documents, evidence_for(*documents))


class LargeSceneTests(unittest.TestCase):
    def setUp(self):
        self.documents = list(fixture())

    def reseal(self):
        self.documents[1] = seal(self.documents[1])
        self.documents[0]["bases"]["grib"]["manifest_sha256"] = self.documents[1]["manifest_sha256"]
        self.documents[0] = seal(self.documents[0])
        self.documents[2]["corpus_sha256"] = self.documents[0]["manifest_sha256"]

    def test_complete_history_and_prospective_selectivity(self):
        corpus, case, scene = normalise(self.documents)
        self.assertEqual(len(corpus["inventory"]), 64)
        self.assertEqual((case["parameters"]["N"], case["parameters"]["V_stored"], case["parameters"]["D"]), (8, 8, 4))
        self.assertEqual((case["parameters"]["C_paths"], case["parameters"]["C_versions"]), (8, 64))
        self.assertEqual(case["parameters"]["expected_K"], {"K_files": 2, "K_file_versions": 16, "K_units": 16})
        self.assertEqual(len(case["expected_matches"]), 16)
        self.assertEqual(scene["owned_output_bytes"], 16 * 6 * 5)
        self.assertEqual(case["parameters"]["roi"]["shape"], [2, 3])
        self.assertEqual(scene["native_binary"], "/synthetic/work/n8-version-matrix-pilot-20261017/native-build/version_matrix_native")

    def test_all_versions_reuse_evidenced_eight_physical_base_files(self):
        corpus, _, scene = normalise(self.documents)
        raw = corpus["source_evidence"]["raw_files"]
        self.assertEqual((len(raw), len({record["file"] for record in raw})), (64, 8))
        self.assertFalse(corpus["source_evidence"]["raw_source_hashes_reverified"])
        self.assertFalse(scene["database_read"])
        self.assertEqual(corpus["parameters"]["S"]["min"], 32)
        self.assertEqual(corpus["parameters"]["S"]["max"], 39)
        self.assertIsNone(corpus["parameters"]["S"]["constant"])

    def test_real_database_versions_never_replaced_by_ordinals(self):
        corpus, _, _ = normalise(self.documents)
        self.assertTrue(all(r["binding"]["version"] > 1_000_000 for r in corpus["inventory"]))
        self.documents[2]["namespaces"][0]["bindings"][1]["version"] = self.documents[2]["namespaces"][0]["bindings"][0]["version"]
        with self.assertRaises(IdentityError): normalise(self.documents)

    def test_missing_history_or_duplicate_record_rejected(self):
        for mutation in (lambda entries: entries.pop(), lambda entries: entries.append(deepcopy(entries[0]))):
            with self.subTest(mutation=mutation):
                self.documents = list(fixture())
                mutation(self.documents[0]["entries"]); self.reseal()
                with self.assertRaises(IdentityError): normalise(self.documents)

    def test_missing_duplicate_and_extraneous_publication_rejected(self):
        for mode in ("missing", "duplicate", "extra"):
            with self.subTest(mode=mode):
                self.documents = list(fixture()); bindings = self.documents[2]["namespaces"][0]["bindings"]
                if mode == "missing": bindings.pop()
                else:
                    item = deepcopy(bindings[0])
                    if mode == "extra": item["ordinal"] = 9
                    bindings.append(item)
                with self.assertRaises(IdentityError): normalise(self.documents)

    def test_source_sha_blob_and_unit_evidence_cannot_change(self):
        for mode in ("sha", "blob", "units"):
            with self.subTest(mode=mode):
                self.documents = list(fixture()); entry = self.documents[0]["entries"][0]
                if mode == "sha": entry["sha256"] = fingerprint("different")
                elif mode == "blob": entry["blob"]["kind"] = "new"
                else: entry["units"][0]["attributes"]["step"] = 999
                self.reseal()
                with self.assertRaises(IdentityError): normalise(self.documents)

    def test_missing_native_mask_and_coordinate_hashes_never_synthesized(self):
        for key in ("mask_sha256", "latitudes_sha256", "longitudes_sha256"):
            with self.subTest(key=key):
                self.documents = list(fixture())
                self.documents[1]["sources"][0]["units"][0]["oracle"].pop(key)
                self.documents[0]["entries"][0]["units"][0]["oracle"].pop(key)
                self.reseal()
                with self.assertRaises(IdentityError): normalise(self.documents)

    def test_changed_selectivity_rejected_even_if_legacy_template_resealed(self):
        for entry in self.documents[0]["entries"]:
            if entry["logical_file"] == self.documents[1]["sources"][2]["file"]:
                entry["units"][0]["attributes"] = dict(self.documents[0]["controls"]["query"])
        self.documents[1]["sources"][2]["units"][0]["attributes"] = dict(self.documents[0]["controls"]["query"])
        self.reseal()
        with self.assertRaisesRegex(IdentityError, "declared K"):
            normalise(self.documents)

    def test_report_must_be_completed_prepared_and_unique(self):
        for mode in ("complete", "restored", "prepared", "duplicate_namespace"):
            with self.subTest(mode=mode):
                self.documents = list(fixture()); report = self.documents[2]
                if mode == "complete": report["complete"] = False
                elif mode == "restored": report["original_service_restored"] = False
                elif mode == "prepared": report["namespaces"][0]["prepared"] = False
                else: report["namespaces"].append(deepcopy(report["namespaces"][0]))
                with self.assertRaises(IdentityError): normalise(self.documents)

    def test_bad_fs_copy_index_or_path_rejected(self):
        for mode in ("missing", "sha", "link", "index", "unsafe_path"):
            with self.subTest(mode=mode):
                self.documents = list(fixture()); fs = self.documents[2]["fs"][0]
                if mode == "missing": fs["files"].pop()
                elif mode == "sha": fs["files"][0]["sha256"] = fingerprint("different")
                elif mode == "link": fs["files"][0]["nlink"] = 2
                elif mode == "index": fs["index_build"]["units"] = 255
                else: fs["root"] = "/synthetic/../escape"
                with self.assertRaises(IdentityError): normalise(self.documents)

    def test_deterministic_inventory_sort_and_no_input_mutation(self):
        original = deepcopy(self.documents)
        corpus, case, scene = normalise(self.documents)
        self.assertEqual(self.documents, original)
        changed = deepcopy(self.documents)
        changed[0]["entries"].reverse(); changed[1]["sources"].reverse()
        changed[2]["namespaces"][0]["bindings"].reverse()
        # Original byte evidence and internal seals intentionally bind ordering;
        # a rewritten/resealed historical artifact must acquire a different ID.
        changed[1] = seal(changed[1]); changed[0]["bases"]["grib"]["manifest_sha256"] = changed[1]["manifest_sha256"]
        changed[0] = seal(changed[0]); changed[2]["corpus_sha256"] = changed[0]["manifest_sha256"]
        corpus2, case2, _ = normalise(changed)
        self.assertNotEqual(corpus["corpus_id"], corpus2["corpus_id"])
        self.assertEqual(corpus["inventory"], corpus2["inventory"])
        self.assertNotEqual(case["case_id"], case2["case_id"])

    def test_prepare_registers_idempotently_only_from_accepted_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); materials = {}
            for key, document in zip(("manifest", "template", "publication"), self.documents):
                path = root / (key + ".json")
                path.write_text(json.dumps(document))
                materials[key] = (Path(path.name), hashlib.sha256(path.read_bytes()).hexdigest())
            registry = Registry(root / "registry")
            with patch.object(large_scene, "MATERIALS", materials):
                first = large_scene.prepare_large_scene(root, registry)
                self.assertEqual(first, large_scene.prepare_large_scene(root, registry))
                self.assertEqual(registry.get_case(first["case_id"])["corpus_id"], first["corpus_id"])
                self.assertFalse((root / "registry/runs").exists())
                (root / "manifest.json").write_text("{}")
                with self.assertRaisesRegex(IdentityError, "SHA differs"):
                    large_scene.prepare_large_scene(root, registry)

    def test_internal_seal_or_missing_material_rejected(self):
        self.documents[0]["versions"] = 7
        with self.assertRaisesRegex(IdentityError, "internal seal"):
            normalise(self.documents)
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(IdentityError, "artifact missing"):
                large_scene.prepare_large_scene(directory, Registry(Path(directory) / "registry"))


if __name__ == "__main__":
    unittest.main()
