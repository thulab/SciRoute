"""Temporary-file registry tests; no database or benchmark is invoked."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from experiments.identity.model import IdentityError, build_case, build_corpus, seal, verify
from experiments.identity.registry import Registry, file_sha, read_json
from experiments.identity.tests.test_model import imported_population, request_for, run_for, fingerprint


def reseal(document, kind, change):
    payload = copy.deepcopy(document)
    payload.pop(kind + "_id")
    payload.pop("protocol")
    change(payload)
    return seal(kind, payload)


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.registry = Registry(self.root / "registry")
        self.corpus = build_corpus(imported_population())
        self.case = build_case(self.corpus, request_for(self.corpus))
        self.run = run_for(self.case)

    def register(self):
        self.registry.register_corpus(self.corpus)
        self.registry.register_case(self.case)
        self.registry.register_run(self.run)

    def path(self, directory, identity):
        return self.registry.root / directory / (identity + ".json")

    def artifact(self, name="historical.json", data=None):
        path = self.root / name
        path.write_text(json.dumps(data if data is not None else {"original_rows": [
            {"query_id": "historical-row-1", "arm": "IoTDB-FS", "repeat": 1},
            {"query_id": "another-original-row", "arm": "FS-index", "repeat": 1}]}))
        return {"path": str(path), "sha256": file_sha(path)}

    def mapping(self, artifact=None, case=None):
        return {"artifact": artifact or self.artifact(),
            "row_selector": {"query_id": "historical-row-1"},
            "case_id": (case or self.case)["case_id"],
            "interpretation": "Explicit synthetic row identity; no measurements are rewritten"}

    def another_case(self):
        request = request_for(self.corpus)
        request.update(operation="discover", output={"projection": ["locators"], "dtype": "none"})
        another = build_case(self.corpus, request)
        self.registry.register_case(another)
        return another

    def raw_corpus(self, *, mapping="complete"):
        imported = imported_population(n=2, versions=2)
        raw_root = self.root / "raw"
        raw_root.mkdir()
        raw_files = []
        for record in imported["inventory"]:
            relative = "v%03d/%s" % (record["ordinal"], record["logical_file"])
            path = raw_root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = hashlib.sha256((record["logical_file"] + ":" + str(record["ordinal"])).encode()).digest()
            path.write_bytes(payload)
            record.update(bytes=len(payload), sha256=file_sha(path))
            raw_files.append({"logical_file": record["logical_file"], "ordinal": record["ordinal"], "file": relative})
        by_key = {(r["logical_file"], r["ordinal"]): r for r in imported["inventory"]}
        for binding in imported["publication_evidence"]["bindings"]:
            record = by_key[binding["logical_file"], binding["ordinal"]]
            binding.update(bytes=record["bytes"], sha256=record["sha256"])
        if mapping == "partial": raw_files.pop()
        if mapping == "duplicate": raw_files[-1] = copy.deepcopy(raw_files[0])
        if mapping != "missing": imported["source_evidence"]["raw_files"] = raw_files
        corpus = build_corpus(imported)
        self.registry.register_corpus(corpus)
        return corpus, raw_root, raw_files

    def test_registration_is_idempotent_immutable_and_returns_detached_documents(self):
        self.register()
        for kind, directory, document, register, get in (
            ("corpus", "corpora", self.corpus, self.registry.register_corpus, self.registry.get_corpus),
            ("case", "cases", self.case, self.registry.register_case, self.registry.get_case),
            ("run", "runs", self.run, self.registry.register_run, self.registry.get_run)):
            with self.subTest(kind=kind):
                path = self.path(directory, document[kind + "_id"])
                before_bytes, before_stat = path.read_bytes(), path.stat()
                returned = register(copy.deepcopy(document))
                returned["caller_mutation"] = True
                self.assertEqual(get(document[kind + "_id"]), document)
                self.assertEqual(path.read_bytes(), before_bytes)
                self.assertEqual((path.stat().st_ino, path.stat().st_mtime_ns),
                                 (before_stat.st_ino, before_stat.st_mtime_ns))
                self.assertEqual(len(list(path.parent.glob("*.json"))), 1)

    def test_same_variables_cannot_republish_as_a_new_corpus(self):
        self.registry.register_corpus(self.corpus)
        imported = imported_population()
        imported["publication_evidence"].update(namespace="different-publication",
            receipt_sha256=fingerprint("new-publication-receipt"))
        for record, binding in zip(imported["inventory"], imported["publication_evidence"]["bindings"]):
            record["binding"]["path"] = "/republished/" + record["logical_file"]
            record["binding"]["version"] += 100000
            binding.update(record["binding"])
        another = build_corpus(imported)
        self.assertEqual(self.corpus["parameters"], another["parameters"])
        self.assertNotEqual(self.corpus["corpus_id"], another["corpus_id"])
        with self.assertRaisesRegex(IdentityError, "same corpus variables"):
            self.registry.register_corpus(another)
        self.assertEqual(self.registry.get_corpus(self.corpus["corpus_id"]), self.corpus)

    def test_resealed_corpus_cannot_override_derived_population_semantics(self):
        forged = reseal(self.corpus, "corpus", lambda p: p["parameters"].update(N=1))
        self.assertEqual(verify(forged, "corpus"), forged["corpus_id"])
        with self.assertRaises(IdentityError): self.registry.register_corpus(forged)

    def test_resealed_case_cannot_override_native_matches_or_derived_parameters(self):
        self.registry.register_corpus(self.corpus)
        for mutation in (lambda p: p.update(expected_matches=[]),
                         lambda p: p["parameters"].update(C_paths=1)):
            forged = reseal(self.case, "case", mutation)
            self.assertEqual(verify(forged, "case"), forged["case_id"])
            with self.assertRaises(IdentityError): self.registry.register_case(forged)

    def test_resealed_run_cannot_claim_a_different_corpus_from_its_case(self):
        self.registry.register_corpus(self.corpus)
        self.registry.register_case(self.case)
        forged = reseal(self.run, "run", lambda p: p.update(corpus_id=fingerprint("another-corpus")))
        self.assertEqual(verify(forged, "run"), forged["run_id"])
        with self.assertRaises(IdentityError): self.registry.register_run(forged)

    def test_get_revalidates_hash_and_semantics_after_filesystem_tamper(self):
        self.register()
        for kind, directory, original, get in (
            ("corpus", "corpora", self.corpus, self.registry.get_corpus),
            ("case", "cases", self.case, self.registry.get_case),
            ("run", "runs", self.run, self.registry.get_run)):
            with self.subTest(kind=kind):
                path = self.path(directory, original[kind + "_id"])
                payload = copy.deepcopy(original); payload["tampered"] = True
                path.write_text(json.dumps(payload))
                with self.assertRaises(IdentityError): get(original[kind + "_id"])
                path.write_text(json.dumps(original))
        forged = reseal(self.case, "case", lambda p: p.update(expected_matches=[]))
        self.path("cases", forged["case_id"]).write_text(json.dumps(forged))
        with self.assertRaises(IdentityError): self.registry.get_case(forged["case_id"])

    def test_get_rejects_valid_document_under_a_different_identity_filename(self):
        self.registry.register_corpus(self.corpus)
        different = fingerprint("wrong-filename")
        self.path("corpora", different).write_text(json.dumps(self.corpus))
        with self.assertRaisesRegex(IdentityError, "filename"):
            self.registry.get_corpus(different)

    def test_missing_or_malformed_ids_fail_without_registering_a_document(self):
        for get in (self.registry.get_corpus, self.registry.get_case, self.registry.get_run):
            for identity in (None, "", "../outside", fingerprint("unknown")):
                with self.subTest(get=get.__name__, identity=identity), self.assertRaises(IdentityError):
                    get(identity)
        missing = copy.deepcopy(self.corpus); missing.pop("corpus_id")
        with self.assertRaises(IdentityError): self.registry.register_corpus(missing)
        with self.assertRaises(IdentityError): self.registry.register_case(self.case)
        with self.assertRaises(IdentityError): self.registry.register_run(self.run)

    def test_registry_json_parser_rejects_duplicate_keys_and_nonfinite_numbers(self):
        path = self.root / "invalid.json"
        for content in ('{"value": 1, "value": 2}', '{"value": NaN}', '{"value": Infinity}'):
            with self.subTest(content=content):
                path.write_text(content)
                with self.assertRaises(IdentityError): read_json(path)

    def test_interrupted_pending_file_cannot_replace_the_authoritative_corpus(self):
        self.registry.register_corpus(self.corpus)
        (self.registry.root / "corpora/.pending-interrupted").write_text('{"incomplete":')
        self.assertEqual(self.registry.register_corpus(self.corpus), self.corpus)
        self.assertEqual(self.registry.get_corpus(self.corpus["corpus_id"]), self.corpus)

    def test_verify_raw_reads_every_file_version_and_is_idempotent(self):
        corpus, root, files = self.raw_corpus()
        self.assertEqual(len(files), 4)
        verification = self.registry.verify_raw(corpus["corpus_id"], root)
        self.assertEqual(verify(verification, "verification"), verification["verification_id"])
        self.assertEqual(verification["corpus_id"], corpus["corpus_id"])
        self.assertIn("all raw file-version bytes", verification["scope"])
        self.assertEqual(self.registry.verify_raw(corpus["corpus_id"], root), verification)
        self.assertEqual(len(list((self.registry.root / "verifications").glob("*.json"))), 1)

    def test_verify_raw_rejects_same_size_hash_changes_in_a_historical_version(self):
        corpus, root, files = self.raw_corpus()
        path = root / files[0]["file"]
        before = path.read_bytes(); path.write_bytes(b"X" * len(before))
        with self.assertRaisesRegex(IdentityError, "raw source bytes changed"):
            self.registry.verify_raw(corpus["corpus_id"], root)
        self.assertEqual(self.registry.get_corpus(corpus["corpus_id"]), corpus)

    def test_verify_raw_requires_complete_unique_mapping(self):
        for mapping in ("missing", "partial", "duplicate"):
            with self.subTest(mapping=mapping):
                with tempfile.TemporaryDirectory() as separate:
                    previous_root, previous_registry = self.root, self.registry
                    try:
                        self.root = Path(separate); self.registry = Registry(self.root / "registry")
                        corpus, root, _ = self.raw_corpus(mapping=mapping)
                        with self.assertRaisesRegex(IdentityError, "complete raw file mapping"):
                            self.registry.verify_raw(corpus["corpus_id"], root)
                    finally:
                        self.root, self.registry = previous_root, previous_registry

    def test_verify_raw_rejects_missing_files_and_symlink_escape(self):
        corpus, root, files = self.raw_corpus()
        path = root / files[0]["file"]
        content = path.read_bytes(); path.unlink()
        with self.assertRaises((IdentityError, OSError)):
            self.registry.verify_raw(corpus["corpus_id"], root)
        outside = self.root / "outside-source"; outside.write_bytes(content)
        path.symlink_to(outside)
        with self.assertRaisesRegex(IdentityError, "escapes"):
            self.registry.verify_raw(corpus["corpus_id"], root)

    def test_historical_mapping_is_idempotent_and_does_not_modify_the_artifact(self):
        self.register()
        artifact = self.artifact()
        before = Path(artifact["path"]).read_bytes()
        payload = self.mapping(artifact)
        payload["evidence"] = [self.artifact("support.json", {"support": True})]
        mapped = self.registry.register_mapping(payload)
        self.assertEqual(verify(mapped, "mapping"), mapped["mapping_id"])
        self.assertEqual(self.registry.register_mapping(payload), mapped)
        self.assertEqual(Path(artifact["path"]).read_bytes(), before)
        self.assertEqual(len(list((self.registry.root / "mappings").glob("*.json"))), 1)

    def test_historical_artifact_or_support_changes_prevent_mapping_reuse(self):
        self.register()
        for which in ("artifact", "evidence"):
            with self.subTest(which=which):
                artifact = self.artifact(which + "-history.json")
                support = self.artifact(which + "-support.json", {"support": True})
                payload = self.mapping(artifact); payload["evidence"] = [support]
                self.registry.register_mapping(payload)
                changed = artifact if which == "artifact" else support
                Path(changed["path"]).write_text('{"changed":true}')
                with self.assertRaisesRegex(IdentityError, "missing or changed"):
                    self.registry.register_mapping(payload)

    def test_same_historical_row_cannot_acquire_a_conflicting_case(self):
        self.register()
        another = self.another_case()
        payload = self.mapping(); self.registry.register_mapping(payload)
        conflict = copy.deepcopy(payload); conflict["case_id"] = another["case_id"]
        with self.assertRaisesRegex(IdentityError, "already binds another case"):
            self.registry.register_mapping(conflict)
        independent_row = copy.deepcopy(conflict)
        independent_row["row_selector"] = {"query_id": "another-original-row"}
        self.assertEqual(self.registry.register_mapping(independent_row)["case_id"], another["case_id"])

    def test_artifact_aliases_or_identical_copies_cannot_bypass_row_conflicts(self):
        self.register()
        another = self.another_case()
        artifact = self.artifact()
        payload = self.mapping(artifact); self.registry.register_mapping(payload)
        for kind in ("symlink", "copy"):
            with self.subTest(kind=kind):
                alias = self.root / (kind + "-same-artifact.json")
                if kind == "symlink": alias.symlink_to(artifact["path"])
                else: alias.write_bytes(Path(artifact["path"]).read_bytes())
                conflict = copy.deepcopy(payload)
                conflict["artifact"] = {"path": str(alias), "sha256": file_sha(alias)}
                self.assertEqual(conflict["artifact"]["sha256"], artifact["sha256"])
                conflict["case_id"] = another["case_id"]
                with self.assertRaises(IdentityError): self.registry.register_mapping(conflict)

    def test_compatible_overlapping_scalar_selectors_cannot_bind_different_cases(self):
        self.register()
        another = self.another_case()
        payload = self.mapping()
        payload["row_selector"] = {"query_id": "historical-row-1", "arm": "IoTDB-FS"}
        self.registry.register_mapping(payload)
        selectors = [{"query_id": "historical-row-1"}, {"arm": "IoTDB-FS"},
            {"query_id": "historical-row-1", "arm": "IoTDB-FS", "repeat": 1}]
        for selector in selectors:
            with self.subTest(selector=selector):
                conflict = copy.deepcopy(payload)
                conflict.update(case_id=another["case_id"], row_selector=selector)
                with self.assertRaises(IdentityError): self.registry.register_mapping(conflict)

    def test_mapping_needs_an_existing_case_explicit_selector_and_current_artifact(self):
        self.register()
        for change in ("case", "selector", "artifact"):
            with self.subTest(change=change):
                payload = self.mapping(self.artifact(change + ".json"))
                if change == "case": payload["case_id"] = fingerprint("missing-case")
                if change == "selector": payload["row_selector"] = {}
                if change == "artifact": Path(payload["artifact"]["path"]).unlink()
                with self.assertRaises(IdentityError): self.registry.register_mapping(payload)


if __name__ == "__main__":
    unittest.main()
