"""Scene preflight tests using tiny text bytes, never scientific data or DB IO."""
from copy import deepcopy
import hashlib
from pathlib import Path
import tempfile
import unittest

from experiments.cross_version.resource_limited import scenes
from experiments.identity.model import IdentityError, build_case, build_corpus
from experiments.identity.registry import Registry, file_sha
from experiments.identity.tests.test_model import imported_population, request_for


class ScenePreflightTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.registry = Registry(self.root / "registry")
        imported = imported_population(n=2, versions=1, units=1)
        imported["source_evidence"]["raw_files"] = []
        self.raw_root = self.root / "raw"
        checks = []
        for index, record in enumerate(imported["inventory"]):
            content = ("tiny-text-identity-fixture-%d" % index).encode()
            raw = self.raw_root / record["logical_file"]
            raw.parent.mkdir(parents=True, exist_ok=True); raw.write_bytes(content)
            record["bytes"] = len(content); record["sha256"] = hashlib.sha256(content).hexdigest()
            binding = imported["publication_evidence"]["bindings"][index]
            binding["bytes"] = record["bytes"]; binding["sha256"] = record["sha256"]
            imported["source_evidence"]["raw_files"].append({"logical_file": record["logical_file"],
                "ordinal": 1, "file": record["logical_file"]})
            copied = self.root / "fs" / record["logical_file"]
            copied.parent.mkdir(parents=True, exist_ok=True); copied.write_bytes(content)
            checks.append({"path": str(copied), "bytes": len(content), "sha256": record["sha256"], "nlink": 1})
        corpus = build_corpus(imported)
        case = build_case(corpus, request_for(corpus, expected={"K_files": 2, "K_file_versions": 2, "K_units": 2}))
        self.registry.register_corpus(corpus); self.registry.register_case(case)
        self.scene = scenes._base_scene("grib-small", corpus, case)
        self.index = self.root / "existing-index"
        self.index.write_bytes(b"existing-index-fixture")
        checks.append({"path": str(self.index), "sha256": file_sha(self.index)})
        self.binary = self.root / "native-fixture"
        self.binary.write_bytes(b"opaque-native-fixture-not-executable")
        self.owner = self.root / "owner"; self.owner.mkdir()
        (self.owner / "state").write_text('{"synthetic_unit_fixture":true}\n')
        (self.owner / "owner.lock").write_bytes(b"")
        self.scene.update(raw_root=str(self.raw_root), byte_checks=checks,
            native_binary=str(self.binary), native_binary_sha256=file_sha(self.binary),
            original_owner=str(self.owner), journal_digest=scenes._journal(self.owner))

    def tearDown(self):
        self.temporary.cleanup()

    def test_full_byte_inspection_requires_controller_head_gate(self):
        receipt = scenes.inspect(self.scene, self.registry)
        self.assertTrue(receipt["complete"])
        self.assertTrue(receipt["live_head_check_required"])
        self.assertFalse(receipt["database_read"])
        self.assertEqual(len(receipt["checked_files"]), 4)
        self.assertEqual(receipt["case_id"], self.scene["case_id"])

    def test_raw_population_mutation_rejected(self):
        raw = next(p for p in self.raw_root.rglob("*") if p.is_file())
        raw.write_bytes(b"changed source")
        with self.assertRaises(IdentityError): scenes.inspect(self.scene, self.registry)

    def test_fs_index_and_native_binary_mutation_rejected(self):
        for field in ("copy", "index", "binary"):
            with self.subTest(field=field):
                path = Path(self.scene["byte_checks"][0]["path"]) if field == "copy" else (
                    self.index if field == "index" else self.binary)
                original = path.read_bytes(); path.write_bytes(b"changed bytes")
                try:
                    with self.assertRaises(IdentityError): scenes.inspect(self.scene, self.registry)
                finally:
                    path.write_bytes(original)

    def test_owner_state_change_or_pending_entry_rejected(self):
        original = (self.owner / "state").read_bytes()
        (self.owner / "state").write_bytes(b"changed owner")
        with self.assertRaisesRegex(IdentityError, "journal changed"):
            scenes.inspect(self.scene, self.registry)
        (self.owner / "state").write_bytes(original)
        (self.owner / "pending").write_text("incomplete")
        with self.assertRaisesRegex(IdentityError, "pending"):
            scenes.inspect(self.scene, self.registry)

    def test_symlinked_native_or_copy_rejected(self):
        target = self.root / "target"; target.write_bytes(self.binary.read_bytes())
        self.binary.unlink(); self.binary.symlink_to(target)
        with self.assertRaisesRegex(IdentityError, "symlinked"):
            scenes.inspect(self.scene, self.registry)

    def test_scene_cannot_relabel_another_expected_result(self):
        bad = deepcopy(self.scene); bad["expected_matches"][0]["ordinal"] = 2
        with self.assertRaisesRegex(IdentityError, "scientific case"):
            scenes.inspect(bad, self.registry)

    def test_expected_returns_defensive_copy_and_checks_registry(self):
        matches = scenes.expected(self.scene, self.registry)
        matches.clear()
        self.assertEqual(len(self.scene["expected_matches"]), 2)

    def test_result_without_scientific_verification_or_full_output_rejected(self):
        row = {"oracle_verified": False, "selected_units": 2, "selected_owned_rows": 8}
        with self.assertRaisesRegex(IdentityError, "did not verify"):
            scenes._result(self.scene, row)
        row["oracle_verified"] = True; row["selected_units"] = 1
        with self.assertRaisesRegex(IdentityError, "count differs"):
            scenes._result(self.scene, row)


if __name__ == "__main__":
    unittest.main()
