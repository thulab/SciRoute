"""Native acceptance tests for space-saving, cumulative version corpora."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from experiments.cross_version.pilot import corpus


def version_spec():
    return {"protocol": "iotdbfs-cross-version-spec/1", "versions": 3,
            "scenarios": ["identical", "single_unit"], "max_new_raw_bytes": 16 * 1024**2}


class CorpusTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.fixture.cleanup)
        cls.fixture_root = Path(cls.fixture.name)
        cls.base = cls.fixture_root / "bases"
        cls.base.mkdir()
        for fmt in ("hdf5", "grib"):
            spec = corpus.multi.read_spec(corpus.MULTI / "specs" / f"runner-{fmt}-small.json")
            corpus.multi.generate(spec, cls.base / f"{fmt}-data")
        cls.base_bytes = {p: p.read_bytes() for p in cls.base.rglob("*") if p.is_file()}
        cls.prepared = cls.fixture_root / "prepared"
        corpus.prepare(version_spec(), cls.base, cls.prepared)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.output = self.root / "versions"
        shutil.copytree(self.prepared, self.output)

    def manifest(self):
        return corpus.load(self.output)

    def rewrite_manifest(self, manifest):
        # Re-sealing tests semantic validation rather than only checksum rejection.
        (self.output / "manifest.json").write_bytes(corpus.canonical(corpus.seal(manifest)) + b"\n")

    def local_entry(self):
        return next(e for e in self.manifest()["entries"] if e["blob"]["kind"] == "local")

    def test_reuse_cumulative_changes_and_unchanged_baselines(self):
        manifest = self.manifest()
        result = corpus.verify(self.output)
        self.assertEqual(result["version_bindings"], 2 * 2 * 8 * 3)
        self.assertEqual(result["local_files"], 2 * 8 * 2)
        entries = {(e["format"], e["scenario"], e["file_index"], e["ordinal"]): e
                   for e in manifest["entries"]}
        local_paths = set()
        for fmt in ("hdf5", "grib"):
            baseline = corpus.load_manifest(self.base / f"{fmt}-data")
            for index, source in enumerate(baseline["sources"]):
                base_file = self.base / f"{fmt}-data" / source["file"]
                initial = entries[fmt, "single_unit", index, 1]
                for ordinal in range(1, 4):
                    same = entries[fmt, "identical", index, ordinal]
                    self.assertEqual(same["blob"]["kind"], "base")
                    self.assertEqual(corpus.resolve(self.output, manifest, same), base_file)
                    self.assertEqual(same["sha256"], source["sha256"])
                    self.assertEqual(same["units"], initial["units"])
                    changed = entries[fmt, "single_unit", index, ordinal]
                    if ordinal == 1:
                        self.assertEqual(corpus.resolve(self.output, manifest, changed), base_file)
                        continue
                    path = corpus.resolve(self.output, manifest, changed)
                    self.assertTrue(path.is_relative_to(self.output / "objects"))
                    local_paths.add(path)
                    previous = entries[fmt, "single_unit", index, ordinal - 1]
                    actual = [i for i, (a, b) in enumerate(zip(previous["units"], changed["units"]))
                              if a["unit_id"] != b["unit_id"]]
                    self.assertEqual(actual, [ordinal - 2])
                    # Both prior changes survive in v3, proving cumulative updates.
                    from_base = [i for i, (a, b) in enumerate(zip(initial["units"], changed["units"]))
                                 if a["oracle"]["value_sha256"] != b["oracle"]["value_sha256"]]
                    self.assertEqual(from_base, list(range(ordinal - 1)))
        self.assertEqual(local_paths, {p for p in (self.output / "objects").rglob("*") if p.is_file()})
        self.assertEqual(manifest["new_raw_bytes"], sum(p.stat().st_size for p in local_paths))
        self.assertEqual({p: p.read_bytes() for p in self.base.rglob("*") if p.is_file()}, self.base_bytes)

    def test_budget_failure_is_atomic(self):
        target = self.root / "too-small"
        spec = {**version_spec(), "max_new_raw_bytes": 1}
        before = set(self.root.iterdir())
        with self.assertRaisesRegex(ValueError, "budget"):
            corpus.prepare(spec, self.base, target)
        self.assertFalse(target.exists())
        self.assertEqual(set(self.root.iterdir()), before)
        self.assertEqual({p: p.read_bytes() for p in self.base.rglob("*") if p.is_file()}, self.base_bytes)

    def test_existing_output_is_preserved(self):
        before = {p.relative_to(self.output): p.read_bytes()
                  for p in self.output.rglob("*") if p.is_file()}
        with self.assertRaises(FileExistsError):
            corpus.prepare(version_spec(), self.base, self.output)
        self.assertEqual({p.relative_to(self.output): p.read_bytes()
                          for p in self.output.rglob("*") if p.is_file()}, before)

    def test_tampered_local_object_is_rejected(self):
        manifest = self.manifest()
        path = corpus.resolve(self.output, manifest, self.local_entry())
        with path.open("ab") as stream:
            stream.write(b"corrupt")
        with self.assertRaises(ValueError):
            corpus.verify(self.output)

    def test_changed_baseline_is_rejected(self):
        manifest = self.manifest()
        entry = next(e for e in manifest["entries"] if e["blob"]["kind"] == "base")
        path = corpus.resolve(self.output, manifest, entry)
        original = path.read_bytes()
        try:
            with path.open("ab") as stream:
                stream.write(b"corrupt")
            with self.assertRaises(ValueError):
                corpus.verify(self.output)
        finally:
            path.write_bytes(original)

    def test_extra_local_object_is_rejected(self):
        (self.output / "objects" / "unexpected.bin").write_bytes(b"extra")
        with self.assertRaisesRegex(ValueError, "extra or missing"):
            corpus.verify(self.output)

    def test_duplicate_version_entry_is_rejected_after_resealing(self):
        manifest = self.manifest()
        manifest["entries"].append(copy.deepcopy(manifest["entries"][0]))
        self.rewrite_manifest(manifest)
        with self.assertRaisesRegex(ValueError, "version matrix"):
            corpus.verify(self.output)

    def test_missing_version_entry_is_rejected_after_resealing(self):
        manifest = self.manifest()
        manifest["entries"].pop()
        self.rewrite_manifest(manifest)
        with self.assertRaisesRegex(ValueError, "version matrix"):
            corpus.verify(self.output)

    def test_identical_version_cannot_point_to_local_copy(self):
        manifest = self.manifest()
        entry = next(e for e in manifest["entries"] if e["scenario"] == "identical")
        entry["blob"] = copy.deepcopy(self.local_entry()["blob"])
        self.rewrite_manifest(manifest)
        with self.assertRaisesRegex(ValueError, "storage locator"):
            corpus.verify(self.output)

    def test_unsealed_manifest_edit_is_rejected(self):
        manifest = self.manifest()
        manifest["logical_raw_bytes"] += 1
        (self.output / "manifest.json").write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, "fingerprint"):
            corpus.load(self.output)


if __name__ == "__main__":
    unittest.main()
