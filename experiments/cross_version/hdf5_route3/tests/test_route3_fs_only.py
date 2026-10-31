import json
import hashlib
from pathlib import Path
import tempfile
import unittest

from experiments.cross_version.hdf5_route3 import route3_fs_only as runner


class FsOnlyTests(unittest.TestCase):
    def test_query_boundary_returns_real_owned_arrays_and_checks_outside_oracle(self):
        try:
            import numpy as np
        except ImportError:
            self.skipTest("NumPy query boundary runs in pinned remote environment")
        values = np.full(runner.data.ROI, 42, dtype=np.float32)
        mask = np.ones(runner.data.ROI, dtype=np.uint8)
        oracle = {("a.h5", 1, "/fields/d00"): {"elements": values.size,
            "value_sha256": hashlib.sha256(values.tobytes()).hexdigest(),
            "mask_sha256": hashlib.sha256(mask.tobytes()).hexdigest()}}
        class Context:
            reads = 0
            def stats(self):
                return {"read_calls": self.reads}
            def select(self, *_):
                return {"units": [{"logical_path": runner.LOGICAL_PREFIX + "a.h5",
                    "path": "/fixture/a.h5", "dataset": "/fields/d00", "ordinal": 1}],
                    "selected_sources": 512, "candidate_datasets": 1}
            def read(self, *_):
                self.reads += 1
                return values.copy(), mask.copy()
        row = runner.query(Context(), "FS-index", {"kind": "latest"}, [0] * 4, oracle)
        self.assertTrue(row["owned_client_arrays"] and row["oracle_verified"])
        self.assertEqual(row["owned_bytes"], 4096 * 5)
        self.assertEqual(row["stats"]["read_calls"], 1)
        oracle["a.h5", 1, "/fields/d00"]["value_sha256"] = "incorrect"
        with self.assertRaisesRegex(ValueError, "independent source ROI oracle"):
            runner.query(Context(), "FS-index", {"kind": "latest"}, [0] * 4, oracle)

    def test_selection_windows_are_fixed_small_and_historical(self):
        self.assertEqual(runner.selections(1), [("latest", {"kind": "latest"}, [1])])
        for V in (8, 32):
            groups = runner.selections(V)
            self.assertEqual([len(x[2]) for x in groups], [1, 1, 2])
            self.assertEqual(groups[0][2], [V])
            for _, request, ordinals in groups[1:]:
                self.assertTrue(all(1 < n < V for n in ordinals))
                self.assertEqual(ordinals, list(range(request["lower"], request["upper"])))
                self.assertEqual(request["kind"], "ordinal")
        with self.assertRaises(ValueError):
            runner.selections(2)

    def test_declared_universe_scopes_do_not_depend_on_oracle(self):
        entries = [{"file_id": n, "ordinal": v, "ignored_oracle": n % 7}
                   for v in range(1, 33) for n in range(1024)]
        manifest = {"entries": entries}
        for N in runner.NS:
            for V in runner.VERSIONS:
                selected = runner.scoped_entries(manifest, N, V)
                self.assertEqual(len(selected), N * V)
                self.assertTrue(all(row["file_id"] < N and row["ordinal"] <= V
                                    for row in selected))
        broken = {"entries": entries[:-1]}
        with self.assertRaisesRegex(ValueError, "complete unique"):
            runner.scoped_entries(broken, 1024, 32)
        duplicate = {"entries": entries[:-1] + [entries[0]]}
        with self.assertRaises(ValueError):
            runner.scoped_entries(duplicate, 1024, 32)

    def test_generic_catalog_omits_attributes_and_answer_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            raw = Path(directory)
            (raw / "v001").mkdir()
            path = raw / "v001/a.h5"
            path.write_bytes(b"fixture")
            catalog = runner.catalog_for(raw, [{"file_id": 17, "ordinal": 1,
                "logical_file": "a.h5", "file": "v001/a.h5", "bytes": 7,
                "mtime_ns": path.stat().st_mtime_ns,
                "units": [{"oracle": "must not enter selection"}]}])
            source = catalog["sources"][0]
            self.assertEqual(set(source), {"logical_path", "ordinal", "version",
                                           "path", "size", "mtime_ns"})
            self.assertEqual(source["version"], source["ordinal"])
            self.assertNotIn("oracle", json.dumps(catalog))
            self.assertEqual(source["path"], str(path.resolve()))

    def test_campaign_rotates_cases_and_alternates_each_cases_arms(self):
        universe = {(N, V) for N in runner.NS for V in runner.VERSIONS}
        for campaign in range(runner.CAMPAIGNS):
            self.assertEqual(set(runner.case_order(campaign)), universe)
        for N, V in universe:
            firsts = [runner.arm_order(c, N, V)[0] for c in range(runner.CAMPAIGNS)]
            self.assertTrue(all(a != b for a, b in zip(firsts, firsts[1:])))
            self.assertEqual(sorted(firsts.count(a) for a in runner.ARMS), [2, 3])

    def test_complete_matrix_summary_preserves_first_and_warm_observations(self):
        rows = []
        for N in runner.NS:
            for V in runner.VERSIONS:
                for label, _, ordinals in runner.selections(V):
                    for roi in (0, 1):
                        for arm in runner.ARMS:
                            for campaign in range(runner.CAMPAIGNS):
                                for repeat in range(runner.REPEATS):
                                    rows.append(dict(N=N, V=V, selection=label, roi=roi,
                                        arm=arm, campaign=campaign, repeat=repeat,
                                        total_ns=2000000 if repeat == 0 else 1000000,
                                        selected_sources=N * len(ordinals),
                                        candidate_datasets=N * len(ordinals) * 4,
                                        selected_units=2 * len(ordinals),
                                        owned_bytes=2 * len(ordinals) * 4096 * 5,
                                        stats={"read_calls": 2 * len(ordinals)}))
        self.assertEqual(len(rows), 1400)
        summary = runner.summarize(rows)
        self.assertEqual(len(summary["cells"]), 56)
        for cell in summary["cells"]:
            self.assertEqual(cell["first_samples"], 5)
            self.assertEqual(cell["warm_samples"], 20)
            self.assertEqual(cell["warm_median_ms"], 1)
            self.assertEqual(cell["first_median_ms"], 2)
        self.assertTrue(all(row["warm_median_ratio"] == 1 for row in summary["growth_ratios"]))
        with self.assertRaisesRegex(ValueError, "incomplete"):
            runner.summarize(rows[:-1])


if __name__ == "__main__":
    unittest.main(verbosity=2)
