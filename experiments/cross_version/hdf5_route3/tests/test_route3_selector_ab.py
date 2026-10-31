import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from experiments.cross_version.hdf5_route3 import route3_selector_ab as ab


class SelectorAbTests(unittest.TestCase):
    def test_bounded_probe_and_full_counts_without_republication(self):
        probe = ab.plan("probe", 2, 3)
        self.assertEqual(probe["expected_queries"], 120)
        self.assertEqual(probe["mounts"], 2)
        self.assertEqual(probe["cases"], ["n128r3"])
        full = ab.plan("full", 3, 5)
        self.assertEqual(full["expected_queries"], 1500)
        self.assertEqual(full["mounts"], 6)
        self.assertEqual(full["binary_order"], ["baseline", "candidate"])
        with self.assertRaises(RuntimeError):
            ab.plan("probe", 2, 1)

    def test_candidate_only_subset_counts_only_new_actual_queries(self):
        selected = ab.parse_cases("n128r3,n512r1")
        plan = ab.plan("full", 3, 5, binaries="candidate", cases=selected)
        self.assertEqual(plan["cases"], selected)
        self.assertEqual(plan["binary_order"], ["candidate"])
        self.assertEqual(plan["mounts"], 2)
        self.assertEqual(plan["expected_queries"], 600)
        self.assertEqual(plan["inherited_query_rows"], 0)
        self.assertEqual(ab.plan("probe", 2, 3, binaries="candidate")["expected_queries"], 60)
        self.assertEqual(ab.plan("full", 3, 5, binaries="both", cases=selected)["expected_queries"], 1200)
        for invalid in ("", "n128r3,n128r3", "n128r3,unknown", "n128r3,", "N128,N512"):
            with self.assertRaises(RuntimeError):
                ab.parse_cases(invalid)
        with self.assertRaises(RuntimeError):
            ab.plan("probe", 2, 3, cases=["n512r1"])

    def test_original_timestamp_selections_and_stored_v8_label(self):
        selections = [{"name": "latest", "kind": "latest", "fence": None, "rounds": [8]},
            {"name": "historical-one", "kind": "between", "fence": [100, 200], "rounds": [4]},
            {"name": "historical-two", "kind": "between", "fence": [50, 200], "rounds": [3, 4]}]
        phase = {"selections": selections}
        probe = ab.choose_groups(phase, "probe")
        self.assertEqual(len(probe), 2)
        self.assertEqual(probe[1][0]["fence"], [100, 200])
        self.assertTrue(all(origin == [0, 0, 0, 0] for _, origin in probe))
        self.assertEqual(len(ab.choose_groups(phase, "full")), 6)
        aligned = ab.choose_groups(phase, "full", roi="aligned")
        self.assertEqual(len(aligned), 3)
        self.assertEqual([selection for selection, _ in aligned], selections)
        self.assertTrue(all(origin == [0, 0, 0, 0] for _, origin in aligned))
        self.assertEqual(ab.CASES["n128r3"], (128, 8))
        with self.assertRaises(RuntimeError):
            ab.choose_groups({"selections": [{"name": "all", "kind": "all", "rounds": [1, 2]}]}, "full")

    def test_reduced_candidate_and_fresh_baseline_are_separate_observations(self):
        candidate = ab.plan("full", 2, 3, binaries="candidate",
                            cases=["n128r3", "n512r1"], roi="aligned")
        self.assertEqual(candidate["expected_queries"], 120)
        self.assertEqual(candidate["mounts"], 2)
        self.assertEqual(candidate["roi"], "aligned")
        baseline = ab.plan("full", 2, 3, binaries="baseline",
                           cases=["n512r1"], roi="aligned")
        self.assertEqual(baseline["expected_queries"], 30)
        self.assertEqual(baseline["mounts"], 1)
        self.assertEqual(baseline["binary_order"], ["baseline"])
        self.assertIn("baseline-only", baseline["comparison_scope"])
        self.assertEqual(baseline["inherited_query_rows"], 0)
        with self.assertRaisesRegex(RuntimeError, "ROI"):
            ab.plan("full", 2, 3, roi="unknown")

    def test_independent_journal_copies_and_symlink_rejection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original = root / "original"
            original.mkdir()
            (original / "owner.lock").write_bytes(b"")
            (original / "state").write_bytes(b"committed-exact-state")
            digest = ab.journal_snapshot(original)
            copy = root / "copy"
            ab.copy_journal(original, copy, digest)
            self.assertEqual(ab.journal_snapshot(copy), digest)
            (copy / "state").write_bytes(b"modified-copy")
            self.assertEqual(ab.journal_snapshot(original), digest)
            with self.assertRaises(RuntimeError):
                ab.copy_journal(original, copy, digest)
            (original / "foreign").symlink_to(copy / "state")
            with self.assertRaisesRegex(RuntimeError, "unexpected"):
                ab.journal_snapshot(original)

    def test_launcher_explicit_allowlist_and_no_initialization(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "candidate"
            binary.write_bytes(b"candidate-binary")
            class Prior:
                @staticmethod
                def launcher(path, exe, prefix, journal, initialize):
                    self.assertFalse(initialize)
                    settings = {"storage.version_directory.initialize": "false"}
                    path.write_text("SETTINGS=" + repr(settings) + "\n")
                    return {"binary_sha256": ab.sha(exe), "settings": settings}
            launch = ab.create_launcher(Prior, root / "launcher", binary, ab.sha(binary),
                                        "existing_prefix", root / "journal")
            self.assertEqual(launch["settings"]["query.frontend.max_registrations"], "8192")
            self.assertEqual(launch["sha256"], ab.sha(root / "launcher"))
            with self.assertRaisesRegex(RuntimeError, "allowlist"):
                ab.create_launcher(Prior, root / "other", binary, "0" * 64,
                                   "existing_prefix", root / "journal")

    def test_sdk_zero_requires_both_profile_query_channels(self):
        call = {"server_profile": {"iotdb_delta": {"query_calls": 0, "version_store_query_calls": 0}}}
        self.assertEqual(ab.warm_sdk_queries({"discovery_calls": [call], "value_calls": [call]}), 0)
        self.assertIsNone(ab.warm_sdk_queries({"discovery_calls": [], "value_calls": []}))
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            ab.warm_sdk_queries({"discovery_calls": [{"server_profile": {"iotdb_delta": {"query_calls": 0}}}], "value_calls": []})

    def test_summary_uses_complete_query_and_keeps_campaigns(self):
        rows = []
        for binary, scale in (("baseline", 1), ("candidate", 0.8)):
            for campaign in range(2):
                for repeat in range(3):
                    rows.append(dict(case="n128r3", stored_V=8, selection="latest", origin=[0] * 4,
                        arm="typed", binary=binary, campaign=campaign, repeat=repeat,
                        total_ns=int(1000000 * scale), discovery_client_ns=700000,
                        selected_value_client_ns=300000, profiled_sdk_query_calls=0))
        report = ab.summarize(rows)
        self.assertEqual(len(report["pairs"]), 1)
        self.assertEqual(report["pairs"][0]["candidate_over_baseline"], 0.8)
        self.assertEqual(report["pairs"][0]["campaign_ratios"], [0.8, 0.8])
        self.assertTrue(all(c["warm_samples"] == 4 for c in report["cells"]))
        candidate_rows = [row for row in rows if row["binary"] == "candidate"]
        candidate_report = ab.summarize(candidate_rows)
        self.assertEqual(candidate_report["actual_query_records"], 6)
        self.assertEqual(candidate_report["inherited_query_rows"], 0)
        self.assertEqual(candidate_report["observed_binaries"], ["candidate"])
        self.assertEqual(candidate_report["pairs"], [])
        self.assertFalse(candidate_report["same_run_baseline_available"])
        self.assertIn("no current paired A/B", candidate_report["comparison_scope"])
        baseline_report = ab.summarize([row for row in rows if row["binary"] == "baseline"])
        self.assertEqual(baseline_report["observed_binaries"], ["baseline"])
        self.assertEqual(baseline_report["actual_query_records"], 6)
        self.assertEqual(baseline_report["pairs"], [])
        self.assertFalse(baseline_report["same_run_baseline_available"])

    def test_optional_context_probe_receipt_with_mocked_process_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "context_probe"
            binary.write_bytes(b"mock-probe-not-executed")
            args = SimpleNamespace(context_probe=binary, context_probe_sha=ab.sha(binary),
                work=root, report=root / "report.json", env={}, context_probe_timeout=30)
            def fake_run(command, **kwargs):
                output = {"completed": True, "case_count": len(ab.CONTEXT_CASES), "history_gap_commits": 257,
                    "cases": sorted(ab.CONTEXT_CASES),
                    "prefix": command[2], "journal": command[4]}
                kwargs["stdout"].write(json.dumps(output) + "\n")
                kwargs["stdout"].flush()
                return SimpleNamespace(returncode=0)
            report = {}
            with patch.object(ab.subprocess, "run", side_effect=fake_run) as mocked:
                ab.context_probe(args, report)
            self.assertEqual(mocked.call_count, 1)
            self.assertTrue(report["context_probe"]["complete"])
            self.assertEqual(report["context_probe"]["case_count"], 11)
            self.assertTrue(report["context_probe"]["prefix"].startswith("vd_v03_select_"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
