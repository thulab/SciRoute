import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from experiments.cross_version.hdf5_route3 import route3_membership_ablation as run


class MembershipAblationTests(unittest.TestCase):
    def variants_fixture(self, directory):
        root = Path(directory)
        records = []
        for label, factors in run.FACTORS.items():
            binary = root / label
            binary.write_bytes(("fresh-relinked-" + label).encode())
            records.append(dict(label=label, record_membership_set=factors[0],
                manifest_membership_set=factors[1], daemon=str(binary), sha256=run.ab.sha(binary)))
        config = root / "variants.json"
        config.write_text(json.dumps({"variants": records}))
        return config, records

    def rows(self):
        result = []
        for label, scale in (("A", 1.0), ("B", 0.8), ("C", 0.9), ("D", 0.7)):
            for campaign in range(5):
                for repeat in range(5):
                    result.append(dict(binary=label, variant=label, case="n512r1", stored_V=1,
                        selection="latest", origin=[0] * 4, arm="typed", campaign=campaign,
                        repeat=repeat, total_ns=int(scale * 1000000), discovery_client_ns=100,
                        selected_value_client_ns=100, profiled_sdk_query_calls=0))
        return result

    def test_five_campaign_factorial_has_500_actual_queries_and_four_mounts(self):
        plan = run.plan(20261018, 5, 5)
        self.assertEqual(plan["expected_queries"], 500)
        self.assertEqual(plan["mounts"], 4)
        self.assertEqual({task["case"] for task in plan["tasks"]}, {"n512r1"})
        self.assertEqual({task["variant"] for task in plan["tasks"]}, set("ABCD"))
        self.assertEqual(plan["inherited_query_rows"], 0)
        self.assertIn("not a counterbalanced", plan["order_scope"])

    def test_order_is_frozen_by_seed_and_not_copied_from_config(self):
        first = run.plan(20261018, 5, 5)
        self.assertEqual(first["variant_order"], run.plan(20261018, 2, 3)["variant_order"])
        self.assertEqual(first["variant_order"], [task["variant"] for task in first["tasks"]])

    def test_invalid_bounded_plan_fails_before_remote_work(self):
        for args in ((True, 5, 5), (-1, 5, 5), (2**32, 5, 5), (1, 6, 5), (1, 5, 1)):
            with self.assertRaises(RuntimeError):
                run.plan(*args)

    def test_four_explicit_binary_identities_accept_relinked_A(self):
        with tempfile.TemporaryDirectory() as directory:
            config, records = self.variants_fixture(directory)
            variants = run.variants(config)
            self.assertEqual(set(variants), set("ABCD"))
            self.assertNotEqual(variants["A"]["sha256"], run.R4_REFERENCE_SHA)
            self.assertEqual(variants["D"]["sha256"], records[3]["sha256"])

    def test_changed_binary_and_incomplete_factorial_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            config, records = self.variants_fixture(directory)
            Path(records[1]["daemon"]).write_bytes(b"changed-after-manifest")
            with self.assertRaisesRegex(RuntimeError, "SHA mismatch"):
                run.variants(config)
            config.write_text(json.dumps({"variants": [records[0], records[2], records[3]]}))
            with self.assertRaisesRegex(RuntimeError, "all four"):
                run.variants(config)

    def test_same_binary_and_wrong_factors_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            config, records = self.variants_fixture(directory)
            records[1]["daemon"], records[1]["sha256"] = records[0]["daemon"], records[0]["sha256"]
            config.write_text(json.dumps({"variants": records}))
            with self.assertRaisesRegex(RuntimeError, "four distinct"):
                run.variants(config)
            records[1]["record_membership_set"] = False
            config.write_text(json.dumps({"variants": records}))
            with self.assertRaisesRegex(RuntimeError, "factors"):
                run.variants(config)

    def test_factorial_summary_keeps_variants_campaigns_and_controls_separate(self):
        rows = self.rows()
        control = [dict(row, arm="FS-index", total_ns=200000) for row in rows]
        report = run.summarize(rows + control)
        self.assertEqual(report["actual_query_records"], 200)
        self.assertEqual(report["inherited_query_rows"], 0)
        self.assertEqual(report["pairs"], [])
        self.assertEqual(len(report["cells"]), 8)
        self.assertTrue(all(cell["warm_samples"] == 20 for cell in report["cells"]))
        typed = next(cell for cell in report["factorial_cells"] if cell["arm"] == "typed")
        self.assertEqual(typed["B_over_A"], 0.8)
        self.assertEqual(typed["C_over_A"], 0.9)
        self.assertEqual(typed["D_over_A"], 0.7)
        self.assertAlmostEqual(typed["multiplicative_interaction_DA_over_BC"], 0.7 / 0.72)
        self.assertEqual(typed["campaign_ratios_over_A"]["D"], [0.7] * 5)
        fs = next(cell for cell in report["factorial_cells"] if cell["arm"] == "FS-index")
        self.assertEqual(fs["D_over_A"], 1)

    def test_incomplete_factorial_cell_cannot_be_presented_as_complete(self):
        with self.assertRaisesRegex(RuntimeError, "incomplete four-way"):
            run.summarize([row for row in self.rows() if row["variant"] != "C"])

    def test_probe_list_requires_both_explicit_A_D_binaries(self):
        self.assertEqual(run.probe_configs(None), [])
        with tempfile.TemporaryDirectory() as directory:
            config, variants = self.variants_fixture(directory)
            probes = [dict(variant=item["label"], binary=item["daemon"], sha256=item["sha256"])
                      for item in variants if item["label"] in ("A", "D")]
            config.write_text(json.dumps({"probes": list(reversed(probes))}))
            self.assertEqual([item["variant"] for item in run.probe_configs(config)], ["A", "D"])
            config.write_text(json.dumps({"probes": probes[:1]}))
            with self.assertRaisesRegex(RuntimeError, "both A and D"):
                run.probe_configs(config)

    def actual_probe_helper_fixture(self, root, mutate=None):
        config, variants = self.variants_fixture(root)
        probes = [dict(variant=item["label"], binary=item["daemon"], sha256=item["sha256"])
                  for item in variants if item["label"] in ("A", "D")]
        args = SimpleNamespace(work=root, report=root / "report.json", env={}, context_probe_timeout=123)
        def fake_process(command, **kwargs):
            label = Path(command[0]).name
            output = {"completed": True, "case_count": len(run.ab.CONTEXT_CASES),
                "cases": sorted(run.ab.CONTEXT_CASES), "history_gap_commits": 257,
                "prefix": command[2], "journal": command[4],
                "membership_checks": {name: i + 1 for i, name in enumerate(sorted(run.MEMBERSHIP_CHECKS))},
                "metadata_budget_observations": {"membership_fixture_with_rejections_bytes": 83471}}
            if mutate:
                mutate(label, output)
            kwargs["stdout"].write(json.dumps(output) + "\n")
            kwargs["stdout"].flush()
            return SimpleNamespace(returncode=0)
        return args, probes, fake_process

    def test_real_context_helper_with_only_process_mock_saves_each_report_and_compares(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args, probes, fake_process = self.actual_probe_helper_fixture(root)
            report = {}
            with patch.object(run.ab.subprocess, "run", side_effect=fake_process) as mocked:
                run.run_context_probes(args, report, probes)
            self.assertEqual(mocked.call_count, 2)
            self.assertEqual([item["variant"] for item in report["context_probes"]], ["A", "D"])
            self.assertNotEqual(report["context_probes"][0]["journal"], report["context_probes"][1]["journal"])
            self.assertEqual(json.loads(args.report.read_text())["context_probes"], report["context_probes"])
            for label in ("A", "D"):
                saved = json.loads((root / "context-probes" / label / "report.json").read_text())
                self.assertTrue(saved["context_probe"]["complete"])
                self.assertEqual(saved["context_probe"]["case_count"], 11)
                self.assertEqual(saved["context_probe"]["history_gap_commits"], 257)
            comparison = json.loads((root / "context-probes/comparison.json").read_text())
            self.assertTrue(comparison["complete"])
            self.assertTrue(all(comparison["checks"].values()))
            self.assertEqual(comparison["metadata_budget_observations_by_variant"]["A"],
                             comparison["metadata_budget_observations_by_variant"]["D"])

    def test_actual_helper_budget_or_membership_difference_gates_before_mount(self):
        for changed_field in ("membership_checks", "metadata_budget_observations"):
            with self.subTest(field=changed_field), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                def mutate(label, output):
                    if label == "D":
                        key = next(iter(output[changed_field]))
                        output[changed_field][key] += 1
                args, probes, fake_process = self.actual_probe_helper_fixture(root, mutate)
                report = {}
                with patch.object(run.ab.subprocess, "run", side_effect=fake_process):
                    with self.assertRaisesRegex(RuntimeError, "no mount permitted"):
                        run.run_context_probes(args, report, probes)
                self.assertFalse(report["context_probe_consistency"]["complete"])
                self.assertTrue(all(item["complete"] for item in report["context_probes"]))
                self.assertNotIn("mounts", report)

    def test_actual_helper_incomplete_observations_rejected_even_if_equal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def mutate(label, output):
                output["membership_checks"].pop("reverse_source_order_preserved")
            args, probes, fake_process = self.actual_probe_helper_fixture(root, mutate)
            report = {}
            with patch.object(run.ab.subprocess, "run", side_effect=fake_process):
                with self.assertRaisesRegex(RuntimeError, "incomplete"):
                    run.run_context_probes(args, report, probes)
            checks = report["context_probe_consistency"]["checks"]
            self.assertTrue(checks["membership_checks_equal"])
            self.assertFalse(checks["membership_checks_complete"])

    def test_probes_cannot_be_skipped_before_mount(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(RuntimeError, "required before mounting"):
                run.run_context_probes(SimpleNamespace(work=root), {}, [])
            self.assertFalse((root / "context-probes").exists())

    def test_shared_query_and_safety_helper_revision_is_pinned(self):
        self.assertEqual(run.ab.sha(run.ab.__file__), run.BASE_RUNNER_SHA)
        with patch.object(run.ab, "sha", return_value="unexpected"):
            with self.assertRaisesRegex(RuntimeError, "frozen allowed revision"):
                run.run(SimpleNamespace())


if __name__ == "__main__":
    unittest.main(verbosity=2)
