import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from summarize_shared_training_views import ARMS, CASES, summarize


def fixture():
    method = dict(protocol="iotdbfs-shared-views-experiment/1", repetitions=2,
                  requests_per_experiment=2, experiments=4, passes=2)
    rows = []
    for rep in range(2):
        for layout, pattern in CASES:
            for backend, sharing in ARMS:
                cohorts = [[i] for i in range(4)] if (backend, sharing) == ("typed", "isolated") else [list(range(4))]
                for repeat in range(2):
                    for cohort in cohorts:
                        # Paired speedups differ between repetitions: 2 and 3.
                        per_request = (2 + rep) if sharing == "isolated" else 1
                        per_experiment = 100 if sharing == "isolated" else 25
                        count = len(cohort)
                        runtime = dict(read_bytes=count * per_experiment if not repeat else 0,
                                       **{"decode_count" if backend == "typed" else "batch_decodes": count if not repeat else 0})
                        rows.append(dict(repetition=rep, layout=layout, pattern=pattern,
                            backend=backend, sharing=sharing, pass_index=repeat, cohort=cohort,
                            passed=True, requests=count*2, samples=count*4,
                            request_seconds={str(i): [per_request]*2 for i in cohort},
                            execution_seconds=count*2*per_request,
                            setup_seconds=count*.25 if not repeat else 0,
                            prepare_seconds=count*.5 if not repeat else 0,
                            cache_owned_bytes=count*per_experiment, runtime_delta=runtime,
                            server_seconds={"ipc_encode": count*.1},
                            client_metrics={str(i): {"read_bytes": 10**9} for i in cohort}))
    return method, rows


class SharedSummaryTests(unittest.TestCase):
    def test_aggregate_cohorts_and_paired_ratios(self):
        method, rows = fixture()
        result = summarize(method, rows)
        self.assertEqual(result["observation_count"], 84)
        self.assertEqual(result["complete_arm_count"], 48)
        self.assertEqual(result["requests"], 384)
        self.assertEqual(result["samples"], 768)
        arm = next(r for r in result["arms"] if r["backend"] == "typed" and
                   r["sharing"] == "isolated" and r["pass_index"] == 0)
        self.assertEqual(arm["cache_owned_bytes"], 400)
        self.assertEqual(arm["setup_seconds"], 1)
        self.assertEqual(arm["prepare_seconds"], 2)
        self.assertEqual(arm["read_bytes"], 400)
        shared = next(r for r in result["arms"] if r["backend"] == "fs" and
                      r["sharing"] == "shared" and r["pass_index"] == 0)
        self.assertEqual(shared["read_bytes"], 100)  # Ignore aliased client metrics.
        pair = next(r for r in result["paired_contrasts"] if r["pass_index"] == 1)
        self.assertEqual(pair["metrics"]["primary_speedup"]["by_repetition"], [2, 3])
        self.assertEqual(pair["metrics"]["primary_speedup"]["mean"], 2.5)
        json.dumps(result, allow_nan=False)

    def test_missing_duplicate_and_wrong_cohorts(self):
        method, rows = fixture()
        for bad in (rows[:-1], rows + [copy.deepcopy(rows[0])]):
            with self.assertRaises(ValueError):
                summarize(method, bad)
        rows[0]["cohort"] = [0, 1, 2]
        with self.assertRaises(ValueError):
            summarize(method, rows)

    def test_failures_and_warm_reads_rejected(self):
        method, rows = fixture()
        rows[0]["passed"] = False
        with self.assertRaisesRegex(ValueError, "failed"):
            summarize(method, rows)
        rows[0]["passed"] = True
        next(r for r in rows if r["pass_index"] == 1)["runtime_delta"]["read_bytes"] = 1
        with self.assertRaisesRegex(ValueError, "warm"):
            summarize(method, rows)

    def test_incomplete_request_metrics_rejected(self):
        method, rows = fixture()
        rows[0]["request_seconds"]["0"].pop()
        with self.assertRaisesRegex(ValueError, "incomplete request"):
            summarize(method, rows)
        method, rows = fixture()
        rows[0]["requests"] -= 1
        with self.assertRaisesRegex(ValueError, "request/sample"):
            summarize(method, rows)

    def test_invalid_numbers_and_missing_counter_rejected(self):
        method, rows = fixture()
        rows[0]["cache_owned_bytes"] = float("nan")
        with self.assertRaisesRegex(ValueError, "finite"):
            summarize(method, rows)
        method, rows = fixture()
        del rows[0]["runtime_delta"]["batch_decodes"]
        with self.assertRaisesRegex(ValueError, "missing counter"):
            summarize(method, rows)

    def test_cli_completion_marker(self):
        method, rows = fixture()
        script = Path(__file__).with_name("summarize_shared_training_views.py")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "method.json").write_text(json.dumps(method))
            (root / "results.json").write_text(json.dumps(rows))
            (root / "complete.json").write_text(json.dumps(dict(passed=True, observations=len(rows))))
            command = [sys.executable, str(script), "--input", str(root), "--output", str(root / "summary.json")]
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads((root / "summary.json").read_text())["requests"], 384)
            (root / "complete.json").write_text(json.dumps(dict(passed=False, observations=len(rows))))
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)

    def test_pressure_warm_reads_require_explicit_flag(self):
        method, rows = fixture()
        next(r for r in rows if r["pass_index"] == 1)["runtime_delta"]["read_bytes"] = 50
        with self.assertRaisesRegex(ValueError, "warm"):
            summarize(method, rows)
        method["warm_zero_reads_expected"] = False
        result = summarize(method, rows)
        self.assertEqual(sum(r["read_bytes"] for r in result["arms"] if r["pass_index"] == 1), 50)
        method["warm_zero_reads_expected"] = "false"
        with self.assertRaisesRegex(ValueError, "boolean"):
            summarize(method, rows)
        method["warm_zero_reads_expected"] = False
        next(r for r in rows if r["pass_index"] == 1)["prepare_seconds"] = 1
        with self.assertRaisesRegex(ValueError, "warm"):
            summarize(method, rows)

    def test_declared_subset_still_requires_exact_complete_coverage(self):
        method, rows = fixture()
        method["cases"] = [["sm128", "overlap"], ["sm128", "disjoint"]]
        subset = [row for row in rows if row["layout"] == "sm128"]
        result = summarize(method, subset)
        self.assertEqual(result["complete_arm_count"], 32)
        self.assertEqual(len(result["paired_contrasts"]), 8)
        self.assertEqual(result["requests"], 256)
        for bad in (subset[:-1], rows, subset + [copy.deepcopy(subset[0])]):
            with self.assertRaises(ValueError):
                summarize(method, bad)
        for invalid in ([], [["sm128", "overlap"]] * 2, [["original", "disjoint"]],
                        ["sm128"], None):
            method["cases"] = invalid
            with self.assertRaises(ValueError):
                summarize(method, subset)

    def test_independent_physical_demand_oracle(self):
        method, rows = fixture()
        fields = ("fs_private_batch_units", "fs_shared_batch_units",
                  "typed_private_fragment_units", "typed_shared_fragment_units")
        method["demand_units"] = {f"{layout}-{pattern}": dict.fromkeys(fields, 4)
                                  for layout, pattern in CASES}
        result = summarize(method, rows)
        self.assertTrue(all(row["expected_decode_count"] == 4
                            for row in result["arms"] if row["pass_index"] == 0))
        method["demand_units"]["original-overlap"]["typed_shared_fragment_units"] = 3
        with self.assertRaisesRegex(ValueError, "physical demand oracle"):
            summarize(method, rows)
        method["warm_zero_reads_expected"] = False
        summarize(method, rows)  # Evictions may make pressure decode counts larger.
        method["warm_zero_reads_expected"] = True
        del method["demand_units"]["original-overlap"]["typed_shared_fragment_units"]
        with self.assertRaisesRegex(ValueError, "incomplete physical demand"):
            summarize(method, rows)

    def test_warm_decode_without_read_rejected(self):
        method, rows = fixture()
        warm = next(row for row in rows if row["pass_index"] == 1)
        warm["runtime_delta"]["batch_decodes"] = 1
        with self.assertRaisesRegex(ValueError, "warm"):
            summarize(method, rows)
        method["warm_zero_reads_expected"] = False
        summarize(method, rows)


if __name__ == "__main__":
    unittest.main()
