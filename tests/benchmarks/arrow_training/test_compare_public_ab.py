import copy
import unittest

from compare_public_ab import compare


class ComparePublicABTests(unittest.TestCase):
    def setUp(self):
        self.local = {
            "backend": "local", "dataset": "ETTm1", "model": "patchtst",
            "source_sha256": "source", "normalization_artifact_id": "stats",
            "window_artifact_id": "windows", "channels": [0, 1],
            "batch_size": 2, "max_steps": 2, "seed": 7,
            "visible_gpus": 1,
            "samplers": {"train": "RandomSampler",
                         "validation": "SequentialSampler"},
            "train_window_population": 100,
            "eval_window_population": 4,
            "traces": {"train": {"sample_ids": ["a", "b"],
                                  "count": 2, "sha256": "trace"},
                       "validation": {"sample_ids": ["v"],
                                      "count": 1, "sha256": "eval"}},
            "train_loss": 1.0, "eval_loss": 2.0,
        }

    def test_identical_pair_passes(self):
        other = copy.deepcopy(self.local)
        other["backend"] = "iotdbfs"
        other["source_version"] = 1
        result = compare(self.local, other)
        self.assertTrue(result["passed"])
        self.assertTrue(result["scope"].startswith("2-step"))

    def test_visible_gpu_and_trace_mismatches_fail(self):
        other = copy.deepcopy(self.local)
        other["backend"] = "iotdbfs"
        other["source_version"] = 1
        other["visible_gpus"] = 8
        other["traces"]["train"]["sample_ids"] = ["a"]
        result = compare(self.local, other)
        self.assertFalse(result["passed"])
        self.assertIn("visible_gpus", result["differences"])
        self.assertIn("train sample trace", result["differences"])

    def test_long_run_scope_uses_reported_step_count(self):
        other = copy.deepcopy(self.local)
        other["backend"] = "iotdbfs"
        other["source_version"] = 1
        self.local["max_steps"] = other["max_steps"] = 50
        self.assertTrue(compare(self.local, other)["scope"].startswith("50-step"))


if __name__ == "__main__":
    unittest.main()
