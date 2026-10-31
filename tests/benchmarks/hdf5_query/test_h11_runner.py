"""H11 acceptance checks need no live service or native dependencies."""
import copy
import tempfile
import unittest
from pathlib import Path
import run_h11 as runner


class H11ValidationTests(unittest.TestCase):
    def setUp(self):
        self.case = dict(id="sample", source="s", dataset="/x", origin=[0], shape=[2],
                         dtype="float64", value_bytes=16, values_sha256="a"*64)
        self.spec = dict(path="B", clients=2, passes=4, cache_bytes=1048576, cache_policy="retain",
                         parallelism=1, max_call_ranges=32, target_batch_rows=4096, cases=[self.case])
        self.result = dict(protocol=runner.RESULT, config=copy.deepcopy(self.spec), setup={},
            requests=[dict(pass_index=p, **{"pass": p}, client=c, case_id="sample", source_id="s",
                dataset="/x", origin=[0], shape=[2], dtype="float64", value_bytes=16,
                values_sha256="a"*64, steady_start_ns=1, steady_end_ns=2, timings_ns=dict(total=1))
                for p in range(4) for c in range(2)],
            passes=[{"pass": p, "wall_ns": 100*(p+1)} for p in range(4)])

    def validate(self):
        return runner.validate_result(self.result, self.spec)

    def test_warm_passes_are_aggregated_within_campaign(self):
        result = self.validate()
        self.assertEqual(result["cold_app_wall_ns"], 100)
        self.assertEqual(result["warm_app_mean_wall_ns"], 300)
        self.assertEqual(result["warm_passes"], 3)
        self.assertEqual(result["request_count"], 8)

    def test_missing_sample(self):
        self.result["requests"].pop()
        with self.assertRaisesRegex(ValueError, "missing"):
            self.validate()

    def test_duplicate_sample(self):
        self.result["requests"].append(self.result["requests"][0])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self.validate()

    def test_wrong_case(self):
        self.result["requests"][0]["case_id"] = "other"
        with self.assertRaisesRegex(ValueError, "unexpected"):
            self.validate()

    def test_wrong_client(self):
        self.result["requests"][0]["client"] = 9
        with self.assertRaisesRegex(ValueError, "unexpected"):
            self.validate()

    def test_wrong_source(self):
        self.result["requests"][0]["source_id"] = "other"
        with self.assertRaisesRegex(ValueError, "source_id"):
            self.validate()

    def test_wrong_hash(self):
        self.result["requests"][0]["values_sha256"] = "b"*64
        with self.assertRaisesRegex(ValueError, "values_sha256"):
            self.validate()

    def test_wrong_shape_dtype_or_byte_count(self):
        for key, value in (("shape", [1]), ("dtype", "float32"), ("value_bytes", 8)):
            with self.subTest(key=key):
                original = self.result["requests"][0][key]
                self.result["requests"][0][key] = value
                with self.assertRaisesRegex(ValueError, key):
                    self.validate()
                self.result["requests"][0][key] = original

    def test_wrong_budget(self):
        self.result["config"]["cache_bytes"] *= 2
        with self.assertRaisesRegex(ValueError, "configuration"):
            self.validate()

    def test_wrong_cache_policy(self):
        self.result["config"]["cache_policy"] = "encoded"
        with self.assertRaisesRegex(ValueError, "configuration"):
            self.validate()

    def test_wrong_parallelism(self):
        self.result["config"]["parallelism"] = 4
        with self.assertRaisesRegex(ValueError, "configuration"):
            self.validate()

    def test_missing_or_reordered_pass(self):
        self.result["passes"].reverse()
        with self.assertRaisesRegex(ValueError, "pass"):
            self.validate()

    def test_invalid_timing(self):
        for value in (-1, True, 1.5):
            self.result["requests"][0]["timings_ns"]["total"] = value
            with self.assertRaisesRegex(ValueError, "timing"):
                self.validate()

    def test_invalid_interval(self):
        self.result["requests"][0]["steady_end_ns"] = 0
        with self.assertRaisesRegex(ValueError, "interval"):
            self.validate()

    def test_case_id_stable_and_selection_sensitive(self):
        source = {"sha256": "a"*64}
        case = dict(dataset="/x", origin=[0], shape=[2], label="small")
        first = runner.case_identity(source, case)
        self.assertEqual(first, runner.case_identity(dict(source, local_path="other"), case))
        case["origin"] = [1]
        self.assertNotEqual(first, runner.case_identity(source, case))

    def test_pressure_and_catalog_case_selection(self):
        prep = dict(sources=[dict(id="s", dataset_count=1), dict(id="g", dataset_count=4)],
            cases=[dict(source="s", label="small"), dict(source="g", label="aligned"),
                   dict(source="s", label="pressure_scan"), dict(source="g", label="pressure_scan")])
        self.assertEqual(len(runner.selected_cases(prep, "full")), 2)
        self.assertEqual(len(runner.selected_cases(prep, "pressure")), 2)
        self.assertEqual(runner.selected_cases(prep, "catalog"), [prep["cases"][1]])

    def test_duplicate_or_nonfinite_json(self):
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory)/"bad.json"
            for value in ('{"x":1,"x":2}', '{"x":NaN}'):
                file.write_text(value)
                with self.assertRaises(ValueError):
                    runner.read_json(file)


class H11EvidenceTests(unittest.TestCase):
    setUp = H11ValidationTests.setUp
    def evidence(self, directory, scenario="full", policy="retain"):
        self.spec.update(scenario=scenario, cache_policy=policy)
        self.result.update(path="B", worker_reports=["worker.json"], ok=True)
        self.result["config"] = copy.deepcopy(self.spec)
        self.result["config"]["decoded_cache_budget_bytes"] = self.spec["cache_bytes"]
        for item in self.result["passes"]:
            item["wall_ns"] = 1
            item["runtime_delta"] = dict(unique_decode_trace_complete=1,
                unique_decoded_bytes_cumulative=self.spec["cache_bytes"]+1,
                evictions=1, redecodes=1, decode_count=1, encoded_cache_hits_cumulative=1,
                backend_payload_bytes=0, payload_read_bytes=16)
        runner.dump(directory/"worker.json", self.result)

    def test_native_pass_evidence_and_interval_must_agree(self):
        with tempfile.TemporaryDirectory() as value:
            directory = Path(value)
            self.evidence(directory)
            runner.validate_workers(self.result, directory, self.spec)
            self.result["passes"][0]["wall_ns"] = 9
            with self.assertRaisesRegex(ValueError, "normalized pass"):
                runner.validate_workers(self.result, directory, self.spec)
            runner.dump(directory/"worker.json", self.result)
            with self.assertRaisesRegex(ValueError, "owned-read"):
                runner.validate_workers(self.result, directory, self.spec)

    def test_pressure_requires_real_budget_exhaustion_and_redecodes(self):
        with tempfile.TemporaryDirectory() as value:
            directory = Path(value)
            self.evidence(directory, scenario="pressure")
            runner.validate_workers(self.result, directory, self.spec)
            for item in self.result["passes"]:
                item["runtime_delta"]["unique_decoded_bytes_cumulative"] = 100
            runner.dump(directory/"worker.json", self.result)
            with self.assertRaisesRegex(ValueError, "working set"):
                runner.validate_workers(self.result, directory, self.spec)
            self.evidence(directory, scenario="pressure")
            for item in self.result["passes"]:
                item["runtime_delta"]["redecodes"] = 0
            runner.dump(directory/"worker.json", self.result)
            with self.assertRaisesRegex(ValueError, "eviction and redecode"):
                runner.validate_workers(self.result, directory, self.spec)

    def test_cache_diagnostics_must_show_observed_effect(self):
        with tempfile.TemporaryDirectory() as value:
            directory = Path(value)
            for policy in ("encoded", "evict_before_request"):
                self.evidence(directory, policy=policy)
                runner.validate_workers(self.result, directory, self.spec)
                for item in self.result["passes"]:
                    item["runtime_delta"].update(redecodes=0, encoded_cache_hits_cumulative=0)
                runner.dump(directory/"worker.json", self.result)
                with self.assertRaises(ValueError):
                    runner.validate_workers(self.result, directory, self.spec)

    def test_e_requires_profiles_and_recomputed_waves(self):
        with tempfile.TemporaryDirectory() as value:
            directory = Path(value)
            self.spec["path"] = "E"
            self.result.update(path="E", config=copy.deepcopy(self.spec))
            profiles = []
            for index, row in enumerate(self.result["requests"]):
                row["route"] = "/route/"+str(index)
                profiles.append(dict(protocol="iotdbfs-query-profile/1", path=row["route"], ok=True,
                    steady_start_ns=1, steady_end_ns=2,
                    runtime_config=dict(memory_budget_bytes=self.spec["cache_bytes"]),
                    server_hdf5_execution=dict(read_parallelism=1)))
            for item in self.result["passes"]:
                item.update(wall_ns=1, dispatch_join_ns=3,
                    waves=[dict(case_id="sample", steady_start_ns=1, steady_end_ns=2,
                                dispatch_start_ns=0, dispatch_end_ns=3)])
            runner.dump(directory/"server-profiles.json", profiles)
            runner.validate_workers(self.result, directory, self.spec)
            self.result["passes"][0]["wall_ns"] = 2
            with self.assertRaisesRegex(ValueError, "edited E"):
                runner.validate_workers(self.result, directory, self.spec)
            self.result["passes"][0]["wall_ns"] = 1
            runner.dump(directory/"server-profiles.json", profiles[:-1])
            with self.assertRaisesRegex(ValueError, "missing E"):
                runner.validate_workers(self.result, directory, self.spec)

    def test_native_manifest_preserves_pressure_scenario(self):
        import argparse
        args = argparse.Namespace(passes=4, clients=1, cache_bytes=1048576,
            cache_policy="retain", scenario="pressure", parallelism=1, max_call_ranges=32,
            target_batch_rows=4096)
        value = runner.native_manifest("B", args, [], [], {"config":"example"}, Path("/tmp/sidecars"))
        self.assertEqual(value["scenario"], "pressure")


class H11AggregateTests(unittest.TestCase):
    def test_incomplete_missing_and_duplicate_cells_are_rejected(self):
        with tempfile.TemporaryDirectory() as value:
            directory = Path(value)
            cell = dict(variant="base", campaign=0, directory="base-c0/run")
            valid = dict(protocol="iotdbfs-hdf5-h11-campaign/1", complete=True,
                         expected_runs=[cell], runs=[cell])
            variants = [dict(valid, complete=False), dict(valid, runs=[]),
                        dict(valid, expected_runs=[cell, cell]), dict(valid, runs=[cell, cell])]
            for campaign in variants:
                runner.dump(directory/"campaign.json", campaign)
                runner.dump(directory/"summary.json", dict(passed=True))
                with self.assertRaises(ValueError):
                    runner.aggregate(directory/"campaign.json", directory/"summary.json")
                self.assertFalse(runner.read_json(directory/"summary.json")["passed"])

    def test_aggregate_uses_independent_campaign_means_and_rejects_changed_config(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as value:
            directory = Path(value)
            cells = [dict(variant="base", campaign=i, directory=f"base-c{i}/run") for i in range(3)]
            campaign = dict(protocol="iotdbfs-hdf5-h11-campaign/1", complete=True,
                            expected_runs=cells, runs=cells)
            runner.dump(directory/"campaign.json", campaign)
            summaries = [dict(matrix=dict(clients=1), runs=[dict(campaign=i, path="A",
                warm_app_mean_wall_ns=10*(i+1), cold_app_wall_ns=100)],
                aggregate=dict(A=dict(warm_mean_ns=10*(i+1)))) for i in range(3)]
            with patch.object(runner, "summarize", side_effect=summaries):
                result = runner.aggregate(directory/"campaign.json", directory/"summary.json")
            values = result["variants"]["base"]["paths"]["A"]
            self.assertEqual(values["independent_campaigns"], 3)
            self.assertEqual(values["warm_mean_ns"], 20)
            self.assertEqual(values["warm_campaign_means_ns"], [10, 20, 30])
            summaries[-1]["matrix"]["clients"] = 2
            with patch.object(runner, "summarize", side_effect=summaries):
                with self.assertRaisesRegex(ValueError, "configuration changed"):
                    runner.aggregate(directory/"campaign.json", directory/"summary.json")


if __name__ == "__main__":
    unittest.main()
