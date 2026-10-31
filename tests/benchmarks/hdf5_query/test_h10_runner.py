"""Strict summary self-tests; no native probe, network or live service."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
import run_h10 as runner


class H10SummaryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.run = self.root / "run"
        self.run.mkdir()
        self.values = b"\0" * 8
        item = dict(dataset="/x", origin=[0], shape=[1], phase="first", cache_action="retain",
                    element_bytes=8, useful_bytes=8, dtype="<f8", full_shape=[1],
                    value_sha256=runner.digest(self.values))
        item["sample_id"] = runner.sample_identity("a" * 64, item)
        self.sid = item["sample_id"]
        self.trace = dict(protocol=runner.TRACE_PROTOCOL, source_sha256="a"*64, samples=[item])
        self.raw = dict(protocol=runner.RAW_PROTOCOL, source_sha256="a"*64, setup={},
            config=dict(backend="recording-memory", expect_resident=False, source_path="/fixture", source_version=17), requests=[dict(
                sample_id=self.sid, dataset="/x", origin=[0], shape=[1], cache_action="retain",
                timings_ns=dict(total=1), stages_ns={}, counters={}, metrics=dict(decode_count=1, read_bytes=4, read_ranges=1,
                    payload_read_calls=1, payload_read_bytes=4, payload_read_ranges=1, recorded_payload_calls=1, recorded_payload_ranges=1,
                    recorded_payload_bytes=4, useful_bytes=8))])
        self.client = dict(protocol=runner.CLIENT_PROTOCOL, source_sha256="a"*64, requests=[dict(
            sample_id=self.sid, source_path="/fixture", source_version=17, value_sha256=runner.digest(self.values), ipc_sha256=runner.digest(b"ipc"),
            ipc_bytes=3, timings_ns=dict(mmap_setup=1), metrics=dict(minor_faults_during_touch=0))])
        self.manifest = self.root / "manifest.json"
        (self.root / (self.sid + ".oracle.bin")).write_bytes(self.values)
        (self.run / (self.sid + ".values.bin")).write_bytes(self.values)
        (self.run / (self.sid + ".ipc")).write_bytes(b"ipc")

    def evaluate(self):
        runner.dump(self.manifest, self.trace)
        runner.dump(self.run / "raw.json", self.raw)
        runner.dump(self.run / "client.json", self.client)
        return runner.summarize(self.manifest, self.run, self.run / "summary.json")

    def test_complete_trace_passes(self):
        self.assertTrue(self.evaluate()["passed"])

    def test_failed_revalidation_invalidates_old_summary(self):
        self.evaluate()
        self.raw["requests"] = []
        with self.assertRaises(ValueError):
            self.evaluate()
        self.assertFalse(runner.read_json(self.run / "summary.json")["passed"])

    def test_missing_native_sample_fails(self):
        self.raw["requests"] = []
        with self.assertRaisesRegex(ValueError, "missing"):
            self.evaluate()

    def test_duplicate_native_sample_fails(self):
        self.raw["requests"] *= 2
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self.evaluate()

    def test_wrong_sample_fails(self):
        self.client["requests"][0]["sample_id"] = "wrong"
        with self.assertRaisesRegex(ValueError, "unexpected"):
            self.evaluate()

    def test_changed_coordinates_fail(self):
        self.raw["requests"][0]["origin"] = [1]
        with self.assertRaisesRegex(ValueError, "origin mismatch"):
            self.evaluate()

    def test_stable_identity_rejects_edited_trace(self):
        self.trace["samples"][0]["shape"] = [2]
        with self.assertRaisesRegex(ValueError, "edited sample"):
            self.evaluate()

    def test_exact_source_version_mismatch_fails(self):
        self.client["requests"][0]["source_version"] = 18
        with self.assertRaisesRegex(ValueError, "exact source"):
            self.evaluate()

    def test_source_hash_mismatch_fails(self):
        self.raw["source_sha256"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "source hash"):
            self.evaluate()

    def test_changed_values_artifact_fails(self):
        (self.run / (self.sid + ".values.bin")).write_bytes(b"different")
        with self.assertRaisesRegex(ValueError, "value artifact"):
            self.evaluate()

    def test_changed_ipc_artifact_fails(self):
        (self.run / (self.sid + ".ipc")).write_bytes(b"bad")
        with self.assertRaisesRegex(ValueError, "IPC artifact"):
            self.evaluate()

    def test_false_client_hash_fails(self):
        self.client["requests"][0]["value_sha256"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "client value hash"):
            self.evaluate()

    def test_changed_oracle_fails(self):
        (self.root / (self.sid + ".oracle.bin")).write_bytes(b"bad")
        with self.assertRaisesRegex(ValueError, "oracle artifact"):
            self.evaluate()

    def test_negative_or_boolean_counter_fails(self):
        for value in (-1, True, 1.2):
            self.raw["requests"][0]["metrics"]["read_bytes"] = value
            with self.assertRaisesRegex(ValueError, "counter"):
                self.evaluate()

    def test_instrumentation_disagreement_fails(self):
        self.raw["requests"][0]["metrics"]["recorded_payload_bytes"] = 5
        with self.assertRaisesRegex(ValueError, "runtime/recording"):
            self.evaluate()

    def test_warm_zero_only_for_explicit_resident_contract(self):
        self.trace["samples"][0]["phase"] = "warm"
        new_id = runner.sample_identity("a" * 64, self.trace["samples"][0])
        self.trace["samples"][0]["sample_id"] = new_id
        raw = copy.deepcopy(self.raw)
        raw["requests"][0]["sample_id"] = new_id
        runner.validate_raw(self.trace, raw)  # capacity-limited warm decode is allowed
        raw["config"]["expect_resident"] = True
        with self.assertRaisesRegex(ValueError, "resident warm"):
            runner.validate_raw(self.trace, raw)
        raw["config"]["evict_before_request"] = True
        raw["requests"][0]["cache_action"] = "evict_before_request"
        runner.validate_raw(self.trace, raw)

    def test_live_backend_crosscheck(self):
        self.raw["config"]["backend"] = "iotdb_live"
        self.raw["requests"][0]["iotdb_delta"] = dict(payload_returned_bytes=4,
            payload_ranges=1, payload_calls=1, payload_failures=0)
        self.assertTrue(self.evaluate()["passed"])
        self.raw["requests"][0]["iotdb_delta"]["payload_calls"] = 2
        with self.assertRaisesRegex(ValueError, "runtime/IoTDB"):
            self.evaluate()

    def test_duplicate_json_keys_rejected(self):
        self.manifest.write_text('{"a":1,"a":2}')
        with self.assertRaisesRegex(ValueError, "duplicate JSON"):
            runner.read_json(self.manifest)

    def test_nonfinite_json_rejected(self):
        self.manifest.write_text('{"a":NaN}')
        with self.assertRaisesRegex(ValueError, "nonfinite"):
            runner.read_json(self.manifest)


if __name__ == "__main__":
    unittest.main()
