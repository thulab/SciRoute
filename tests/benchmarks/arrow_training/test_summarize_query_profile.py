"""Pure-stdlib checks for attribution boundaries and profile joins."""
import json
from pathlib import Path
import tempfile
import unittest

from summarize_query_profile import client_requests, read_server_log, summarize


def record(path="/mount/result-a.ipc", *, pid=10, sequence=1, start=0, end=100):
    return {"protocol": "iotdbfs-query-profile/1", "path": path, "pid": pid,
            "sequence": sequence, "ok": True, "steady_start_ns": start,
            "steady_end_ns": end, "server_total_ns": 1000000000,
            "stages_ns": {"snapshot_prepare": 100000000, "publish_route": 800000000,
                          "window_execute": 600000000, "window_assemble": 400000000,
                          "ipc_encode": 100000000},
            "runtime_delta": {"misses": 7, "read_bytes": 7 * 16384},
            "runtime_after": {"live_bytes": 12345}}


class ProfileSummaryTests(unittest.TestCase):
    def test_nested_preparation_details_do_not_inflate_parent_total(self):
        server = record()
        server["stages_ns"].update(snapshot_fingerprint=30000000,
                                  snapshot_catalog_scan=20000000,
                                  snapshot_view_validate=40000000)
        result = summarize([server], [{"path": server["path"],
            "request_encode_seconds": .002, "socket_roundtrip_seconds": 1.0}])
        self.assertEqual(result["server_seconds"]["prepare_total"]["sum"], .1)
        self.assertEqual(result["server_seconds"]["snapshot_fingerprint"]["sum"], .03)
        self.assertEqual(result["client_seconds"]["request_encode_seconds"]["sum"], .002)

    def test_join_units_and_parent_child_boundaries(self):
        server = record()
        result = summarize([server], [{"path": server["path"], "ipc_bytes": 500,
                                      "registration_seconds": 1.1}])
        self.assertEqual(result["attributable_count"], 1)
        self.assertEqual(result["server_seconds"]["prepare_total"]["sum"], .1)
        self.assertEqual(result["server_seconds"]["server_total"]["sum"], 1)
        self.assertEqual(result["server_seconds"]["window_execute"]["sum"], .6)
        self.assertEqual(result["server_seconds"]["window_assemble"]["sum"], .4)
        self.assertEqual(result["server_seconds"]["window_transform"]["sum"], 0)
        self.assertEqual(result["runtime_delta"]["misses"]["sum"], 7)
        self.assertEqual(result["requests"][0]["server"]["runtime_after"]["live_bytes"], 12345)
        self.assertEqual(result["client_seconds"]["stat_seconds"]["count"], 0)

    def test_unselected_failed_request_still_prevents_attribution(self):
        server = record()
        concurrent = record("", sequence=2, start=90, end=120)
        concurrent["ok"] = False
        result = summarize([server, concurrent], [{"path": server["path"]}])
        self.assertEqual(result["attributable_count"], 0)
        self.assertEqual(result["runtime_delta"]["misses"]["count"], 0)
        self.assertEqual(result["requests"][0]["reason"], "overlapping_daemon_requests")
        self.assertEqual(result["requests"][0]["overlapping_requests"][0]["sequence"], 2)

    def test_different_pid_and_touching_intervals_do_not_conflict(self):
        server = record()
        others = [record("other-pid", pid=11), record("next", sequence=2, start=100, end=200)]
        result = summarize([server] + others, [{"path": server["path"]}])
        self.assertEqual(result["attributable_count"], 1)

    def test_ambiguous_missing_failed_paths_are_retained(self):
        rows = [record(), record(sequence=2, start=110, end=210)]
        result = summarize(rows, [{"path": rows[0]["path"]}, {"path": "unknown"}, {"path": None}])
        self.assertEqual(result["excluded_count"], 3)
        self.assertEqual([item["reason"] for item in result["requests"]],
                         ["ambiguous_path", "unmatched_path", "client_returned_no_path"])
        failed = record("failed")
        failed["ok"] = False
        self.assertEqual(summarize([failed], [{"path": "failed"}])["requests"][0]["reason"],
                         "server_failed")

    def test_training_split_and_probe_failures(self):
        report = {"backend_metrics": {"train": {"request_timings": [{"path": "train"}]},
                                      "validation": {"request_timings": [{"path": "eval"}]}}}
        self.assertEqual(client_requests(report, "validation"), [{"path": "eval"}])
        probe = {"anchors": [50, 90], "cases": [{"paths": ["ok", None], "seconds": [.1, .2],
                                                  "channels": 7, "valid_coordinate": False}]}
        requests = client_requests(probe, "train", True)
        self.assertEqual([r["path"] for r in requests], ["ok", None])
        self.assertEqual(requests[1]["registration_seconds"], .2)
        self.assertEqual(requests[1]["anchors"], [50, 90])
        probe["cases"][0]["paths"].pop()
        with self.assertRaises(ValueError):
            client_requests(probe, "train", True)
        with self.assertRaises(ValueError):
            client_requests({"backend_metrics": {"train": {}}}, "train")

    def test_summary_statistics_and_nanosecond_counters_stay_in_units(self):
        servers = [record(str(i), sequence=i, start=100*i, end=100*i+99) for i in range(3)]
        for i, server in enumerate(servers):
            server["runtime_delta"]["payload_read_nanoseconds"] = i * 1000000000
        result = summarize(servers, [{"path": str(i)} for i in range(3)])
        stat = result["runtime_delta"]["payload_read_nanoseconds"]
        self.assertEqual(stat["sum"], 3000000000)
        self.assertEqual(stat["p50"], 1000000000)
        self.assertEqual(stat["p95"], 1900000000)

    def test_payload_shape_aggregates_and_rejects_duplicate_cells(self):
        server = record()
        server["payload_read_shape"] = [
            {"range_bin": 4, "byte_bin": 3, "calls": 2,
             "nanoseconds": 6000000, "bytes": 1000000, "ranges": 100}]
        server["runtime_delta"].update(payload_read_calls=2,
                                       payload_read_nanoseconds=6000000)
        result = summarize([server], [{"path": server["path"]}])
        self.assertEqual(result["payload_read_shape"]["cells"][0]["mean_milliseconds_per_call"], 3)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "server.jsonl"
            path.write_text(json.dumps(server) + "\n")
            self.assertEqual(len(read_server_log(path)), 1)
            server["payload_read_shape"] *= 2
            path.write_text(json.dumps(server) + "\n")
            with self.assertRaisesRegex(ValueError, "duplicate|bucket"):
                read_server_log(path)
            server["payload_read_shape"] = server["payload_read_shape"][:1]
            server["runtime_delta"]["payload_read_calls"] = 3
            path.write_text(json.dumps(server) + "\n")
            with self.assertRaisesRegex(ValueError, "does not match"):
                read_server_log(path)

    def test_log_validation_rejects_truncation_duplicate_identity_and_bad_times(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "server.jsonl"
            path.write_text(json.dumps(record()) + "\n")
            self.assertEqual(len(read_server_log(path)), 1)
            path.write_text(path.read_text() * 2)
            with self.assertRaisesRegex(ValueError, "duplicate"):
                read_server_log(path)
            path.write_text('{"protocol":')
            with self.assertRaisesRegex(ValueError, ":1:"):
                read_server_log(path)
            path.write_text(json.dumps(record(start=200, end=100)))
            with self.assertRaisesRegex(ValueError, "reversed"):
                read_server_log(path)


if __name__ == "__main__":
    unittest.main()
