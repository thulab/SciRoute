"""Small synthetic aggregate fixtures; no server, model or native library."""
from copy import deepcopy
import gzip
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import grib_paired_summary as summary


def fixture():
    digest = "a" * 64
    data = {"protocol": summary.PROTOCOL, "complete": True, "smoke_only": False,
            "q_values": [1, 10, 100], "tiers": ["small", "medium"], "campaigns": [],
            "data_manifest_sha256": digest, "daemon_sha256": digest, "native_sha256": digest,
            "frozen_session_scope": "readonly", "publication_scope": "fsync+close",
            "timing_scope": "query only"}
    for campaign_index in range(3):
        campaign = {"index": campaign_index, "prefix": f"/pair/{campaign_index}",
                    "query_order": ["fs", "iotdb"], "mode_order": ["unbound", "bound"],
                    "sources": {}, "cases": []}
        for tier in summary.TIERS:
            source = {"raw": tier + ".grib", "logical_path": f"/{campaign_index}/{tier}.grib",
                "version": campaign_index + 1, "bytes": 1000, "sha256": digest,
                "writes": {"fs": {"total_ns": 20}, "iotdb": {"total_ns": 40}},
                "source_locator_ns": 10, "full_copy_sha_verified": True}
            campaign["sources"][tier] = source
            for name in summary.CASES:
                rows = 1 if name != "roi" else 16
                message = {"offset": 100, "length": 100, "shape": [], "rows": rows,
                           **{key: digest for key in summary.HASH_KEYS}}
                oracle = {"candidate_messages": 22, "selected_messages": 1, "rows": rows,
                          "message_offsets": [100], "message_lengths": [100], "messages": [message],
                          **{key: digest for key in summary.HASH_KEYS}}
                selection = {"view": "binary" if name == "binary_one" else "values",
                             "filter": {"level": 500}, "oracle": oracle}
                for mode in summary.MODES:
                    case = {"tier": tier, "case": name, "mode": mode,
                            "selection": deepcopy(selection), "sessions": []}
                    for q in summary.QS:
                        session = {"q": q, "arms": {}, "summary": {}}
                        for arm in ("fs", "iotdb"):
                            bind = 15 if mode == "bound" else 0
                            release = 5 if mode == "bound" else 0
                            info = {"binding_ns": bind, "release_ns": release, "observations": []}
                            if arm == "fs":
                                info["native_process_wall_ns"] = 10000
                            elif mode == "bound":
                                info.update(released=True,
                                    prepared_id=f"{campaign_index}-{tier}-{name}-{q}")
                            for index in range(q):
                                row = {"rows": rows, "total_ns": (3 if arm == "fs" else 6),
                                       **{key: digest for key in summary.HASH_KEYS}}
                                if arm == "fs":
                                    row.update(iteration=index, mode=mode, view=selection["view"],
                                        source_bytes=1000, session_bind_ns=bind if index == 0 else 0,
                                        session_close_ns=release if index == q - 1 else 0,
                                        **{key: deepcopy(oracle[key]) for key in ("candidate_messages",
                                            "selected_messages", "message_offsets", "message_lengths", "messages")})
                                    row.update({key: 0 for key in ("discovery_ns", "payload_read_ns",
                                        "decode_ns", "selection_setup_ns", "owned_copy_ns",
                                        "actual_payload_read_bytes", "payload_read_calls", "decoded_messages",
                                        "cache_hits", "cache_misses", "cache_resident_bytes")})
                                else:
                                    row.update(registration_ns=1, mmap_open_ns=1,
                                               ipc_decode_ns=1, owned_copy_close_ns=1)
                                    counters = {"typed_cursor_next_calls": 2, "typed_ipc_batches": 1,
                                                "typed_ipc_rows": rows}
                                    if mode == "bound":
                                        counters["typed_prepared_reuse"] = 1
                                    row.update(index=index,
                                        result_path=f"/route/{campaign_index}/{tier}/{name}/{mode}/{q}/{index}",
                                        server_profile={"ok": True, "operation": "typed" if mode == "unbound" else "experiment_typed_read",
                                            "counters": counters, "server_total_ns": 5,
                                            "iotdb_delta": {"query_calls": 1, "payload_calls": 1,
                                                             "payload_returned_bytes": 100}})
                                info["observations"].append(row)
                            session["arms"][arm] = info
                            session["summary"][arm] = summary.accounting(source, arm, info, q)
                        case["sessions"].append(session)
                    campaign["cases"].append(case)
        data["campaigns"].append(campaign)
    return data


class SummaryTests(unittest.TestCase):
    def setUp(self):
        self.data = fixture()

    def test_valid_and_independent_totals(self):
        report = summary.summarize(self.data)
        self.assertEqual(report["observations_per_arm"], 3996)
        case = report["cases"][0]
        bound = case["modes"]["bound"]["q"]["10"]
        self.assertEqual(bound["fs"]["query_sum_ns"], 30)
        self.assertEqual(bound["fs"]["session_total_ns"], 50)
        self.assertEqual(bound["fs"]["lifecycle_total_ns"], 70)
        self.assertEqual(bound["iotdb"]["session_total_ns"], 80)
        self.assertEqual(bound["iotdb"]["lifecycle_total_ns"], 130)
        self.assertEqual(bound["paired_session_total_ns_ba_ratio"], 1.6)
        self.assertEqual(case["modes"]["bound"]["warm"]["fs"]["observations"], 297)
        self.assertIn("Shared-runtime decode counters", summary.markdown(report))
        self.assertIn("Publication scope: fsync+close", summary.markdown(report))
        self.assertEqual(report["native_sha256"], "a" * 64)

    def test_actual_reads_must_equal_q(self):
        self.data["campaigns"][0]["cases"][0]["sessions"][1]["arms"]["fs"]["observations"].pop()
        with self.assertRaisesRegex(ValueError, "exactly Q"):
            summary.validate_formal(self.data)

    def test_q100_prefix_cannot_substitute_close(self):
        case = next(c for c in self.data["campaigns"][0]["cases"] if c["mode"] == "bound")
        case["sessions"][0]["arms"]["fs"]["observations"] = deepcopy(
            case["sessions"][2]["arms"]["fs"]["observations"][:1])
        with self.assertRaisesRegex(ValueError, "session close"):
            summary.validate_formal(self.data)

    def test_q10_prefix_cannot_substitute_close(self):
        case = next(c for c in self.data["campaigns"][0]["cases"] if c["mode"] == "bound")
        case["sessions"][1]["arms"]["fs"]["observations"] = deepcopy(
            case["sessions"][2]["arms"]["fs"]["observations"][:10])
        with self.assertRaisesRegex(ValueError, "session close"):
            summary.validate_formal(self.data)

    def test_tampered_oracle(self):
        self.data["campaigns"][0]["cases"][0]["selection"]["oracle"]["value_sha256"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "raw oracle"):
            summary.validate_formal(self.data)

    def test_partial_and_smoke_rejected(self):
        for key, value in (("complete", False), ("smoke_only", True)):
            data = deepcopy(self.data)
            data[key] = value
            with self.assertRaisesRegex(ValueError, "incomplete"):
                summary.validate_formal(data)

    def test_native_session_bind_repeated_or_omitted(self):
        row = self.data["campaigns"][0]["cases"][0]["sessions"][0]["arms"]["fs"]["observations"][0]
        row["session_bind_ns"] = 1
        with self.assertRaisesRegex(ValueError, "bind must"):
            summary.validate_formal(self.data)

    def test_native_bind_only_charged_on_first_row(self):
        case = next(c for c in self.data["campaigns"][0]["cases"] if c["mode"] == "bound")
        row = case["sessions"][1]["arms"]["fs"]["observations"][1]
        row["session_bind_ns"] = case["sessions"][1]["arms"]["fs"]["binding_ns"]
        with self.assertRaisesRegex(ValueError, "bind must"):
            summary.validate_formal(self.data)

    def test_modified_accounting(self):
        self.data["campaigns"][0]["cases"][0]["sessions"][0]["summary"]["fs"]["lifecycle_total_ns"] += 1
        with self.assertRaisesRegex(ValueError, "accounting"):
            summary.validate_formal(self.data)

    def test_modified_source_fingerprint(self):
        self.data["campaigns"][1]["sources"]["small"]["sha256"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "raw bytes"):
            summary.validate_formal(self.data)

    def test_cannot_count_old_ipc_result_as_query(self):
        rows = self.data["campaigns"][0]["cases"][0]["sessions"][1]["arms"]["iotdb"]["observations"]
        rows[1]["result_path"] = rows[0]["result_path"]
        with self.assertRaisesRegex(ValueError, "earlier IPC route"):
            summary.validate_formal(self.data)

    def test_cursor_and_encode_must_run(self):
        profile = self.data["campaigns"][0]["cases"][0]["sessions"][0]["arms"]["iotdb"]["observations"][0]["server_profile"]
        profile["counters"]["typed_ipc_batches"] = 0
        with self.assertRaisesRegex(ValueError, "fresh cursor"):
            summary.validate_formal(self.data)

    def test_bound_binding_release_required(self):
        case = next(c for c in self.data["campaigns"][0]["cases"] if c["mode"] == "bound")
        case["sessions"][0]["arms"]["iotdb"]["released"] = False
        with self.assertRaisesRegex(ValueError, "not released"):
            summary.validate_formal(self.data)

    def test_parsed_summary_from_gzip_cli(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "campaign.json.gz"
            raw = json.dumps(self.data).encode()
            source.write_bytes(gzip.compress(raw))
            result = subprocess.run([sys.executable, str(Path(summary.__file__)),
                "--campaign", str(source), "--output", str(root / "result")],
                capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads((root / "result" / "summary.json").read_text())
            self.assertEqual(report["campaign_sha256"], __import__("hashlib").sha256(raw).hexdigest())
            self.assertTrue((root / "result" / "RESULTS.md").is_file())


if __name__ == "__main__":
    unittest.main()
