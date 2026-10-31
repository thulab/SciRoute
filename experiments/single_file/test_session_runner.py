import json
import copy
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import session_runner as run
import session_summary as summarize


class AccountingTests(unittest.TestCase):
    def test_session_charges_bind_once_and_its_own_release(self):
        rows = [{"total_ns": n, "session_bind_ns": 100} for n in (10, 20, 30)]
        summary = run.session_summary(1000, 50, 100, 7, rows, 3)
        self.assertEqual(summary["query_sum_ns"], 60)
        self.assertEqual(summary["session_total_ns"], 167)
        self.assertEqual(summary["lifecycle_total_ns"], 1217)
        self.assertEqual(summary["first_query_ns"], 10)
        self.assertEqual(summary["warm_p50_ns"], 25)

    def test_q1_has_its_own_close_not_a_100_read_prefix_close(self):
        summary = run.session_summary(1, 2, 3, 40, [{"total_ns": 5}], 1)
        self.assertEqual(summary["session_total_ns"], 48)
        self.assertIsNone(summary["warm_p50_ns"])
        with self.assertRaisesRegex(ValueError, "exactly Q"):
            run.session_summary(1, 2, 3, 40, [{"total_ns": 5}] * 100, 1)

    def test_smoke_cannot_be_formal_evidence(self):
        with self.assertRaisesRegex(ValueError, "smoke"):
            summarize.validate_formal({"protocol": run.PROTOCOL, "complete": True, "smoke_only": True})

    def test_formal_arguments_require_all_tiers_and_three_campaigns(self):
        argv = ["--manifest", "a", "--environment", "b", "--source-pid", "1",
                "--database", "geostime20261007", "--writer-lock", "lock", "--output", "out"]
        self.assertEqual(run.parse_args(argv).q_values, [1, 10, 100])
        with self.assertRaisesRegex(ValueError, "three paired"):
            run.parse_args(argv + ["--campaigns", "1"])
        smoke = run.parse_args(argv + ["--smoke"])
        self.assertEqual((smoke.campaigns, smoke.tiers, smoke.q_values), (1, ["small"], [1, 2]))


class ProfileTests(unittest.TestCase):
    def profile(self):
        return {"counters": {"typed_prepared_reuse": 1, "typed_cursor_next_calls": 2,
                             "typed_ipc_batches": 1, "typed_ipc_rows": 1}, "iotdb_delta": {"query_calls": 0},
                "runtime_delta": {"decode_count": 0}}

    def test_first_read_real_payload_sql_is_allowed_but_warm_is_not(self):
        p = self.profile()
        p["iotdb_delta"].update(query_calls=2, payload_calls=2)
        p["runtime_delta"]["decode_count"] = 2
        run.validate_bound_profile(p, 0)
        with self.assertRaisesRegex(ValueError, "warm"):
            run.validate_bound_profile(p, 1)

    def test_bound_read_must_reuse_prepared_plan_and_not_compile(self):
        for key in ("registration_catalog_calls", "catalog_scan_calls", "typed_prepare_calls",
                    "hdf5.selection.validation_calls", "hdf5.selection.decoder_compiles"):
            p = self.profile()
            p["counters"][key] = 1
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "compilation"):
                run.validate_bound_profile(p, 0)
        with self.assertRaises(ValueError):
            run.validate_bound_profile({"counters": {}}, 0)

    def test_each_read_requires_fresh_cursor_and_complete_ipc_encoding(self):
        for key in ("typed_cursor_next_calls", "typed_ipc_batches", "typed_ipc_rows"):
            p = self.profile()
            p["counters"][key] = 0
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "fresh cursor"):
                run.validate_bound_profile(p, 0, 1)

    def test_profile_poll_keeps_offset_until_complete_newline(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queries.jsonl"
            prefix = b"already consumed\n"
            path.write_bytes(prefix)
            stages = [b'{"ok":true,', b'{"ok":true,"operation":"bind"}',
                      b'{"ok":true,"operation":"bind"}\n']
            def delayed(_):
                path.write_bytes(prefix + stages.pop(0))
            with patch.object(run.time, "sleep", side_effect=delayed) as sleep:
                p, offset = run.consume_profile({"root": Path(directory)}, len(prefix), "bind", True)
            self.assertEqual(p, {"ok": True, "operation": "bind"})
            self.assertEqual(offset, path.stat().st_size)
            self.assertEqual(sleep.call_count, 3)

    def test_profile_poll_is_bounded_and_rejects_multiple_records(self):
        with tempfile.TemporaryDirectory() as directory:
            context = {"root": Path(directory)}
            with patch.object(run.time, "monotonic", side_effect=[0, 1, 6]), \
                 patch.object(run.time, "sleep"):
                with self.assertRaisesRegex(ValueError, "timed out"):
                    run.consume_profile(context, 0, "bind", True)
            path = Path(directory) / "queries.jsonl"
            path.write_text('{"ok":true,"operation":"bind"}\n' * 2)
            with self.assertRaisesRegex(ValueError, "count differs"):
                run.consume_profile(context, 0, "bind", True)

    def test_partial_invalid_json_is_retried_from_the_original_offset(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queries.jsonl"
            path.write_text('{"ok":\n')
            def completed(_):
                path.write_text('{"ok":true,"operation":"read"}\n')
            with patch.object(run.time, "sleep", side_effect=completed):
                p, offset = run.consume_profile({"root": Path(directory)}, 0, "read", True)
            self.assertEqual(p["operation"], "read")
            self.assertEqual(offset, path.stat().st_size)


class SessionControlTests(unittest.TestCase):
    def setUp(self):
        self.context = {"socket": "unused", "root": Path("unused")}
        self.bound = {"logical_path": "/x", "version": 9}
        self.case = {"origin": [0, 0, 0, 0], "shape": [1, 1, 1, 1], "rows": 1}
        self.source = {"shape": [2, 1, 728, 1152]}

    def test_each_bound_read_has_a_new_result_and_binding_is_released(self):
        calls = []
        def control(path, request):
            calls.append(request)
            if request["kind"] == "experiment_typed_bind":
                return {"prepared_id": "id", "ttl_ms": 300000}, 100
            return {"released": True}, 20
        with patch.object(run.base, "control", side_effect=control), \
             patch.object(run.base, "query_iotdb", side_effect=[
                {"total_ns": 10, "result_path": "/result-1"},
                {"total_ns": 12, "result_path": "/result-2"}]) as query:
            session = run.bound_iotdb_session(self.context, self.bound, self.source, self.case, 2, False)
        self.assertEqual(session["binding"]["total_ns"], 100)
        self.assertEqual(session["release"]["total_ns"], 20)
        self.assertEqual(session["unique_result_paths"], 2)
        self.assertTrue(session["release"]["released"])
        self.assertEqual(calls[-1], {"kind": "experiment_typed_release", "prepared_id": "id"})
        self.assertEqual(query.call_args.kwargs, {"prepared_id": "id"})

    def test_duplicate_old_result_is_rejected_and_still_released(self):
        with patch.object(run.base, "control", side_effect=[
                ({"prepared_id": "id", "ttl_ms": 300000}, 100), ({"released": True}, 20)]) as control, \
             patch.object(run.base, "query_iotdb", return_value={"total_ns": 1, "result_path": "/same"}):
            with self.assertRaisesRegex(ValueError, "previous result"):
                run.bound_iotdb_session(self.context, self.bound, self.source, self.case, 2, False)
        self.assertEqual(control.call_args.args[1]["kind"], "experiment_typed_release")

    def test_failed_read_still_releases_and_failed_release_is_not_accepted(self):
        with patch.object(run.base, "control", side_effect=[
                ({"prepared_id": "id", "ttl_ms": 300000}, 100), ({"released": True}, 20)]) as control, \
             patch.object(run.base, "query_iotdb", side_effect=ValueError("bad values")):
            with self.assertRaisesRegex(ValueError, "bad values"):
                run.bound_iotdb_session(self.context, self.bound, self.source, self.case, 1, False)
        self.assertEqual(control.call_count, 2)
        with patch.object(run.base, "control", side_effect=[
                ({"prepared_id": "id", "ttl_ms": 300000}, 100), ({"released": False}, 20)]), \
             patch.object(run.base, "query_iotdb", return_value={"total_ns": 1, "result_path": "/new"}):
            with self.assertRaisesRegex(ValueError, "retire binding"):
                run.bound_iotdb_session(self.context, self.bound, self.source, self.case, 1, False)

    def test_profile_offsets_are_consumed_for_bind_read_and_release(self):
        good = {"counters": {"typed_prepared_reuse": 1, "typed_cursor_next_calls": 2,
                             "typed_ipc_batches": 1, "typed_ipc_rows": 1}, "iotdb_delta": {"query_calls": 0}}
        calls = []
        def profile(context, offset, operation, enabled):
            calls.append((offset, operation))
            return good, offset + 20
        with patch.object(run.base, "control", side_effect=[
                ({"prepared_id": "id", "ttl_ms": 300000}, 100), ({"released": True}, 20)]), \
             patch.object(run.base, "query_iotdb", return_value={"total_ns": 1, "result_path": "/new"}), \
             patch.object(run, "consume_profile", side_effect=profile):
            run.bound_iotdb_session(self.context, self.bound, self.source, self.case, 1, True)
        self.assertEqual(calls, [(0, "experiment_typed_bind"), (20, "experiment_typed_read"),
                                 (40, "experiment_typed_release")])


class NativeTests(unittest.TestCase):
    def test_bound_native_uses_one_bind_and_last_close(self):
        case = {"origin": [0, 0, 0, 0], "shape": [1, 1, 1, 1], "rows": 1,
                "dtype": "<f4", "value_sha256": "v", "mask_sha256": "m"}
        source = {"shape": [2, 1, 728, 1152]}
        rows = [dict(case, source_shape=source["shape"], mode="bound", iteration=i,
                     runtime_hdf5="1.14.4", library_threadsafe=True, session_bind_ns=100,
                     session_close_ns=20 if i == 1 else 0, open_file_ns=0, open_dataset_ns=0)
                for i in range(2)]
        result = subprocess.CompletedProcess([], 0, "\n".join(json.dumps(r) for r in rows), "")
        with patch.object(run.subprocess, "run", return_value=result) as execute:
            session = run.native_bound_wave("probe", "file", source, case, 2, {})
        self.assertEqual(session["binding"]["total_ns"], 100)
        self.assertEqual(session["release"]["total_ns"], 20)
        self.assertEqual(execute.call_args.args[0][-1], "--bound")
        rows[0]["session_close_ns"] = 9
        result.stdout = "\n".join(json.dumps(r) for r in rows)
        with patch.object(run.subprocess, "run", return_value=result):
            with self.assertRaisesRegex(ValueError, "before last"):
                run.native_bound_wave("probe", "file", source, case, 2, {})


class FormalSummaryTests(unittest.TestCase):
    def fixture(self):
        data = {"protocol": run.PROTOCOL, "complete": True, "smoke_only": False,
                "q_values": [1, 10, 100], "profiling": True, "campaigns": [],
                "cache_semantics": {}, "fixed_session_scope": "private root-only",
                "durability_semantics": "fsync then close"}
        profile = {"server_total_ns": 100, "stages_ns": {"typed_ipc_encode": 30},
                   "counters": {"typed_prepared_reuse": 1, "typed_cursor_next_calls": 2,
                                "typed_ipc_batches": 1, "typed_ipc_rows": 1}, "iotdb_delta": {"query_calls": 0},
                   "runtime_delta": {"decode_count": 0}}
        for index in range(3):
            campaign = {"sources": {}, "unbound_cases": [], "bound_cases": []}
            data["campaigns"].append(campaign)
            for tier in ("small", "medium", "large"):
                campaign["sources"][tier] = {"bytes": 1000, "version": index + 1,
                    "full_copy_sha_verified": True, "source_locator_ns": 50,
                    "writes": {"fs": {"total_ns": 1000}, "iotdb": {"total_ns": 2000}}}
                for name in ("point", "small_roi", "cross_chunk"):
                    rows = {arm: [{"total_ns": 20 if arm == "fs" else 200,
                                   "server_profile": profile} for _ in range(100)]
                            for arm in ("fs", "iotdb")}
                    campaign["unbound_cases"].append({"tier": tier, "case": name,
                        "observations": rows, "summary": {arm: run.base.summary(
                            campaign["sources"][tier]["writes"][arm]["total_ns"],
                            50 if arm == "iotdb" else 0, rows[arm], [1, 10, 100])
                            for arm in ("fs", "iotdb")}})
                    case = {"tier": tier, "case": name, "sessions": [], "selection": {"rows": 1}}
                    campaign["bound_cases"].append(case)
                    for q in (1, 10, 100):
                        arms = {"fs": {"binding": {"total_ns": 100}, "release": {"total_ns": 10},
                            "observations": [{"mode": "bound", "total_ns": 5,
                                "session_bind_ns": 100, "session_close_ns": 10 if i + 1 == q else 0}
                                for i in range(q)]},
                            "iotdb": {"binding": {"total_ns": 500, "prepared_id": f"id-{q}",
                                                  "server_profile": profile},
                                "release": {"total_ns": 20, "released": True, "server_profile": profile},
                                "unique_result_paths": q, "observations": [
                                    {"total_ns": 50, "result_path": f"/q{q}-read{i}",
                                     "server_profile": profile} for i in range(q)]}}
                        case["sessions"].append({"q": q, "arms": arms, "summary": {
                            arm: run.session_summary(campaign["sources"][tier]["writes"][arm]["total_ns"],
                                50 if arm == "iotdb" else 0, arms[arm]["binding"]["total_ns"],
                                arms[arm]["release"]["total_ns"], arms[arm]["observations"], q)
                            for arm in ("fs", "iotdb")}})
        return data

    def test_formal_summary_covers_all_reads_and_generates_comparison(self):
        report = summarize.summarize(self.fixture())
        self.assertEqual(report["observations_per_arm"], 5697)
        self.assertEqual(len(report["cases"]), 9)
        q = report["cases"][0]["q"]["1"]
        self.assertEqual(q["bound"]["fs"]["session_total_ns"], 115)
        self.assertEqual(q["bound"]["iotdb"]["session_total_ns"], 570)
        text = summarize.markdown(report)
        self.assertIn("5,697 actual reads per arm", text)
        self.assertIn("FS bound session", text)

    def test_missing_close_cost_and_cross_q_result_reuse_are_rejected(self):
        original = self.fixture()
        changed = copy.deepcopy(original)
        session = changed["campaigns"][0]["bound_cases"][0]["sessions"][0]
        session["summary"]["fs"]["session_total_ns"] -= 10
        with self.assertRaisesRegex(ValueError, "accounting"):
            summarize.validate_formal(changed)
        changed = copy.deepcopy(original)
        sessions = changed["campaigns"][0]["bound_cases"][0]["sessions"]
        sessions[1]["arms"]["iotdb"]["observations"][0]["result_path"] = \
            sessions[0]["arms"]["iotdb"]["observations"][0]["result_path"]
        with self.assertRaisesRegex(ValueError, "reused an IPC"):
            summarize.validate_formal(changed)


if __name__ == "__main__":
    unittest.main()
