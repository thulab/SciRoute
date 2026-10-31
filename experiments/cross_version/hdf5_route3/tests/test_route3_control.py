#!/usr/bin/env python3
"""Offline cancellation/restoration tests; never signal or start a real process."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import signal
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from experiments.cross_version.hdf5_route3 import route3_control as control


def request_cancel(sig=signal.SIGTERM):
    """Exercise the handler actually installed by sync_window, without os.kill."""
    installed = signal.getsignal(sig)
    if not callable(installed):
        raise AssertionError("sync_window did not install a callable handler")
    installed(sig, None)


class ControlTests(unittest.TestCase):
    @contextmanager
    def fixture(self, *, enable_hook=None, restore_hook=None, verify_hook=None):
        before = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGHUP)}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, work = root / "runtime", root / "work"
            (runtime / "run").mkdir(parents=True)
            (runtime / "current").mkdir()
            work.mkdir()
            args = SimpleNamespace(runtime=runtime, release=(runtime / "current").resolve(),
                work=work, report=work / "report.json", datanode_pid=5, repo=root, env={"IOTDB_PASS": "test"})
            calls, instances, report = [], [], {"original_service_restored": True}

            class FakeInstance:
                def __init__(self, supplied, event):
                    self.pid = supplied.datanode_pid
                    self.original = b"wal_mode=ASYNC\nenable_partial_insert=true\n"
                    self.config = supplied.work / "original.properties"
                    self.config.write_bytes(self.original)
                    self.changed = False
                    self.event = event
                    calls.append("factory")
                    instances.append(self)

                def enable_sync(self):
                    calls.append("enable")
                    if enable_hook:
                        enable_hook(self, calls)
                    self.config.write_bytes(b"wal_mode=SYNC\nenable_partial_insert=false\n")
                    self.changed = True
                    self.pid = 6
                    calls.append("enable-finished")

                def restore(self):
                    calls.append("restore")
                    if restore_hook:
                        restore_hook(self, calls)
                    self.config.write_bytes(self.original)
                    self.pid = 7
                    calls.append("restore-finished")

            def verifier(supplied, instance, expected):
                calls.append("verify-" + expected[0])
                if verify_hook:
                    verify_hook(instance, expected, calls)
                return {"pid": instance.pid,
                    "config_sha256": hashlib.sha256(instance.config.read_bytes()).hexdigest(),
                    "effective_settings": "/".join(expected),
                    "protected_hashes_verified": True, "operation": "fake SHOW settings"}

            with patch.object(control.sys, "platform", "linux"), \
                 patch.object(control.os, "geteuid", return_value=0):
                yield args, report, calls, instances, FakeInstance, verifier
            for sig, previous in before.items():
                self.assertEqual(signal.getsignal(sig), previous, "installed signal handler leaked")

    def test_cancel_during_enable_finishes_transition_restores_and_never_yields(self):
        def hook(instance, calls):
            request_cancel()
            calls.append("cancel-deferred")
        with self.fixture(enable_hook=hook) as (args, report, calls, instances, factory, verifier):
            with self.assertRaisesRegex(RuntimeError, "canceled after safe service transition"):
                with control.sync_window(args, report, instance_factory=factory, verifier=verifier):
                    self.fail("canceled enable must not yield the experiment body")
            self.assertEqual(calls, ["factory", "enable", "cancel-deferred", "enable-finished",
                                    "verify-SYNC", "restore", "restore-finished", "verify-ASYNC"])
            self.assertTrue(report["original_service_restored"])
            self.assertTrue(report["cancel_requested"])
            self.assertEqual(report["effective_settings"], "ASYNC/true")
            self.assertEqual(instances[0].config.read_bytes(), instances[0].original)

    def test_cancel_during_sync_settings_verification_is_also_deferred(self):
        def hook(instance, expected, calls):
            if expected[0] == "SYNC":
                request_cancel(signal.SIGHUP)
                calls.append("verify-cancel-deferred")
        with self.fixture(verify_hook=hook) as (args, report, calls, instances, factory, verifier):
            with self.assertRaisesRegex(RuntimeError, "canceled after safe service transition"):
                with control.sync_window(args, report, instance_factory=factory, verifier=verifier):
                    self.fail("canceled verification must not yield")
            self.assertIn("verify-cancel-deferred", calls)
            self.assertTrue(report["original_service_restored"])

    def test_body_cancel_restores_and_repeated_restore_cancellation_cannot_interrupt(self):
        def restore(instance, calls):
            request_cancel()
            request_cancel(signal.SIGHUP)
            calls.append("restore-cancel-deferred")
        def verify(instance, expected, calls):
            if expected[0] == "ASYNC":
                request_cancel()
                calls.append("restore-verify-cancel-deferred")
        with self.fixture(restore_hook=restore, verify_hook=verify) as (args, report, calls, instances, factory, verifier):
            with self.assertRaisesRegex(RuntimeError, "canceled; restore original service"):
                with control.sync_window(args, report, instance_factory=factory, verifier=verifier):
                    calls.append("body")
                    request_cancel()
                    self.fail("body cancel must interrupt promptly")
            self.assertEqual(calls[-5:], ["restore", "restore-cancel-deferred", "restore-finished",
                                         "verify-ASYNC", "restore-verify-cancel-deferred"])
            self.assertTrue(report["original_service_restored"])
            self.assertTrue(report["controller_restoration_verified"])
            self.assertTrue(report["cancel_requested"])

    def test_early_enable_failure_still_restores_and_verifies(self):
        def fail(instance, calls):
            raise ValueError("early-enable-failure")
        with self.fixture(enable_hook=fail) as (args, report, calls, instances, factory, verifier):
            with self.assertRaisesRegex(ValueError, "early-enable-failure"):
                with control.sync_window(args, report, instance_factory=factory, verifier=verifier):
                    self.fail("failed enable must not yield")
            self.assertEqual(calls, ["factory", "enable", "restore", "restore-finished", "verify-ASYNC"])
            self.assertTrue(report["original_service_restored"])

    def test_disconnected_progress_stream_cannot_interrupt_service_transitions(self):
        def transition(instance, calls):
            instance.event("before_service_signal", pid=instance.pid, signal="SIGTERM")
            calls.append("signal-logged")
        with self.fixture(enable_hook=transition, restore_hook=transition) as (args, report, calls, instances, factory, verifier):
            with patch("builtins.print", side_effect=BrokenPipeError("closed SSH progress stream")):
                with control.sync_window(args, report, instance_factory=factory, verifier=verifier):
                    calls.append("body")
            self.assertTrue(report["original_service_restored"])
            self.assertTrue(report["controller_restoration_verified"])
            self.assertEqual(calls.count("signal-logged"), 2)
            self.assertIn("body", calls)
            self.assertEqual(instances[0].config.read_bytes(), instances[0].original)
            events = [json.loads(line)["event"] for line in
                      (args.work / "control/operations.jsonl").read_text().splitlines()]
            self.assertEqual(events, ["before_service_signal", "route3_effective_sync_verified",
                                      "before_service_signal", "route3_service_restored_verified"])

    def test_failed_effective_settings_never_reports_restored(self):
        def fail(instance, expected, calls):
            if expected[0] == "ASYNC":
                raise RuntimeError("actual settings are SYNC/false")
        with self.fixture(verify_hook=fail) as (args, report, calls, instances, factory, verifier):
            with self.assertRaisesRegex(RuntimeError, "actual settings"):
                with control.sync_window(args, report, instance_factory=factory, verifier=verifier):
                    pass
            self.assertFalse(report["original_service_restored"])
            self.assertEqual(report["restoration_failure"], {"type": "RuntimeError"})
            persisted = json.loads(args.report.read_text())
            self.assertFalse(persisted["original_service_restored"])
            self.assertNotIn("controller_restoration_verified", persisted)

    def test_changed_config_bytes_prevent_restoration_success(self):
        def corrupt(instance, expected, calls):
            if expected[0] == "ASYNC":
                instance.config.write_bytes(b"different")
        with self.fixture(verify_hook=corrupt) as (args, report, calls, instances, factory, verifier):
            with self.assertRaisesRegex(RuntimeError, "original config bytes differ"):
                with control.sync_window(args, report, instance_factory=factory, verifier=verifier):
                    pass
            self.assertFalse(report["original_service_restored"])

    def test_stop_marks_intent_before_parent_and_passes_crash(self):
        instance = control.ProtectedExistingInstance.__new__(control.ProtectedExistingInstance)
        instance.stop_requested = False
        instance.live = lambda: True
        def parent_stop(supplied, crash=False):
            self.assertIs(supplied, instance)
            self.assertTrue(supplied.stop_requested)
            self.assertTrue(crash)
        with patch.object(control.ExistingInstance, "stop", autospec=True, side_effect=parent_stop):
            instance.stop(crash=True)

    def test_restore_drains_stop_intent_before_unchanged_parent_restore(self):
        for requested, changed, expected in ((True, False, ["adopt", "drain", "parent-restore"]),
                                             (True, True, ["adopt", "parent-restore"]),
                                             (False, False, ["adopt", "parent-restore"])):
            with self.subTest(stop_requested=requested, changed=changed):
                calls = []
                instance = control.ProtectedExistingInstance.__new__(control.ProtectedExistingInstance)
                instance.stop_requested, instance.changed = requested, changed
                instance.adopt_started = lambda: calls.append("adopt")
                instance.live = lambda: True
                with patch.object(control.ExistingInstance, "stop", autospec=True,
                                  side_effect=lambda self, crash=False: calls.append("drain")), \
                     patch.object(control.ExistingInstance, "restore", autospec=True,
                                  side_effect=lambda self: calls.append("parent-restore")):
                    instance.restore()
                self.assertEqual(calls, expected)

    def test_early_stop_failure_is_drained_by_real_protected_restore(self):
        with self.fixture() as (args, report, calls, instances, factory, verifier):
            instance = control.ProtectedExistingInstance.__new__(control.ProtectedExistingInstance)
            instance.stop_requested, instance.changed = False, False
            instance.pid = 5
            instance.config = args.work / "protected.properties"
            instance.original = b"original"
            instance.config.write_bytes(instance.original)
            instance.live = lambda: True
            instance.adopt_started = lambda: calls.append("adopt")
            instance.enable_sync = lambda: instance.stop()
            stop_calls = []
            def parent_stop(self, crash=False):
                stop_calls.append(self.stop_requested)
                calls.append("parent-stop")
                if len(stop_calls) == 1:
                    raise ValueError("interrupted-before-config-change")
            with patch.object(control.ExistingInstance, "stop", autospec=True, side_effect=parent_stop), \
                 patch.object(control.ExistingInstance, "restore", autospec=True,
                              side_effect=lambda self: calls.append("parent-restore")):
                with self.assertRaisesRegex(ValueError, "interrupted-before-config-change"):
                    with control.sync_window(args, report, instance_factory=lambda args, event: instance,
                                             verifier=verifier):
                        self.fail("failed stop must not yield")
            self.assertEqual(stop_calls, [True, True])
            self.assertEqual(calls, ["parent-stop", "adopt", "parent-stop", "parent-restore", "verify-ASYNC"])
            self.assertTrue(report["original_service_restored"])

    def test_effective_verifier_rejects_wrong_actual_settings_response(self):
        with self.fixture() as (args, report, calls, instances, factory, verifier):
            control_dir = args.work / "control"
            control_dir.mkdir()
            instance = factory(SimpleNamespace(work=control_dir, datanode_pid=5), lambda *a, **k: None)
            instance.live = lambda: True
            instance.pid_file = args.work / "pid"
            instance.pid_file.write_text("5")
            instance.identity = {"pid": 5}
            instance.process_identity = lambda pid: {"pid": pid}
            instance.check_protected = lambda: None
            worker = args.repo / "experiments/cross_version/work/v00-head-effective-20261015-r3/v00_head_worker"
            worker.parent.mkdir(parents=True)
            worker.write_bytes(b"fake-worker")
            real_sha = hashlib.sha256
            def digest(value):
                if value == b"fake-worker":
                    return SimpleNamespace(hexdigest=lambda: "2575ba1f3c5b151a00521bd94cc90fdc30e1f91d1fdcd0490a80f985102e8221")
                return real_sha(value)
            with patch.object(control.hashlib, "sha256", side_effect=digest), \
                 patch.object(control.subprocess, "run", return_value=SimpleNamespace(
                     returncode=0, stdout="READY\nCONFIG\tSYNC\tfalse\n")):
                with self.assertRaisesRegex(RuntimeError, "effective settings were not verified"):
                    control.verify_effective(args, instance, ("ASYNC", "true"))


if __name__ == "__main__":
    unittest.main()
