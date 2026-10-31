"""Supervisor failure-boundary tests; every process/service/resource action is mocked.

Only temporary JSON reports are real. No cgroup, affinity, signal handler,
service process, or database endpoint is touched.
"""
from contextlib import contextmanager, ExitStack
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from experiments.cross_version.resource_limited import run
from experiments.cross_version.hdf5_route3 import route3_control
from experiments.cross_version.version_directory import v03_startup_wrapper
from experiments.cross_version import pilot


class SupervisorFailureTests(unittest.TestCase):
    def exercise_failure(self, *, exit_code, snapshot_failure=None, running=False):
        events = []
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo, work = root/'repo', root/'work'
            repo.mkdir(); work.mkdir()
            (root/'runtime/current').mkdir(parents=True)
            (root/'runtime/run').mkdir()
            (root/'runtime/run/datanode.pid').write_text('12345')
            (work/'report.json').write_text(json.dumps({'complete':False,'query_records':7}))
            args = SimpleNamespace(repo=repo, work=work, registry=root/'registry',
                                   prepare_only=False, cpus='2,3')

            class QueryProcess:
                pid = 45678
                returncode = None if running else exit_code
                def poll(self): return self.returncode
                def send_signal(self, value): events.append(('query_signal', value))
                def terminate(self): events.append('query_terminate'); self.returncode = -signal.SIGTERM
                def wait(self, timeout=None):
                    events.append('query_wait'); self.returncode=exit_code; return self.returncode

            query = QueryProcess()

            class Manager:
                paths = {'parent':'/synthetic-owned/parent','query':'/synthetic-owned/query',
                         'database':'/synthetic-owned/database'}
                state = {'phase':'new'}
                snapshots = 0
                def create(self): events.append('resource_create'); self.state['phase']='active'
                def attach_query(self, pid):
                    self.asserted_pid=pid; events.append('query_attach')
                def snapshot(self):
                    self.snapshots += 1; events.append(('snapshot',self.snapshots))
                    if self.snapshots == snapshot_failure:
                        raise RuntimeError('synthetic snapshot failure')
                    return {'synthetic':True,'sequence':self.snapshots}
                def close(self): events.append('resource_close'); self.state['phase']='closed'

            manager = Manager()

            @contextmanager
            def service_window(arguments, report):
                events.append('service_enter')
                try:
                    yield
                finally:
                    # This stands for the existing protected external restore,
                    # deliberately surviving all exceptions from the worker.
                    events.append('service_restored')
                    report.update(original_service_restored=True,
                                  controller_restoration_verified=True,
                                  effective_settings='ASYNC/true')

            @contextmanager
            def startup_proxy(*arguments):
                events.append('database_proxy_enter')
                try: yield
                finally: events.append('database_proxy_exit')

            def popen(*arguments, **kwargs):
                events.append('query_popen')
                command=arguments[0]
                self.assertIn('enter-leaf-exec', command)
                self.assertIn('/synthetic-owned/query', command)
                self.assertIn('--gate-fd', command)
                return query

            def finish(process, resource, output):
                self.assertIs(process,query); self.assertIs(resource,manager)
                events.append('query_children_finished')
                process.returncode=exit_code

            # Per-module proxies avoid touching global OS functions or handlers.
            fake_os=SimpleNamespace(**vars(os))
            fake_os.geteuid=lambda:0
            fake_os.pipe=Mock(return_value=(21001,21002))
            fake_os.close=Mock(); fake_os.write=Mock(return_value=1)
            fake_signal=SimpleNamespace(**vars(signal))
            fake_signal.signal=Mock(return_value='previous-test-handler')
            fake_subprocess=SimpleNamespace(**vars(subprocess))
            fake_subprocess.Popen=Mock(side_effect=popen)
            fake_sys=SimpleNamespace(platform='linux',executable=sys.executable,modules=sys.modules)
            publisher=ModuleType('experiments.cross_version.pilot.publish')
            publisher.DATABASE='synthetic-no-endpoint'
            publisher.runtime=Mock(return_value=(None,None,{}))
            paired=SimpleNamespace()
            with ExitStack() as stack:
                stack.enter_context(patch.object(run,'prepare',return_value={}))
                stack.enter_context(patch.object(run,'resource_identity',return_value={'native_threads':{}}))
                stack.enter_context(patch.object(run,'CgroupV2Resources',return_value=manager))
                stack.enter_context(patch.object(run,'database_start_in_leaf',side_effect=startup_proxy))
                stack.enter_context(patch.object(run,'finish_query_children',side_effect=finish))
                stack.enter_context(patch.object(run,'os',fake_os))
                stack.enter_context(patch.object(run,'signal',fake_signal))
                stack.enter_context(patch.object(run,'subprocess',fake_subprocess))
                stack.enter_context(patch.object(run,'sys',fake_sys))
                stack.enter_context(patch.object(route3_control,'sync_window',side_effect=service_window))
                stack.enter_context(patch.object(v03_startup_wrapper,'load_runner',return_value=paired))
                stack.enter_context(patch.object(pilot,'publish',publisher,create=True))
                stack.enter_context(patch.dict(sys.modules,{'small_runner':paired,
                    'experiments.cross_version.pilot.publish':publisher}))
                with self.assertRaises(RuntimeError) as raised:
                    run.supervise(args)

            self.assertEqual(manager.asserted_pid,query.pid)
            self.assertIn('service_restored',events)
            self.assertIn('resource_close',events)
            self.assertLess(events.index('query_children_finished'),events.index('service_restored'))
            self.assertLess(events.index('service_restored'),events.index('resource_close'))
            final=json.loads((work/'report.json').read_text())
            self.assertIs(final['complete'],False)
            self.assertIs(final['scientific_complete'],False)
            self.assertIs(final['original_service_restored'],True)
            self.assertIs(final['controller_restoration_verified'],True)
            self.assertIs(final['resources_restored'],True)
            self.assertEqual(final['query_records'],7)
            snapshot_receipt=work/'resource-final-snapshot-failure.json'
            return dict(events=events,error=str(raised.exception),
                        final_snapshot_failed=snapshot_receipt.exists())

    def test_worker_failure_restores_service_before_resource_close(self):
        result=self.exercise_failure(exit_code=1)
        self.assertIn('worker failed',result['error'])
        self.assertFalse(result['final_snapshot_failed'])

    def test_oom_worker_and_final_snapshot_failure_still_attempt_close_after_service_restore(self):
        # An OOM SIGKILL has no Python worker finally; the outer guard is decisive.
        result=self.exercise_failure(exit_code=-signal.SIGKILL,snapshot_failure=2)
        self.assertIn('worker failed',result['error'])
        self.assertTrue(result['final_snapshot_failed'])

    def test_monitoring_snapshot_exception_unwinds_live_worker_then_service_then_resources(self):
        result=self.exercise_failure(exit_code=-signal.SIGTERM,snapshot_failure=2,running=True)
        self.assertIn('synthetic snapshot failure',result['error'])
        self.assertFalse(result['final_snapshot_failed'])
        self.assertIn(('snapshot',3),result['events'])


if __name__=='__main__':unittest.main()
