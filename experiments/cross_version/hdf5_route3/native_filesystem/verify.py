"""Original-database functional gate, not a new scientific performance case.

Uses the approved two-CPU/query-2GiB envelope and the existing service/license.
Default: fresh functional-fixture prefix, zero scientific queries. Optional
timing: supplement file locations on two sealed FM1 cases, then collect 72
queries; preserve scientific versions, original journals, oracle and FS index.
"""
from pathlib import Path
import argparse
import json
import os
import signal
import subprocess
import sys
import time
import uuid

from experiments.fixed_matrix import run, telemetry
from experiments.cross_version.hdf5_route3 import route3_control
from experiments.cross_version.resource_limited.resources import CgroupV2Resources, atomic_json
from experiments.identity.registry import file_sha
from experiments.identity.model import digest


def functional_result(lines, prefix):
    results = [json.loads(line) for line in lines.splitlines() if line.startswith('{')]
    run.need(len(results) == 1, 'functional result count differs')
    result = results[0]
    run.need(result.get('completed') is True and result.get('command') == 'native_file_functional'
        and result.get('prefix') == prefix and result.get('mode') == 'native-hdf5'
        and result.get('data_backend') == 'managed-filesystem'
        and all(result.get(key) is True for key in ('actual_backend', 'existing_database',
            'real_hdf5_fixture', 'fixtures_preserved')), 'functional backend completion missing')
    cases = result.get('cases', [])
    run.need(len(cases) == len(set(cases)) == result.get('case_count') and len(cases) >= 8,
        'functional cases incomplete')
    observations = result['observations']
    run.need(all(observations.get(key) == 0 for key in ('cold_execute_payload_calls',
            'warm_execute_payload_calls', 'cold_execute_sql', 'warm_execute_sql',
            'cold_execute_store_queries', 'warm_execute_store_queries',
            'unbound_execute_payload_calls', 'missing_execute_payload_calls',
            'replaced_execute_payload_calls'))
        and all(observations.get(key, 0) > 0 for key in ('cold_filesystem_calls', 'warm_filesystem_calls')),
        'functional filesystem data path not verified')
    run.need(all(result.get('recovery', {}).get(key) is True for key in ('unbound_namespace',
        'bound_namespace', 'exact_record_preserved', 'locators_preserved')), 'recovery proof incomplete')
    return result


def gate_complete(report, *, sampler_ok, timing_required):
    """A restored service alone must never turn a failed timing gate green."""
    return bool('failure' not in report and sampler_ok
        and all(report.get(key) is True for key in ('functional_complete',
            'original_service_restored', 'controller_restoration_verified', 'resources_restored'))
        and (report.get('timing_probe_complete') is True and report.get('scientific_queries') == 72
             if timing_required else report.get('scientific_queries') == 0))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('repo', 'work', 'probe'):
        parser.add_argument('--' + key, type=Path, required=True)
    parser.add_argument('--timing-probe', action='store_true', help='Existing FM1 HDF5 N8/N128 only, 72 queries, one diagnostic block')
    parser.add_argument('--reuse-functional', type=Path, help='Reuse completed raw functional proof with separately restored resources')
    for key in ('binary', 'build-binding', 'library', 'source-collection', 'inspector'):
        parser.add_argument('--' + key, type=Path)
    args = parser.parse_args()
    args.repo = args.repo.resolve(strict=True)
    args.probe = args.probe.resolve(strict=True)
    if args.timing_probe:
        for key in ('binary', 'build_binding', 'library', 'source_collection', 'inspector'):
            run.need(getattr(args, key) is not None, 'timing-probe dependencies must be explicit')
            setattr(args, key, getattr(args, key).resolve(strict=True))
    args.work = args.work.resolve()
    args.work.mkdir(parents=True, mode=0o700, exist_ok=False)
    paired, publisher, args.env, startup = run.dependencies(args.repo)
    args.runtime = args.repo.parent / 'runtime'
    args.release = (args.runtime / 'current').resolve(strict=True)
    args.datanode_pid = int((args.runtime / 'run/datanode.pid').read_text())
    args.report = args.work / 'service-report.json'
    software = run.software_inventory(args.repo)
    atomic_json(args.work / 'software.json', software)
    atomic_json(args.work / 'environment.json', telemetry.environment(
        args.repo, artifact_paths=[args.probe], process_roles=software['process_roles'], data_path=args.work))
    atomic_json(args.work / 'support-roles.json', {
        key: value for key, value in software['process_roles'].items() if key != 'datanode'})
    manager = CgroupV2Resources([2, 3], args.work / 'resources.json',
        database_pid_file=args.runtime / 'run/datanode.pid', keeper_seconds=1800)
    atomic_json(args.work / 'resource-paths.json', manager.paths)
    report = dict(protocol='iotdbfs-native-filesystem-functional/1', complete=False,
        scientific_queries=0, performance_claims=False, prefix='vd_native_fs_' + uuid.uuid4().hex,
        probe_sha256=file_sha(args.probe), started_ns=time.time_ns())
    reuse = None
    if args.reuse_functional:
        previous = args.reuse_functional.resolve(strict=True)
        prior = json.loads((previous / 'report.json').read_text())
        run.need(prior.get('probe_exit_code') == 0 and prior.get('probe_sha256') == file_sha(args.probe)
            and all(prior.get(key) is True for key in ('original_service_restored',
                'controller_restoration_verified', 'resources_restored'))
            and prior.get('scientific_queries') == 0, 'functional source/restoration not verified')
        reuse = functional_result((previous / 'probe.stdout').read_text(), prior['prefix'])
        report['prefix'] = prior['prefix']
        atomic_json(args.work / 'reused-functional-proof.json', dict(complete=True,
            source=str(previous), report_sha256=file_sha(previous / 'report.json'),
            stdout_sha256=file_sha(previous / 'probe.stdout'), probe_sha256=file_sha(args.probe),
            source_controller_complete=prior.get('complete'), source_controller_failure=prior.get('failure'),
            note='Raw C++ probe completed all cases; prior controller read the wrong completion key. Evidence reused, not rerun.'))
    service = {}
    process = sampler = None
    handlers = {}
    def cancelled(signum, frame):
        if process is not None and process.poll() is None:
            process.terminate()
        raise RuntimeError('functional gate cancelled; restore original service and resources')
    for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
        handlers[sig] = signal.signal(sig, cancelled)
    try:
        manager.create()
        atomic_json(args.work / 'resource-before.json', manager.snapshot())
        with (args.work / 'sampler.private.stderr').open('x') as err:
            sampler = subprocess.Popen([sys.executable, '-m', 'experiments.fixed_matrix.telemetry',
                '--paths', str(args.work / 'resource-paths.json'), '--roles', str(args.work / 'support-roles.json'),
                '--output', str(args.work / 'resource-samples.jsonl'), '--stop-file', str(args.work / 'sampler.stop'),
                '--interval', '1', '--max-seconds', '1800'], cwd=args.repo,
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=err)
        resources_script = Path(run.infrastructure.__file__).with_name('resources.py')
        with run.infrastructure.database_start_in_leaf(resources_script, manager.paths['database'], args.work / 'resources.json'):
            with route3_control.sync_window(args, service):
                try:
                    if reuse is not None:
                        atomic_json(args.work / 'probe-result.json', reuse)
                        report.update(functional_complete=True, functional_reused=True,
                            result_sha256=file_sha(args.work / 'probe-result.json'))
                    else:
                        command = [sys.executable, str(resources_script), 'enter-leaf-exec',
                            '--leaf', manager.paths['query'], '--journal', str(args.work / 'resources.json'), '--',
                            str(args.probe), '--prefix', report['prefix'], '--journal', str(args.work / 'owner'),
                            '--native-root', str(args.work / 'native-files')]
                        with (args.work / 'probe.stdout').open('x') as out, (args.work / 'probe.private.stderr').open('x') as err:
                            process = subprocess.Popen(command, cwd=args.repo, env=args.env,
                                stdin=subprocess.DEVNULL, stdout=out, stderr=err, start_new_session=True)
                            manager.attach_query(process.pid)
                            process.wait(timeout=600)
                        report['probe_exit_code'] = process.returncode
                        run.need(process.returncode == 0, 'functional probe failed; no automatic retry')
                        result = functional_result((args.work / 'probe.stdout').read_text(), report['prefix'])
                        atomic_json(args.work / 'probe-result.json', result)
                        report.update(functional_complete=True, result_sha256=file_sha(args.work / 'probe-result.json'))
                finally:
                    run.infrastructure.finish_query_children(process, manager, args.work)
                if args.timing_probe:
                    timing = args.work / 'timing'
                    timing.mkdir(mode=0o700)
                    sources = run.closure(args.repo)
                    for name in ('verify.py', 'fixed_probe.py'):
                        relative = 'experiments/cross_version/hdf5_route3/native_filesystem/' + name
                        sources[relative] = file_sha(args.repo / relative)
                    resource_id = digest(dict(resources=run.infrastructure.resource_identity([2, 3]),
                        environment_sha256=file_sha(args.work / 'environment.json')))
                    atomic_json(timing / 'resource-paths.json', manager.paths)
                    atomic_json(timing / 'prepared.json', dict(resource_id=resource_id, sources=sources,
                        product=dict(path=str(args.binary), sha256=file_sha(args.binary)), inspector=str(args.inspector)))
                    # Validate all frozen inputs before modifying a scientific
                    # namespace's supplemental location directory.
                    base = [sys.executable, '-u', '-m',
                        'experiments.cross_version.hdf5_route3.native_filesystem.fixed_probe',
                        '--repo', str(args.repo), '--work', str(timing), '--source-collection', str(args.source_collection),
                        '--binary', str(args.binary), '--build-binding', str(args.build_binding),
                        '--library', str(args.library), '--inspector', str(args.inspector)]
                    with (timing / 'preflight.stdout').open('x') as out, (timing / 'preflight.private.stderr').open('x') as err:
                        check = subprocess.run([*base, '--prepare-only'], cwd=args.repo, env=run.worker_environment(),
                            stdin=subprocess.DEVNULL, stdout=out, stderr=err, timeout=180)
                    run.need(check.returncode == 0, 'fixed-case preflight failed before supplement')
                    command = [sys.executable, str(resources_script), 'enter-leaf-exec',
                        '--leaf', manager.paths['query'], '--journal', str(args.work / 'resources.json'), '--', *base]
                    try:
                        with (timing / 'worker.stdout').open('x') as out, (timing / 'worker.private.stderr').open('x') as err:
                            process = subprocess.Popen(command, cwd=args.repo, env=run.worker_environment(),
                                stdin=subprocess.DEVNULL, stdout=out, stderr=err, start_new_session=True)
                            manager.attach_query(process.pid)
                            process.wait(timeout=1200)
                        run.need(process.returncode == 0, 'fixed-case diagnostic failed; no automatic retry')
                        timing_report = json.loads((timing / 'report.json').read_text())
                        run.need(timing_report.get('complete') is True, 'fixed-case diagnostic completion missing')
                        report.update(scientific_queries=72, timing_probe_complete=True,
                            timing_report_sha256=file_sha(timing / 'report.json'), performance_claims='single-block descriptive only')
                    finally:
                        run.finish_children(process, manager, timing)
    except BaseException as error:
        report['failure'] = dict(type=type(error).__name__, message=str(error)[:500])
        raise
    finally:
        try:
            manager.close()
            atomic_json(args.work / 'resources-restored.json', manager.state)
        finally:
            (args.work / 'sampler.stop').touch()
            if sampler is not None:
                try:
                    sampler.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    sampler.terminate()
                    sampler.wait(timeout=10)
            rows = args.work / 'timing/queries.jsonl'
            if rows.exists():
                report['scientific_queries'] = sum(1 for _ in rows.open())
            report.update(service, resources_restored=manager.state.get('restoration_complete', False), ended_ns=time.time_ns())
            report['complete'] = gate_complete(report,
                sampler_ok=sampler is not None and sampler.returncode == 0,
                timing_required=args.timing_probe)
            atomic_json(args.work / 'report.json', report)
            for sig, handler in handlers.items():
                signal.signal(sig, handler)
    run.need(report['complete'], 'functional gate or restoration incomplete')
    print(json.dumps(dict(complete=True, scientific_queries=report['scientific_queries'],
                          report=str(args.work / 'report.json'))))


if __name__ == '__main__':
    main()
