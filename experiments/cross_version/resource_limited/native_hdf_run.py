"""Native-HDF5-only retest of existing independent N128 and N512/B4/C128.

Reuses the accepted binary, scientific publications, native workers and resource
recovery primitives. No indexed HDF5 arm, source publication or index building.
The supervisor stays outside the limited worker so service recovery survives OOM.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import statistics
import subprocess
import sys
import time

from experiments.cross_version.resource_limited import run as bounded
from experiments.cross_version.resource_limited.resources import CgroupV2Resources, atomic_json
from experiments.identity.model import build_run, digest
from experiments.identity.registry import Registry, file_sha, read_json

need = bounded.need
SCENES = ['hdf5-n128', 'hdf5-c128']
ARMS = ['FS-index', 'IoTDB-FS']


def launcher(path, binary, expected_sha, prefix, journal):
    receipt = bounded.launcher(path, binary, expected_sha, prefix, journal)
    old = 'SETTINGS=' + repr(receipt['settings']) + '\n'
    settings = dict(receipt['settings'], **{'query.hdf5.mode': 'native-hdf5'})
    source = path.read_text()
    need(source.count(old) == 1, 'frozen launcher settings layout changed')
    path.write_text(source.replace(old, 'SETTINGS=' + repr(settings) + '\n', 1))
    receipt.update(settings=settings, sha256=file_sha(path))
    return receipt


def prepare(args):
    from . import native_hdf_scene as scenes
    from experiments.cross_version.version_directory import v03_startup_wrapper as startup
    frozen = startup.load_runner(args.repo/'experiments/multi_file/work/hdf5-predicate-compiled-20261015/runner-source/experiments/multi_file/small_runner.py')
    sys.modules['small_runner'] = frozen
    from experiments.cross_version.version_matrix.n8_matrix_prepare import _probe
    need(file_sha(frozen.sf.__file__) == startup.HELPER_SHA, 'frozen helper import differs before service control')
    args.work.mkdir(mode=0o700, parents=True, exist_ok=False)
    registry = Registry(args.registry)
    entries = scenes.prepare(args.repo, registry)
    need([s['scene_id'] for s in entries] == SCENES, 'exact two existing HDF scenes required')
    binary = bounded.verify_binaries(args.repo)['candidate']
    inspector = args.repo/bounded.WORK/'large-file-upper-r3-20261017/store-inspector/large_store_probe'
    need(file_sha(inspector) == bounded.INSPECTOR_SHA, 'head inspector changed')
    sources = bounded.source_closure(args.repo)
    for relative, expected in sources.items():
        destination = args.work/'runner-source'/relative
        destination.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(args.repo/relative,destination)
        need(file_sha(destination) == expected, 'runner snapshot copy differs')
    result = dict(protocol='iotdbfs-native-hdf-retest-prepared/1', scenes=entries,
        binary=binary, sources=sources, inspector=str(inspector),
        scientific_publication=False, index_rebuilt=False,
        validations=[scenes.inspect(s, registry) for s in entries])
    atomic_json(args.work/'prepared.json', result)
    return result


def summarize(rows):
    need(len(rows) == 48, 'native retest requires all 48 new queries')
    indexed = {}
    for row in rows:
        key = row['scene_id'], row['arm'], row['repeat']
        need(key not in indexed and key[0] in SCENES and key[1] in ARMS and
             type(key[2]) is int and key[2] in range(12), 'duplicate/unknown query')
        need(row.get('ok') is True and row.get('oracle_verified') is True and
             row.get('owned_output_bytes') == 40960, 'unverified/incomplete owned output')
        times = [row[k] for k in ('total_ns', 'discovery_client_ns', 'selected_value_client_ns')]
        need(all(type(t) is int and t > 0 for t in times) and times[0] == sum(times[1:]),
             'invalid/nonpartitioning client phase clocks')
        need(row['phase'] == ('warmup' if key[2] < 2 else 'measurement'), 'wrong warmup label')
        if key[1] == 'IoTDB-FS':
            need(row.get('native_mode_verified') is True, 'unverified native HDF5 engine')
        indexed[key] = row
    need(set(indexed) == {(s,a,r) for s in SCENES for a in ARMS for r in range(12)}, 'missing query')
    cells = []
    for scene in SCENES:
        arm_stats = {}
        for arm in ARMS:
            selected = [indexed[scene, arm, r] for r in range(2,12)]
            need(len({r['run_id'] for r in selected}) == 1 and len({r['case_id'] for r in selected}) == 1,
                 'mixed run/case identity')
            arm_stats[arm] = {key: statistics.median(r[key] for r in selected)/1e6
                for key in ('total_ns','discovery_client_ns','selected_value_client_ns')}
            arm_stats[arm]['samples'] = {key:[r[key] for r in selected]
                for key in ('total_ns','discovery_client_ns','selected_value_client_ns')}
            arm_stats[arm]['run_id'] = selected[0]['run_id']
        fs, db = arm_stats['FS-index'], arm_stats['IoTDB-FS']
        cells.append(dict(scene_id=scene, corpus_id=indexed[scene,ARMS[0],0]['corpus_id'],
            case_id=indexed[scene,ARMS[0],0]['case_id'], arms=arm_stats,
            R=db['total_ns']/fs['total_ns'], Rd=db['discovery_client_ns']/fs['discovery_client_ns'],
            Rq=db['selected_value_client_ns']/fs['selected_value_client_ns']))
    return dict(complete=True, cells=cells, new_queries=48, measured_queries=40,
        mode='native-hdf5', statistic='quotient of within-scene warm medians; no discarded rows',
        limitation='different existing native handle caches and dataset geometry across scenes; not a controlled N-only ablation')


def worker(args):
    need(os.read(args.gate_fd,1) == b'1', 'supervisor did not admit worker')
    os.close(args.gate_fd)
    def interrupt(signum, frame):
        raise RuntimeError('native query worker cancelled; unwind owned mount')
    for sig in (signal.SIGTERM, signal.SIGHUP): signal.signal(sig, interrupt)
    from . import native_hdf_scene as scenes
    from experiments.cross_version.version_directory import v03_startup_wrapper as startup
    paired = startup.load_runner(args.repo/'experiments/multi_file/work/hdf5-predicate-compiled-20261015/runner-source/experiments/multi_file/small_runner.py')
    sys.modules['small_runner'] = paired
    from experiments.cross_version.pilot import publish
    # n8_matrix_prepare imports publish/small_runner at module scope. Import it
    # only AFTER binding the frozen paired module, otherwise Python retains the
    # maintained single-file helper under the shared top-level name "runner".
    from experiments.cross_version.version_matrix.n8_matrix_prepare import _probe
    prepared = read_json(args.work/'prepared.json')
    need(prepared['sources'] == bounded.source_closure(args.repo), 'prepared source closure changed')
    resource = read_json(args.work/'resource-identity.json')
    paths = read_json(args.work/'resource-paths.json')
    bounded.observe(paths)
    _,_,env = publish.runtime(args.repo/'experiments/multi_file/work/runner-20261013/environment.json',1001546,publish.DATABASE)
    need(env['IOTDB_PORT'] == '33667' and env['IOTDB_HOST'] in ('localhost','127.0.0.1'), 'wrong original endpoint')
    env.update(resource['native_threads'])
    for name in bounded.DIAGNOSTIC: env.pop(name, None)
    startup.adjust_mount(paired.sf,1800,args.work/'startup-receipts.jsonl')
    registry = Registry(args.registry)
    report = dict(protocol='iotdbfs-native-hdf-retest/1', complete=False,
        expected_queries=48, query_records=0, inherited_queries=0, mode='native-hdf5', mounts=[],
        prepared_sha256=file_sha(args.work/'prepared.json'), credentials_recorded=False)
    atomic_json(args.work/'report.json', report)
    writer = args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
    rows = []
    try:
        for scene in prepared['scenes']:
            root = args.work/(scene['scene_id']+'-native'); root.mkdir(mode=0o700)
            need(bounded.journal_snapshot(scene['original_owner']) == scene['journal_digest'], 'original journal changed')
            journal = root/'owner'; shutil.copytree(scene['original_owner'], journal)
            for name in ('head-before','head-after'): (root/name).mkdir(mode=0o700)
            before, proof = _probe(Path(prepared['inspector']),'head',scene['prefix'],env,root/'head-before')
            need(before == scene['head'], 'scientific head changed before mount')
            binary = prepared['binary']
            launch = launcher(root/'launcher',Path(binary['path']),binary['sha256'],scene['prefix'],journal)
            mount = dict(scene_id=scene['scene_id'], head_before=before, head_before_receipt=proof, launcher=launch)
            report['mounts'].append(mount); atomic_json(args.work/'report.json',report)
            with scenes.open_native(scene, paired, env) as native:
                receipt = scenes.supplement_mask(scene,native,paired,root/'supplemental-mask.json')
                environment = dict(resource, supplemental_mask_evidence_sha256=file_sha(root/'supplemental-mask.json'),
                    supplemental_mask_scope=receipt.get('scope','scene'),
                    actual_delivery='owned Float32 value and uint8 validity mask, 40960 bytes')
                case = registry.get_case(scene['case_id'])
                runs = {}
                for arm in ARMS:
                    run = build_run(case, execution=str(root),arm=arm,
                        implementation=dict(binary_sha256=binary['sha256'] if arm=='IoTDB-FS' else scene['native_binary_sha256'],
                            runner_sha256=digest(prepared['sources']),engine='native-hdf5-VFD' if arm=='IoTDB-FS' else 'native-hdf5-SQLite-index'),
                        clock=dict(kind='owned-full-query',includes='discovery, values, validity mask',
                            excludes='setup, oracle validation, profile harvesting',endpoint=scene['clock'][arm]),
                        cache=dict(kind=scene['cache'][arm],warmup_pairs=2,measured_pairs=10,OS_DB_flush=False),
                        environment=environment,replication=dict(block=scene['scene_id'],warmup=[0,1],measured=list(range(2,12)),
                            arm_order='FS first on even repeat, IoTDB first on odd repeat', independent=False))
                    runs[arm] = registry.register_run(run)
                atomic_json(root/'design.json',dict(scene=scene,runs=runs,mask_evidence=receipt))
                print(json.dumps(dict(event='mount_start',scene=scene['scene_id'])),flush=True)
                with paired.sf.private_mount(Path(launch['path']),root/'mount',env,writer,profile=True) as context:
                    mount.update(daemon_pid=context['pid'], startup_ns=context['startup_ns'])
                    actual = read_json(context['root']/'profile.launch.json')
                    need(actual['settings'] == launch['settings'] and actual['actual_binary_sha256'] == binary['sha256'] and
                        actual['pid'] == context['pid'] and file_sha(Path('/proc')/str(context['pid'])/'exe') == binary['sha256'],
                        'actual running binary/mode differs')
                    offset = 0
                    for repeat in range(12):
                        for arm in (ARMS if repeat%2 == 0 else list(reversed(ARMS))):
                            observed_before = bounded.observe(paths)
                            if arm == 'FS-index': row = scenes.query_native(native,scene)
                            else: row, offset = scenes.query_iotdb(paired,context,scene,offset)
                            observed_after = bounded.observe(paths)
                            row.update(scene_id=scene['scene_id'],arm=arm,repeat=repeat,
                                phase='warmup' if repeat<2 else 'measurement',ok=True,
                                corpus_id=scene['corpus_id'],case_id=scene['case_id'],run_id=runs[arm]['run_id'],
                                resource_observations=dict(before=observed_before,after=observed_after))
                            bounded.append(args.work/'queries.jsonl',row);rows.append(row)
                            report['query_records'] = len(rows);atomic_json(args.work/'report.json',report)
                        print(json.dumps(dict(event='pair',scene=scene['scene_id'],repeat=repeat,rows=len(rows))),flush=True)
            after, proof = _probe(Path(prepared['inspector']),'head',scene['prefix'],env,root/'head-after')
            need(after == before and bounded.journal_snapshot(journal) == bounded.journal_snapshot(scene['original_owner']) == scene['journal_digest'],
                 'scientific head/journals changed')
            mount.update(complete=True,head_after=after,head_after_receipt=proof,head_unchanged=True,journals_unchanged=True)
            atomic_json(args.work/'report.json',report)
        atomic_json(args.work/'analysis.json',summarize(rows))
        atomic_json(args.work/'final-input-verification.json',[scenes.inspect(s,registry) for s in prepared['scenes']])
        need(bounded.verify_binaries(args.repo)['candidate'] == prepared['binary'], 'accepted binary changed')
        need(bounded.source_closure(args.repo) == prepared['sources'], 'runner changed during experiment')
        report.update(complete=True,queries_sha256=file_sha(args.work/'queries.jsonl'),analysis_sha256=file_sha(args.work/'analysis.json'))
    except BaseException as error:
        report['failure'] = dict(type=type(error).__name__,message=str(error)[:500])
        raise
    finally:
        atomic_json(args.work/'report.json',report)


def supervise(args):
    prepare(args)
    if args.prepare_only:
        print(json.dumps(dict(prepared=True,service_modified=False,cgroup_modified=False))); return
    need(sys.platform=='linux' and os.geteuid()==0,'original Linux/root host required')
    identity = bounded.resource_identity([2,3])
    manager = CgroupV2Resources([2,3],args.work/'resources.json',
        database_pid_file=args.repo.parent/'runtime/run/datanode.pid',keeper_seconds=14400)
    atomic_json(args.work/'resource-identity.json',identity);atomic_json(args.work/'resource-paths.json',manager.paths)
    from experiments.cross_version.hdf5_route3 import route3_control
    from experiments.cross_version.version_directory import v03_startup_wrapper as startup
    paired = startup.load_runner(args.repo/'experiments/multi_file/work/hdf5-predicate-compiled-20261015/runner-source/experiments/multi_file/small_runner.py')
    sys.modules['small_runner'] = paired
    from experiments.cross_version.pilot import publish
    _,_,args.env = publish.runtime(args.repo/'experiments/multi_file/work/runner-20261013/environment.json',1001546,publish.DATABASE)
    args.runtime=args.repo.parent/'runtime';args.release=(args.runtime/'current').resolve(strict=True)
    args.datanode_pid=int((args.runtime/'run/datanode.pid').read_text())
    argv=(Path('/proc')/str(args.datanode_pid)/'cmdline').read_bytes().split(b'\0')
    need(all(f in argv for f in (b'-Xms15232M',b'-Xmx15232M',b'-XX:MaxDirectMemorySize=2176M')), 'original fixed database memory changed')
    args.report=args.work/'service-report.json';service_report={}
    process=None;write_fd=None;handlers={};cancelled=False
    def cancel(signum,frame):
        nonlocal cancelled
        cancelled=True
        if process is not None and process.poll() is None: process.send_signal(signal.SIGTERM)
    try:
        for sig in (signal.SIGTERM,signal.SIGHUP,signal.SIGINT): handlers[sig]=signal.signal(sig,cancel)
        manager.create();atomic_json(args.work/'resource-before.json',manager.snapshot())
        with bounded.database_start_in_leaf(Path(bounded.__file__).with_name('resources.py'),manager.paths['database'],args.work/'resources.json'):
            with route3_control.sync_window(args,service_report):
                try:
                    read_fd,write_fd=os.pipe()
                    command=[sys.executable,str(Path(bounded.__file__).with_name('resources.py')),'enter-leaf-exec',
                        '--leaf',manager.paths['query'],'--journal',str(args.work/'resources.json'),'--',
                        sys.executable,'-m',__package__+'.native_hdf_run','--worker','--repo',str(args.repo),
                        '--work',str(args.work),'--registry',str(args.registry),'--gate-fd',str(read_fd)]
                    with (args.work/'worker.stdout.log').open('x') as out,(args.work/'worker.private.stderr').open('x') as err:
                        process=subprocess.Popen(command,cwd=args.repo,env=dict(os.environ,**identity['native_threads'],PYTHONDONTWRITEBYTECODE='1'),
                            pass_fds=(read_fd,),stdin=subprocess.DEVNULL,stdout=out,stderr=err,start_new_session=True)
                        os.close(read_fd);manager.attach_query(process.pid)
                        if cancelled: process.terminate()
                        else: os.write(write_fd,b'1')
                        os.close(write_fd);write_fd=None
                        start=time.monotonic();cancel_sent=False
                        while process.poll() is None:
                            bounded.append(args.work/'resource-samples.jsonl',manager.snapshot())
                            if time.monotonic()-start>7200 and not cancel_sent:
                                process.send_signal(signal.SIGTERM);cancel_sent=True
                            time.sleep(5)
                        atomic_json(args.work/'worker-exit.json',dict(returncode=process.returncode,cancelled=cancelled,elapsed_seconds=time.monotonic()-start))
                    need(process.returncode==0,'native worker failed; diagnostics retained privately')
                    need(read_json(args.work/'report.json').get('complete'),'native matrix incomplete')
                finally:
                    bounded.finish_query_children(process,manager,args.work)
    finally:
        if write_fd is not None: os.close(write_fd)
        if process is not None and process.poll() is None:
            process.send_signal(signal.SIGTERM);process.wait()
        try:
            try: atomic_json(args.work/'resource-after-measurement.json',manager.snapshot())
            finally:
                manager.close();atomic_json(args.work/'resource-restored.json',manager.state)
        finally:
            for sig,handler in handlers.items(): signal.signal(sig,handler)
            path=args.work/'report.json';report=read_json(path) if path.exists() else dict(complete=False,query_records=0)
            complete=report.get('complete') is True
            report.update(service_report,resources_restored=manager.state['phase']=='closed',scientific_complete=complete)
            report['complete']=bool(complete and report.get('original_service_restored') and
                report.get('controller_restoration_verified') and report['resources_restored'])
            atomic_json(path,report)
    print(json.dumps(dict(complete=report['complete'],report=str(path))))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('repo','work','registry'): parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--prepare-only',action='store_true')
    parser.add_argument('--worker',action='store_true',help=argparse.SUPPRESS)
    parser.add_argument('--gate-fd',type=int,help=argparse.SUPPRESS)
    args=parser.parse_args();args.repo=args.repo.resolve(strict=True);args.work=args.work.resolve();args.registry=args.registry.resolve()
    if args.worker: worker(args)
    else: supervise(args)


if __name__ == '__main__': main()
