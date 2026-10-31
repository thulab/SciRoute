"""All 16 sealed FM1 HDF5 primary cases, filesystem-native, three campaigns.

Query-only science: never generate, publish or rebuild an index. New supplemental
file locations are explicit preparation. Two previously bound corpora use their
accepted current journals and file roots, never the archived publication head.
"""
from __future__ import annotations
import argparse
from copy import deepcopy
import json
import os
from pathlib import Path
import shutil
import signal
import statistics
import subprocess
import sys
import time

from experiments.fixed_matrix import run, telemetry
from experiments.identity.model import digest
from experiments.identity.registry import Registry, file_sha, read_json
from experiments.cross_version.hdf5_route3 import route3_control
from experiments.cross_version.hdf5_route3.native_filesystem import fixed_probe
from experiments.cross_version.resource_limited.resources import CgroupV2Resources, atomic_json

need = run.need
PROTOCOL = 'iotdbfs-fixed-hdf5-filesystem/1'
PRODUCT_SHA = '3d58c7d56740fbb2a2b962e8b3558b5c12ee65703545e93ecb15f69a85d27cb3'
BOUND_PINS = {
    'report.json': '7499df5a2cf6d49901e26d1e2749d202d9a0e045066ba045aa0f924a145949bc',
    'acceptance-audit.json': 'b065c3cffbafe1f149217921f38cab6e5181353958702407239692ee0a1ea54d',
    'timing/report.json': '0c386713ad75a035b5f8f948baea494a8b47a03d918aa78ebedff7cc579cc673',
    'timing/bindings.json': 'fc0be0aeb013c22148bd0ec371df62d043158cba5f1117ea681713d9a3b58b56',
}
CAMPAIGNS = (0, 1, 2)
EXPECTED = 16 * 3 * 3 * 12


def groups(repo):
    selected = [g for g in read_json(repo / 'experiments/fixed_matrix/catalog.json')['groups']
                if g['format'] == 'hdf5' and g['collection_tier'] == 'primary']
    need(len(selected) == 16 and len({g['group_id'] for g in selected}) == 16,
         'entire fixed HDF5 primary suite required')
    return selected


def closure(repo):
    value = run.closure(repo)
    relative = 'experiments/cross_version/hdf5_route3/native_filesystem/fixed_probe.py'
    value[relative] = file_sha(repo / relative)
    return value


def preflight(args):
    selected = groups(args.repo)
    need(file_sha(args.binary) == PRODUCT_SHA, 'accepted product changed; no new product version allowed')
    proof = fixed_probe.source_proof(args, group_ids=[g['group_id'] for g in selected], campaigns=CAMPAIGNS)
    need(proof['expected_queries'] == EXPECTED and len({e['corpus_id'] for e in proof['cases']}) == 7,
         'fixed suite/corpus coverage differs')
    for relative, expected in BOUND_PINS.items():
        need(file_sha(args.bound_source / relative) == expected, 'accepted native binding proof changed')
    prior = read_json(args.bound_source / 'timing/report.json')
    audit = read_json(args.bound_source / 'acceptance-audit.json')
    need(audit['complete'] and audit['resources_restored'] and audit['original_service_restored'],
         'prior native acceptance incomplete')
    bound = []
    for item in prior['corpora']:
        receipt_path = Path(item['file_binding_receipt'])
        need(file_sha(receipt_path) == item['file_binding_receipt_sha256'], 'prior binding receipt changed')
        receipt = read_json(receipt_path)
        journal = Path(item['active_owner_journal'])
        need(receipt['complete'] and receipt['scientific_identities_unchanged']
             and run.infrastructure.journal_snapshot(journal) == receipt['active_journal_digest'],
             'current bound journal changed')
        root = journal.parent / 'native-files'
        need(root.is_dir() and not root.is_symlink(), 'accepted managed file root missing')
        bound.append(dict(corpus_id=item['corpus_id'], namespace=item['namespace'],
            head=receipt['head_after'], journal=str(journal), journal_digest=receipt['active_journal_digest'],
            native_root=str(root), receipt=str(receipt_path), receipt_sha256=file_sha(receipt_path)))
    need(len(bound) == 2 and {x['corpus_id'] for x in bound} <= {x['corpus_id'] for x in proof['cases']},
         'previously bound corpus set differs')
    proof.update(protocol=PROTOCOL, bound_source=str(args.bound_source), bound_source_pins=BOUND_PINS,
        existing_bindings=bound, source_files=closure(args.repo),
        note='All 16 HDF5 primary cases; three new campaigns. The earlier 72-row probe is not pooled.')
    return proof


def plans(args, registry, report, proof):
    original = read_json(args.source_collection / 'report.json')
    old_items = {x['corpus_id']: x for x in original['corpora']}
    old_bindings = read_json(args.source_collection / 'bindings.json')
    old_registry = Registry(args.source_collection / 'registry')
    selected = {g['group_id']: g for g in groups(args.repo)}
    prior = {x['corpus_id']: x for x in proof['existing_bindings']}
    out, bindings, by_corpus = [], {}, {}
    for entry in proof['cases']:
        by_corpus.setdefault(entry['corpus_id'], []).append(entry)
        gid = entry['group_id']
        registry.register_corpus(old_registry.get_corpus(entry['corpus_id']))
        registry.register_case(old_registry.get_case(entry['case_id']))
        bindings[gid] = deepcopy(old_bindings[gid]); bindings[gid]['run_ids'] = {}
    for cid, entries in by_corpus.items():
        base = Path(entries[0]['source_root'])
        item = deepcopy(old_items[cid]); item.update(complete=False, mounts=[], source_root=str(base))
        corpus = registry.register_corpus(old_registry.get_corpus(cid))
        execution = args.work / 'corpora' / item['corpus_key']; execution.mkdir(parents=True, mode=0o700)
        active = execution / 'owner-active'
        current = prior.get(cid)
        source_journal = Path(current['journal']) if current else base / 'owner'
        expected_digest = current['journal_digest'] if current else item['journal_digest']
        shutil.copytree(source_journal, active)
        need(run.infrastructure.journal_snapshot(active) == expected_digest, 'active journal copy differs')
        item.update(active_owner_journal=str(active), archived_owner_journal=str(base / 'owner'))
        native_root = Path(current['native_root']) if current else execution / 'native-files'
        scientific_head = current['head'] if current else item['head']
        item.update(head=scientific_head, journal_digest=expected_digest, native_file_root=str(native_root),
                    reused_binding_receipt=current['receipt'] if current else None)
        report['corpora'].append(item)
        out.append(dict(spec=corpus['parameters']['generation']['specification'], root=base,
            execution_root=execution, manifest=read_json(base / 'input/manifest.json'), fsroot=base / 'fs',
            journal=active, item=item, scientific_head=scientific_head, journal_digest=expected_digest,
            corpus=corpus, groups=[selected[e['group_id']] for e in entries], native_root=native_root,
            cfg=dict(catalog_path=str(base / 'catalog.json'), index_path=str(base / 'index.sqlite'),
                cache_files=8, cache_datasets=8, metadata_cache_bytes=2097152, raw_chunk_cache_bytes=262144),
            prep=read_json(base / 'preparation.json')))
    out.sort(key=lambda p:(p['spec']['N'], p['spec']['V_stored'], p['spec']['size_profile'],
                           p['spec']['history_scenario'], p['spec']['format']))
    return out, bindings


def analyze(path, bindings, selected):
    rows = [json.loads(s) for s in path.read_text().splitlines()]
    wanted = {(g['group_id'], c, a, r) for g in selected for c in CAMPAIGNS for a in run.ARMS for r in range(12)}
    observed = {(r['group_id'], r['campaign'], r['arm'], r['repeat']) for r in rows}
    need(len(rows) == EXPECTED and observed == wanted and len(observed) == len(rows), 'whole HDF5 matrix incomplete')
    group_map = {g['group_id']: g for g in selected}
    for row in rows:
        b = bindings[row['group_id']]
        need(all(row[k] == b[k] for k in ('corpus_id', 'case_id', 'oracle_sha256')), 'case identity differs')
        need(row['run_id'] == b['run_ids'][str(row['campaign'])][row['arm']], 'run identity differs')
        need(all(row.get(k) is True for k in ('ok', 'mode_verified', 'oracle_verified'))
             and row['execution_mode'] == 'native-hdf5', 'mode or oracle unverified')
        need(row['phase'] == ('warmup' if row['repeat'] < 2 else 'measurement')
             and all(type(row[k]) is int and row[k] > 0 for k in ('total_ns', 'discovery_ns', 'value_ns'))
             and row['total_ns'] == row['discovery_ns'] + row['value_ns'], 'clock/phase differs')
        for label in ('resource_before', 'resource_after'): run.validate_sample(row[label])
        if row['arm'] == 'IoTDB-FS':
            fixed_probe.validate_filesystem_row(row, expected_units=group_map[row['group_id']]['K_units'])
            for call in row['value_calls']:
                delta = call['server_profile']['iotdb_delta']
                # Native data bytes must stay on FS. Directory/authorization
                # metadata SQL is legal and is reported, not silently forbidden.
                need(delta['payload_calls'] == 0, 'value path read database payload')
    cells = []
    for group in selected:
        gid = group['group_id']; blocks = []
        for campaign in CAMPAIGNS:
            arms = {}
            for arm in run.ARMS:
                sample = [r for r in rows if r['group_id'] == gid and r['campaign'] == campaign
                          and r['arm'] == arm and r['phase'] == 'measurement']
                need(len(sample) == 10, 'cell sample count differs')
                arms[arm] = {key: statistics.median(r[key] for r in sample) / 1e6
                             for key in ('total_ns', 'discovery_ns', 'value_ns')}
            db, fs = arms['IoTDB-FS'], arms['FS-index']
            blocks.append(dict(campaign=campaign, warm_median_ms=arms, R=db['total_ns']/fs['total_ns'],
                Rd=db['discovery_ns']/fs['discovery_ns'], Rq=db['value_ns']/fs['value_ns']))
        cells.append(dict(group_id=gid, table_id=group['table_id'], corpus_id=bindings[gid]['corpus_id'],
            case_id=bindings[gid]['case_id'], blocks=blocks,
            warm_median_ms={arm:{k:statistics.median(b['warm_median_ms'][arm][k] for b in blocks)
                for k in ('total_ns', 'discovery_ns', 'value_ns')} for arm in run.ARMS},
            **{k:statistics.median(b[k] for b in blocks) for k in ('R', 'Rd', 'Rq')},
            R_range=[min(b['R'] for b in blocks), max(b['R'] for b in blocks)]))
    return dict(complete=True, protocol=PROTOCOL, cases=16, campaigns=list(CAMPAIGNS), query_rows=len(rows),
        measured_rows=1440, warmup_rows=288, cells=cells,
        value_database_calls={key:sum(call['server_profile']['iotdb_delta'].get(key,0)
            for row in rows if row['arm']=='IoTDB-FS' for call in row['value_calls'])
            for key in ('query_calls','payload_calls','layout_calls','version_store_query_calls')},
        minimum_R_group=min(cells, key=lambda c:c['R'])['group_id'],
        statistic='Median of three campaign medians; R/Rd/Rq are medians of same-campaign ratios.',
        limitations='Descriptive same-host time blocks; no significance, cold-disk or concurrency claim.')


def worker(args):
    def stop(sig, frame): raise RuntimeError('fixed HDF5 run cancelled; preserve partial evidence')
    for sig in (signal.SIGTERM, signal.SIGHUP): signal.signal(sig, stop)
    proof = preflight(args)
    frozen = read_json(args.work / 'prepared.json')
    need(proof == read_json(args.work / 'reuse-source.json') and closure(args.repo) == frozen['sources'],
         'preflight/source identity changed')
    paired, _, env, startup = run.dependencies(args.repo)
    startup.adjust_mount(paired.sf, 1800, args.work / 'startup-receipts.jsonl')
    run.PRODUCT_SHA = PRODUCT_SHA
    report = dict(protocol=PROTOCOL, complete=False, expected_queries=EXPECTED, query_records=0,
        corpora=[], product_sha256=PRODUCT_SHA, resource_id=frozen['resource_id'],
        catalog_sha256=run.CATALOG_SHA, payload_backend='filesystem', scientific_publication=False,
        source_collection=str(args.source_collection), index_rebuilt=False, new_cases=False, credentials_recorded=False)
    atomic_json(args.work / 'report.json', report)
    try:
        registry = Registry(args.work / 'registry'); execution, bindings = plans(args, registry, report, proof)
        by_namespace = {p['item']['namespace']:p for p in execution}
        def launch(path, binary, prefix, journal, initialize):
            return fixed_probe.filesystem_launcher(path, binary, prefix, journal, initialize,
                native_root=by_namespace[prefix]['native_root'], expected_sha=PRODUCT_SHA)
        run.launcher = launch
        for plan in execution:
            receipt = launch(plan['execution_root'] / 'binding-launcher', args.binary,
                             plan['item']['namespace'], plan['journal'], False)
            fixed_probe.bind_files(args, plan, env, receipt)
            atomic_json(args.work / 'report.json', report)
            print(json.dumps(dict(event='locations_ready', corpus=plan['spec']['corpus_key'],
                reused=plan['item']['reused_binding_receipt'] is not None)), flush=True)
        atomic_json(args.work / 'bindings.json', bindings)
        refs = read_json(args.source_collection / 'preparation-refs.json')
        atomic_json(args.work / 'preparation-refs.json', {p['corpus']['corpus_id']:refs[p['corpus']['corpus_id']] for p in execution})
        atomic_json(args.work / 'execution-order.json', dict(corpora=[p['spec']['corpus_key'] for p in execution],
            campaigns=list(CAMPAIGNS), case_order='unchanged catalog rotation', arm_order='six permutations twice'))
        from experiments.fixed_matrix import client
        original = client.query_iotdb
        def checked(*positional, **keywords):
            row = original(*positional, **keywords)
            return fixed_probe.validate_filesystem_row(row, expected_units=positional[1]['K_units'])
        client.query_iotdb = checked
        try: count = run.collect_campaigns(args, execution, bindings, registry, frozen, report, env, paired, campaigns=CAMPAIGNS)
        finally: client.query_iotdb = original
        need(count == EXPECTED, 'matrix stopped before all fixed cells')
        need(preflight(args) == proof and closure(args.repo) == frozen['sources'], 'frozen inputs changed during run')
        for plan in execution:
            plan['item'].update(complete=True, final_raw_validation=run.verify_raw(plan['manifest'], plan['root']/'input', plan['fsroot']))
        report.update(complete=True, original_scientific_source_unchanged=True,
            queries_sha256=file_sha(args.work / 'queries.jsonl'), bindings_sha256=file_sha(args.work / 'bindings.json'))
    except BaseException as error:
        report['failure'] = dict(type=type(error).__name__, message=str(error)[:500]); raise
    finally: atomic_json(args.work / 'report.json', report)


def supervise(args, proof):
    args.work.mkdir(parents=True, mode=0o700, exist_ok=False)
    atomic_json(args.work / 'reuse-source.json', proof)
    run.bootstrap(args)  # Client/product Arrow environments stay separate.
    sources = closure(args.repo)
    for relative, expected in sources.items():
        target = args.work/'source'/relative; target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(args.repo/relative, target); need(file_sha(target) == expected, 'source copy differs')
    paired, _, args.env, startup = run.dependencies(args.repo)
    args.runtime = args.repo.parent/'runtime'; args.release=(args.runtime/'current').resolve(strict=True)
    args.datanode_pid=int((args.runtime/'run/datanode.pid').read_text()); args.report=args.work/'service-report.json'
    argv=(Path('/proc')/str(args.datanode_pid)/'cmdline').read_bytes().split(b'\0')
    need(all(x in argv for x in (b'-Xms15232M',b'-Xmx15232M',b'-XX:MaxDirectMemorySize=2176M')), 'database memory differs')
    software=run.software_inventory(args.repo); atomic_json(args.work/'software.json', software)
    environment=telemetry.environment(args.repo, artifact_paths=[args.binary,args.library,args.inspector],
        process_roles=software['process_roles'],data_path=args.work)
    atomic_json(args.work/'environment.json', environment)
    atomic_json(args.work/'support-roles.json',{k:v for k,v in software['process_roles'].items() if k!='datanode'})
    prepared=dict(protocol=PROTOCOL, sources=sources, product=dict(path=str(args.binary),sha256=PRODUCT_SHA),
        native_library_sha256=run.NATIVE_SHA, inspector=str(args.inspector),
        resource_id=digest(dict(resources=run.infrastructure.resource_identity([2,3]),environment_sha256=file_sha(args.work/'environment.json'))))
    atomic_json(args.work/'prepared.json',prepared)
    manager=CgroupV2Resources([2,3],args.work/'resources.json',database_pid_file=args.runtime/'run/datanode.pid',keeper_seconds=14400)
    atomic_json(args.work/'resource-paths.json',manager.paths)
    process=sampler=None; service={}; failure=None; handlers={}
    def cancelled(sig,frame):
        if process and process.poll() is None: process.terminate()
        raise RuntimeError('fixed HDF5 supervisor cancelled; restore service/resources')
    for sig in (signal.SIGTERM,signal.SIGHUP,signal.SIGINT):handlers[sig]=signal.signal(sig,cancelled)
    try:
        manager.create(); atomic_json(args.work/'resource-before.json',manager.snapshot())
        with (args.work/'sampler.private.stderr').open('x') as err:
            sampler=subprocess.Popen([sys.executable,'-m','experiments.fixed_matrix.telemetry','--paths',str(args.work/'resource-paths.json'),
                '--roles',str(args.work/'support-roles.json'),'--output',str(args.work/'resource-samples.jsonl'),
                '--stop-file',str(args.work/'sampler.stop'),'--interval','1','--max-seconds','14000'],
                cwd=args.repo,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=err)
        resources_script=Path(run.infrastructure.__file__).with_name('resources.py')
        with run.infrastructure.database_start_in_leaf(resources_script,manager.paths['database'],args.work/'resources.json'):
            with route3_control.sync_window(args,service):
                try:
                    command=[sys.executable,str(resources_script),'enter-leaf-exec','--leaf',manager.paths['query'],
                        '--journal',str(args.work/'resources.json'),'--',sys.executable,'-u','-m','experiments.fixed_matrix.hdf5_filesystem','--worker']
                    for key in ('repo','work','binary','build_binding','library','inspector','source_collection','bound_source'):
                        command.extend(['--'+key.replace('_','-'),str(getattr(args,key))])
                    with (args.work/'worker.stdout').open('x') as out,(args.work/'worker.private.stderr').open('x') as err:
                        process=subprocess.Popen(command,cwd=args.repo,env=run.worker_environment(),stdin=subprocess.DEVNULL,
                            stdout=out,stderr=err,start_new_session=True)
                        manager.attach_query(process.pid); started=time.monotonic()
                        while process.poll() is None:
                            run.need(sampler.poll() is None,'resource sampler stopped')
                            run.infrastructure.observe(manager.paths)
                            run.need(time.monotonic()-started <= 12600,'fixed HDF5 worker hard timeout')
                            time.sleep(5)
                    need(process.returncode==0,'HDF5 worker failed; preserve evidence, no automatic retry')
                finally:run.finish_children(process,manager,args.work)
    except BaseException as error:
        failure=dict(type=type(error).__name__,message=str(error)[:500])
        raise
    finally:
        try:manager.close();atomic_json(args.work/'resources-restored.json',manager.state)
        finally:
            (args.work/'sampler.stop').touch()
            if sampler is not None:
                try:sampler.wait(timeout=10)
                except subprocess.TimeoutExpired:sampler.terminate();sampler.wait(timeout=10)
            report=read_json(args.work/'report.json') if (args.work/'report.json').exists() else dict(complete=False,query_records=0)
            science=report.get('complete') is True
            report.update(service,scientific_complete=science,resources_restored=manager.state.get('restoration_complete',False))
            if failure:report['controller_failure']=failure
            report['ready_for_analysis']=bool(science and not failure and sampler is not None and sampler.returncode==0
                and report.get('original_service_restored') is True and report.get('controller_restoration_verified') is True and report['resources_restored'])
            report['complete']=False;atomic_json(args.work/'report.json',report)
            for sig,handler in handlers.items():signal.signal(sig,handler)
    need(report['ready_for_analysis'],'science, sampler or restoration incomplete')
    try:
        rows=[json.loads(s) for s in (args.work/'queries.jsonl').read_text().splitlines()]
        coverage=run.validate_coverage(args.work,rows)
        summary=analyze(args.work/'queries.jsonl',read_json(args.work/'bindings.json'),groups(args.repo))
        atomic_json(args.work/'analysis.json',summary)
        report.update(complete=True,resource_coverage=coverage,analysis_sha256=file_sha(args.work/'analysis.json'))
    except BaseException as error:
        report['analysis_failure']=dict(type=type(error).__name__,message=str(error)[:500]);raise
    finally:atomic_json(args.work/'report.json',report)
    print(json.dumps(dict(complete=True,queries=EXPECTED,report=str(args.work/'report.json'))))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ('repo','work','binary','build-binding','library','inspector','source-collection','bound-source'):
        parser.add_argument('--'+key,type=Path,required=True)
    parser.add_argument('--worker',action='store_true')
    parser.add_argument('--prepare-only',action='store_true')
    args=parser.parse_args()
    for key in ('repo','binary','build_binding','library','inspector','source_collection','bound_source'):
        setattr(args,key,getattr(args,key).resolve(strict=True))
    args.work=args.work.resolve()
    need(not args.work.is_relative_to(args.source_collection) and not args.work.is_relative_to(args.bound_source),
         'new run must not overwrite sealed source evidence')
    if args.worker:worker(args)
    else:
        proof=preflight(args)
        if args.prepare_only:
            args.work.mkdir(parents=True,mode=0o700,exist_ok=False)
            atomic_json(args.work/'preflight.json',proof)
            print(json.dumps(dict(complete=True,scientific_queries=0,cases=16,expected_queries=EXPECTED)))
        else:supervise(args,proof)


if __name__=='__main__':main()
