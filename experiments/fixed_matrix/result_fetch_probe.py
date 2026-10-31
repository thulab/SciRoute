"""Reduced sealed N128 GRIB same-product FUSE/FETCH transport probe.

One resource window, existing eight asynchronous queries, unmodified full-owned
query clock. Two campaigns and two measured waves are descriptive probes only.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import shutil
import signal
import statistics
import xml.etree.ElementTree as ET

from . import client, query_task_client, run, telemetry
# Loaded lazily in collect: the frozen implementation must exist before seal.
from .hdf5_native_ab import NativeABRunner, ProtocolSpec, PATH_ARGS
from fs_index.native import NativeContext
from .owner_reader_pool import OwnerReaderPoolRunner, selected_groups, cmake_cache, resolved_libraries
from experiments.identity.model import build_run, digest
from experiments.identity.registry import Registry, file_sha, read_json
from experiments.cross_version.resource_limited.resources import atomic_json

PROTOCOL = 'iotdbfs-fixed-grib-result-fetch-probe/1'
WORK = 'experiments/cross_version/work/result-fetch-probe-20261025'
CONFIGS = ('S1', 'S2', 'S4')
WORKERS = {'S1': 1, 'S2': 2, 'S4': 4}
VARIANTS = ('FUSE', 'FETCH')
CAMPAIGNS = (0, 1)
ORDERS = (CONFIGS, tuple(reversed(CONFIGS)))
WARMUPS, MEASURED, REPEATS = 1, 2, 3
need = run.need


def configuration_order(campaign):
    need(campaign in CAMPAIGNS, 'unknown campaign')
    return ORDERS[campaign]


def variant_order(campaign, config):
    need(campaign in CAMPAIGNS and config in CONFIGS, 'unknown variant block')
    return VARIANTS if (campaign + CONFIGS.index(config)) % 2 == 0 else VARIANTS[::-1]


def expected_keys(groups, qualification=False):
    return {(g['group_id'], c, s, v, r, q)
            for g in groups for c in ((0,) if qualification else CAMPAIGNS)
            for s in (('S4',) if qualification else CONFIGS) for v in VARIANTS
            for r in range(1 if qualification else REPEATS) for q in range(8)}


def percentile(values, fraction):
    values = sorted(values)
    need(bool(values) and 0 <= fraction <= 1, 'invalid percentile')
    p = (len(values)-1)*fraction; lo = int(p); hi = min(lo+1, len(values)-1)
    return values[lo] + (values[hi]-values[lo])*(p-lo)


class QueryTaskRunner(OwnerReaderPoolRunner):
    def groups(self, repo, tier):
        return [g for g in super().groups(repo, tier) if g['format'] == 'grib']

    def expected_rows(self, args):
        return len(expected_keys(self.groups(args.repo, args.tier)))

    def verify_candidate(self, args):
        binding = read_json(args.build_binding)
        need(binding.get('complete') is True and args.baseline_build_binding == args.build_binding,
             'one complete transport comparison binding required')
        need(args.binary == args.baseline_binary and
             binding['candidate_sha256'] == binding['baseline_sha256'] == self.spec.baseline_sha,
             'both transport arms must use identical candidate product')
        b = binding['product']; binary = args.binary
        need(Path(b['binary']).resolve(strict=True) == binary and
             file_sha(binary) == b['sha256'] == binding['candidate_sha256'], 'product identity changed')
        root = Path(b['source_root']).resolve(strict=True); manifest = Path(b['source_manifest_path'])
        need(file_sha(manifest) == b['source_manifest_sha256'], 'product source manifest changed')
        sources = read_json(manifest)
        need(bool(sources) and all(not Path(n).is_absolute() and '..' not in Path(n).parts and
             (root/n).is_file() and not (root/n).is_symlink() and file_sha(root/n) == h
             for n,h in sources.items()), 'frozen product sources changed')
        need({str(p.relative_to(root)) for p in root.rglob('*') if p.is_file() and
             '__pycache__' not in p.relative_to(root).parts} == set(sources), 'product inventory changed')
        cache_path = binary.parents[1]/'CMakeCache.txt'; cache = cmake_cache(cache_path)
        need(file_sha(cache_path) == b['build_cache_sha256'] and cache['CMAKE_BUILD_TYPE'] == 'Release' and
             Path(cache['CMAKE_HOME_DIRECTORY']).resolve(strict=True) == root, 'build cache changed')
        unit = Path(b['unit_binary']); count = b['expected_unit_tests']; xml_path = Path(b['unit_tests_path'])
        need(unit.resolve(strict=True).parent == binary.parent and file_sha(unit) == b['unit_binary_sha256'] and
             file_sha(xml_path) == b['unit_tests_sha256'] and count > 1990, 'new full unit receipt changed')
        xml = ET.parse(xml_path).getroot(); cases = list(xml.iter('testcase'))
        need(len(cases) == int(xml.get('tests')) == count and
             not any(list(xml.iter(k)) for k in ('failure','error','skipped')) and
             all(int(n.get(k,0)) == 0 for n in xml.iter() if n.tag in ('testsuite','testsuites')
                 for k in ('failures','errors','disabled','skipped')) and
             all(c.get('status') == 'run' and c.get('result') == 'completed' for c in cases),
             'full unit gate incomplete')
        for suite,name in binding['required_regression_tests']:
            need(sum(c.get('classname') == suite and c.get('name') == name for c in cases) == 1,
                 'required fetch regression absent')
        libraries = resolved_libraries(binary)
        receipt = Path(b['build_receipt']); build = read_json(receipt)
        need(libraries == b['runtime_libraries'] and file_sha(receipt) == b['build_receipt_sha256'] and
             build.get('complete') is True and build.get('tests') == count and
             file_sha(root.parent/'source.tar.gz') == b['source_archive_sha256'], 'accepted build evidence changed')
        old = binding['accepted_directed_wakeup']; oldroot = Path(old['source_root'])
        oldmanifest = Path(old['source_manifest_path'])
        need(file_sha(oldmanifest) == old['source_manifest_sha256'] and
             file_sha(Path(old['binary'])) == old['sha256'] ==
             '4caa446dc2dadbec9227fbdafd9062425a3ae56910c05c96352e341e98e40cb7',
             'accepted directed-wakeup baseline changed')
        oldsources = read_json(oldmanifest)
        need(all((oldroot/n).is_file() and file_sha(oldroot/n) == h for n,h in oldsources.items()),
             'accepted baseline frozen source changed')
        product = lambda n: n.startswith(('src/','include/','cmake/')) or n == 'CMakeLists.txt'
        changed = {n for n in set(oldsources)|set(sources) if product(n) and oldsources.get(n) != sources.get(n)}
        need(changed == set(binding['product_diff']) == self.spec.product_diff,
             'fetch product change exceeds declared scope')
        comparison = Path(binding['comparison_environment_receipt']); environment = read_json(comparison)
        need(file_sha(comparison) == binding['comparison_environment_receipt_sha256'] and
             environment.get('complete') is True and
             environment.get('actual_target_flags_and_libraries_equal') is True and
             environment['source_sha256'] == file_sha(args.repo/WORK/'verify_build_environment.py') and
             environment['records']['NEW']['libraries'] == libraries and
             environment['records']['OLD']['libraries'] == resolved_libraries(Path(old['binary'])),
             'accepted baseline/candidate build environment changed')
        need(tuple(tuple(t) for t in binding['required_regression_tests']) == self.spec.required_regression_tests,
             'fetch regression contract differs')
        return dict(complete=True,candidate_sha256=binding['candidate_sha256'],
                    baseline_sha256=binding['baseline_sha256'],unit_tests=count,
                    runtime_libraries=libraries,source_manifest_sha256=file_sha(manifest),
                    changed_product_files=sorted(changed),transport_isolation='same product binary',
                    accepted_directed_wakeup_sha256=old['sha256'],
                    comparison_environment_sha256=file_sha(comparison))

    def source_proof(self, args):
        proof = super().source_proof(args)
        proof['cases'] = [c for c in proof['cases'] if c['format'] == 'grib']
        proof.pop('pools', None)
        proof.update(protocol=PROTOCOL, configurations=list(CONFIGS), query_task_workers=WORKERS,
            reader_pool_size=4, queue_capacity=64, configuration_order=[list(o) for o in ORDERS],
            variants=list(VARIANTS), arms=['burst8'], campaigns=list(CAMPAIGNS),
            expected_queries=self.expected_rows(args), qualification_queries=16,
            warmup_per_arm=WARMUPS, measured_per_arm=MEASURED,
            load_concurrency={'burst8':8}, client_executor='one original asyncio event loop; no workers',
            clock='original complete owned query clock; profile and oracle outside',
            diagnostic_only=True, server_tracing=False, fuse_tracing=False, client_tracing=False,
            scientific_publication=False, index_rebuilt=False, new_cases=False,
            transport_isolation='same candidate binary; only result delivery differs',
            variant_order={str(c):{s:list(variant_order(c,s)) for s in CONFIGS} for c in CAMPAIGNS})
        return proof

    def launcher(self, path, binary, sha, namespace, journal, plan, configuration):
        need(configuration in CONFIGS, 'unknown scheduler configuration')
        receipt = run.infrastructure.launcher(path, binary, sha, namespace, journal)
        declaration = 'SETTINGS='+repr(receipt['settings'])+'\n'; source = path.read_text()
        need(source.count(declaration) == 1, 'launcher settings layout differs')
        settings = dict(receipt['settings'], **{
            'storage.version_directory.reader_pool_size':'4',
            'query.tasks.worker_threads':str(WORKERS[configuration]),
            'query.tasks.max_queued_tasks':'64',
            'query.tasks.max_batch_size':str(min(4, WORKERS[configuration]))})
        source = source.replace(declaration, 'SETTINGS='+repr(settings)+'\n', 1)
        marker = 'os.execv(BINARY,[BINARY,*sys.argv[1:]])'
        need(source.count(marker) == 1, 'launcher exec marker differs')
        clear = "os.environ.pop('IOTDBFS_TAIL_TRACE_FILE',None)\nos.environ.pop('IOTDBFS_TAIL_TRACE_CAPACITY',None)\n"
        path.write_text(source.replace(marker, clear+marker, 1))
        receipt.update(settings=settings, sha256=file_sha(path), tracing=False)
        return receipt

    def collect(self, args, execution, bindings, registry, frozen, report, env, paired, *, qualification=False):
        paths = read_json(args.work/'resource-paths.json'); count = 0
        filename = 'qualification.jsonl' if qualification else 'queries.jsonl'
        waves_file = 'qualification-waves.jsonl' if qualification else 'waves.jsonl'
        writer = args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
        for campaign in ((0,) if qualification else CAMPAIGNS):
            for plan in [p for p in execution if (p['group']['N'] == 8) == qualification]:
                group = plan['group']; gid = group['group_id']; bound = bindings[gid]
                for config in (('S4',) if qualification else configuration_order(campaign)):
                    for variant in variant_order(campaign, config):
                        # One directory below campaign preserves inherited emergency
                        # unmount glob corpora/*/campaign-*/*/mount/mount.
                        block = plan['execution_root']/('campaign-qualification' if qualification else 'campaign-'+str(campaign))/(config+'-'+variant)
                        block.mkdir(parents=True)
                        journal = block/'owner'; shutil.copytree(plan['journal'], journal)
                        need(run.infrastructure.journal_snapshot(journal) == plan['journal_digest'], 'journal copy differs')
                        binary = args.binary; sha = file_sha(binary)
                        launch = self.launcher(block/'launcher', binary, sha, plan['item']['namespace'], journal, plan, config)
                        atomic_json(block/'launcher.receipt.json', launch)
                        before, _ = run.head(args.inspector, plan['item']['namespace'], env, block/'head-before')
                        need(before == plan['scientific_head'], 'head changed before mount')
                        print(json.dumps(dict(event='scheduler_mount', qualification=qualification,
                            campaign=campaign, group=gid, configuration=config, variant=variant)), flush=True)
                        with paired.sf.private_mount(Path(launch['path']), block/'mount', env, writer, profile=True) as context, ExitStack() as stack:
                            need(file_sha(Path('/proc')/str(context['pid'])/'exe') == sha, 'running binary differs')
                            actual = read_json(context['root']/'profile.launch.json')
                            need(actual['settings'] == launch['settings'], 'actual scheduler settings differ')
                            native = stack.enter_context(NativeContext(args.library, dict(plan['cfg'], build_index=False)))
                            environment = telemetry.environment(args.repo, artifact_paths=[args.library,binary],
                                process_roles=dict(client=os.getpid(), daemon=context['pid'],
                                    datanode=int((args.repo.parent/'runtime/run/datanode.pid').read_text())), data_path=plan['root'])
                            atomic_json(block/'environment.json', environment)
                            mount = dict(campaign=campaign, configuration=config, variant=variant, qualification=qualification,
                                daemon_pid=context['pid'], startup_ns=context['startup_ns'], product_sha256=sha,
                                scheduler_workers=WORKERS[config], settings=launch['settings'],
                                environment_sha256=file_sha(block/'environment.json'))
                            plan['item']['mounts'].append(mount)
                            atomic_json(block/'query-plan.json', native.explain('index', bound['native_selection'], client.PREDICATE, bound['scope']))
                            model = build_run(registry.get_case(bound['case_id']), execution=str(block), arm='burst8',
                                implementation=dict(binary_sha256=sha, runner_sha256=digest(frozen['sources']),
                                    engine='format_catalog + two sequential typed'),
                                clock=dict(kind='client-owned-full-query', d='entry through discovery delivery',
                                    q='discovery through owned Float32/mask', profile_oracle='outside',
                                    throughput='query count / first entry through last owned output'),
                                cache=dict(protocol=PROTOCOL, configuration=config, variant=variant,
                                    reader_pool_size=4, daemon_settings=launch['settings']),
                                environment=dict(resource_id=frozen['resource_id'], process_receipt_sha256=mount['environment_sha256']),
                                replication=dict(campaign=campaign, qualification=qualification,
                                    warmup=0 if qualification else WARMUPS, measured=0 if qualification else MEASURED,
                                    inflight=8, schedule='opposite campaign configuration and per-configuration variant order'))
                            registered = registry.register_run(model)
                            bound['run_ids'].setdefault(str(campaign), {}).setdefault(config, {})[variant] = registered['run_id']
                            atomic_json(args.work/'bindings.json', bindings)
                            for repeat in range(1 if qualification else REPEATS):
                                rb = telemetry.sample(paths); run.validate_sample(rb)
                                metadata = dict(group_id=gid, corpus_id=bound['corpus_id'], case_id=bound['case_id'],
                                    oracle_sha256=bound['oracle_sha256'], run_id=registered['run_id'], campaign=campaign,
                                    configuration=config, variant=variant, scheduler_workers=WORKERS[config], reader_pool_size=4,
                                    load='burst8', arm='IoTDB-FS', repeat=repeat,
                                    phase='qualification' if qualification else 'warmup' if repeat < WARMUPS else 'measurement',
                                    resource_id=frozen['resource_id'], environment_sha256=mount['environment_sha256'], product_sha256=sha)
                                try:
                                    from . import result_fetch_client
                                    transport = query_task_client if variant == 'FUSE' else result_fetch_client
                                    wave = transport.run_wave(context, group, bound['scope'], bound['physical_versions'],
                                        bound['expected_matches'], inflight=8)
                                except query_task_client.WaveError as error:
                                    atomic_json(block/('failed-wave-'+str(repeat)+'.json'), error.result)
                                    raise
                                for profile in wave['profiles']:
                                    need(profile['runtime_config']['query_task_workers'] == WORKERS[config], 'actual worker count differs')
                                need(wave['intervals']['execution']['max_parallel'] <= WORKERS[config], 'execution exceeds worker bound')
                                ra = telemetry.sample(paths); run.validate_sample(ra)
                                need(wave['complete'] and len(wave['rows']) == 8, 'incomplete wave')
                                common = dict(metadata, resource_before=rb, resource_after=ra, ok=True)
                                wave_id = digest(dict(group=gid, campaign=campaign, configuration=config,
                                    variant=variant, load='burst8', repeat=repeat, qualification=qualification))
                                run.append(args.work/waves_file, dict(common, wave_id=wave_id,
                                    **{k:v for k,v in wave.items() if k != 'rows'}))
                                for row in wave['rows']:
                                    row.update(common, wave_id=wave_id); run.append(args.work/filename, row); count += 1
                                report['qualification_records' if qualification else 'query_records'] = count
                                atomic_json(args.work/'report.json', report)
                        after, _ = run.head(args.inspector, plan['item']['namespace'], env, block/'head-after')
                        need(after == before and run.infrastructure.journal_snapshot(journal) == plan['journal_digest'], 'query changed head/journal')
                        need(file_sha(plan['root']/'index.sqlite') == plan['prep']['index_sha256'], 'query changed sealed FS index')
                        mount.update(complete=True, scientific_head_unchanged=True, journal_unchanged=True)
                        atomic_json(args.work/'report.json', report)
        return count

    def validate_rows(self, rows, bindings, selected, resource_id, candidate_sha, *, qualification=False):
        wanted = expected_keys(selected, qualification)
        keys = {(r['group_id'], r['campaign'], r['configuration'], r['variant'], r['repeat'], r['query_index']) for r in rows}
        need(len(rows) == len(wanted) and keys == wanted, 'exact unique query matrix required; no replacement')
        groups = {g['group_id']:g for g in selected}
        for row in rows:
            b = bindings[row['group_id']]; g = groups[row['group_id']]
            need(all(row[k] == b[k] for k in ('corpus_id','case_id','oracle_sha256')) and
                 row['run_id'] == b['run_ids'][str(row['campaign'])][row['configuration']][row['variant']], 'query identity differs')
            need(row['resource_id'] == resource_id and
                 row['product_sha256'] == (self.spec.baseline_sha if row['variant'] == 'FUSE' else candidate_sha) and
                 row['scheduler_workers'] == WORKERS[row['configuration']] and row['reader_pool_size'] == 4 and
                 row['load'] == 'burst8' and row['arm'] == 'IoTDB-FS', 'query configuration/product differs')
            need(all(row.get(k) is True for k in ('ok','oracle_verified','mode_verified')) and
                 row['owned_output_bytes'] == g['owned_output_bytes'] and row['selected_units'] == g['K_units'], 'owned output differs')
            need(all(type(row[k]) is int and row[k] > 0 for k in ('total_ns','discovery_ns','value_ns')) and
                 all(type(row[k]) is int for k in ('t0_ns','t1_ns','t2_ns')) and
                 row['t0_ns'] < row['t1_ns'] < row['t2_ns'] and row['total_ns'] == row['discovery_ns']+row['value_ns'] and
                 row['total_ns'] == row['t2_ns']-row['t0_ns'] and row['discovery_ns'] == row['t1_ns']-row['t0_ns'] and
                 row['value_ns'] == row['t2_ns']-row['t1_ns'], 'original full query clock differs')
            need(row['phase'] == ('qualification' if qualification else 'warmup' if row['repeat'] < WARMUPS else 'measurement'), 'phase differs')
            run.validate_sample(row['resource_before']); run.validate_sample(row['resource_after'])
            need(row['resource_before']['monotonic_ns'] < row['resource_after']['monotonic_ns'], 'resource clock reversed')

            if row['variant'] == 'FETCH':
                need(row.get('transport') == 'result_fetch_v1' and len(row['value_calls']) == 2,
                     'FETCH query shape differs')
                previous = row['t1_ns']
                for call in row['value_calls']:
                    fields = ('fetch_start_ns','fetch_end_ns','decode_start_ns','decode_end_ns',
                              'append_start_ns','owned_ready_ns','fetch_ns','decode_owned_ns','append_owned_ns')
                    need(all(type(call.get(k)) is int for k in fields), 'missing integer fetch clock')
                    a,b,c,d,e,f = (call[k] for k in fields[:6])
                    need(previous <= a <= b == c <= d <= e <= f <= row['t2_ns'] and
                         call['fetch_ns'] == b-a and call['append_owned_ns'] == f-e and
                         call['decode_owned_ns'] == d-c+f-e, 'fetch/owned clock closure differs')
                    previous = f
            else:
                need('transport' not in row and all('fetch_start_ns' not in c for c in row['value_calls']),
                     'FUSE baseline must retain original client body')

    def validate_waves(self, waves, rows, bindings, selected, resource_id, candidate_sha, *, qualification=False):
        wanted = {k[:-1] for k in expected_keys(selected, qualification)}
        key_of = lambda r: (r['group_id'],r['campaign'],r['configuration'],r['variant'],r['repeat'])
        need(len(waves) == len(wanted) and {key_of(w) for w in waves} == wanted, 'exact unique wave matrix required')
        grouped = {}
        for row in rows: grouped.setdefault(key_of(row), []).append(row)
        for wave in waves:
            members = grouped[key_of(wave)]
            need(wave.get('complete') is True and wave.get('ok') is True and not wave.get('errors') and
                 wave['query_count'] == len(members) == wave['inflight'] == wave['client_peak_inflight'] == 8,
                 'wave count/complete differs')
            need(type(wave['makespan_ns']) is int and wave['makespan_ns'] > 0 and
                 abs(wave['throughput_qps']-8e9/wave['makespan_ns']) < 1e-6, 'wave QPS differs')
            expected_id = digest(dict(group=wave['group_id'], campaign=wave['campaign'], configuration=wave['configuration'],
                variant=wave['variant'], load='burst8', repeat=wave['repeat'], qualification=qualification))
            need(wave['wave_id'] == expected_id and all(r['wave_id'] == expected_id for r in members), 'wave identity differs')
            need(all(wave[k] == members[0][k] for k in ('group_id','corpus_id','case_id','oracle_sha256','run_id',
                'configuration','variant','scheduler_workers','reader_pool_size','resource_id','product_sha256','phase',
                'resource_before','resource_after','environment_sha256','load','arm')), 'wave/query provenance differs')
            start = min(r['t0_ns'] for r in members); end = max(r['t2_ns'] for r in members)
            need(wave['start_ns'] == start and wave['end_ns'] == end and wave['makespan_ns'] == end-start,
                 'wave must end at final owned output')
            profiles = wave['profiles']
            need(len(profiles) == 24 and len({p['sequence'] for p in profiles}) == 24, 'profile sequence count differs')
            for profile in profiles:
                cfg = profile['runtime_config']
                need(profile.get('ok') is True and start <= profile['steady_start_ns'] <= profile['steady_end_ns'] <= end and
                     profile['steady_end_ns']-profile['steady_start_ns'] == profile['server_total_ns'] and
                     cfg['query_task_workers'] == WORKERS[wave['configuration']] and cfg['query_task_max_queued'] == 64 and
                     cfg['query_task_max_batch'] == min(4, WORKERS[wave['configuration']]), 'profile clock/configuration differs')
            recalculated = query_task_client.analyze_intervals(profiles, (start,end))
            need(wave['intervals'] == recalculated and recalculated['execution']['max_parallel'] <= WORKERS[wave['configuration']],
                 'execution interval recomputation differs')
        # Physical order is part of the drift-control design, not just a set.
        for campaign in ((0,) if qualification else CAMPAIGNS):
            actual = [(w['configuration'],w['variant'],w['repeat']) for w in waves if w['campaign'] == campaign]
            order = [('S4')] if qualification else configuration_order(campaign)
            expected = [(s,v,r) for s in order for v in variant_order(campaign,s)
                        for r in range(1 if qualification else REPEATS)]
            need(actual == expected, 'physical configuration/variant order differs')

    def analyze(self, path, bindings, selected, resource_id, candidate_sha):
        work = Path(path).parent
        load = lambda p: [json.loads(s) for s in p.read_text().splitlines()]
        rows = load(Path(path)); waves = load(work/'waves.jsonl')
        qual = load(work/'qualification.jsonl'); qwaves = load(work/'qualification-waves.jsonl')
        gate = read_json(work/'qualification-report.json')
        need(gate.get('complete') is True and gate['query_records'] == 16 and
             gate['queries_sha256'] == file_sha(work/'qualification.jsonl'), 'qualification receipt incomplete/changed')
        qgroups = gate['groups']
        need(len(qgroups) == 1 and qgroups[0]['group_id'] == 'FM1-grib-N8-V1-C8-S0-base-latest', 'qualification case differs')
        self.validate_rows(rows,bindings,selected,resource_id,candidate_sha)
        self.validate_waves(waves,rows,bindings,selected,resource_id,candidate_sha)
        self.validate_rows(qual,bindings,qgroups,resource_id,candidate_sha,qualification=True)
        self.validate_waves(qwaves,qual,bindings,qgroups,resource_id,candidate_sha,qualification=True)
        cells = []; effects = []; scaling = []
        for group in selected:
            indexed = {}
            for config in CONFIGS:
                for variant in VARIANTS:
                    blocks = []
                    for campaign in CAMPAIGNS:
                        rr = [r for r in rows if (r['group_id'],r['configuration'],r['variant'],r['campaign'],r['phase']) ==
                              (group['group_id'],config,variant,campaign,'measurement')]
                        ww = [w for w in waves if (w['group_id'],w['configuration'],w['variant'],w['campaign'],w['phase']) ==
                              (group['group_id'],config,variant,campaign,'measurement')]
                        need(len(rr) == 16 and len(ww) == 2, 'measured cell size differs')
                        lat = [r['total_ns']/1e6 for r in rr]; span = sum(w['makespan_ns'] for w in ww)
                        blocks.append(dict(campaign=campaign, queries=len(rr), waves=len(ww), makespan_sum_ns=span,
                            throughput_qps=len(rr)*1e9/span,
                            latency_ms=dict(median=statistics.median(lat), mean=statistics.mean(lat),
                                descriptive_p95=percentile(lat,.95), minimum=min(lat), maximum=max(lat)),
                            discovery_median_ms=statistics.median(r['discovery_ns']/1e6 for r in rr),
                            value_median_ms=statistics.median(r['value_ns']/1e6 for r in rr),
                            execution_peak=max(w['intervals']['execution']['max_parallel'] for w in ww),
                            execution_mean=sum(w['intervals']['execution']['integral_active_ns'] for w in ww)/span,
                            value_execution_peak=max(w['intervals']['operations'].get('typed',{}).get('max_parallel',0) for w in ww),
                            discovery_execution_peak=max(w['intervals']['operations'].get('format_catalog',{}).get('max_parallel',0) for w in ww),
                            queue_median_ms=statistics.median(p['counters'].get('query_tasks.queue_nanoseconds',0)/1e6 for w in ww for p in w['profiles'])))
                    cell = dict(group_id=group['group_id'], configuration=config, scheduler_workers=WORKERS[config],
                        variant=variant, load='burst8', blocks=blocks,
                        throughput_qps=statistics.median(b['throughput_qps'] for b in blocks),
                        latency_median_ms=statistics.median(b['latency_ms']['median'] for b in blocks),
                        latency_p95_ms=statistics.median(b['latency_ms']['descriptive_p95'] for b in blocks),
                        execution_peak=max(b['execution_peak'] for b in blocks))
                    cells.append(cell); indexed[(config,variant)] = cell
                ratios = [dict(campaign=n['campaign'], throughput_ratio=n['throughput_qps']/o['throughput_qps'],
                    latency_ratio=n['latency_ms']['median']/o['latency_ms']['median'])
                    for o,n in zip(indexed[(config,'FUSE')]['blocks'],indexed[(config,'FETCH')]['blocks'])]
                effects.append(dict(group_id=group['group_id'], configuration=config, reference='FUSE', candidate='FETCH',
                    blocks=ratios, throughput_ratio=statistics.median(r['throughput_ratio'] for r in ratios),
                    latency_ratio=statistics.median(r['latency_ratio'] for r in ratios)))
            for variant in VARIANTS:
                for config in CONFIGS[1:]:
                    ratios = [dict(campaign=n['campaign'],throughput_ratio=n['throughput_qps']/o['throughput_qps'],
                        latency_ratio=n['latency_ms']['median']/o['latency_ms']['median'])
                        for o,n in zip(indexed[('S1',variant)]['blocks'],indexed[(config,variant)]['blocks'])]
                    scaling.append(dict(group_id=group['group_id'],variant=variant,configuration=config,reference='S1',blocks=ratios,
                        throughput_ratio=statistics.median(r['throughput_ratio'] for r in ratios),
                        latency_ratio=statistics.median(r['latency_ratio'] for r in ratios)))
        return dict(protocol=PROTOCOL, complete=True, query_rows=len(rows), wave_rows=len(waves), qualification_rows=len(qual),
            qualification_resource_coverage=run.validate_coverage(work,qual), cells=cells, effects=effects, scaling=scaling,
            statistic='Two measured eight-query waves per campaign/block; all fixed warm samples retained. QPS=sum queries/sum wave spans. Latency median/p95 within campaign, then median of two campaign statistics. Effects paired by campaign.',
            limitation='Reduced descriptive optimization probe, no significance or tail guarantee. Two CPU quota; profile execution excludes post-profile tail and Ready wait, and is not physical CPU parallelism or whole-slot occupancy. Existing single asyncio loop, no within-query splitting; warm process/OS, not cold disk.')

    def worker(self, args):
        def stop(sig, frame):
            raise RuntimeError('result fetch probe interrupted; preserve evidence')
        for sig in (signal.SIGTERM,signal.SIGHUP): signal.signal(sig,stop)
        proof = self.source_proof(args); frozen = read_json(args.work/'prepared.json')
        need(proof == read_json(args.work/'reuse-source.json') and self.closure(args.repo) == frozen['sources'], 'frozen preflight differs')
        paired, _, env, startup = run.dependencies(args.repo)
        startup.adjust_mount(paired.sf,1800,args.work/'startup-receipts.jsonl')
        report = dict(protocol=PROTOCOL,complete=False,tier=args.tier,query_records=0,qualification_records=0,
            expected_queries=self.expected_rows(args),corpora=[],product_sha256=file_sha(args.binary),
            baseline_sha256=self.spec.baseline_sha,resource_id=frozen['resource_id'],implementation='same-product-result-fetch-AB',
            scientific_publication=False,index_rebuilt=False,new_cases=False,location_bindings_created=False,credentials_recorded=False)
        atomic_json(args.work/'report.json',report)
        try:
            registry = Registry(args.work/'registry'); execution,bindings = self.plans(args,registry,report,proof)
            atomic_json(args.work/'execution-order.json',dict(configurations=list(CONFIGS),variants=list(VARIANTS),
                formal_orders=[list(o) for o in ORDERS],variant_order=proof['variant_order'],qualification_first=True,
                qualification_queries=16,formal_queries=288,formal_waves=36,measured_queries=192,measured_waves=24))
            q = self.collect(args,execution,bindings,registry,frozen,report,env,paired,qualification=True)
            need(q == 16, 'N8 qualification count differs')
            qgroups = [g for g in selected_groups(args.repo,8) if g['format'] == 'grib']
            qrows = [json.loads(s) for s in (args.work/'qualification.jsonl').read_text().splitlines()]
            qwaves = [json.loads(s) for s in (args.work/'qualification-waves.jsonl').read_text().splitlines()]
            self.validate_rows(qrows,bindings,qgroups,frozen['resource_id'],frozen['product']['sha256'],qualification=True)
            self.validate_waves(qwaves,qrows,bindings,qgroups,frozen['resource_id'],frozen['product']['sha256'],qualification=True)
            atomic_json(args.work/'qualification-report.json',dict(complete=True,query_records=q,groups=qgroups,
                queries_sha256=file_sha(args.work/'qualification.jsonl'),performance_statistics=False))
            report['qualification_complete'] = True
            count = self.collect(args,execution,bindings,registry,frozen,report,env,paired)
            need(count == self.expected_rows(args), 'formal row count differs')
            need(self.source_proof(args) == proof and self.closure(args.repo) == frozen['sources'], 'final frozen inputs differ')
            for plan in execution:
                plan['item'].update(complete=True,final_raw_validation=run.verify_raw(plan['manifest'],plan['root']/'input',plan['root']/'fs'))
            report.update(complete=True,queries_sha256=file_sha(args.work/'queries.jsonl'),
                waves_sha256=file_sha(args.work/'waves.jsonl'),bindings_sha256=file_sha(args.work/'bindings.json'))
        except BaseException as error:
            report['failure'] = dict(type=type(error).__name__,message=str(error)[:500]); raise
        finally:
            atomic_json(args.work/'report.json',report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in PATH_ARGS: parser.add_argument('--'+key.replace('_','-'),type=Path,required=True)
    parser.add_argument('--tier',choices=('n128',),default='n128'); parser.add_argument('--gate-report',type=Path)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--worker',action='store_true'); modes.add_argument('--prepare-only',action='store_true')
    modes.add_argument('--verify-prepared',action='store_true'); args = parser.parse_args()
    for key in PATH_ARGS: setattr(args,key,getattr(args,key).resolve(strict=key != 'work'))
    for source in (args.source_collection,args.current_collection):
        need(not args.work.is_relative_to(source) and not source.is_relative_to(args.work),'run/source overlap')
    binding = read_json(args.build_binding)
    spec = ProtocolSpec(PROTOCOL,'experiments.fixed_matrix.result_fetch_probe',__doc__,binding['baseline_sha256'],
        frozenset(binding['product_diff']),tuple(tuple(t) for t in binding['required_regression_tests']),
        ('experiments/fixed_matrix/result_fetch_probe.py','experiments/fixed_matrix/grib_single_message.py',
         'experiments/fixed_matrix/query_task_client.py','experiments/fixed_matrix/result_fetch_client.py',WORK+'/protocol/offline_gate.py',
         WORK+'/experiment-plan.json',*tuple(binding['extra_closure'])))
    runner = QueryTaskRunner(spec)
    if args.worker: runner.worker(args)
    else:
        proof = runner.source_proof(args)
        if args.prepare_only: runner.prepare_only(args,proof)
        elif args.verify_prepared: runner.verify_prepared(args,proof)
        else: runner.supervise(args,proof)


if __name__ == '__main__': main()
