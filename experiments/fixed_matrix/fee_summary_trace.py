"""Attribution-only real fee traces from sealed independent N8 and N128 cases.

One historical diagnostic product, one mount per tier, five warmups and ten
formal discovery-only requests. No product performance comparison is made.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import signal
import statistics

from . import cscale_diagnosis as diag, run, telemetry
from .hdf5_native_ab import NativeABRunner, ProtocolSpec
from experiments.cross_version.hdf5_route3.native_filesystem.fixed_probe import filesystem_launcher
from experiments.cross_version.resource_limited.resources import atomic_json
from experiments.identity.model import build_run, digest
from experiments.identity.registry import Registry, file_sha, read_json

PROTOCOL = 'iotdbfs-fixed-fee-summary-smalltrace/1'
WARMUPS = 5
MEASURED = 10
CONFIGURATIONS = ('B',)
CAMPAIGNS = (0,)
PROBE_SHA = '3fa55b1c012d281a11c3e5a0538520d7c87f82c9d0621e4b65679253e3e488c4'
need = run.need
RESTORATION_FLAGS = ('complete', 'scientific_complete', 'ready_for_analysis',
                     'original_service_restored', 'resources_restored',
                     'controller_restoration_verified')


def analyze_rows(rows, bindings, group, resource_id, candidate_sha):
    gid = group['group_id']
    bound = bindings[gid]
    need(candidate_sha == PROBE_SHA, 'unexpected historical diagnostic product')
    need(len(rows) == WARMUPS+MEASURED and
         [(r.get('campaign'), r.get('configuration'), r.get('repeat')) for r in rows] ==
         [(0, 'B', r) for r in range(WARMUPS+MEASURED)],
         'all fifteen preregistered rows required, in order; no exclusions')
    for row in rows:
        need(row.get('group_id') == gid and row.get('N') == group['N'] and
             row.get('C') == group['C_paths'] and row.get('arm') == 'IoTDB-FS',
             'independent scientific case/namespace differs')
        need(all(row.get(k) == bound[k] for k in ('corpus_id', 'case_id', 'oracle_sha256')) and
             row.get('run_id') == bound['run_ids']['0']['B'], 'scientific/run identity differs')
        need(row.get('product_sha256') == candidate_sha and row.get('resource_id') == resource_id and
             row.get('ok') is True and row.get('phase') ==
             ('warmup' if row['repeat'] < WARMUPS else 'measurement'), 'product/resource/phase differs')
        need(row.get('client_components') is not None, 'diagnostic client boundary clocks missing')
        diag.validate_discovery_row(row, bound['scope'], bound['expected_matches'])
        if row['phase'] == 'measurement':
            diag.validate_formal_profile(row, group['C_paths'], 'B')
        for side in ('resource_before', 'resource_after'):
            run.validate_sample(row[side])
        need(row['resource_before']['monotonic_ns'] < row['resource_after']['monotonic_ns'],
             'query resource clock reversed')
    formal = rows[WARMUPS:]
    fee_counters = [{k: r['discovery_call']['server_profile']['counters'][k]
                     for k in diag.FORMAL_POSITIVE} for r in formal]
    need(all(v == fee_counters[0] for v in fee_counters), 'real fee counters differ across formal requests')
    return dict(protocol=PROTOCOL, complete=True, query_rows=len(rows), warmup_rows=WARMUPS,
                formal_rows=MEASURED, configurations=['B'], mounts=1, N=group['N'], C=group['C_paths'],
                performance_claim=False, diagnostic_only=True,
                discovery_ns=dict(mean=statistics.mean(r['discovery_ns'] for r in formal),
                                  median=statistics.median(r['discovery_ns'] for r in formal)),
                server_profile=diag.profile_summary(formal), fee_counters=fee_counters[0],
                statistic='All fixed ten formal diagnostic requests retained; timings are attribution only.',
                limitation='One real corpus/trace per N; repeated requests are not independent scientific inputs; no full query/value or speedup claim.')


class SmallTraceRunner(diag.CScaleRunner):
    def __init__(self, spec):
        # The same historical binary binding verifier is inherited unchanged;
        # this protocol changes only the trace collection schedule and cases.
        self.spec = spec

    def groups(self, repo, tier):
        need(tier in ('n8', 'n128'), 'only sealed independent N8/N128 cases permitted')
        n = 8 if tier == 'n8' else 128
        by = {g['group_id']: g for g in read_json(repo/'experiments/fixed_matrix/catalog.json')['groups']}
        return [by[f'FM1-hdf5-N{n}-V1-C{n}-S0-base-latest']]

    def campaigns(self, group):
        return CAMPAIGNS

    def configuration_order(self, campaign):
        need(campaign == 0, 'one preregistered campaign only')
        return CONFIGURATIONS

    def expected_rows(self, args):
        return WARMUPS+MEASURED

    def validate_n8_gate(self, gate, baseline_sha, candidate_sha):
        need(gate.get('protocol') == PROTOCOL and gate.get('tier') == 'n8' and
             gate.get('query_records') == gate.get('expected_queries') == WARMUPS+MEASURED and
             all(gate.get(k) is True for k in RESTORATION_FLAGS) and
             gate.get('product_sha256') == candidate_sha == PROBE_SHA and
             gate.get('baseline_sha256') == baseline_sha == diag.BASELINE_SHA and
             gate.get('diagnostic_only') is True and gate.get('performance_claim') is False and
             not any(k in gate for k in ('failure', 'controller_failure', 'analysis_failure')),
             'new N8 trace gate incomplete, unrestored, failed or different historical product')

    def source_proof(self, args):
        target = args.work
        while not target.exists():
            target = target.parent
        space = shutil.disk_usage(target)
        need(space.total > 0 and space.free / space.total >= .01, 'less than approved 1% disk remains')
        need(file_sha(args.repo/'experiments/fixed_matrix/client.py') == diag.CLIENT_SHA,
             'original discovery client operations changed')
        proof = NativeABRunner.source_proof(self, args)
        need(proof['build_gate']['candidate_sha256'] == PROBE_SHA, 'historical probe binary differs')
        need(len(proof['cases']) == 1 and len(self.groups(args.repo, args.tier)) == 1,
             'each tier must use one independently sealed corpus')
        group, = self.groups(args.repo, args.tier)
        bound = read_json(args.source_collection/'bindings.json')[group['group_id']]
        need(len(bound['scope']) == group['N'] == group['C_paths'] and
             len(set(bound['scope'])) == len(bound['scope']) and
             bound['physical_versions'] == {'kind': 'latest'}, 'sealed scope/Latest selection differs')
        proof.update(configurations=['B'], arms=['IoTDB-FS discovery only'],
                     warmup_per_arm=WARMUPS, measured_per_arm=MEASURED,
                     warmup_per_scope=WARMUPS, measured_per_scope=MEASURED,
                     campaigns={group['group_id']: [0]}, expected_mounts=1,
                     diagnostic_only=True, performance_claim=False,
                     scope_semantics='One independent full namespace per tier; N=C, no N512 subset.',
                     product_role='Same-source historical baseline diagnostic overlay; no current selector candidate speedup claim.')
        return proof

    def plans(self, args, registry, report, proof):
        return NativeABRunner.plans(self, args, registry, report, proof)

    def collect(self, args, execution, bindings, registry, frozen, report, env, paired):
        plan, = execution
        group = plan['group']
        b = bindings[group['group_id']]
        paths = read_json(args.work/'resource-paths.json')
        writer = args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
        import numpy, pyarrow  # noqa: F401; preserve original client import seam before warmup
        block = plan['execution_root']/'campaign-0'/'B'
        block.mkdir(parents=True)
        journal = block/'owner'
        shutil.copytree(plan['journal'], journal)
        need(run.infrastructure.journal_snapshot(journal) == plan['journal_digest'], 'private journal differs')
        binary = args.binary
        sha = file_sha(binary)
        launch = filesystem_launcher(block/'launcher', binary, plan['item']['namespace'], journal,
                                     False, native_root=plan['native_root'], expected_sha=sha)
        trace_dir = block/'fee-traces'
        trace_dir.mkdir(mode=0o700)
        mount_env = dict(env, IOTDB_FS_CSCALE_TRACE_DIR=str(trace_dir))
        launch['diagnostic_trace_directory'] = str(trace_dir)
        atomic_json(block/'launcher.receipt.json', launch)
        before, _ = run.head(args.inspector, plan['item']['namespace'], env, block/'head-before')
        need(before == plan['scientific_head'], 'namespace head changed before mount')
        print(json.dumps(dict(event='fee_summary_trace_mount', N=group['N'], group=group['group_id'],
                              campaign=0, configuration='B')), flush=True)
        formal_fee_counters = []
        with paired.sf.private_mount(Path(launch['path']), block/'mount', mount_env, writer, profile=True) as context:
            need(file_sha(Path('/proc')/str(context['pid'])/'exe') == sha, 'running product differs')
            actual = read_json(context['root']/'profile.launch.json')
            need(actual['settings'] == launch['settings'], 'actual daemon settings differ')
            environment = telemetry.environment(args.repo, artifact_paths=[binary],
                process_roles=dict(client=os.getpid(), daemon=context['pid'],
                    datanode=int((args.repo.parent/'runtime/run/datanode.pid').read_text())), data_path=plan['root'])
            atomic_json(block/'environment.json', environment)
            mount = dict(campaign=0, configuration='B', daemon_pid=context['pid'], N=group['N'],
                         startup_ns=context['startup_ns'], product_sha256=sha,
                         environment_sha256=file_sha(block/'environment.json'))
            plan['item']['mounts'].append(mount)
            model = build_run(registry.get_case(b['case_id']), execution=str(block), arm='IoTDB-FS',
                implementation=dict(binary_sha256=sha, runner_sha256=digest(frozen['sources']),
                                    engine='HDF5 JSON dataset_catalog diagnostic discovery'),
                clock=dict(kind='client-discovery-response', d='request construction through parsed rows and scope validation',
                           profile_oracle='outside Td', value_queries=0),
                cache=dict(protocol=PROTOCOL, configuration='B', daemon_settings=launch['settings']),
                environment=dict(resource_id=frozen['resource_id'], process_receipt_sha256=mount['environment_sha256']),
                replication=dict(campaign=0, warmup=WARMUPS, measured=MEASURED,
                                 schedule='single B mount; five warmup then ten formal discovery requests'))
            b['run_ids'] = {'0': {'B': registry.register_run(model)['run_id']}}
            atomic_json(args.work/'bindings.json', bindings)
            for repeat in range(WARMUPS+MEASURED):
                rb = telemetry.sample(paths)
                run.validate_sample(rb)
                row = diag.discovery_query(context, b['scope'], b['physical_versions'], b['expected_matches'], diagnostic=True)
                diag.validate_discovery_row(row, b['scope'], b['expected_matches'])
                ra = telemetry.sample(paths)
                run.validate_sample(ra)
                row.update(group_id=group['group_id'], C=group['C_paths'], N=group['N'],
                    corpus_id=b['corpus_id'], case_id=b['case_id'], oracle_sha256=b['oracle_sha256'],
                    run_id=b['run_ids']['0']['B'], campaign=0, configuration='B', arm='IoTDB-FS', repeat=repeat,
                    phase='warmup' if repeat < WARMUPS else 'measurement', ok=True,
                    resource_id=frozen['resource_id'], resource_before=rb, resource_after=ra,
                    environment_sha256=mount['environment_sha256'], product_sha256=sha)
                if row['phase'] == 'measurement':
                    diag.validate_formal_profile(row, group['C_paths'], 'B')
                    formal_fee_counters.append({k: row['discovery_call']['server_profile']['counters'][k]
                                                for k in diag.FORMAL_POSITIVE})
                run.append(args.work/'queries.jsonl', row)
                report['query_records'] = repeat+1
                atomic_json(args.work/'report.json', report)
        after, _ = run.head(args.inspector, plan['item']['namespace'], env, block/'head-after')
        need(after == before and run.infrastructure.journal_snapshot(journal) == plan['journal_digest'],
             'query changed namespace head/private journal')
        need(file_sha(plan['root']/'index.sqlite') == plan['prep']['index_sha256'], 'existing FS-index changed')
        trace = diag.verify_fee_trace(trace_dir/f"c{group['C_paths']}-fees.json", group['C_paths'])
        expected = trace['expected_formal_fee_counters']
        need(len(formal_fee_counters) == MEASURED and all(o == expected for o in formal_fee_counters),
             'every formal profile must reconcile with actual prelude and real fee union')
        atomic_json(block/'fee-traces.receipt.json', dict(complete=True, traces=[trace]))
        mount.update(complete=True, scientific_head_unchanged=True, journal_unchanged=True,
                     fee_trace_receipt_sha256=file_sha(block/'fee-traces.receipt.json'))
        atomic_json(args.work/'report.json', report)
        return WARMUPS+MEASURED

    def analyze(self, path, bindings, selected, resource_id, candidate_sha):
        group, = selected
        return analyze_rows([json.loads(s) for s in Path(path).read_text().splitlines()],
                            bindings, group, resource_id, candidate_sha)

    def worker(self, args):
        def stop(sig, frame):
            raise RuntimeError('small trace worker cancelled; preserve evidence')
        for sig in (signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, stop)
        proof = self.source_proof(args)
        frozen = read_json(args.work/'prepared.json')
        need(proof == read_json(args.work/'reuse-source.json') and self.closure(args.repo) == frozen['sources'],
             'preflight source/product/scientific evidence changed')
        paired, _, env, startup = run.dependencies(args.repo)
        startup.adjust_mount(paired.sf, 1800, args.work/'startup-receipts.jsonl')
        report = dict(protocol=PROTOCOL, complete=False, tier=args.tier, query_records=0,
            expected_queries=self.expected_rows(args), corpora=[], product_sha256=file_sha(args.binary),
            baseline_sha256=diag.BASELINE_SHA, resource_id=frozen['resource_id'],
            configurations=['B'], arms=['IoTDB-FS discovery only'], expected_mounts=1,
            diagnostic_only=True, performance_claim=False, scientific_publication=False,
            index_rebuilt=False, new_cases=False, location_bindings_created=False, credentials_recorded=False)
        atomic_json(args.work/'report.json', report)
        try:
            registry = Registry(args.work/'registry')
            plans, bindings = self.plans(args, registry, report, proof)
            atomic_json(args.work/'bindings.json', bindings)
            atomic_json(args.work/'execution-order.json', dict(protocol=PROTOCOL,
                configurations=['B'], arms=['IoTDB-FS discovery only'], fresh_mount_per_tier=True,
                mounts=[dict(group=g['group_id'], N=g['N'], campaign=0, configuration='B',
                    repeats=list(range(WARMUPS+MEASURED)), warmup=WARMUPS, measured=MEASURED)
                    for g in self.groups(args.repo, args.tier)]))
            count = self.collect(args, plans, bindings, registry, frozen, report, env, paired)
            need(count == self.expected_rows(args), 'trace collection incomplete')
            need(self.source_proof(args) == proof and self.closure(args.repo) == frozen['sources'],
                 'source/product/scientific inputs changed')
            for plan in plans:
                plan['item'].update(complete=True, final_raw_validation=run.verify_raw(
                    plan['manifest'], plan['root']/'input', plan['root']/'fs'))
            report.update(complete=True, queries_sha256=file_sha(args.work/'queries.jsonl'),
                          bindings_sha256=file_sha(args.work/'bindings.json'))
        except BaseException as error:
            report['failure'] = dict(type=type(error).__name__, message=str(error)[:500])
            raise
        finally:
            atomic_json(args.work/'report.json', report)

    def main(self):
        return NativeABRunner.main(self)


SPEC = ProtocolSpec(protocol=PROTOCOL, module='experiments.fixed_matrix.fee_summary_trace',
    description=__doc__, baseline_sha=diag.BASELINE_SHA, product_diff=diag.PRODUCT_DIFF,
    required_regression_tests=(), extra_closure=(
        'experiments/fixed_matrix/fee_summary_trace.py',
        'experiments/fixed_matrix/tests/test_fee_summary_trace.py',
        'experiments/fixed_matrix/tests/test_cscale_diagnosis.py',
        'experiments/cross_version/hdf5_route3/native_filesystem/fixed_probe.py',
        'experiments/cross_version/cscale_diagnosis/README.md',
        'experiments/cross_version/cscale_diagnosis/run.sh'))
runner = SmallTraceRunner(SPEC)

if __name__ == '__main__':
    runner.main()
