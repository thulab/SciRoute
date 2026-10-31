"""Query-only HDF5 discovery measurement at fixed N=512, C=128 and C=512.

The clean product supplies performance evidence. The same-source diagnostic
product supplies attribution evidence only; its timings are never a speedup.
"""
from __future__ import annotations

import json
import argparse
from copy import deepcopy
import os
from pathlib import Path
import shutil
import signal
import statistics
import time
from types import SimpleNamespace
import xml.etree.ElementTree as ET

from . import client, run
from . import telemetry
from .hdf5_native_ab import NativeABRunner, ProtocolSpec, PATH_ARGS
from .hdf5_cache_batch import verify_build_binding
from .shared_candidates import compare_build_environment
from experiments.cross_version.hdf5_route3.native_filesystem.fixed_probe import filesystem_launcher
from experiments.cross_version.resource_limited.resources import atomic_json
from experiments.identity.model import build_run, digest
from experiments.identity.registry import Registry, file_sha, read_json

PROTOCOL = 'iotdbfs-fixed-cscale-discovery-diagnosis/1'
COUNTS = (128, 512)
CONFIGURATIONS = ('A', 'B')
CAMPAIGNS = (0, 1, 2, 3)
BASELINE_SHA = '3d58c7d56740fbb2a2b962e8b3558b5c12ee65703545e93ecb15f69a85d27cb3'
ORIGINAL_RECEIPT_SHA = '413bfde5f8d2a764285cc746c790243116122d94dff13c21f0d6357b4b4b2cc6'
CLIENT_SHA = 'c163cadfdbee49076c24a84e3e61b5226f6155b592c180260cf6acf52533c563'
PRODUCT_DIFF = frozenset({'include/iotdbfs/query/core/QueryProfile.h',
                          'include/iotdbfs/query/core/QueryMetadataBudget.h',
                          'src/query/core/QueryPlanning.cpp',
                          'src/IoTDBStructuredBlobBackend.cpp',
                          'src/query/frontend/QueryRegistrationSocket.cpp'})
# Counts are frozen before any server-side collection. No latency-driven retry.
WARMUPS = 5
MEASURED = 30
CLIENT_COMPONENTS = ('request_build_ns', 'call_ns', 'row_materialize_ns', 'scope_validation_ns')
FORMAL_CALLS = dict(serve=1, registration=1, selector_parse=1, selector_validate=2,
    domain_key=1, domain_claim=1, fee_selection=1, authorize=1, fee_manifest=1,
    fee_catalog=1, index_lookup=1, result_build=1, encode_precheck=1,
    encode_response=1, final_admit=1, admission=2, proof_replay=2, fee_ledger=5)
FORMAL_COUNTERS = {'cdiag.trace_exports': 0, 'cdiag.export_errors': 0,
    'cdiag.capture_overflow': 0, 'cdiag.fee.active_collector': 0,
    'cdiag.prelude_capture_errors': 0, 'cdiag.budget.post_first_node_delta': 0,
    'cdiag.budget.post_first_charge_delta': 0,
    'cdiag.captured_batches': 4, 'cdiag.capture_events': 5,
    'cdiag.fee.dispatches': 5, 'cdiag.fee.first_replays': 4,
    'cdiag.fee.memo_hits': 1}
FORMAL_POSITIVE = ('cdiag.fee.visited_entries', 'cdiag.fee.inserted_nodes',
                  'cdiag.fee.ledger_nodes_final', 'cdiag.fee.consumed_bytes_final',
                  'cdiag.fee.ledger_nodes_before_first', 'cdiag.fee.consumed_before_first')
need = run.need


def groups(repo):
    catalog = {g['group_id']: g for g in read_json(repo/'experiments/fixed_matrix/catalog.json')['groups']}
    return [catalog[f'FM1-hdf5-N512-V1-C{c}-S0-base-latest'] for c in COUNTS]


def scope_order(campaign, repeat):
    """Both scopes share one mount; ABBA across campaigns and alternating pairs."""
    need(campaign in CAMPAIGNS and type(repeat) is int and repeat >= 0, 'invalid schedule coordinate')
    return COUNTS if (repeat + (campaign in (1, 2))) % 2 == 0 else tuple(reversed(COUNTS))


def verify_discovered(rows, scope, expected, k=2):
    keys = [client.unit_key('hdf5', row) for row in rows]
    wanted = {(m['binding']['path'], m['binding']['version'], m['unit'][8:]) for m in expected}
    need(len(rows) == k and len(keys) == len(set(keys)) == len(wanted) and set(keys) == wanted,
         'discovery identities differ from sealed full native oracle')
    need(all(row['path'] in scope for row in rows), 'discovery escaped fixed candidate scope')


def discovery_query(context, scope, versions, expected, *, diagnostic=False):
    """Preserve the original client's exact Td operations and add boundary clocks.

    Full response, locators, profile and oracle checks are retained outside Td.
    There is deliberately no value read or native helper query in this protocol.
    """
    stats = []
    if diagnostic:
        t0 = time.perf_counter_ns()
        request = client.discover_request('hdf5', scope, versions)
        tr = time.perf_counter_ns()
        response, parts, stat = client.call(context, request)
        stats.append(stat)
        tc = time.perf_counter_ns()
        rows = client.discovered('hdf5', response, parts)
        tm = time.perf_counter_ns()
        need(all(row['path'] in scope for row in rows), 'discovery escaped fixed candidate scope')
        t1 = time.perf_counter_ns()
        components = dict(request_build_ns=tr-t0, call_ns=tc-tr,
                          row_materialize_ns=tm-tc, scope_validation_ns=t1-tm)
    else:
        # Keep clean A's original two-clock Td seam. Interior clocks would
        # change the evidence used to resolve a small residual over 4x.
        t0 = time.perf_counter_ns()
        response, parts, stat = client.call(context, client.discover_request('hdf5', scope, versions))
        stats.append(stat)
        rows = client.discovered('hdf5', response, parts)
        need(all(row['path'] in scope for row in rows), 'discovery escaped fixed candidate scope')
        t1 = time.perf_counter_ns()
        components = None
    profiles = client.profiles(context, 1)
    verify_discovered(rows, scope, expected)
    need(not parts and stat.get('response_path') is None and stat.get('ipc_bytes') == 0,
         'HDF5 discovery unexpectedly returned an IPC result')
    stat['server_profile'] = profiles[0]
    return dict(discovery_ns=t1-t0, client_components=components,
                selected_units=len(rows), discovered_rows=rows,
                discovery_response=response, discovery_call=stat,
                oracle_verified=True, value_queries_executed=0)


def validate_discovery_row(row, scope, expected):
    need(type(row['discovery_ns']) is int and row['discovery_ns'] > 0, 'discovery clock mismatch')
    components = row.get('client_components')
    if components is not None:
        need(set(components) == set(CLIENT_COMPONENTS) and
             all(type(components[k]) is int and components[k] >= 0 for k in CLIENT_COMPONENTS) and
             sum(components.values()) == row['discovery_ns'], 'discovery component clock mismatch')
    need(row.get('oracle_verified') is True and row.get('value_queries_executed') == 0 and row.get('selected_units') == 2,
         'unverified discovery or unexpected value query')
    verify_discovered(row['discovered_rows'], scope, expected)
    profile = row['discovery_call']['server_profile']
    need(profile.get('ok') is True and type(profile.get('sequence')) is int,
         'invalid discovery registration profile')
    need(row['discovery_response'].get('ok') is True and row['discovery_response'].get('datasets') is not None,
         'missing raw discovery response')
    need(client.discovered('hdf5', row['discovery_response'], []) == row['discovered_rows'],
         'raw response and retained discovery locators differ')


def validate_formal_profile(row, candidate_paths, configuration):
    profile = row['discovery_call']['server_profile']
    counters = profile['counters']
    for key, value in {'hot_query.domain_hits': 1, 'hot_query.index_lookups': 1,
                       'hot_query.proof_hits': 2, 'dataset_catalog_selected_sources': candidate_paths,
                       'dataset_catalog_candidate_datasets': 4*candidate_paths}.items():
        need(type(counters.get(key)) is int and counters[key] == value, 'formal discovery is not fully warm: '+key)
    iotdb = profile['iotdb_delta']
    for key in ('query_calls', 'version_store_query_calls', 'metadata_query_calls', 'payload_calls', 'layout_calls'):
        need(type(iotdb.get(key)) is int and iotdb[key] == 0, 'formal discovery submitted database work: '+key)
    if configuration == 'A':
        return
    expected = dict(FORMAL_COUNTERS, **{'cdiag.scope_paths': candidate_paths,
                                       'cdiag.selector.validated_paths': 2*candidate_paths})
    expected.update({'cdiag.calls.'+k: v for k, v in FORMAL_CALLS.items()})
    for key, value in expected.items():
        need(type(counters.get(key)) is int and counters[key] == value, 'formal diagnostic counter mismatch: '+key)
    for key in FORMAL_POSITIVE:
        need(type(counters.get(key)) is int and counters[key] > 0, 'formal diagnostic counter missing/nonpositive: '+key)
    stages = profile['stages_ns']
    for label in FORMAL_CALLS:
        for prefix in ('cdiag.self.', 'cdiag.total.'):
            key = prefix+label
            need(type(stages.get(key)) is int and stages[key] >= 0, 'formal diagnostic timing missing: '+key)
    root = stages['cdiag.total.serve']
    self_times = [v for k, v in stages.items() if k.startswith('cdiag.self.')]
    need(all(type(v) is int and v >= 0 for v in self_times) and sum(self_times) == root and
         type(profile.get('server_total_ns')) is int and 0 < root <= profile['server_total_ns'],
         'exclusive diagnostic clock partition does not close against original server clock')


def profile_summary(rows):
    """Keep absent metrics absent and expose observation counts explicitly."""
    profiles = [r['discovery_call']['server_profile'] for r in rows]
    result = {}
    for section in ('stages_ns', 'counters', 'runtime_delta', 'iotdb_delta'):
        keys = sorted({k for p in profiles for k, v in p.get(section, {}).items() if type(v) is int})
        result[section] = {}
        for key in keys:
            values = [p[section][key] for p in profiles if type(p.get(section, {}).get(key)) is int]
            result[section][key] = dict(observed_rows=len(values), total_rows=len(rows),
                                        median=statistics.median(values), minimum=min(values), maximum=max(values))
    values = [p['server_total_ns'] for p in profiles if type(p.get('server_total_ns')) is int]
    if values:
        result['server_total_ns'] = dict(observed_rows=len(values), total_rows=len(rows),
                                         median=statistics.median(values), minimum=min(values), maximum=max(values))
    result['semantics'] = ('Original stages and cdiag.total.* are inclusive/nested; cdiag.self.* are exclusive '
                           'and their sum must equal cdiag.total.serve. Absent observations are not zero.')
    return result


def verify_fee_trace(path, candidate_paths):
    need(path.is_file() and not path.is_symlink(), 'real-fee trace missing or symlinked')
    trace = read_json(path)
    need(trace.get('protocol') == 'iotdbfs-cscale-real-fees/2' and
         trace.get('candidate_paths') == candidate_paths and
         trace.get('identity_encoding') == 'latin1-bytes', 'real-fee trace scope/protocol/identity encoding differs')
    batches, events = trace.get('batches', []), trace.get('events', [])
    need(len(batches) == 4 and [b.get('id') for b in batches] == list(range(4)) and
         len(events) == 5, 'real-fee capture requires four unique batches/five replay events')
    need({b.get('phase') for b in batches} == {'fee_selection', 'fee_manifest', 'fee_catalog', 'proof_replay'},
         'real-fee trace did not capture all domain and proof batches')
    summary = []
    prelude = trace.get('prelude_fees', [])
    need(prelude and all(isinstance(f, list) and len(f) == 2 and isinstance(f[0], str) and
         all(ord(ch) <= 255 for ch in f[0]) and type(f[1]) is int and f[1] > 0 and
         not f[0].startswith(('v04-payload-bytes/', 'v04-extents/')) for f in prelude) and
         len({f[0] for f in prelude}) == len(prelude),
         'real-fee prelude missing, duplicate, non-byte, zero or reserved demand entry')
    prelude_consumed = trace.get('prelude_consumed_bytes')
    need(type(prelude_consumed) is int and prelude_consumed > 0 and
         sum(f[1] for f in prelude) == prelude_consumed,
         'real-fee prelude does not reconcile to captured initial consumption')
    union = dict(prelude)
    for batch in batches:
        fees = batch.get('fees', [])
        need(fees and all(isinstance(f, list) and len(f) == 2 and isinstance(f[0], str) and
                         all(ord(ch) <= 255 for ch in f[0]) and
                         type(f[1]) is int and f[1] >= 0 for f in fees), 'real-fee entries malformed or non-byte identity')
        need(len({f[0] for f in fees}) == len(fees), 'real-fee batch has duplicate identity')
        for identity, value in fees:
            if value > union.get(identity, 0):
                union[identity] = value
        summary.append(dict(id=batch['id'], phase=batch['phase'], entries=len(fees),
                            sum_fee_values_raw=sum(f[1] for f in fees),
                            raw_identity_bytes=sum(len(f[0].encode('latin1')) for f in fees)))
    need(all(type(e.get('batch_id')) is int and 0 <= e['batch_id'] < len(batches) and
             isinstance(e.get('phase'), str) for e in events), 'real-fee event references invalid batch')
    phases = [e['phase'] for e in events]
    need(all(phases.count(p) == n for p, n in {'fee_selection': 1, 'fee_manifest': 1,
         'fee_catalog': 1, 'proof_replay': 2}.items()) and
         all(e['phase'] == batches[e['batch_id']]['phase'] for e in events) and
         len({e['batch_id'] for e in events if e['phase'] == 'proof_replay'}) == 1,
         'real-fee replay events do not match one repeated proof alias')
    metadata = {k: v for k, v in union.items() if not k.startswith(('v04-payload-bytes/', 'v04-extents/'))}
    reconciliation = {'cdiag.fee.visited_entries': sum(len(b['fees']) for b in batches),
                      'cdiag.fee.inserted_nodes': len(union)-len(prelude), 'cdiag.fee.ledger_nodes_final': len(union),
                      'cdiag.fee.consumed_bytes_final': sum(metadata.values()),
                      'cdiag.fee.ledger_nodes_before_first': len(prelude),
                      'cdiag.fee.consumed_before_first': prelude_consumed}
    return dict(path=str(path), sha256=file_sha(path), candidate_paths=candidate_paths, batches=summary,
                events=events, complete=True, expected_formal_fee_counters=reconciliation,
                identity_encoding='latin1-bytes',
                prelude=dict(nodes=len(prelude), consumed_bytes=prelude_consumed,
                             raw_identity_bytes=sum(len(f[0].encode('latin1')) for f in prelude)),
                value_semantics='Raw fee values include reserved non-byte demand domains and overlap across batches; not request charged bytes.')


def analyze_rows(rows, bindings, warmups, measured, baseline_sha, probe_sha, resource_id):
    """No FS-index normalization and no cross-product performance comparison."""
    wanted = {(c, cfg, count, r) for c in CAMPAIGNS for cfg in CONFIGURATIONS
              for count in COUNTS for r in range(warmups+measured)}
    keys = [(r['campaign'], r['configuration'], r['C'], r['repeat']) for r in rows]
    need(len(keys) == len(wanted) and set(keys) == wanted, 'all preregistered discovery rows required; no exclusions')
    scheduled = [(campaign, cfg, c, repeat) for campaign in CAMPAIGNS
                 for cfg in (CONFIGURATIONS if campaign in (0, 3) else tuple(reversed(CONFIGURATIONS)))
                 for repeat in range(warmups+measured) for c in scope_order(campaign, repeat)]
    need(keys == scheduled, 'raw row order differs from preregistered scope/product crossover')
    for row in rows:
        bound = bindings[row['group_id']]
        need(row['group_id'] == f"FM1-hdf5-N512-V1-C{row['C']}-S0-base-latest" and
             all(row[k] == bound[k] for k in ('corpus_id', 'case_id', 'oracle_sha256')) and
             row['run_id'] == bound['run_ids'][str(row['campaign'])][row['configuration']], 'scientific/run identity mismatch')
        need(row['phase'] == ('warmup' if row['repeat'] < warmups else 'measurement') and
             row['product_sha256'] == (baseline_sha if row['configuration'] == 'A' else probe_sha) and
             row.get('resource_id') == resource_id and row.get('ok') is True, 'product, phase or resource mismatch')
        need((row.get('client_components') is None) == (row['configuration'] == 'A'),
             'interior client probes must be diagnostic-only')
        need(row.get('N') == 512 and row.get('arm') == 'IoTDB-FS' and
             row.get('pair_position') == scope_order(row['campaign'], row['repeat']).index(row['C']),
             'fixed namespace or scope-pair order mismatch')
        validate_discovery_row(row, bound['scope'], bound['expected_matches'])
        if row['phase'] == 'measurement':
            validate_formal_profile(row, row['C'], row['configuration'])
        for side in ('resource_before', 'resource_after'):
            run.validate_sample(row[side])
        need(row['resource_before']['monotonic_ns'] < row['resource_after']['monotonic_ns'], 'resource clock reversed')
    results = []
    for cfg in CONFIGURATIONS:
        blocks = []
        for campaign in CAMPAIGNS:
            cells = {}
            for count in COUNTS:
                selected = [r for r in rows if r['campaign'] == campaign and r['configuration'] == cfg
                            and r['C'] == count and r['phase'] == 'measurement']
                cells[str(count)] = dict(discovery_ns=statistics.median(r['discovery_ns'] for r in selected))
                cells[str(count)]['server_profile'] = profile_summary(selected)
                if cfg == 'B':
                    cells[str(count)]['client_components'] = {k: statistics.median(r['client_components'][k] for r in selected)
                                                               for k in CLIENT_COMPONENTS}
            ratio = cells['512']['discovery_ns'] / cells['128']['discovery_ns']
            paired_growth = []
            for repeat in range(warmups, warmups+measured):
                pair = {r['C']: r['discovery_ns'] for r in rows if r['campaign'] == campaign and
                        r['configuration'] == cfg and r['repeat'] == repeat}
                paired_growth.append(dict(repeat=repeat, growth=pair[512]/pair[128],
                                          normalized_growth=pair[512]/pair[128]/4))
            blocks.append(dict(campaign=campaign, medians_ns=cells, growth=ratio,
                               normalized_growth=ratio/4,
                               excess_ns=cells['512']['discovery_ns']-4*cells['128']['discovery_ns'],
                               paired_growth=paired_growth))
        results.append(dict(configuration=cfg, role='clean performance' if cfg == 'A' else 'diagnostic attribution only',
                            blocks=blocks, growth=statistics.median(b['growth'] for b in blocks),
                            normalized_growth=statistics.median(b['normalized_growth'] for b in blocks)))
    return dict(protocol=PROTOCOL, complete=True, query_rows=len(rows), configurations=results,
                statistic='Within-mount per-scope warm medians; C512/C128 paired in each campaign; median of four ratios.',
                limitation='Warm discovery only; diagnostic-product timings are not performance A/B evidence; no slow-row exclusions.')


class CScaleRunner(NativeABRunner):
    def __init__(self, spec):
        need(spec.protocol == PROTOCOL and spec.baseline_sha == BASELINE_SHA and
             spec.product_diff == PRODUCT_DIFF and not spec.required_regression_tests,
             'fixed historical diagnostic declaration required')
        self.spec = spec

    def groups(self, repo, tier):
        need(tier == 'c512', 'fixed N512 C128/C512 diagnosis only')
        return groups(repo)

    def expected_rows(self, args):
        return len(CAMPAIGNS)*len(CONFIGURATIONS)*len(COUNTS)*(WARMUPS+MEASURED)

    def verify_candidate(self, args):
        gates = {}
        for label, binary, binding in (('A', args.baseline_binary, args.baseline_build_binding),
                                       ('B', args.binary, args.build_binding)):
            gates[label] = verify_build_binding(SimpleNamespace(binary=binary, build_binding=binding))
        a, b = read_json(args.baseline_build_binding), read_json(args.build_binding)
        original_path = Path(a.get('original_receipt', ''))
        need(original_path.is_absolute() and not original_path.is_symlink() and
             a.get('original_receipt_sha256') == ORIGINAL_RECEIPT_SHA and
             file_sha(original_path) == ORIGINAL_RECEIPT_SHA and a.get('metadata_augmentation_only') is True,
             'historical original receipt provenance missing or changed')
        original = read_json(original_path)
        need(all(a[k] == original[k] for k in ('source_files', 'binary_sha256', 'unit_binary_sha256',
                                              'focused_tests_sha256', 'all_unit_tests_sha256')),
             'augmented clean binding changed original source/binary/test evidence')
        for receipt in (a, b):
            for label in ('focused_tests', 'all_unit_tests'):
                root = ET.parse(receipt[label]).getroot()
                cases = list(root.iter('testcase'))
                need(cases and len(cases) == int(root.attrib.get('tests', 0)) and
                     all(n.get('status') == 'run' and n.get('result') == 'completed' for n in cases) and
                     not any(list(root.iter(k)) for k in ('failure', 'error', 'skipped')) and
                     all(int(n.attrib.get(k, 0)) == 0 for n in root.iter() if n.tag in ('testsuite', 'testsuites')
                         for k in ('failures', 'errors', 'disabled', 'skipped')),
                     'clean/probe test XML includes skipped, disabled or incomplete cases')
        need(file_sha(args.baseline_binary) == BASELINE_SHA and file_sha(args.binary) != BASELINE_SHA,
             'accepted historical clean product or distinct diagnostic product missing')
        overlay = b.get('diagnostic_overlay', {})
        need(overlay.get('protocol') == PROTOCOL and overlay.get('baseline_sha256') == BASELINE_SHA and
             overlay.get('instrumentation_only') is True and overlay.get('performance_claim') is False,
             'explicit same-baseline instrumentation declaration missing')
        product = lambda n: n.startswith(('src/', 'include/', 'cmake/')) or n == 'CMakeLists.txt'
        names = {n for n in a['source_files'] if product(n)} | {n for n in b['source_files'] if product(n)}
        changed = {n for n in names if a['source_files'].get(n) != b['source_files'].get(n)}
        allowed = overlay.get('changed_product_files', [])
        need(allowed and len(allowed) == len(set(allowed)) and changed == set(allowed) == PRODUCT_DIFF,
             'diagnostic source differs outside frozen instrumentation declaration')
        need(all(n in a['source_files'] and n in b['source_files'] for n in changed),
             'diagnostic overlay may not add or remove product files')
        return dict(candidate_sha256=file_sha(args.binary), baseline_sha256=BASELINE_SHA,
                    gates=gates, product_files=len(names), changed_product_files=sorted(changed),
                    diagnostic_overlay=overlay,
                    build_environment=compare_build_environment(args.baseline_binary, args.binary))

    def source_proof(self, args):
        target = args.work
        while not target.exists():
            target = target.parent
        space = shutil.disk_usage(target)
        need(space.total > 0 and space.free / space.total >= .01, 'less than approved 1% disk remains')
        need(file_sha(args.repo/'experiments/fixed_matrix/client.py') == CLIENT_SHA,
             'original discovery client operations changed')
        proof = super().source_proof(args)
        entries = proof['cases']
        # These are two sealed cases over ONE full N512 corpus, never subsets
        # passed off as separate physical namespaces or independently mounted.
        same = ('corpus_id', 'corpus_key', 'source_root', 'head', 'journal', 'journal_digest', 'namespace', 'native_root')
        need(len(entries) == 2 and all(entries[0][k] == entries[1][k] for k in same),
             'both scopes must share one exact N512 namespace and original location binding')
        old = read_json(args.source_collection/'bindings.json')
        bound = [old[e['group_id']] for e in entries]
        need([len(b['scope']) for b in bound] == list(COUNTS) and
             all(len(set(b['scope'])) == len(b['scope']) for b in bound) and
             set(bound[0]['scope']).issubset(set(bound[1]['scope'])) and
             all(b['physical_versions'] == {'kind': 'latest'} for b in bound),
             'sealed C scopes or Latest selector differ')
        proof.update(arms=['IoTDB-FS discovery only'], warmup_per_scope=WARMUPS,
                     measured_per_scope=MEASURED, warmup_per_arm=WARMUPS,
                     measured_per_arm=MEASURED, minimum_disk_free_fraction=.01,
                     same_mount_for_scopes=True, diagnostic_timing_performance_evidence=False)
        return proof

    def plans(self, args, registry, report, proof):
        old = read_json(args.source_collection/'bindings.json')
        source_registry = Registry(args.source_collection/'registry')
        bindings = {}
        entry = proof['cases'][0]
        cid = entry['corpus_id']
        corpus = registry.register_corpus(source_registry.get_corpus(cid))
        for e in proof['cases']:
            gid = e['group_id']
            registry.register_case(source_registry.get_case(e['case_id']))
            bindings[gid] = deepcopy(old[gid])
            bindings[gid]['run_ids'] = {}
        base = Path(entry['source_root'])
        execution = args.work/'corpora'/entry['corpus_key']
        execution.mkdir(parents=True, mode=0o700)
        item = deepcopy(next(v for v in read_json(args.source_collection/'report.json')['corpora'] if v['corpus_id'] == cid))
        item.update(complete=False, mounts=[], source_root=str(base), head=entry['head'], journal_digest=entry['journal_digest'])
        report['corpora'].append(item)
        return [dict(root=base, execution_root=execution, corpus=corpus, item=item,
                     journal=Path(entry['journal']), journal_digest=entry['journal_digest'],
                     scientific_head=entry['head'], native_root=Path(entry['native_root']),
                     prep=read_json(base/'preparation.json'), manifest=read_json(base/'input/manifest.json'))], bindings

    def collect(self, args, execution, bindings, registry, frozen, report, env, paired):
        plan, = execution
        paths = read_json(args.work/'resource-paths.json')
        writer = args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
        count = 0
        by_c = {g['C_paths']: g for g in self.groups(args.repo, args.tier)}
        # Match the original client's imported call seam before either scope's
        # warmup. No scientific query is issued by these imports.
        import numpy, pyarrow  # noqa: F401
        for campaign in CAMPAIGNS:
            for config in self.configuration_order(campaign):
                block = plan['execution_root']/('campaign-'+str(campaign))/config
                block.mkdir(parents=True)
                journal = block/'owner'
                shutil.copytree(plan['journal'], journal)
                need(run.infrastructure.journal_snapshot(journal) == plan['journal_digest'], 'private journal differs')
                binary = args.baseline_binary if config == 'A' else args.binary
                sha = file_sha(binary)
                launch = filesystem_launcher(block/'launcher', binary, plan['item']['namespace'], journal,
                                             False, native_root=plan['native_root'], expected_sha=sha)
                mount_env = dict(env)
                mount_env.pop('IOTDB_FS_CSCALE_TRACE_DIR', None)
                if config == 'B':
                    (block/'fee-traces').mkdir(mode=0o700)
                    mount_env['IOTDB_FS_CSCALE_TRACE_DIR'] = str(block/'fee-traces')
                launch['diagnostic_trace_directory'] = mount_env.get('IOTDB_FS_CSCALE_TRACE_DIR')
                atomic_json(block/'launcher.receipt.json', launch)
                before, _ = run.head(args.inspector, plan['item']['namespace'], env, block/'head-before')
                need(before == plan['scientific_head'], 'namespace head changed before mount')
                print(json.dumps(dict(event='cscale_discovery_mount', campaign=campaign,
                                      configuration=config, scopes=list(COUNTS))), flush=True)
                with paired.sf.private_mount(Path(launch['path']), block/'mount', mount_env, writer, profile=True) as context:
                    need(file_sha(Path('/proc')/str(context['pid'])/'exe') == sha, 'running product differs')
                    actual = read_json(context['root']/'profile.launch.json')
                    need(actual['settings'] == launch['settings'], 'actual daemon settings differ')
                    environment = telemetry.environment(args.repo, artifact_paths=[binary],
                        process_roles=dict(client=os.getpid(), daemon=context['pid'],
                            datanode=int((args.repo.parent/'runtime/run/datanode.pid').read_text())), data_path=plan['root'])
                    atomic_json(block/'environment.json', environment)
                    mount = dict(campaign=campaign, configuration=config, daemon_pid=context['pid'],
                                 startup_ns=context['startup_ns'], product_sha256=sha,
                                 environment_sha256=file_sha(block/'environment.json'), shared_scopes=list(COUNTS))
                    plan['item']['mounts'].append(mount)
                    formal_fee_counters = {c: [] for c in COUNTS}
                    for c in COUNTS:
                        b = bindings[by_c[c]['group_id']]
                        model = build_run(registry.get_case(b['case_id']), execution=str(block), arm='IoTDB-FS',
                            implementation=dict(binary_sha256=sha, runner_sha256=digest(frozen['sources']),
                                                engine='HDF5 JSON dataset_catalog discovery'),
                            clock=dict(kind='client-discovery-response', d='request construction through parsed rows and scope validation',
                                       profile_oracle='outside Td', value_queries=0),
                            cache=dict(protocol=PROTOCOL, configuration=config, daemon_settings=launch['settings'],
                                       scopes_share_mount=True),
                            environment=dict(resource_id=frozen['resource_id'], process_receipt_sha256=mount['environment_sha256']),
                            replication=dict(campaign=campaign, warmup=WARMUPS, measured=MEASURED,
                                             schedule='AB/BA/BA/AB products; alternating scope pairs, ABBA starting scope'))
                        b['run_ids'].setdefault(str(campaign), {})[config] = registry.register_run(model)['run_id']
                    atomic_json(args.work/'bindings.json', bindings)
                    for repeat in range(WARMUPS+MEASURED):
                        for ordinal, c in enumerate(scope_order(campaign, repeat)):
                            group = by_c[c]
                            b = bindings[group['group_id']]
                            rb = telemetry.sample(paths)
                            run.validate_sample(rb)
                            row = discovery_query(context, b['scope'], b['physical_versions'], b['expected_matches'], diagnostic=config == 'B')
                            validate_discovery_row(row, b['scope'], b['expected_matches'])
                            ra = telemetry.sample(paths)
                            run.validate_sample(ra)
                            row.update(group_id=group['group_id'], C=c, N=512,
                                corpus_id=b['corpus_id'], case_id=b['case_id'], oracle_sha256=b['oracle_sha256'],
                                run_id=b['run_ids'][str(campaign)][config], campaign=campaign,
                                configuration=config, arm='IoTDB-FS', repeat=repeat, pair_position=ordinal,
                                phase='warmup' if repeat < WARMUPS else 'measurement', ok=True,
                                resource_id=frozen['resource_id'], resource_before=rb, resource_after=ra,
                                environment_sha256=mount['environment_sha256'], product_sha256=sha)
                            if row['phase'] == 'measurement':
                                validate_formal_profile(row, c, config)
                                if config == 'B':
                                    formal_fee_counters[c].append({k: row['discovery_call']['server_profile']['counters'][k]
                                                                  for k in FORMAL_POSITIVE})
                            run.append(args.work/'queries.jsonl', row)
                            count += 1
                            report['query_records'] = count
                        atomic_json(args.work/'report.json', report)
                after, _ = run.head(args.inspector, plan['item']['namespace'], env, block/'head-after')
                need(after == before and run.infrastructure.journal_snapshot(journal) == plan['journal_digest'],
                     'query changed namespace head/private journal')
                need(file_sha(plan['root']/'index.sqlite') == plan['prep']['index_sha256'], 'existing FS-index changed')
                if config == 'B':
                    traces = [verify_fee_trace(block/'fee-traces'/f'c{c}-fees.json', c) for c in COUNTS]
                    for trace in traces:
                        expected = trace['expected_formal_fee_counters']
                        observations = formal_fee_counters[trace['candidate_paths']]
                        need(len(observations) == MEASURED and all(o == expected for o in observations),
                             'formal fee ledger does not reconcile to independent real-batch union/replay')
                    atomic_json(block/'fee-traces.receipt.json', dict(complete=True, traces=traces))
                    mount['fee_trace_receipt_sha256'] = file_sha(block/'fee-traces.receipt.json')
                mount.update(complete=True, scientific_head_unchanged=True, journal_unchanged=True)
                atomic_json(args.work/'report.json', report)
        return count

    def analyze(self, path, bindings, selected, resource_id, candidate_sha):
        return analyze_rows([json.loads(s) for s in Path(path).read_text().splitlines()], bindings,
                            WARMUPS, MEASURED, BASELINE_SHA, candidate_sha, resource_id)

    def worker(self, args):
        def stop(sig, frame):
            raise RuntimeError('discovery worker cancelled; preserve evidence')
        for sig in (signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, stop)
        proof = self.source_proof(args)
        frozen = read_json(args.work/'prepared.json')
        need(proof == read_json(args.work/'reuse-source.json') and self.closure(args.repo) == frozen['sources'],
             'preflight changed')
        paired, _, env, startup = run.dependencies(args.repo)
        startup.adjust_mount(paired.sf, 1800, args.work/'startup-receipts.jsonl')
        report = dict(protocol=PROTOCOL, complete=False, tier=args.tier, query_records=0,
            expected_queries=self.expected_rows(args), corpora=[], product_sha256=file_sha(args.binary),
            baseline_sha256=BASELINE_SHA, resource_id=frozen['resource_id'], scientific_publication=False,
            index_rebuilt=False, new_cases=False, location_bindings_created=False, credentials_recorded=False)
        atomic_json(args.work/'report.json', report)
        try:
            registry = Registry(args.work/'registry')
            plans, bindings = self.plans(args, registry, report, proof)
            atomic_json(args.work/'execution-order.json', dict(
                configurations=list(CONFIGURATIONS), scopes=list(COUNTS), fresh_mount_per_configuration=True,
                shared_scope_mount=True, campaigns=[dict(campaign=c, order=list(self.configuration_order(c)),
                    pairs=[list(scope_order(c, r)) for r in range(WARMUPS+MEASURED)]) for c in CAMPAIGNS]))
            count = self.collect(args, plans, bindings, registry, frozen, report, env, paired)
            need(count == self.expected_rows(args), 'discovery collection incomplete')
            need(self.source_proof(args) == proof and self.closure(args.repo) == frozen['sources'],
                 'source/product/scientific input changed')
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
        parser = argparse.ArgumentParser(description=__doc__)
        for key in PATH_ARGS:
            parser.add_argument('--'+key.replace('_', '-'), type=Path, required=True)
        parser.add_argument('--tier', choices=('c512',), required=True)
        modes = parser.add_mutually_exclusive_group()
        for name in ('worker', 'prepare-only', 'verify-prepared'):
            modes.add_argument('--'+name, action='store_true')
        args = parser.parse_args()
        args.gate_report = None
        for key in PATH_ARGS:
            if key != 'work':
                setattr(args, key, getattr(args, key).resolve(strict=True))
        args.work = args.work.resolve()
        for source in (args.source_collection, args.current_collection):
            need(not args.work.is_relative_to(source) and not source.is_relative_to(args.work), 'run/source paths overlap')
        if args.worker:
            self.worker(args)
        else:
            proof = self.source_proof(args)
            if args.prepare_only:
                self.prepare_only(args, proof)
            elif args.verify_prepared:
                self.verify_prepared(args, proof)
            else:
                self.supervise(args, proof)


SPEC = ProtocolSpec(protocol=PROTOCOL, module='experiments.fixed_matrix.cscale_diagnosis', description=__doc__,
                    baseline_sha=BASELINE_SHA, product_diff=PRODUCT_DIFF,
                    required_regression_tests=(),
                    extra_closure=('experiments/cross_version/hdf5_route3/native_filesystem/fixed_probe.py',
                                   'experiments/fixed_matrix/tests/test_cscale_diagnosis.py',
                                   'experiments/cross_version/cscale_diagnosis/README.md',
                                   'experiments/cross_version/cscale_diagnosis/run.sh'))
runner = CScaleRunner(SPEC)


if __name__ == '__main__':
    runner.main()
