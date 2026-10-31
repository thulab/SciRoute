"""Diagnostic A/A position control; A/B denote slots, never product versions."""
from __future__ import annotations

from contextlib import contextmanager
import gc
import json
import os
import time
import xml.etree.ElementTree as ET

from . import client, hdf5_batch_client, run
from .hdf5_native_ab import NativeABRunner, ProtocolSpec
from .hdf5_cache_batch import verify_build_binding
from .fee_stage_summary import BASELINE_SHA
from experiments.identity.registry import file_sha, read_json

PROTOCOL = 'iotdbfs-fixed-tail-aa-diagnostic/1'
ARCHIVED_N8_REPORT_SHA = '4c54930a01130d16c9be255045d5ed94094b78cd9d01f888a94fc67e4db91f87'
ARCHIVED_N8_QUERIES_SHA = '75be5e63171d0d63b07537921bb9ac818592ebdbc07b47271258987652c8d056'
ARCHIVED_CANDIDATE_SHA = 'c2c0784c4b0cbb897840efbda912a0d34820b07f5f458b2a91acb7225233adc8'
FLAGS = ('complete', 'scientific_complete', 'ready_for_analysis',
         'original_service_restored', 'resources_restored',
         'controller_restoration_verified')
SPEC = ProtocolSpec(
    protocol=PROTOCOL, module='experiments.fixed_matrix.tail_controls',
    description=__doc__, baseline_sha=BASELINE_SHA,
    product_diff=frozenset(), required_regression_tests=(),
    extra_closure=('experiments/fixed_matrix/tests/test_tail_controls.py',
        'experiments/cross_version/work/fs-index-tail-controls-20261023/run_control.py',
        'experiments/cross_version/work/fs-index-tail-controls-20261023/instrumentation/pid_cpu_sampler.py'))


class TailControlRunner(NativeABRunner):
    def diagnostic_metadata(self):
        return dict(kind='same-A-eight-blocks', configuration_labels='slots only',
                    optimization_claim=False)

    def __init__(self):
        # A/A deliberately has no candidate product diff or candidate-only tests.
        run.need(SPEC.product_diff == frozenset() and not SPEC.required_regression_tests,
                 'A/A diagnostic must have no product change')
        self.spec = SPEC

    def verify_candidate(self, args):
        run.need(args.binary.resolve() == args.baseline_binary.resolve() and
                 args.build_binding.resolve() == args.baseline_build_binding.resolve(),
                 'A/A must use the same accepted binary and build receipt')
        run.need(file_sha(args.binary) == BASELINE_SHA, 'accepted A binary differs')
        gate = verify_build_binding(args)
        receipt = read_json(args.build_binding)
        counts = {}
        for label in ('focused_tests', 'all_unit_tests'):
            root = ET.parse(receipt[label]).getroot()
            cases = list(root.iter('testcase'))
            run.need(cases and len(cases) == int(root.attrib.get('tests', 0)) and
                     all(int(node.attrib.get(key, 0)) == 0
                         for node in root.iter() if node.tag in ('testsuite', 'testsuites')
                         for key in ('failures', 'errors', 'disabled', 'skipped')) and
                     not any(list(root.iter(key)) for key in ('failure', 'error', 'skipped')) and
                     all(node.get('status') == 'run' and node.get('result') == 'completed'
                         for node in cases), 'accepted A XML gate incomplete: ' + label)
            counts[label] = len(cases)
        return dict(candidate_sha256=BASELINE_SHA, baseline_sha256=BASELINE_SHA,
                    gates={'A': gate, 'B': gate}, product_files=len(receipt['source_files']),
                    changed_product_files=[], test_counts=counts,
                    diagnostic='same accepted A in both slots; no optimization comparison')

    def validate_n8_gate(self, gate, baseline_sha, candidate_sha):
        run.need(baseline_sha == candidate_sha == BASELINE_SHA and
                 gate.get('protocol') == 'iotdbfs-fixed-fee-stage-summary/1' and
                 gate.get('tier') == 'n8' and gate.get('query_records') == 240 and
                 gate.get('baseline_sha256') == BASELINE_SHA and
                 gate.get('product_sha256') == ARCHIVED_CANDIDATE_SHA and
                 all(gate.get(key) is True for key in FLAGS),
                 'archived accepted-A N8 qualification incomplete')

    def source_proof(self, args):
        run.need(args.tier == 'n128' and args.gate_report is not None,
                 'this approved position control is N128 only')
        run.need(file_sha(args.gate_report) == ARCHIVED_N8_REPORT_SHA,
                 'archived N8 report differs')
        queries = args.gate_report.parent / 'queries.jsonl'
        run.need(file_sha(queries) == ARCHIVED_N8_QUERIES_SHA,
                 'archived N8 query evidence differs')
        # Revalidate historical rows only in the preflight/supervisor process.
        # Loading their large resource snapshots into the measured worker would
        # itself alter the allocator/GC history under investigation.
        if not args.worker:
            count, qualified = 0, set()
            with queries.open() as stream:
                for line in stream:
                    row = json.loads(line)
                    count += 1
                    if not (row['phase'] == 'measurement' and
                            row['configuration'] == 'A' and row['arm'] == 'IoTDB-FS'):
                        continue
                    run.need(row['product_sha256'] == BASELINE_SHA and
                             row.get('ok') is True and row.get('oracle_verified') is True and
                             row.get('mode_verified') is True and row['selected_units'] == 2 and
                             row['owned_output_bytes'] == 40960 and
                             row['total_ns'] == row['discovery_ns'] + row['value_ns'] and
                             min(row['total_ns'], row['discovery_ns'], row['value_ns']) > 0,
                             'archived formal A qualification invalid')
                    key = row['campaign'], row['repeat']
                    run.need(key not in qualified, 'duplicate archived formal A row')
                    qualified.add(key)
            run.need(count == 240 and qualified ==
                     {(campaign, repeat) for campaign in range(4) for repeat in range(5, 15)},
                     'archived formal A qualification grid differs')
        else:
            prior = read_json(args.work / 'reuse-source.json')['diagnostic_control']
            run.need(prior.get('qualified_formal_A_rows') == 40 and
                     prior.get('qualification_queries_sha256') == ARCHIVED_N8_QUERIES_SHA,
                     'supervisor archived qualification missing')
        proof = super().source_proof(args)
        proof['diagnostic_control'] = dict(
            kind='same-A-eight-blocks', configuration_labels='slots only',
            client_lifetime='one worker for all eight blocks',
            qualification='archived accepted baseline N8; not a new diagnostic N8',
            qualification_report_sha256=ARCHIVED_N8_REPORT_SHA,
            qualification_queries_sha256=ARCHIVED_N8_QUERIES_SHA,
            qualified_formal_A_rows=40,
            no_new_candidate=True, optimization_claim=False)
        return proof

    def collect(self, args, execution, bindings, registry, frozen, report, env, paired):
        report['diagnostic_control'] = self.diagnostic_metadata()
        event_path = args.work / 'diagnostic-events.jsonl'
        gc_events = []
        current = {'arm': None, 'query_ordinal': None}
        ordinal = 0

        def stamp():
            return dict(unix_ns=time.time_ns(), monotonic_ns=time.monotonic_ns(),
                        pid=os.getpid())

        with event_path.open('x') as stream:
            def emit(event, **data):
                stream.write(json.dumps(dict(event=event, **stamp(), **data),
                                        sort_keys=True) + '\n')
                stream.flush()

            def gc_callback(phase, info):
                # Buffer only; never perform file I/O inside a timed query/GC.
                gc_events.append((phase, info.get('generation'), info.get('collected'),
                    info.get('uncollectable'), time.time_ns(), time.monotonic_ns(),
                    current['arm'], current['query_ordinal']))

            def flush_gc():
                for phase, generation, collected, uncollectable, wall, mono, arm, query_id in gc_events:
                    record = dict(event='gc', phase=phase,
                        info=dict(generation=generation, collected=collected,
                                  uncollectable=uncollectable),
                        unix_ns=wall, monotonic_ns=mono, pid=os.getpid(),
                        arm=arm, query_ordinal=query_id,
                        scope='query-call envelope; includes untimed validation/profile/stats')
                    stream.write(json.dumps(record, sort_keys=True) + '\n')
                gc_events.clear()
                stream.flush()

            original_mount = paired.sf.private_mount
            original_native = client.query_native
            original_iotdb = hdf5_batch_client.query

            @contextmanager
            def mount(*pos, **kw):
                root = str(pos[1])
                emit('mount_begin', root=root)
                try:
                    with original_mount(*pos, **kw) as context:
                        emit('mount_ready', root=root, daemon_pid=context['pid'],
                             startup_ns=context['startup_ns'])
                        try:
                            yield context
                        finally:
                            emit('mount_stop_begin', root=root)
                finally:
                    flush_gc()
                    emit('mount_stopped', root=root)

            def wrap(original, arm):
                def query(*pos, **kw):
                    nonlocal ordinal
                    current.update(arm=arm, query_ordinal=ordinal)
                    emit('query_begin', arm=arm, query_ordinal=ordinal,
                         gc_count=list(gc.get_count()))
                    try:
                        row = original(*pos, **kw)
                        emit('query_end', arm=arm, query_ordinal=ordinal,
                             total_ns=row['total_ns'], gc_count=list(gc.get_count()))
                        return row
                    finally:
                        flush_gc()
                        ordinal += 1
                        current.update(arm=None, query_ordinal=None)
                return query

            emit('collect_begin', query_clock_changed=False, gc_enabled=gc.isenabled(),
                 gc_threshold=list(gc.get_threshold()),
                 query_event_scope='call envelope; includes untimed validation/profile/stats',
                 instrumentation='buffered GC tuples; phase writes outside query timers')
            paired.sf.private_mount = mount
            client.query_native = wrap(original_native, 'FS-index')
            hdf5_batch_client.query = wrap(original_iotdb, 'IoTDB-FS')
            gc.callbacks.append(gc_callback)
            try:
                return super().collect(args, execution, bindings, registry,
                                       frozen, report, env, paired)
            finally:
                paired.sf.private_mount = original_mount
                client.query_native = original_native
                hdf5_batch_client.query = original_iotdb
                gc.callbacks.remove(gc_callback)
                flush_gc()
                emit('collect_end', queries_observed=ordinal)

    def analyze(self, *args):
        result = super().analyze(*args)
        result.update(diagnostic_only=True, optimization_claim=False,
                      configuration_labels='same accepted A; A/B are position slots')
        return result


runner = TailControlRunner()
if __name__ == '__main__':
    runner.main()
