"""Section 4.2 supplementary collection; the caller owns all live mutations.

This module only composes existing readers. It does not launch a campaign,
publish sources, create mounts, persist evidence, or adjust resource limits.
"""
from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
import time

try:
    from .extensions_cases import source_registry, case_registry
    from .extensions_reader import (ArrowLocalReader, ScientificNativeReader,
        typed_execute, control, validate_typed, digest_owned, hdf5_oracle, _owned)
    from .read_matrix_native import NativeHdf5, need
except ImportError:
    from extensions_cases import source_registry, case_registry
    from extensions_reader import (ArrowLocalReader, ScientificNativeReader,
        typed_execute, control, validate_typed, digest_owned, hdf5_oracle, _owned)
    from read_matrix_native import NativeHdf5, need


METHODS = ('native FS', 'iotdb-indexed', 'native-hdf5')
REPEATS = 13


def _normal_record(record):
    record = dict(record)
    record.update(total_ns=record['T_ns'], discovery_ns=record['d_ns'], value_ns=record['q_ns'])
    need(record['total_ns'] == record['discovery_ns'] + record['value_ns'],
         'supplementary complete query clock does not close')
    need(record['t1_ns'] - record['t0_ns'] == record['discovery_ns'] and
         record['t2_ns'] - record['t1_ns'] == record['value_ns'],
         'supplementary absolute clocks do not close')
    return record


def _read(reader, case):
    record, owned = reader.execute(case)
    return _normal_record(record), owned


def _read_iotdb(context, source, case):
    """Attach durable partial evidence to a genuine failed typed attempt.

    The supervisor may inspect ``error.partial_record`` before persisting the
    failure. Wrapper timestamps deliberately identify their clock scope; they
    are never substituted for successful typed query T/d/q.
    """
    entered = time.perf_counter_ns()
    partial = dict(t0_ns=entered, clock_scope='wrapper entry to observed exception',
                   path=None, expected_source=dict(path=source['logical_path'], version=source['version']))
    if case.get('format') == 'arrow':
        partial.update(t1_ns=entered, discovery_ns=0, expected_profile_count=1)
    else:
        partial['expected_profile_count'] = 2
    def send(socket_path, request):
        response = control(socket_path, request)
        if request['kind'] in ('dataset_catalog', 'format_catalog'):
            partial.update(t1_ns=time.perf_counter_ns(), catalog=response)
        if request['kind'] == 'typed':
            partial['path'] = response.get('path')
        return response
    try:
        record, owned = typed_execute(context['socket'], source, case, send=send)
        return _normal_record(record), owned
    except Exception as error:
        stopped = time.perf_counter_ns()
        partial.update(t2_ns=stopped, total_ns=stopped-entered,
            status='failed', error_type=type(error).__name__, error=str(error))
        if isinstance(getattr(error, 'response', None), dict):
            partial['response'] = error.response
        if 't1_ns' in partial:
            partial.update(discovery_ns=partial['t1_ns']-entered,
                           value_ns=stopped-partial['t1_ns'])
        error.partial_record = partial
        raise


def _oracle(source, case):
    """Independent source decoding, outside every accepted query clock."""
    if case['format'] == 'hdf5':
        metadata, owned = hdf5_oracle(source, case)
    else:
        import numpy as np
        import pyarrow as pa
        with pa.memory_map(source['raw'], 'r') as mapped:
            stream = pa.ipc.open_stream(mapped)
            for _ in range(case['batch']+1):
                batch = stream.read_next_batch()
            parent = batch.column(case.get('field', 'target'))[case['row']]
            need(parent.is_valid, 'frozen Arrow oracle has null parent')
            outer = parent.values
            columns = []
            for child in case['children']:
                scalar = outer[child]
                need(scalar.is_valid, 'frozen Arrow oracle has null child list')
                columns.append(_owned(scalar.values.slice(case['begin'], case['length'])))
            index = np.arange(case['begin'], case['begin']+case['length'], dtype='<u8')
            del batch, parent, outer, scalar, stream
        owned = dict(columns=columns, index=index)
        metadata = {}
    expected = digest_owned(case, owned)
    del owned
    return expected, metadata


def _binding(bindings_by_campaign, campaign, source_key):
    current = bindings_by_campaign[campaign]
    binding = current.get(source_key, bindings_by_campaign[0].get(source_key))
    need(isinstance(binding, dict), 'new owner source binding missing: '+source_key)
    binding = binding.get('exact_source', binding)
    need(isinstance(binding.get('logical_path'), str) and
         type(binding.get('version')) is int and binding['version'] > 0,
         'new owner exact path/version missing: '+source_key)
    return dict(logical_path=binding['logical_path'], version=binding['version'])


def collect_extensions(repo, bindings_by_campaign, mount_factory, measure):
    """Collect 1,254 attempts across fourteen case/variants and three campaigns.

    Index 0 is first, indices 1/2 are retained warmups, and 3..12 are the ten
    measured warm queries. Only GEOS-ALL product blocks are single attempts:
    the existing 64 MiB typed result cap is preserved and a genuine failure
    is recorded by ``measure(expected_failure=True)``. Native FS still runs
    thirteen complete owned reads for that case.

    Supplementary raw sources were published only in publication campaign 0;
    all three timing campaigns use those new immutable owner identities.
    Each product method has one daemon per campaign and shares its caches
    across the ordered cases. Repeat 0 is the first query of that case and
    may reuse data already warmed by an earlier case; it is not a cache-cold
    claim. Native FS creates a fresh reader for every case.
    """
    repo = Path(repo)
    need(len(bindings_by_campaign) == 3, 'extensions require three independent campaigns')
    sources = source_registry()
    cases = case_registry()
    need(len(cases) == 14 and len({(c['id'], c.get('variant', 'default')) for c in cases}) == 14,
         'supplementary case/variant registry is incomplete')
    hdf5_library = repo/'experiments/single_file/work/session-20261012/hdf5-prefix/lib/libhdf5.so.310'
    native = NativeHdf5(hdf5_library)
    # Hashes/coordinates/native oracle validation are outside every query T.
    # The supervisor already verifies source bytes in its frozen preflight.
    oracles = {}
    for case in cases:
        identity = (case['id'], case.get('variant', 'default'))
        oracles[identity] = _oracle(sources[case['source']], case)
    count = 0
    for campaign in range(3):
        order = METHODS[campaign:] + METHODS[:campaign]
        for method in order:
            manager = (nullcontext(None) if method == 'native FS' else
                       mount_factory(campaign, 'extension-all-'+method, method))
            with manager as context:
                for case in cases:
                    if method == 'native-hdf5' and case['format'] != 'hdf5':
                        continue
                    source = dict(sources[case['source']],
                                  **_binding(bindings_by_campaign, campaign, case['source']))
                    spec = dict(source, **case, source_id=case['source'],
                                case_id=case['id'], format_id=case['format'])
                    expected, oracle_metadata = oracles[(case['id'], case.get('variant', 'default'))]
                    def audit(record, owned, current=case, current_source=source,
                              current_expected=expected, current_metadata=oracle_metadata):
                        if 'catalog' in record:
                            validate_typed(record, current_source, current)
                        actual = digest_owned(current, owned)
                        need(actual == current_expected,
                             'supplementary owned output differs: '+current['id'])
                        return dict(validated=True, **actual,
                            selection_coordinates_verified=(current['format']=='hdf5'),
                            oracle_origin=current_metadata.get('origin'),
                            oracle_shape=current_metadata.get('shape'))
                    failed_full = method != 'native FS' and case['id']=='H-F8-GEOS-ALL'
                    if method == 'native FS':
                        reader = (ArrowLocalReader(source, 'FS-native') if case['format']=='arrow'
                                  else ScientificNativeReader(native, source))
                        reader.prepare()
                        read = lambda current=case, current_reader=reader: _read(current_reader, current)
                    else:
                        read = lambda current=case, current_source=source, mounted=context: \
                            _read_iotdb(mounted, current_source, current)
                    for repeat in range(1 if failed_full else REPEATS):
                        kwargs = dict(case=spec, method=method, campaign=campaign, repeat=repeat,
                                      read=read, audit=audit, context=context)
                        if failed_full:
                            kwargs['expected_failure'] = True
                        measure(**kwargs)
                        count += 1
    need(count == 1254, 'supplementary collection omitted attempts')
    return count
