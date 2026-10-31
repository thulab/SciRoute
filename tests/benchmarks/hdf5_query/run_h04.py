#!/usr/bin/env python3
"""H04 typed-query oracle through the real storage/catalog/query interfaces.

An in-memory storage backend isolates format execution from live IoTDB latency.
Compare canonical bytes against both h5py and coordinate formulas, and validate
rectangular C-order batches, projected axes, source identity and physical reads.
Contiguous reads may cover the fixed 4096-element tiles touched by selected runs;
the oracle rejects unrelated tiles and bytes outside the selected dataset.
This is correctness evidence, not a mounted filesystem performance benchmark.
"""
from __future__ import annotations
import argparse
import hashlib
import importlib.util
import itertools
import json
import math
from pathlib import Path
import platform
import subprocess
import time
from urllib.parse import quote
import h5py
import numpy as np

HERE = Path(__file__).resolve().parent


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(value, message):
    if not value:
        raise AssertionError(message)


def canonical(values):
    return np.asarray(values).astype(values.dtype.newbyteorder('<'), copy=False).tobytes(order='C')


def coords(origin, shape):
    return list(itertools.product(*(range(o, o + s) for o, s in zip(origin, shape))))


def covered(offset, length, allowed):
    """Require the entire physical read to lie inside the union of allowed runs."""
    position, end = offset, offset + length
    for begin, stop in sorted(allowed):
        if begin > position:
            break
        if stop > position:
            position = stop
        if position >= end:
            return True
    return position >= end


def range_union(ranges):
    """Canonical physical coverage independent of transport merging/splitting."""
    result = []
    for begin, end in sorted(ranges):
        if begin == end:
            continue
        if result and begin <= result[-1][1]:
            result[-1] = (result[-1][0], max(end, result[-1][1]))
        else:
            result.append((begin, end))
    return result


def allowed_ranges(dataset, origin, shape):
    if not math.prod(shape):
        return []
    if dataset.chunks:
        ranges = []
        for index in range(dataset.id.get_num_chunks()):
            chunk = dataset.id.get_chunk_info(index)
            if all(c < o + s and o < c + width for c, o, s, width in
                   zip(chunk.chunk_offset, origin, shape, dataset.chunks)):
                ranges.append((chunk.byte_offset, chunk.byte_offset + chunk.size))
        return ranges
    strides = [math.prod(dataset.shape[a + 1:]) for a in range(dataset.ndim)]
    start = dataset.id.get_offset()
    if start is None or dataset.id.get_storage_size() == 0:
        # Native H5Dget_offset can add a userblock base to HADDR_UNDEF, yielding
        # userblock-1; allocation size remains the independent no-storage proof.
        return []
    # Match the frozen H04 default tile contract, not the implementation
    # traversal: collect every tile intersecting each logical selected run.
    tile_elements = 4096
    total_elements = math.prod(dataset.shape)
    tiles = set()
    for outer in itertools.product(*(range(o, o+s) for o, s in zip(origin[:-1], shape[:-1]))):
        element = sum(c * stride for c, stride in zip(outer, strides)) + origin[-1]
        tiles.update(range(element // tile_elements,
                           (element + shape[-1] - 1) // tile_elements + 1))
    return [(start + tile * tile_elements * dataset.dtype.itemsize,
             start + min((tile + 1) * tile_elements, total_elements) * dataset.dtype.itemsize)
            for tile in sorted(tiles)]


def check(case, output, generator):
    result = json.loads((output / 'query.json').read_text())
    require(result['schema_version'] == 'iotdbfs-hdf5-h04-probe/1', 'incorrect report schema')
    for read in result['reads']:
        size = result['source_bytes'] if read['kind'] == 'payload' else result['layout_bytes']
        require(0 <= read['offset'] <= size and 0 <= read['length'] <= size-read['offset'],
                'out-of-bounds query source read')
    payload_reads = [r for r in result['reads'] if r['kind'] == 'payload']
    require(not any(r['phase'] == 'prepare' for r in payload_reads), 'planning read value payload')
    if case['reject']:
        require(not result['ok'] and result['error'] == 'UnsupportedCapability',
                'expected explicit UnsupportedCapability, got ' + str(result))
        require(not result['batches'], 'unsupported query yielded a partial batch')
        require(not payload_reads, 'unsupported query read raw value payload')
        return {'status': 'unsupported', 'error': result['message']}
    require(result['ok'], result['error'] + ': ' + result['message'])
    origin, shape = case['origin'], case['shape']
    with h5py.File(case['file'], 'r') as file:
        native = file[case['dataset']]
        selection = tuple(slice(o, o+s) for o, s in zip(origin, shape))
        expected = native[selection]
        encoded = (output / 'values.bin').read_bytes()
        require(encoded == canonical(expected), 'values differ bitwise from independent h5py H5Dread')
        if case.get('recipe'):
            formula = generator.expected_selection(case['recipe'], origin, shape, native.dtype.str)
            require(encoded == canonical(formula), 'values differ bitwise from coordinate formula')
        permitted = allowed_ranges(native, origin, shape)
        for read in payload_reads:
            require(read['length'] <= 8 * 1024 * 1024, 'transport range exceeds 8 MiB')
            require(covered(read['offset'], read['length'], permitted),
                    'payload read extends outside selected dataset/intersecting chunks or tiles')
        actual = [(r['offset'], r['offset'] + r['length']) for r in payload_reads]
        require(range_union(actual) == range_union(permitted),
                'payload coverage omits selected allocated chunks/tiles')
        require(sum(end-start for start, end in actual) ==
                sum(end-start for start, end in range_union(permitted)),
                'resident fixture chunks/tiles were fetched repeatedly')
        value_id = 'hdf5.value:' + quote(case['dataset'], safe='-._~')
        axis_ids = ['hdf5.axis.' + str(a) for a in range(len(shape))]
        fields = axis_ids + [value_id] if case['axes'] else [value_id]
        require([f['id'] for f in result['schema']] == fields, 'projection order changed')
        require(result['schema'][-1]['type'] == ('Float32' if native.dtype.itemsize == 4 else 'Float64'),
                'value schema width mismatch')
    seen = []
    for batch in result['batches']:
        require(batch['rows'] == math.prod(batch['shape']) and 0 < batch['rows'] <= case['target_rows'],
                'batch is not a bounded rectangle')
        require(batch['dimensions'] == axis_ids, 'DenseGrid axes disappeared without projection')
        require(len(batch['origin']) == len(shape) == len(batch['shape']), 'domain rank changed')
        require(all(o <= b and b+s <= o+n for o, n, b, s in
                    zip(origin, shape, batch['origin'], batch['shape'])), 'batch outside request')
        expected_coords = coords(batch['origin'], batch['shape'])
        if case['axes']:
            require(batch['axes'] == [[c[a] for c in expected_coords] for a in range(len(shape))],
                    'axis column coordinates disagree with DenseGrid')
        else:
            require(not batch['axes'], 'unrequested axis columns leaked')
        require(batch['sources'] == [{'path': '/baseline/typed-source.bin', 'version': 17, 'format': 'hdf5'}],
                'exact source provenance mismatch')
        seen.extend(expected_coords)
    require(seen == coords(origin, shape), 'batch concatenation lost C order or repeated/omitted positions')
    require(len(encoded) == math.prod(shape) * expected.dtype.itemsize, 'incorrect output byte count')
    return {'status': 'values', 'rows': math.prod(shape), 'batches': len(result['batches']),
            'value_sha256': hashlib.sha256(encoded).hexdigest(), 'value_bytes': len(encoded),
            'payload_calls': len(payload_reads), 'payload_bytes': sum(r['length'] for r in payload_reads),
            'layout_calls': sum(r['kind'] == 'layout' for r in result['reads']),
            'layout_bytes_read': sum(r['length'] for r in result['reads'] if r['kind'] == 'layout')}


def cases_for(manifest, root):
    cases = []
    def add(entry, selection, target_rows, axes, reject=False, recipe=None):
        dataset = next((d for d in entry['datasets'] if d['path'] == selection['dataset']), entry['datasets'][0])
        cases.append({'id': f"{entry['id']}--{selection['id']}--r{target_rows}--axes{int(axes)}",
                      'file': root / entry['path'], 'dataset': selection['dataset'],
                      'origin': selection['start'], 'shape': selection['count'],
                      'target_rows': target_rows, 'axes': axes, 'reject': reject,
                      'recipe': recipe if recipe is not None else dataset['recipe']})
    success_prefixes = ('rank', 'userblock_512', 'userblock_4096', 'minimal_slice', 'hardlink_alias_cycle')
    # H04 regression scope remains uncompressed allocated arrays. H05 owns
    # filtered/fill successes; never keep obsolete UnsupportedCapability expectations.
    rejection_ids = {'chunked_unallocated_undefined_fill',
                     'contiguous_unallocated_undefined_fill'}
    h05_ids = {'deflate', 'shuffle', 'fletcher32', 'deflate_shuffle', 'deflate_fletcher32',
               'shuffle_fletcher32', 'all_filters', 'corrupt_fletcher32',
               'chunked_unallocated_default_fill', 'chunked_unallocated_explicit_fill',
               'contiguous_unallocated_default_fill', 'contiguous_unallocated_explicit_fill'}
    for entry in manifest['files']:
        if entry['id'] in h05_ids or 'chunked_deflate_shuffle' in entry['id']:
            continue
        if entry['id'] in rejection_ids:
            add(entry, entry['selections'][0], 65536, False, reject=True)
            if entry['id'].startswith('chunked_unallocated_'):
                allocated = {'id': 'allocated_only', 'dataset': '/data',
                             'start': [0, 0], 'count': [8, 6]}
                formula = {'kind': 'axis_affine', 'offset': -3.25, 'coefficients': [0.125, 2.125]}
                add(entry, allocated, 5, True, recipe=formula)
        elif entry['id'].startswith(success_prefixes):
            for selection in entry['selections']:
                for target_rows, axes in [(7, True), (65536, False)]:
                    add(entry, selection, target_rows, axes)
            if entry['id'] == 'minimal_slice':
                b = entry['datasets'][1]
                selection = {'id': 'distinct_b_nonzero', 'dataset': '/b', 'start': [2, 1], 'count': [8, 4]}
                add(entry, selection, 5, True, recipe=b['recipe'])
                selection = {'id': 'empty_a', 'dataset': '/a', 'start': [1, 2, 3], 'count': [0, 5, 6]}
                add(entry, selection, 7, False)
    return cases


def extra_contiguous_cases(root, generator):
    """H00 arrays fit one tile; these prove unrelated contiguous tiles stay cold."""
    cases = []
    shape = (137, 131)  # > 4 tiles; selection crosses only the first two.
    recipe = generator.recipe(shape)
    for dtype, label in [('<f4', 'le_f32'), ('>f8', 'be_f64')]:
        path = root / ('h04_multitile_' + label + '.h5')
        with h5py.File(path, 'w', userblock_size=4096) as file:
            file.create_dataset('data', data=generator.formula_array(shape, dtype, recipe), dtype=dtype)
            # An adjacent dataset makes source bounds weaker than dataset bounds.
            file.create_dataset('unselected', data=np.full((4096,), 8192, dtype=dtype))
        for rows, axes in [(7, True), (65536, False)]:
            cases.append({'id': path.stem + f'--cross-tile--r{rows}--axes{int(axes)}',
                          'file': path, 'dataset': '/data', 'origin': [29, 80],
                          'shape': [10, 32], 'target_rows': rows, 'axes': axes,
                          'reject': False, 'recipe': recipe})
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--probe', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--public-dir', type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    spec = importlib.util.spec_from_file_location('h04_generator', HERE.parents[1] / 'fixtures/hdf5/generate_h00.py')
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)
    root = args.output / 'fixtures'
    manifest = generator.generate(root)
    cases = cases_for(manifest, root) + extra_contiguous_cases(root, generator)
    # Public GWOSC sources use filters and are validated by run_h05.py.
    report = {'schema_version': 'iotdbfs-hdf5-h04-validation/1', 'platform': platform.platform(),
              'probe_sha256': sha(args.probe), 'oracle_h5py': h5py.__version__,
              'oracle_hdf5': h5py.version.hdf5_version, 'expected_cases': len(cases), 'cases': [], 'failures': []}
    for case in cases:
        output = args.output / 'artifacts' / case['id']
        output.mkdir(parents=True, exist_ok=True)
        try:
            for name in ['query.json', 'values.bin']:
                (output / name).unlink(missing_ok=True)
            command = [str(args.probe.resolve()), str(case['file'].resolve()), str(output.resolve()),
                       case['dataset'], ','.join(map(str, case['origin'])), ','.join(map(str, case['shape'])),
                       str(case['target_rows']), str(int(case['axes']))]
            start = time.perf_counter()
            run = subprocess.run(command, text=True, capture_output=True, timeout=60)
            require(run.returncode == 0, run.stderr[:2000])
            result = check(case, output, generator)
            result.update(id=case['id'], source_sha256=sha(case['file']),
                          subprocess_wall_seconds=time.perf_counter()-start)
            report['cases'].append(result)
        except Exception as error:
            report['failures'].append({'id': case['id'], 'error': str(error)})
    report['passed'] = not report['failures'] and len(report['cases']) == len(cases)
    report['value_cases'] = sum(c['status'] == 'values' for c in report['cases'])
    report['unsupported_cases'] = sum(c['status'] == 'unsupported' for c in report['cases'])
    (args.output / 'summary.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: report[k] for k in ['passed', 'expected_cases', 'value_cases', 'unsupported_cases', 'failures']}, indent=2))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
