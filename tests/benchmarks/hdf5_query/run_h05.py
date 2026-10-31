#!/usr/bin/env python3
"""H05 raw decoder oracle through production ingestion and QueryModule.

Compare canonical typed output against independent h5py H5Dread and coordinate
formulas. Exercise native pipelines/masks, fill, partial edges and malformed raw
chunks. This is correctness evidence, not a filesystem latency benchmark.
"""
from __future__ import annotations
import argparse
import ctypes
import hashlib
import importlib.util
import itertools
import json
import math
from pathlib import Path
import platform
import subprocess
import sys
import time
import zlib
import h5py
import numpy as np
import run_h04 as h04

HERE = Path(__file__).resolve().parent
FILTERS = (h5py.h5z.FILTER_SHUFFLE, h5py.h5z.FILTER_DEFLATE, h5py.h5z.FILTER_FLETCHER32)


def low_dataset(file, shape, dtype, chunks, filters=(), fill='default', never=False, partial=False):
    dcpl = h5py.h5p.create(h5py.h5p.DATASET_CREATE)
    dcpl.set_obj_track_times(False)
    datatype = h5py.h5t.py_create(np.dtype(dtype))
    if chunks:
        dcpl.set_chunk(chunks)
    for kind in filters:
        dcpl.set_filter(kind, h5py.h5z.FLAG_OPTIONAL if kind != h5py.h5z.FILTER_FLETCHER32 else 0,
                        (6,) if kind == h5py.h5z.FILTER_DEFLATE else ())
    if fill == 'undefined':
        library = ctypes.CDLL(h5py.h5p.__file__)
        setter = library.H5Pset_fill_value
        setter.argtypes = (ctypes.c_longlong, ctypes.c_longlong, ctypes.c_void_p)
        setter.restype = ctypes.c_int
        h04.require(setter(dcpl.id, datatype.id, None) == 0, 'cannot set undefined fill')
    elif fill != 'default':
        dcpl.set_fill_value(np.asarray(fill, dtype=dtype))
    if never or fill == 'undefined':
        dcpl.set_fill_time(h5py.h5d.FILL_TIME_NEVER)
    if partial:
        library = ctypes.CDLL(h5py.h5p.__file__)
        setter = library.H5Pset_chunk_opts
        setter.argtypes = (ctypes.c_longlong, ctypes.c_uint)
        setter.restype = ctypes.c_int
        h04.require(setter(dcpl.id, 2) == 0, 'cannot set DONT_FILTER_PARTIAL_CHUNKS')
    return h5py.Dataset(h5py.h5d.create(file.id, b'data', datatype,
                        h5py.h5s.create_simple(shape), dcpl=dcpl))


def build_cases(root, generator):
    cases = []
    def add(path, shape, origin=None, count=None, *, recipe=None, error=None, label='', rows=7, axes=True):
        cases.append(dict(id=path.stem + ('--' + label if label else ''), file=path, dataset='/data',
                          origin=list(origin or [0]*len(shape)), shape=list(count or shape),
                          target_rows=rows, axes=axes, reject=False, recipe=recipe, expected_error=error))
    # Every nonempty canonical filter subset plus native GWOSC Fletcher→DEFLATE,
    # across rank, width and byte order.
    for rank, shape in generator.SHAPES.items():
        chunks = generator.CHUNKS[rank]
        spec = generator.recipe(shape)
        for dtype in ('<f4', '>f4', '<f8', '>f8'):
            for bits in range(1, 9):
                filters = ((h5py.h5z.FILTER_FLETCHER32, h5py.h5z.FILTER_DEFLATE) if bits == 8 else
                           tuple(f for i, f in enumerate(FILTERS) if bits & (1 << i)))
                path = root / f'pipeline{bits}_rank{rank}_{"le" if dtype[0] == "<" else "be"}_f{np.dtype(dtype).itemsize*8}.h5'
                with generator.create_file(path) as file:
                    low_dataset(file, shape, dtype, chunks, filters)[:] = generator.formula_array(shape, dtype, spec)
                for label, origin, count in generator.selections(shape):
                    add(path, shape, origin, count, recipe=spec, label=label,
                        rows=7 if label == 'interior' else 65536, axes=label != 'whole')
    # Each mask's bytes are produced by a separate native donor pipeline; no
    # decoder helper or reimplementation of Fletcher/shuffle generates inputs.
    for dtype in ('<f4', '>f4', '<f8', '>f8'):
        shape = (8, 6, 4)
        spec = generator.recipe(shape)
        data = generator.formula_array(shape, dtype, spec)
        for pipeline_name, pipeline in [('canonical', FILTERS),
                                         ('gwosc', (h5py.h5z.FILTER_FLETCHER32, h5py.h5z.FILTER_DEFLATE))]:
            for mask in range(1 << len(pipeline)):
                active = tuple(f for i, f in enumerate(pipeline) if not mask & (1 << i))
                donor = root / 'donor.h5'
                with generator.create_file(donor) as file:
                    dataset = low_dataset(file, shape, dtype, shape, active)
                    dataset[:] = data
                    file.flush()
                    donor_mask, raw = dataset.id.read_direct_chunk((0, 0, 0))
                    h04.require(donor_mask == 0, 'donor skipped a filter unexpectedly')
                path = root / f'mask_{pipeline_name}_{mask}_{"le" if dtype[0] == "<" else "be"}_f{np.dtype(dtype).itemsize*8}.h5'
                with generator.create_file(path) as file:
                    dataset = low_dataset(file, shape, dtype, shape, pipeline)
                    dataset.id.write_direct_chunk((0, 0, 0), raw, filter_mask=mask)
                add(path, shape, recipe=spec)
    (root / 'donor.h5').unlink(missing_ok=True)
    # Other triple permutations remain outside the explicitly tested profile.
    for order in itertools.permutations(FILTERS):
        if order == FILTERS:
            continue
        path = root / ('unsupported_order_' + '_'.join(map(str, order)) + '.h5')
        with generator.create_file(path) as file:
            low_dataset(file, (37,), '<f4', (8,), order)[:] = generator.formula_array((37,), '<f4', generator.recipe((37,)))
        add(path, (37,), error='UnsupportedCapability')
    # Known profile exclusions are still explicit even when selected values
    # could have been read without running an unknown filter.
    for label, kind, parameters in [('unknown', 32001, ()), ('scaleoffset', h5py.h5z.FILTER_SCALEOFFSET, (0, 2))]:
        path = root / ('unsupported_' + label + '.h5')
        with generator.create_file(path) as file:
            dcpl = h5py.h5p.create(h5py.h5p.DATASET_CREATE)
            dcpl.set_chunk((8,))
            dcpl.set_filter(kind, h5py.h5z.FLAG_OPTIONAL, parameters)
            dataset = h5py.Dataset(h5py.h5d.create(file.id, b'data', h5py.h5t.IEEE_F32LE,
                                  h5py.h5s.create_simple((8,)), dcpl=dcpl))
            dataset.id.write_direct_chunk((0,), np.arange(8, dtype='<f4').tobytes(), filter_mask=1)
        add(path, (8,), error='UnsupportedCapability')
    # Partial edge chunks with filtering disabled use a dataset option; the
    # native library may store mask 0 even though every filter is bypassed.
    for rank, shape in generator.SHAPES.items():
        for dtype in ('<f4', '>f8'):
            path = root / f'partial_unfiltered_rank{rank}_{"le" if dtype[0] == "<" else "be"}.h5'
            spec = generator.recipe(shape)
            with generator.create_file(path, 4096) as file:
                low_dataset(file, shape, dtype, generator.CHUNKS[rank], FILTERS, partial=True)[:] = generator.formula_array(shape, dtype, spec)
            for label, origin, count in generator.selections(shape):
                add(path, shape, origin, count, recipe=spec, label=label, rows=11)
    # Missing chunk/contiguous extents: fill is a physical value, never NULL.
    # NEVER and undefined both reject holes, but allocated chunks stay readable.
    for chunked in (False, True):
        for dtype in ('<f4', '>f8'):
            for fill in ('default', -7.25, 'undefined'):
                for never in (False, True):
                    if fill == 'undefined' and not never:
                        continue
                    shape, chunks = (19, 11), (8, 6) if chunked else None
                    label = 'explicit' if isinstance(fill, float) else fill
                    path = root / f'fill_{"chunked" if chunked else "contiguous"}_{"le" if dtype[0] == "<" else "be"}_{label}_never{int(never)}.h5'
                    spec = generator.recipe(shape, fill=0.0 if fill == 'default' else fill,
                                            written=[{'start':[0,0], 'count':[8,6]}] if chunked else [])
                    with generator.create_file(path, 4096) as file:
                        dataset = low_dataset(file, shape, dtype, chunks, FILTERS if chunked else (), fill, never)
                        if chunked:
                            dataset[:8,:6] = generator.formula_array((8,6), dtype, generator.recipe(shape))
                    error = 'UnsupportedCapability' if never or fill == 'undefined' else None
                    add(path, shape, recipe=None if error else spec, error=error, label='whole')
                    if chunked:
                        add(path, shape, count=[8,6], recipe=generator.recipe(shape), label='allocated')
                        add(path, shape, origin=[8,6], count=[8,5], recipe=None if error else spec,
                            error=error, label='hole')
    # Special IEEE values must survive all filters/endian conversion bitwise.
    for dtype in ('<f4', '>f4', '<f8', '>f8'):
        path = root / f'ieee_{"le" if dtype[0] == "<" else "be"}_f{np.dtype(dtype).itemsize*8}.h5'
        with generator.create_file(path) as file:
            low_dataset(file, (8,), dtype, (8,), FILTERS)[:] = np.asarray([0.0, -0.0, np.inf, -np.inf, np.nan, 1.25, -3.5, np.finfo(np.dtype(dtype)).tiny], dtype=dtype)
        add(path, (8,), rows=3)
    # Raw malformed chunks preserve valid HDF5 metadata. Corruption reaches the
    # typed decoder rather than failing during format detection or inventory.
    mutations = ('fletcher_bad', 'fletcher_short', 'deflate_truncated', 'deflate_tail', 'deflate_bomb', 'plain_short', 'plain_tail')
    for name in mutations:
        filters = (h5py.h5z.FILTER_FLETCHER32,) if name.startswith('fletcher') else ((h5py.h5z.FILTER_DEFLATE,) if name.startswith('deflate') else ())
        path = root / (name + '.h5')
        donor = root / 'corrupt_donor.h5'
        with generator.create_file(donor) as file:
            dataset = low_dataset(file, (37,), '<f4', (37,), filters)
            dataset[:] = np.arange(37, dtype='<f4')
            file.flush()
            mask, raw = dataset.id.read_direct_chunk((0,))
            if name == 'fletcher_bad': raw = bytes([raw[0] ^ 1]) + raw[1:]
            elif name == 'fletcher_short': raw = raw[:3]
            elif name == 'deflate_truncated': raw = raw[:-2]
            elif name == 'deflate_tail': raw += b'bad tail'
            elif name == 'deflate_bomb': raw = zlib.compress(bytes(1024 * 1024))
            elif name == 'plain_short': raw = raw[:-1]
            elif name == 'plain_tail': raw += b'x'
        with generator.create_file(path) as file:
            dataset = low_dataset(file, (37,), '<f4', (37,), filters)
            dataset.id.write_direct_chunk((0,), raw, filter_mask=mask)
        add(path, (37,), error='CorruptPayload', rows=65536)
    (root / 'corrupt_donor.h5').unlink(missing_ok=True)
    return cases


def inspect_native(case):
    """Run malformed native input in isolation; native tolerance is not correctness.

    Native HDF5 can overrun buffers on malformed unfiltered raw chunk lengths,
    so never load adversarial payload in the runner that owns all evidence.
    """
    script = r'''import hashlib,json,sys,h5py,numpy as np
try:
 with h5py.File(sys.argv[1], 'r') as file:
  origin,shape=json.loads(sys.argv[3]),json.loads(sys.argv[4])
  value=file[sys.argv[2]][tuple(slice(o,o+s) for o,s in zip(origin,shape))]
  encoded=np.asarray(value).astype(value.dtype.newbyteorder('<'),copy=False).tobytes(order='C')
  print(json.dumps({'status':'values','sha256':hashlib.sha256(encoded).hexdigest()}))
except (OSError,ValueError) as error:
 print(json.dumps({'status':'error','message':str(error)[:300]}))
'''
    try:
        run = subprocess.run([sys.executable,'-c',script,str(case['file']),case['dataset'],
                              json.dumps(case['origin']),json.dumps(case['shape'])],
                             text=True,capture_output=True,timeout=10)
        if run.returncode:
            return {'status':'process_failure','returncode':run.returncode,'stderr':run.stderr[:300]}
        return json.loads(run.stdout)
    except subprocess.TimeoutExpired:
        return {'status':'timeout'}


def check(case, output, generator):
    report = json.loads((output / 'query.json').read_text())
    for read in report['reads']:
        bound = report['source_bytes'] if read['kind'] == 'payload' else report['layout_bytes']
        h04.require(0 <= read['offset'] <= bound and 0 <= read['length'] <= bound-read['offset'],
                    'negative case issued out-of-bounds read')
        h04.require(not (read['kind'] == 'payload' and read['phase'] == 'prepare'),
                    'planning read raw values')
    if case.get('expected_error'):
        expected = case['expected_error']
        h04.require(not report['ok'] and report['error'] == expected,
                    'expected ' + expected + ', got ' + str(report))
        h04.require(not report['batches'], 'negative case emitted partial values')
        if expected == 'UnsupportedCapability':
            h04.require(not any(r['kind']=='payload' for r in report['reads']), 'unsupported profile read payload')
        return {'status':expected, 'error':report['message'], 'native':inspect_native(case)}
    result = h04.check(case, output, generator)
    with h5py.File(case['file'], 'r') as file:
        dataset = file[case['dataset']]
        if dataset.chunks:
            chunks = [dataset.id.get_chunk_info(i) for i in range(dataset.id.get_num_chunks())]
            selected = [c for c in chunks if all(p < o+s and o < p+w for p,o,s,w in zip(c.chunk_offset,case['origin'],case['shape'],dataset.chunks))]
            result['oracle_expected_decoded_bytes'] = len(selected)*math.prod(dataset.chunks)*dataset.dtype.itemsize
            result['selected_allocated_chunks'] = len(selected)
            result['oracle_expected_decode_count'] = len(selected)
            result['stored_filter_masks'] = sorted({c.filter_mask for c in selected})
            properties = dataset.id.get_create_plist()
            filters = [properties.get_filter(i)[0] for i in range(properties.get_nfilters())]
            getter = ctypes.CDLL(h5py.h5p.__file__).H5Pget_chunk_opts
            getter.argtypes = (ctypes.c_longlong, ctypes.POINTER(ctypes.c_uint))
            getter.restype = ctypes.c_int
            options = ctypes.c_uint()
            h04.require(getter(properties.id, ctypes.byref(options)) == 0, 'cannot inspect chunk options')
            inflated = 0
            for chunk in selected:
                if options.value & 2 and any(o+w > n for o,w,n in zip(chunk.chunk_offset,dataset.chunks,dataset.shape)):
                    continue
                if h5py.h5z.FILTER_DEFLATE in filters:
                    di = filters.index(h5py.h5z.FILTER_DEFLATE)
                    if not chunk.filter_mask & (1 << di):
                        extra_checksum = any(f == h5py.h5z.FILTER_FLETCHER32 and not chunk.filter_mask & (1 << i)
                                             for i,f in enumerate(filters[:di]))
                        inflated += math.prod(dataset.chunks)*dataset.dtype.itemsize + 4*extra_checksum
            result['oracle_expected_inflated_bytes'] = inflated
        else:
            result['oracle_expected_inflated_bytes'] = 0
            tiles = h04.allowed_ranges(dataset,case['origin'],case['shape'])
            result['oracle_expected_decode_count'] = len(tiles)
            result['oracle_expected_decoded_bytes'] = sum(stop-start for start,stop in tiles)
    metrics = report['metrics']
    h04.require(metrics['useful_bytes'] == result['value_bytes'], 'useful-byte instrumentation mismatch')
    h04.require(metrics['payload_bytes'] == result['payload_bytes'] and
                metrics['payload_ranges'] == result['payload_calls'], 'payload instrumentation mismatch')
    h04.require(metrics['decoded_bytes'] == result['oracle_expected_decoded_bytes'],
                'decode instrumentation disagrees with independently selected native chunks/tiles')
    h04.require(metrics['decode_count'] == result['oracle_expected_decode_count'],
                'cache producer decode count disagrees with native chunk/tile count')
    h04.require(metrics['inflated_bytes'] == result['oracle_expected_inflated_bytes'],
                'inflate instrumentation disagrees with independently active filter pipeline')
    result['metrics'] = metrics
    result['logical_useful_bytes'] = result['value_bytes']
    result['actual_payload_bytes'] = result['payload_bytes']
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--probe', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--public-dir', type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    root = args.output / 'fixtures'
    root.mkdir(parents=True, exist_ok=True)
    spec = importlib.util.spec_from_file_location('h05_generator', HERE.parents[1] / 'fixtures/hdf5/generate_h00.py')
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)
    cases = build_cases(root, generator)
    if args.public_dir:
        public = json.loads((HERE/'public-data.lock.json').read_text())
        for entry in public['files']:
            path = args.public_dir/entry['filename']
            h04.require(h04.sha(path) == entry['sha256'], 'public source hash mismatch')
            for origin, count, rows in [(123,1001,7),(65530,15000,4096)]:
                cases.append(dict(id=path.stem+f'--o{origin}--n{count}',file=path,dataset='/strain/Strain',
                                  origin=[origin],shape=[count],target_rows=rows,axes=True,reject=False))
    report = dict(schema_version='iotdbfs-hdf5-h05-validation/1',platform=platform.platform(),
                  probe_sha256=h04.sha(args.probe),oracle_h5py=h5py.__version__,
                  oracle_hdf5=h5py.version.hdf5_version,expected_cases=len(cases),cases=[],failures=[])
    for case in cases:
        output = args.output/'artifacts'/case['id']
        output.mkdir(parents=True, exist_ok=True)
        try:
            for name in ('query.json','values.bin'):
                (output/name).unlink(missing_ok=True)
            command = [str(args.probe.resolve()),str(case['file'].resolve()),str(output.resolve()),
                       case['dataset'],','.join(map(str,case['origin'])),','.join(map(str,case['shape'])),
                       str(case['target_rows']),str(int(case['axes']))]
            start = time.perf_counter()
            run = subprocess.run(command,text=True,capture_output=True,timeout=60)
            h04.require(run.returncode == 0, run.stderr[:2000])
            result = check(case,output,generator)
            result.update(id=case['id'],source_sha256=h04.sha(case['file']),
                          subprocess_wall_seconds=time.perf_counter()-start)
            report['cases'].append(result)
        except Exception as error:
            report['failures'].append(dict(id=case['id'],error=str(error)))
    report['passed'] = not report['failures'] and len(report['cases']) == len(cases)
    report['value_cases'] = sum(c['status']=='values' for c in report['cases'])
    report['unsupported_cases'] = sum(c['status']=='UnsupportedCapability' for c in report['cases'])
    report['corrupt_cases'] = sum(c['status']=='CorruptPayload' for c in report['cases'])
    (args.output/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({k:report[k] for k in ('passed','expected_cases','value_cases','unsupported_cases','corrupt_cases','failures')},indent=2))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
