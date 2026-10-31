#!/usr/bin/env python3
"""H07 GPS binding oracle. h5py supplies independent coordinates and H5Dread.

The probe uses real ingestion, layout /3 and QueryModule with a recording
in-memory backend. This validates correctness, not live IoTDB/FUSE performance.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import time
import h5py
import numpy as np


def require(ok, message):
    if not ok:
        raise AssertionError(message)


def fixture(path, mode='fixed', change=None):
    with h5py.File(path, 'w') as f:
        d = f.create_dataset('strain/Strain', data=np.arange(127, dtype='<f8') + .25,
                             chunks=(16,), shuffle=True, compression='gzip', fletcher32=True)
        d.attrs['Xstart'] = np.int64(1126259446)
        d.attrs['Xspacing'] = np.float64(1 / 4096)
        d.attrs['Npoints'] = np.int64(127)
        d.attrs['Xunits'] = np.bytes_('second') if mode == 'fixed' else 'second'
        d.attrs['Xlabel'] = np.bytes_('GPS time') if mode == 'fixed' else 'GPS time'
        if change:
            name, value = change
            del d.attrs[name]
            if value is not None:
                d.attrs[name] = value
        # Arbitrary variable strings must not force a value scan or huge decode.
        f.attrs['long_variable_string'] = 'x' * 1000000
        f.attrs['science_name'] = np.bytes_('bounded GWOSC fixture')
        scale = f.create_dataset('ordinal_candidate', data=np.arange(127, dtype='<f8'))
        scale.make_scale('time candidate, never automatic semantics')
        d.dims[0].attach_scale(scale)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--probe', required=True, type=Path)
    ap.add_argument('--output', required=True, type=Path)
    ap.add_argument('--public-data', type=Path)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    fixtures = args.output / 'fixtures'; fixtures.mkdir(exist_ok=True)
    cases = []
    start, dt = 1126259446., 1 / 4096
    for mode in ('fixed', 'vlen'):
        path = fixtures / f'{mode}.h5'; fixture(path, mode)
        bounds = [('whole', start, start+127*dt), ('interior', start+3*dt, start+117*dt),
                  ('empty', start+4*dt, start+4*dt), ('below', start-2, start-1),
                  ('above', start+1, start+2), ('cover', start-1, start+1)]
        for direction in (-np.inf, np.inf):
            bounds.append((f'nextafter_{direction}', np.nextafter(start+3*dt, direction),
                           np.nextafter(start+117*dt, direction)))
        for label, lo, hi in bounds:
            cases.append((f'{mode}_{label}', path, '/strain/Strain', lo, hi, True, True))
        cases.append((f'{mode}_not_enabled', path, '/strain/Strain', start, start+dt, False, False))
    mutations = [('wrong_count', ('Npoints', np.int64(128))),
                 ('count_float', ('Npoints', np.float64(127))),
                 ('missing_count', ('Npoints', None)),
                 ('wrong_unit', ('Xunits', np.bytes_('nanosecond'))),
                 ('wrong_label', ('Xlabel', np.bytes_('UTC'))),
                 ('missing_unit', ('Xunits', None)),
                 ('zero_spacing', ('Xspacing', 0.)),
                 ('negative_spacing', ('Xspacing', -dt)),
                 ('nan_spacing', ('Xspacing', np.nan)),
                 ('inf_spacing', ('Xspacing', np.inf)),
                 ('decimal_spacing', ('Xspacing', .1)),
                 ('collapsed_spacing', ('Xspacing', 2.**-60)),
                 ('huge_start', ('Xstart', np.uint64(2**64-1))),
                 ('start_vector', ('Xstart', np.array([1126259446], dtype='<i8')))]
    for name, change in mutations:
        path = fixtures / f'{name}.h5'; fixture(path, change=change)
        cases.append((name, path, '/strain/Strain', start, start+dt, True, False))
    if args.public_data:
        public = sorted(args.public_data.glob('*_LOSC_4_V2-1126259446-32.hdf5'))
        require(len(public) == 2, 'expected both locked H1/L1 GWOSC public files')
        for path in public:
            for label, lo, hi in [('interior', start+3*dt, start+2049*dt),
                                  ('late', start+31, start+32),
                                  ('subsample', start+dt/2, start+2.5*dt)]:
                cases.append((path.stem+'_'+label, path, '/strain/Strain', lo, hi, True, True))
    results = []
    for name, path, dataset, lo, hi, enabled, expected_ok in cases:
        with h5py.File(path, 'r') as f:
            d = f[dataset]
            if expected_ok:
                coordinates = float(d.attrs['Xstart']) + np.arange(d.size, dtype=np.float64)*float(d.attrs['Xspacing'])
                first, stop = np.searchsorted(coordinates, [lo, hi], side='left')
                expected_values = np.asarray(d[int(first):int(stop)], dtype=d.dtype.newbyteorder('<')).tobytes()
                expected_coordinates = coordinates[first:stop].astype('<f8').tobytes()
            else:
                first, stop = 0, 0
        out = args.output / name
        completed = subprocess.run([str(args.probe.resolve()), str(path.resolve()), str(out.resolve()), dataset,
                                    repr(float(lo)), repr(float(hi)), str(int(enabled)), '7', str(first)],
                                   capture_output=True, text=True, timeout=60)
        require(completed.returncode == 0, f'{name}: probe failed: {completed.stderr}')
        report = json.loads((out/'query.json').read_text())
        require(report['ok'] == expected_ok, f'{name}: unexpected status {report}')
        require((out/'layout.bin').read_bytes().startswith(b'IOTDBFS-HDF5-LAYOUT/3\n'), 'layout version')
        require(not any(r['kind']=='payload' and r['phase'].endswith('prepare') for r in report['reads']),
                f'{name}: binding scanned payload')
        if expected_ok:
            require((out/'values.bin').read_bytes()==expected_values, f'{name}: values differ from H5Dread')
            require((out/'gps.bin').read_bytes()==expected_coordinates, f'{name}: GPS coordinate bits differ')
            require(report['rows']==int(stop-first) and report['physical_equal'], f'{name}: ordinal mismatch')
            require(report['physical_extra_payload_calls']==0, f'{name}: semantic/physical cache duplicated')
            cursor = int(first)
            for origin, count in report['slabs']:
                require(origin==cursor and 0<count<=7, f'{name}: invalid domain')
                cursor += count
            require(cursor==stop, f'{name}: skipped domain')
        else:
            require(not any(r['kind']=='payload' for r in report['reads']), f'{name}: invalid binding read values')
        results.append(dict(id=name, ok=True, rows=report['rows'], expected_status=report['error'],
                            source_sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
    summary = dict(schema_version='iotdbfs-hdf5-h07-oracle/1', platform=platform.platform(),
                   h5py=h5py.__version__, oracle_hdf5=h5py.version.hdf5_version, passed=len(results),
                   probe_sha256=hashlib.sha256(args.probe.read_bytes()).hexdigest(),
                   timestamp=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), cases=results)
    (args.output/'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
    print(json.dumps({k:v for k,v in summary.items() if k!='cases'}, indent=2))


if __name__ == '__main__':
    main()
