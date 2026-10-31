"""Main section 4.2 collection order; caller owns resources and all evidence.

This module starts no processes itself. The supplied mount_factory establishes
one fresh daemon for each product method/campaign block; measure owns
resource snapshots, profile harvesting, audits and durable observation writes.
"""
from __future__ import annotations

from pathlib import Path

try:
    from .read_matrix import main_cases, read_iotdb, validate_owned
    from .read_matrix_native import NativeHdf5, NativeGrib, need
except ImportError:
    from read_matrix import main_cases, read_iotdb, validate_owned
    from read_matrix_native import NativeHdf5, NativeGrib, need


METHODS = ('native FS', 'iotdb-indexed', 'native-hdf5')
REPEATS = 13


def collect_main(repo, bindings_by_campaign, mount_factory, measure):
    """Collect 897 independently owned query results across three campaigns.

    Every case block executes index 0 as its first read, indices 1/2 as warmups,
    and indices 3..12 as measurements. Product cases within one method/campaign
    share a daemon: earlier cases can warm caches for later case blocks, so
    repeat 0 does not imply daemon-first access or a cold cache. No sample is
    omitted or reused. Native FS
    opens its source on every query; no metadata or native handle preparation
    precedes that query. Constructor/library validation is outside its clock.
    """
    repo = Path(repo)
    need(len(bindings_by_campaign) == 3, 'main matrix requires three independent campaigns')
    hdf5_library = repo/'experiments/single_file/work/session-20261012/hdf5-prefix/lib/libhdf5.so.310'
    grib_library = Path('/lib/x86_64-linux-gnu/libeccodes.so.0')
    count = 0
    for campaign in range(3):
        registry = main_cases(repo, bindings_by_campaign[campaign])
        cases = registry['cases']
        need(len(cases) == 9 and len({spec['case_id'] for spec in cases}) == 9,
             'main matrix must contain nine distinct cases')
        method_order = METHODS[campaign:] + METHODS[:campaign]
        for spec in cases:
            need(spec['format_id'] in ('grib', 'hdf5'), 'unexpected main-matrix format')
            need(isinstance(spec.get('logical_path'), str) and
                 type(spec.get('version')) is int and spec['version'] > 0,
                 'campaign exact published source receipt missing')
        for method in method_order:
            applicable = [spec for spec in cases
                          if method != 'native-hdf5' or spec['format_id'] == 'hdf5']
            if method == 'native FS':
                for spec in applicable:
                    audit = lambda record, owned, current=spec: validate_owned(current, owned)
                    reader = (NativeHdf5(hdf5_library) if spec['format_id'] == 'hdf5'
                              else NativeGrib(grib_library))
                    for repeat in range(REPEATS):
                        measure(case=spec, method=method, campaign=campaign, repeat=repeat,
                                read=lambda current=spec, native=reader: native.read(current),
                                audit=audit, context=None)
                        count += 1
            else:
                block_id = 'main-all-' + method
                with mount_factory(campaign, block_id, method) as context:
                    for spec in applicable:
                        audit = lambda record, owned, current=spec: validate_owned(current, owned)
                        for repeat in range(REPEATS):
                            measure(case=spec, method=method, campaign=campaign, repeat=repeat,
                                    read=lambda current=spec, mounted=context:
                                        read_iotdb(mounted, current),
                                    audit=audit, context=context)
                            count += 1
    need(count == 897, 'main collection omitted observations')
    return count
