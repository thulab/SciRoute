"""Bounded native HDF5 handle reuse over an immutable lifecycle catalog.

Every request resolves its exact source/version/dataset through the catalog,
including its live stat-identity check. Only native sessions are cached; every
request performs a fresh native value read. HDF5's own chunk cache survives
with the dataset handle. This reader is single-threaded, like Catalog.
"""
from collections import OrderedDict
from pathlib import Path
import time


class Hdf5HandleCache:
    def __init__(self, catalog, reader, *, capacity=8):
        if type(capacity) is not int or capacity < 1:
            raise ValueError("positive handle capacity required")
        self.catalog, self.reader, self.capacity = catalog, reader, capacity
        self.entries = OrderedDict()
        self.closed = False
        self.counts = dict(opens=0, hits=0, evictions=0, closes=0)

    def __enter__(self):
        return self

    def __exit__(self, kind, value, traceback):
        self.close()

    def stats(self):
        return dict(self.counts, entries=len(self.entries), capacity=self.capacity,
                    closed=self.closed)

    def close(self):
        error = None
        while self.entries:
            _, session = self.entries.popitem(last=False)
            try:
                self.reader.close_session(session)
                self.counts['closes'] += 1
            except BaseException as caught:
                if error is None:
                    error = caught
        self.closed = True
        if error is not None:
            raise error

    def read(self, spec):
        if self.closed or spec['format_id'] != 'hdf5':
            raise ValueError("open HDF5 handle cache required")
        begin = time.perf_counter_ns()
        metadata = self.catalog.lookup(spec)
        selected = time.perf_counter_ns()
        self.reader._verify_metadata(spec, metadata)
        raw = str(Path(spec['raw']).absolute())
        key = (raw, spec.get('logical_path', raw), spec.get('version', 1),
               metadata['dataset'])
        hit = key in self.entries
        if hit:
            session = self.entries[key]
            if session['metadata'] != metadata:
                raise ValueError("cached dataset binding changed")
            self.entries.move_to_end(key)
            self.counts['hits'] += 1
        else:
            if len(self.entries) >= self.capacity:
                _, old = self.entries.popitem(last=False)
                self.reader.close_session(old)
                self.counts['closes'] += 1
                self.counts['evictions'] += 1
            session, _ = self.reader.open_session(spec, metadata=metadata)
            self.entries[key] = session
            self.counts['opens'] += 1
        prepared = time.perf_counter_ns()
        observation, owned = self.reader.read(spec, session=session)
        end = time.perf_counter_ns()
        discovery = prepared-begin + observation.get('discovery_ns', 0)
        return dict(observation, t0_ns=begin, t1_ns=begin+discovery, t2_ns=end,
                    discovery_ns=discovery, value_ns=end-begin-discovery,
                    total_ns=end-begin, catalog_lookup_ns=selected-begin,
                    handle_prepare_ns=prepared-selected, persistent_index=True,
                    metadata_reused=True, handles_reused=True, handle_cache_hit=hit,
                    handle_cache_entries=len(self.entries)), owned
