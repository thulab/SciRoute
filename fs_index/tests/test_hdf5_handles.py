"""Handle reuse must retain exact-version binding and fresh native reads."""
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fs_index.lifecycle import Catalog
from fs_index.hdf5_handles import Hdf5HandleCache
from fs_index.tests.test_lifecycle import HdfReader


class SessionReader(HdfReader):
    def __init__(self):
        self.opened = self.released = self.reads = 0

    def open_session(self, spec, metadata=None):
        self.opened += 1
        return dict(metadata=metadata, id=self.opened), 0

    def close_session(self, session):
        self.released += 1

    def read(self, spec, metadata=None, session=None):
        self.reads += 1
        return dict(discovery_ns=1, native_value_read_calls=1), dict(values=[spec['origin'][0]])


class HandleTests(unittest.TestCase):
    def setUp(self):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.root = Path(root.name)
        self.reader = SessionReader()
        self.catalog = Catalog(self.root/'index.sqlite', create=True)
        self.addCleanup(self.catalog.close)
        self.specs = []
        for version in (1, 2):
            path = self.root/f'v{version}.h5'
            path.write_bytes(str(version).encode())
            spec = dict(raw=str(path), logical_path='/same', version=version,
                        format_id='hdf5', dataset='/T', origin=[0])
            self.catalog.register(spec, self.reader,
                sha256=hashlib.sha256(path.read_bytes()).hexdigest(), version=version)
            self.specs.append(spec)

    def test_each_request_resolves_catalog_and_reads_fresh_selection(self):
        with Hdf5HandleCache(self.catalog, self.reader) as cache:
            with patch.object(self.catalog, 'lookup', wraps=self.catalog.lookup) as lookup:
                a, one = cache.read(self.specs[0])
                b, two = cache.read(dict(self.specs[0], origin=[5]))
                self.assertEqual(lookup.call_count, 2)
            self.assertFalse(a['handle_cache_hit'])
            self.assertTrue(b['handle_cache_hit'])
            self.assertEqual((one['values'], two['values']), ([0], [5]))
            self.assertIsNot(one['values'], two['values'])
            self.assertEqual((self.reader.opened, self.reader.reads), (1, 2))
        self.assertEqual(self.reader.released, 1)

    def test_exact_versions_eviction_and_close(self):
        with Hdf5HandleCache(self.catalog, self.reader, capacity=1) as cache:
            for spec in (self.specs[0], self.specs[1], self.specs[0]):
                row, _ = cache.read(spec)
                self.assertFalse(row['handle_cache_hit'])
            self.assertEqual(cache.stats()['evictions'], 2)
        self.assertEqual(self.reader.released, 3)
        cache.close()
        self.assertEqual(self.reader.released, 3)
        with self.assertRaisesRegex(ValueError, 'open HDF5'):
            cache.read(self.specs[0])

    def test_replaced_source_cannot_hit_retained_handle(self):
        with Hdf5HandleCache(self.catalog, self.reader) as cache:
            cache.read(self.specs[0])
            replacement = self.root/'replacement'
            replacement.write_bytes(b'1')
            replacement.replace(self.specs[0]['raw'])
            with self.assertRaisesRegex(ValueError, 'changed'):
                cache.read(self.specs[0])
            self.assertEqual(self.reader.reads, 1)

    def test_native_failure_still_releases_handles(self):
        with self.assertRaisesRegex(RuntimeError, 'native read failed'):
            with Hdf5HandleCache(self.catalog, self.reader) as cache:
                cache.read(self.specs[0])
                with patch.object(self.reader, 'read', side_effect=RuntimeError('native read failed')):
                    cache.read(self.specs[1])
        self.assertEqual(self.reader.opened, self.reader.released)


if __name__ == '__main__':
    unittest.main()
