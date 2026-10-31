"""Local correctness/measurement checks for the experimental block reader."""
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from derived_window_reader import DerivedWindowReader
from index_original_training_arrow import index_original
from prepare_electricity_derived import build_variant, load_source, target_array
from prepare_arrow_query_variants import sha256_file


class DerivedWindowReaderTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.source = self.root / 'source.arrows'
        schema = pa.schema([
            ('dataset_id', pa.string()), ('entity_id', pa.string()),
            ('series_id', pa.string()), ('segment_index', pa.int32()),
            ('start_step', pa.int64()), ('end_step', pa.int64()),
            ('target', pa.list_(pa.list_(pa.float32())))])
        rows = [dict(dataset_id='test', entity_id='one', series_id='one', segment_index=0,
                     start_step=0, end_step=3, target=[[0., -0., 2.], [None, 11., 12.]]),
                dict(dataset_id='test', entity_id='one', series_id='one', segment_index=1,
                     start_step=3, end_step=5, target=[[3., 4.], [float('nan'), 14.]])]
        with self.source.open('wb') as stream, ipc.new_stream(stream, schema) as writer:
            for row in rows:
                writer.write_batch(pa.RecordBatch.from_pylist([row], schema=schema))
        self.oracle = load_source(self.source)
        self.manifests = {}

    def tearDown(self):
        self.temporary.cleanup()

    def manifest(self, layout='step_major', codec='none'):
        key = (layout, codec)
        if key not in self.manifests:
            self.manifests[key] = build_variant(
                self.source, self.root / f'{layout}-{codec}.arrows',
                layout=layout, codec=codec, block_steps=2)
        return self.manifests[key]

    def assert_windows(self, result, starts, length, channels):
        values, valid = result
        expected_values = np.stack([self.oracle['values'][start:start+length, channels]
                                    for start in starts])
        expected_valid = np.stack([self.oracle['valid'][start:start+length, channels]
                                   for start in starts])
        np.testing.assert_array_equal(valid, expected_valid)
        # Compare all Float32 bits: includes NaN payload and negative zero, with
        # null payload canonically zeroed by both independent source and reader.
        np.testing.assert_array_equal(values.view(np.uint32), expected_values.view(np.uint32))
        self.assertEqual(values.dtype, np.dtype('float32'))
        self.assertEqual(valid.dtype, np.dtype('bool'))
        self.assertTrue(values.flags.owndata)
        self.assertTrue(valid.flags.owndata)

    def test_layout_codec_modes_multiwindow_and_owned_results(self):
        for layout in ('channel_major', 'step_major'):
            for codec in ('none', 'lz4', 'zstd'):
                for mode in ('local', 'read'):
                    with self.subTest(layout=layout, codec=codec, mode=mode):
                        reader = DerivedWindowReader(self.manifest(layout, codec), mode=mode)
                        retained = []
                        try:
                            for channels in (None, (1,), (1, 0)):
                                actual = reader.read_windows([0, 2], 3, channels)
                                selection = slice(None) if channels is None else list(channels)
                                self.assert_windows(actual, [0, 2], 3, selection)
                                for arrays in reader.cache.values():
                                    for output in actual:
                                        for cached in arrays:
                                            self.assertFalse(np.shares_memory(output, cached))
                                retained.append((actual, selection))
                        finally:
                            reader.close()
                        for actual, channels in retained:
                            self.assert_windows(actual, [0, 2], 3, channels)
                        reader.close()  # cleanup is idempotent

    def test_shared_demand_adjacent_merge_tail_and_warm_zero_reads(self):
        for mode in ('local', 'read'):
            with self.subTest(mode=mode):
                manifest = self.manifest()
                reader = DerivedWindowReader(manifest, mode=mode)
                try:
                    # Both windows share the same two blocks. Adjacent complete
                    # IPC messages merge into exactly one read, decoded once each.
                    self.assert_windows(reader.read_windows([0, 1], 3), [0, 1], 3, slice(None))
                    self.assertEqual(reader.metrics['read_calls'], 1)
                    self.assertEqual(reader.metrics['read_ranges'], 1)
                    self.assertEqual(reader.metrics['batch_decodes'], 2)
                    self.assertEqual(reader.metrics['read_bytes'],
                                     sum(b['length'] for b in manifest['batches'][:2]))
                    before = dict(reader.metrics)
                    self.assert_windows(reader.read_windows([0, 1], 3), [0, 1], 3, slice(None))
                    for key in ('read_calls', 'read_ranges', 'read_bytes', 'batch_decodes'):
                        self.assertEqual(reader.metrics[key], before[key])
                    self.assertEqual(reader.metrics['cache_hits']-before['cache_hits'], 2)
                    # A single short final block contains precisely one time step.
                    self.assert_windows(reader.read_windows([4], 1), [4], 1, slice(None))
                    self.assertEqual(reader.metrics['batch_decodes'], 3)
                finally:
                    reader.close()

    def test_zero_and_one_block_cache_preserve_inflight_outputs(self):
        for mode in ('local', 'read'):
            for budget in (0, 20):  # two steps * two channels * (4 value + 1 mask)
                with self.subTest(mode=mode, budget=budget):
                    reader = DerivedWindowReader(self.manifest(), mode=mode, max_cache_bytes=budget)
                    try:
                        for repeat in range(2):
                            self.assert_windows(reader.read_windows([0, 1], 3), [0, 1], 3, slice(None))
                            self.assertLessEqual(reader.cache_bytes, budget)
                            self.assertLessEqual(reader.metrics['peak_cache_bytes'], budget)
                        if budget == 0:
                            self.assertEqual(reader.metrics['batch_decodes'], 4)
                            self.assertFalse(reader.cache)
                        else:
                            self.assertEqual(reader.metrics['batch_decodes'], 3)
                            self.assertEqual(reader.metrics['cache_hits'], 1)
                            self.assertGreater(reader.metrics['cache_evictions'], 0)
                    finally:
                        reader.close()

    def test_invalid_requests_do_not_issue_reads(self):
        reader = DerivedWindowReader(self.manifest())
        try:
            for starts, length, channels in (([], 1, None), ([0], 0, None),
                    ([-1], 1, None), ([4], 2, None), ([0], 1, ()),
                    ([0], 1, (0, 0)), ([0], 1, (-1,)), ([0], 1, (2,))):
                with self.subTest(starts=starts, length=length, channels=channels):
                    with self.assertRaises(ValueError):
                        reader.read_windows(starts, length, channels)
            self.assertEqual(reader.metrics['read_calls'], 0)
        finally:
            reader.close()

    def test_invalid_manifest_coverage_byte_ranges_and_budget(self):
        original = self.manifest()
        mutations = [lambda m: m['batches'][1].update(start_step=3),
                     lambda m: m['batches'][0].update(length=m['bytes'] + 1),
                     lambda m: m.update(total_steps=6),
                     lambda m: m.update(channels=0)]
        for mutate in mutations:
            manifest = copy.deepcopy(original)
            mutate(manifest)
            with self.assertRaises(ValueError):
                DerivedWindowReader(manifest)
        with self.assertRaises(ValueError):
            DerivedWindowReader(original, max_cache_bytes=-1)

    def multirow_source(self, codec=None):
        path = self.root / f'original-multirow-{codec}.arrows'
        schema = self.oracle['schema']
        values = np.arange(20, dtype=np.float32).reshape(10, 2)
        values.view(np.uint32)[1, 0] = 0x80000000  # negative zero
        values.view(np.uint32)[4, 1] = 0x7fc12345  # valid noncanonical NaN payload
        valid = np.ones(values.shape, dtype=bool)
        valid[6, 0] = False
        bounds = (0, 3, 5, 9, 10)
        rows = []
        for i, (start, end) in enumerate(zip(bounds, bounds[1:])):
            scalars = dict(dataset_id='test', entity_id='one', series_id='one',
                           segment_index=i, start_step=start, end_step=end)
            arrays = [target_array(values[start:end], valid[start:end], 'channel_major')
                      if f.name == 'target' else pa.array([scalars[f.name]], type=f.type)
                      for f in schema]
            rows.append(pa.RecordBatch.from_arrays(arrays, schema=schema))
        with path.open('wb') as stream, ipc.new_stream(
                stream, schema, options=ipc.IpcWriteOptions(compression=codec)) as writer:
            for i in (0, 2):
                table = pa.Table.from_batches(rows[i:i+2]).combine_chunks()
                writer.write_batch(table.to_batches()[0])
        return path

    def test_exact_original_multirow_batches_cross_rows_and_tail(self):
        path = self.multirow_source()
        source_hash = sha256_file(path)
        manifest = index_original(path, source_version=123, source_logical_path='/original.arrows')
        self.assertEqual(sha256_file(path), source_hash)
        self.assertEqual(manifest['sha256'], source_hash)
        self.assertEqual(manifest['artifact_kind'], 'original_index')
        self.assertEqual(len(manifest['batches']), 2)
        self.assertEqual([len(b['rows']) for b in manifest['batches']], [2, 2])
        self.assertEqual([(b['start_step'], b['end_step']) for b in manifest['batches']],
                         [(0, 5), (5, 10)])
        self.oracle = load_source(path)
        derived = build_variant(path, self.root / 'matched-step-major.arrows',
                                layout='step_major', block_steps=2, codec='none')
        for indexed in (manifest, derived):
            for mode in ('local', 'read'):
                with self.subTest(original=indexed is manifest, mode=mode):
                    reader = DerivedWindowReader(indexed, mode=mode)
                    try:
                        actual = reader.read_windows([1, 2, 4, 7], 3, (1, 0))
                        self.assert_windows(actual, [1, 2, 4, 7], 3, [1, 0])
                        self.assertEqual(reader.metrics['batch_decodes'], len(indexed['batches']))
                        self.assertEqual(reader.metrics['read_bytes'],
                                         sum(b['length'] for b in indexed['batches']))
                        self.assertEqual(reader.cache_bytes, 10*2*5)
                        before = dict(reader.metrics)
                        self.assert_windows(reader.read_windows([8], 2), [8], 2, slice(None))
                        for key in ('read_calls', 'read_bytes', 'batch_decodes'):
                            self.assertEqual(reader.metrics[key], before[key])
                        if indexed is manifest:
                            block = reader.cache[0]
                            self.assertEqual(len(block.rows), 2)
                            self.assertFalse(block.rows[0][2].flags.c_contiguous)
                            self.assertFalse(block.rows[1][2].flags.c_contiguous)
                    finally:
                        reader.close()
                    self.assert_windows(actual, [1, 2, 4, 7], 3, [1, 0])

    def test_original_whole_batch_cache_serves_unrequested_sibling_row(self):
        path = self.multirow_source()
        self.oracle = load_source(path)
        manifest = index_original(path, source_version=123, source_logical_path='/original.arrows')
        reader = DerivedWindowReader(manifest)
        try:
            self.assert_windows(reader.read_windows([0], 1), [0], 1, slice(None))
            before = dict(reader.metrics)
            self.assert_windows(reader.read_windows([4], 1), [4], 1, slice(None))
            self.assertEqual(reader.metrics['batch_decodes'], 1)
            self.assertEqual(reader.metrics['read_bytes'], manifest['batches'][0]['length'])
            self.assertEqual(reader.metrics['cache_hits'], before['cache_hits']+1)
        finally:
            reader.close()

    def test_original_index_rejects_compression_and_bad_row_index(self):
        for codec in ('lz4', 'zstd'):
            with self.assertRaisesRegex(ValueError, 'uncompressed'):
                index_original(self.multirow_source(codec), source_version=123,
                               source_logical_path='/original.arrows')
        manifest = index_original(self.multirow_source(), source_version=123,
                                  source_logical_path='/original.arrows')
        manifest['batches'][0]['rows'][1]['start_step'] += 1
        with self.assertRaisesRegex(ValueError, 'batch row coverage'):
            DerivedWindowReader(manifest)

    def test_large_local_ranges_avoid_iotdb_transport_split_and_copy(self):
        length = 8*1024**2+17
        buffer = pa.py_buffer(bytearray(length))
        for mode in ('local', 'read', 'fuse', 'iotdb'):
            with self.subTest(mode=mode):
                reader = DerivedWindowReader.__new__(DerivedWindowReader)
                reader.mode = mode
                reader._read_wave = Mock(side_effect=lambda ranges: [
                    buffer.slice(offset-3, size) for offset, size in ranges])
                result = reader._read([(3, length)])
                self.assertEqual(len(result), 1)
                self.assertEqual(len(result[0]), length)
                if mode == 'iotdb':
                    self.assertEqual(reader._read_wave.call_count, 2)
                    for call in reader._read_wave.call_args_list:
                        self.assertLessEqual(sum(n for _, n in call.args[0]), 8*1024**2)
                    self.assertNotEqual(result[0].address, buffer.address)
                else:
                    self.assertEqual(reader._read_wave.call_count, 1)
                    self.assertEqual(result[0].address, buffer.address)


if __name__ == '__main__':
    unittest.main()
