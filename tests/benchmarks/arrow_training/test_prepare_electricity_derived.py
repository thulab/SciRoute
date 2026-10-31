import base64
import json
from pathlib import Path
import tempfile
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from prepare_electricity_derived import build_variant, load_source


class DerivedLayoutTests(unittest.TestCase):
    def source(self, path, *, gap=False, series_change=False):
        schema = pa.schema([
            pa.field('dataset_id', pa.string()), pa.field('entity_id', pa.string()),
            pa.field('series_id', pa.string()), pa.field('segment_index', pa.int32()),
            pa.field('start_step', pa.int64()), pa.field('end_step', pa.int64()),
            pa.field('target', pa.list_(pa.list_(pa.float32())))])
        # The 4-step block crosses two source rows and leaves a one-step tail.
        values = [dict(dataset_id='test', entity_id='one', series_id='one', segment_index=0,
                       start_step=0, end_step=2, target=[[0., -0.], [None, 11.]]),
                  dict(dataset_id='test', entity_id='two' if series_change else 'one',
                       series_id='one', segment_index=1, start_step=3 if gap else 2,
                       end_step=6 if gap else 5, target=[[2., 3., 4.], [12., float('nan'), 14.]])]
        with path.open('wb') as stream, ipc.new_stream(stream, schema) as writer:
            for row in values:
                writer.write_batch(pa.RecordBatch.from_pylist([row], schema=schema))

    def test_all_layouts_codecs_inverse_offsets_tail_and_reuse(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'source.arrows'
            self.source(source)
            original = source.read_bytes()
            for layout in ('channel_major', 'step_major'):
                for codec in ('none', 'lz4', 'zstd'):
                    with self.subTest(layout=layout, codec=codec):
                        destination = root / f'{layout}-{codec}.arrows'
                        result = build_variant(source, destination, layout=layout,
                                               block_steps=4, codec=codec,
                                               source_version=11, source_logical_path='/source')
                        self.assertEqual(result['total_steps'], 5)
                        self.assertEqual(result['channels'], 2)
                        self.assertEqual([(b['start_step'], b['end_step']) for b in result['batches']], [(0, 4), (4, 5)])
                        schema = ipc.read_schema(pa.BufferReader(base64.b64decode(result['schema_ipc_base64'])))
                        batches = []
                        with destination.open('rb') as handle:
                            for block in result['batches']:
                                handle.seek(block['offset'])
                                framed = handle.read(block['length'])
                                batches.append(ipc.read_record_batch(ipc.read_message(pa.BufferReader(framed)), schema))
                        target = batches[0].column('target')[0].as_py()
                        if layout == 'step_major':
                            self.assertEqual(target[:6], [0., None, -0., 11., 2., 12.])
                            self.assertTrue(np.signbit(target[2]))
                        else:
                            self.assertEqual(target[0], [0., -0., 2., 3.])
                            self.assertIsNone(target[1][0])
                            self.assertTrue(np.signbit(target[0][1]))
                        before = destination.stat().st_mtime_ns
                        reused = build_variant(source, destination, layout=layout, block_steps=4,
                                               codec=codec, source_version=11, source_logical_path='/source')
                        self.assertEqual(reused, result)
                        self.assertEqual(before, destination.stat().st_mtime_ns)
                        with self.assertRaisesRegex(ValueError, 'identity differs'):
                            build_variant(source, destination, layout=layout, block_steps=4,
                                          codec=codec, source_version=12)
            self.assertEqual(source.read_bytes(), original)

    def test_reuse_rejects_corrupt_file_and_sidecar(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'source'
            output = Path(directory) / 'derived'
            self.source(source)
            build_variant(source, output, layout='step_major', block_steps=4, codec='none')
            sidecar = output.with_suffix('.json')
            metadata = json.loads(sidecar.read_text())
            metadata['batches'][0]['length'] += 1
            sidecar.write_text(json.dumps(metadata))
            with self.assertRaisesRegex(ValueError, 'sidecar differs'):
                build_variant(source, output, layout='step_major', block_steps=4, codec='none')
            with output.open('ab') as handle:
                handle.write(b'x')
            with self.assertRaisesRegex(ValueError, 'bytes differ'):
                build_variant(source, output, layout='step_major', block_steps=4, codec='none')

    def test_rejects_gap_and_multiple_series(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'source'
            self.source(source, gap=True)
            with self.assertRaisesRegex(ValueError, 'contiguous'):
                load_source(source)
            self.source(source, series_change=True)
            with self.assertRaisesRegex(ValueError, 'one series'):
                load_source(source)


if __name__ == '__main__':
    unittest.main()
