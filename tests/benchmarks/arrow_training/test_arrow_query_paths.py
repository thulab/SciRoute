import hashlib
import copy
import unittest

import numpy as np
import pyarrow as pa

from run_arrow_query_paths import cases, declaration, digest, owned, trimmed
from summarize_arrow_query_paths import summarize


class PhysicalQueryContract(unittest.TestCase):
    def test_null_zero_and_order_sensitive_hash(self):
        a = owned(pa.array([1., None, -0.], type=pa.float32()))
        b = owned(pa.chunked_array([[2.], [3., 4.]], type=pa.float32()))
        self.assertEqual(a[1].tolist(), [1, 0, 1])
        self.assertEqual(a[0].tobytes(), np.array([1., 0., -0.], dtype='<f4').tobytes())
        self.assertTrue(a[0].flags.owndata)
        expected = hashlib.sha256(a[0].tobytes() + bytes([1, 0, 1]) + b[0].tobytes() + bytes([1, 1, 1])).hexdigest()
        self.assertEqual(digest([a, b], [5, 6, 7])['digest'], expected)
        self.assertNotEqual(digest([a, b], [5, 6, 7]), digest([b, a], [5, 6, 7]))

    def test_physical_request_has_no_training_or_predicate(self):
        case = next(c for c in cases(7) if c['id'] == 'c7-boundary')
        request = declaration('/source', 42, case)['request']
        self.assertEqual(request['sources']['versions'], {'kind': 'exact', 'exact': 42})
        subset = request['subset']
        self.assertEqual(subset['projection'], ['index'] + [f'value.{i}' for i in reversed(range(7))])
        self.assertEqual(subset['slices'][0]['upper']['value']['value'], 4160)
        self.assertEqual(subset['slices'][0]['upper']['kind'], 'exclusive')
        self.assertNotIn('predicate', subset)
        self.assertNotIn('view', request)

    def test_paper_statistic_discards_extremes(self):
        self.assertEqual(trimmed([100, 1, 2, 3, 4, 5, 6, 7, 8, 9]), 5.5)

    def test_summary_requires_all_arms_and_equal_physical_work(self):
        records = []
        for arm in ('A', 'A-read', 'B', 'C', 'D', 'E'):
            for repeat in range(3):
                counters = {key: 0 if repeat else 1 for key in
                    ('read_bytes', 'read_ranges', 'payload_read_calls', 'decode_count',
                     'payload_read_nanoseconds', 'evictions')}
                row = dict(dataset='fixture', case='one', arm=arm, campaign=1,
                           temperature='warm' if repeat else 'cold', wall_ns=100,
                           digest='same', index_digest='same', prepare_ns=1,
                           execute_ns=2, consume_ns=3, payload_bytes=1,
                           layout_bytes=1, native_message_body_bytes=8, **counters)
                if arm == 'E':
                    row.update(path=str(repeat), encoded_bytes=10, registration_ns=2,
                               ipc_read_ns=1, close_ns=1,
                               server=dict(runtime_delta=counters, stages_ns={'typed_encode': 1}))
                records.append(row)
        self.assertTrue(summarize(records, repeats=2, campaigns=1)['passed'])
        with self.assertRaisesRegex(ValueError, 'missing arm'):
            summarize([r for r in records if r['arm'] != 'D'], repeats=2, campaigns=1)
        changed = copy.deepcopy(records)
        changed[-3]['server']['runtime_delta']['read_bytes'] = 2
        with self.assertRaisesRegex(ValueError, 'physical work differs'):
            summarize(changed, repeats=2, campaigns=1)


if __name__ == '__main__':
    unittest.main()
