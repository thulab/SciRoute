"""Offline protocol checks for the actual A/B endpoint reversal."""
import unittest
from unittest.mock import patch
from experiments.fixed_matrix import tail_reversed as definition
from experiments.fixed_matrix import fee_stage_summary
from experiments.fixed_matrix import hdf5_native_ab
from experiments.fixed_matrix import tail_controls


class ReversedTailTests(unittest.TestCase):
    def test_only_configuration_order_changes(self):
        self.assertEqual([definition.runner.configuration_order(c) for c in range(4)],
                         [('B','A'),('A','B'),('A','B'),('B','A')])
        self.assertEqual(definition.SPEC.product_diff, fee_stage_summary.SPEC.product_diff)
        self.assertEqual(definition.SPEC.required_regression_tests,
                         fee_stage_summary.SPEC.required_regression_tests)
        self.assertEqual(definition.runner.diagnostic_metadata()['kind'],
                         'actual-AB-reversed-endpoint')

    def test_metadata_schedule_and_global_restore_even_after_failure(self):
        for failure in (False, True):
            def fake_collect(*args):
                value = hdf5_native_ab.build_run(replication={'schedule':'old', 'campaign':3})
                self.assertEqual(value['replication'], {'schedule':definition.SCHEDULE, 'campaign':3})
                if failure:
                    raise RuntimeError('injected')
                return 240
            with patch.object(hdf5_native_ab, 'build_run', side_effect=lambda **kw:kw) as original:
                with patch.object(tail_controls.TailControlRunner, 'collect', fake_collect):
                    if failure:
                        with self.assertRaisesRegex(RuntimeError, 'injected'):
                            definition.runner.collect()
                    else:
                        self.assertEqual(definition.runner.collect(), 240)
                self.assertIs(hdf5_native_ab.build_run, original)


if __name__ == '__main__':
    unittest.main()
