"""Offline A/A control protocol checks; no live service or scientific queries."""
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from experiments.fixed_matrix import tail_controls as definition

REPO = Path(__file__).resolve().parents[3]


class TailControlProtocolTests(unittest.TestCase):
    def test_same_scientific_group_and_complete_eight_block_schedule(self):
        group = definition.runner.groups(REPO, 'n128')[0]
        self.assertEqual((group['N'], group['C_paths'], group['K_units']), (128, 128, 2))
        self.assertEqual(definition.runner.expected_rows(SimpleNamespace(repo=REPO, tier='n128')), 240)
        self.assertEqual([definition.runner.configuration_order(c) for c in range(4)],
                         [('A', 'B'), ('B', 'A'), ('B', 'A'), ('A', 'B')])
        self.assertFalse(definition.SPEC.product_diff)
        self.assertFalse(definition.SPEC.required_regression_tests)

    def test_candidate_binary_or_receipt_must_be_identical(self):
        base = dict(binary=REPO/'a', baseline_binary=REPO/'a',
                    build_binding=REPO/'receipt', baseline_build_binding=REPO/'receipt')
        for key in ('binary', 'build_binding'):
            with self.subTest(key=key):
                with self.assertRaisesRegex(RuntimeError, 'same accepted binary'):
                    definition.runner.verify_candidate(SimpleNamespace(**dict(base, **{key:REPO/'different'})))
        with patch.object(definition, 'file_sha', return_value='0'*64):
            with self.assertRaisesRegex(RuntimeError, 'accepted A binary differs'):
                definition.runner.verify_candidate(SimpleNamespace(**base))

    def test_archived_gate_is_explicit_and_requires_all_restoration_flags(self):
        gate = dict(protocol='iotdbfs-fixed-fee-stage-summary/1', tier='n8', query_records=240,
                    baseline_sha256=definition.BASELINE_SHA,
                    product_sha256=definition.ARCHIVED_CANDIDATE_SHA,
                    **{key:True for key in definition.FLAGS})
        definition.runner.validate_n8_gate(gate, definition.BASELINE_SHA, definition.BASELINE_SHA)
        for key in definition.FLAGS:
            with self.assertRaises(RuntimeError):
                definition.runner.validate_n8_gate(dict(gate, **{key:False}),
                                                 definition.BASELINE_SHA, definition.BASELINE_SHA)
        for key, value in [('protocol', definition.PROTOCOL), ('query_records', 239),
                           ('baseline_sha256', '0'*64), ('product_sha256', definition.BASELINE_SHA)]:
            with self.assertRaises(RuntimeError):
                definition.runner.validate_n8_gate(dict(gate, **{key:value}),
                                                 definition.BASELINE_SHA, definition.BASELINE_SHA)


if __name__ == '__main__':
    unittest.main()
