"""Receipt regressions; these tests do not connect to a database."""
import json
import unittest

from experiments.cross_version.hdf5_route3.native_filesystem.verify import functional_result, gate_complete


class CompletionTest(unittest.TestCase):
    def report(self, **changes):
        return dict(functional_complete=True, original_service_restored=True,
                    controller_restoration_verified=True, resources_restored=True,
                    scientific_queries=72, timing_probe_complete=True, **changes)

    def test_complete_timing_requires_all_queries(self):
        report = self.report()
        self.assertTrue(gate_complete(report, sampler_ok=True, timing_required=True))
        for count in (0, 36, 71):
            report['scientific_queries'] = count
            self.assertFalse(gate_complete(report, sampler_ok=True, timing_required=True))

    def test_restoration_does_not_hide_failure(self):
        report = self.report(failure={'message': 'timing failed'})
        self.assertFalse(gate_complete(report, sampler_ok=True, timing_required=True))

    def test_timing_completion_is_required(self):
        report = self.report()
        del report['timing_probe_complete']
        self.assertFalse(gate_complete(report, sampler_ok=True, timing_required=True))

    def test_sampler_and_each_restoration_flag_are_required(self):
        self.assertFalse(gate_complete(self.report(), sampler_ok=False, timing_required=True))
        for key in ('functional_complete', 'original_service_restored',
                    'controller_restoration_verified', 'resources_restored'):
            report = self.report(); report[key] = False
            self.assertFalse(gate_complete(report, sampler_ok=True, timing_required=True))

    def test_functional_only_has_zero_scientific_queries(self):
        report = self.report(); report['scientific_queries'] = 0
        del report['timing_probe_complete']
        self.assertTrue(gate_complete(report, sampler_ok=True, timing_required=False))
        report['scientific_queries'] = 1
        self.assertFalse(gate_complete(report, sampler_ok=True, timing_required=False))


class FunctionalReceiptTest(unittest.TestCase):
    def receipt(self):
        observations = {key: 0 for key in ('cold_execute_payload_calls',
            'warm_execute_payload_calls', 'cold_execute_sql', 'warm_execute_sql',
            'cold_execute_store_queries', 'warm_execute_store_queries',
            'unbound_execute_payload_calls', 'missing_execute_payload_calls',
            'replaced_execute_payload_calls')}
        observations.update(cold_filesystem_calls=16, warm_filesystem_calls=16)
        return dict(completed=True, command='native_file_functional', prefix='fixture',
            mode='native-hdf5', data_backend='managed-filesystem', actual_backend=True,
            existing_database=True, real_hdf5_fixture=True, fixtures_preserved=True,
            cases=['case' + str(i) for i in range(8)], case_count=8, observations=observations,
            recovery={k: True for k in ('unbound_namespace', 'bound_namespace',
                'exact_record_preserved', 'locators_preserved')})

    def test_actual_completed_schema(self):
        receipt = self.receipt()
        self.assertEqual(functional_result(json.dumps(receipt), 'fixture'), receipt)

    def test_wrong_completion_key_is_rejected(self):
        receipt = self.receipt(); receipt['complete'] = receipt.pop('completed')
        with self.assertRaises(RuntimeError): functional_result(json.dumps(receipt), 'fixture')

    def test_database_fallback_is_rejected(self):
        for key in ('cold_execute_sql', 'warm_execute_store_queries', 'missing_execute_payload_calls'):
            receipt = self.receipt(); receipt['observations'][key] = 1
            with self.assertRaises(RuntimeError): functional_result(json.dumps(receipt), 'fixture')


if __name__ == '__main__':
    unittest.main()
