"""Publication /3 acceptance checks without GEOS, native libraries or a service."""
import copy
import argparse
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest

import geos_publication_profile_validate as validator


def metric(calls=0, nanoseconds=0):
    return {'calls': calls, 'failures': 0, 'nanoseconds': nanoseconds}


class ConstructionProfileTests(unittest.TestCase):
    def setUp(self):
        self.chunks, self.checkpoints = 8, 1
        self.payload = {
            'query_submit': metric(1, 20), 'tablet': metric(8, 100),
            'nonquery': metric(1, 20), 'tablet_build': metric(8, 5),
            'object_encode': metric(8, 10), 'session_acquire': metric(10, 5),
            'sdk_insert': metric(8, 80), 'details': {
                'payload_source_read': metric(8, 5),
                'payload_adapter_consume': metric(8, 5),
                'mutation_lock_wait': metric(1, 5)}}

    def validate(self):
        validator.validate_construction_profile(self.payload, self.chunks, self.checkpoints)

    def test_complete_batch_profile_passes_without_mutating_metrics(self):
        original = copy.deepcopy(self.payload)
        self.validate()
        self.assertEqual(self.payload, original)

    def test_single_chunk_checkpoint_profile_passes(self):
        self.checkpoints = 8
        self.payload['query_submit']['calls'] = 8
        self.payload['nonquery']['calls'] = 8
        self.payload['session_acquire']['calls'] = 24
        self.payload['details']['mutation_lock_wait']['calls'] = 8
        self.validate()

    def test_each_construction_count_must_match(self):
        for op in validator.CONSTRUCTION_OPERATIONS:
            with self.subTest(op=op):
                self.payload[op]['calls'] += 1
                with self.assertRaisesRegex(AssertionError, 'expected'):
                    self.validate()
                self.payload[op]['calls'] -= 1

    def test_each_detail_count_must_match(self):
        for label in validator.PAYLOAD_DETAILS:
            with self.subTest(label=label):
                self.payload['details'][label]['calls'] += 1
                with self.assertRaisesRegex(AssertionError, 'expected'):
                    self.validate()
                self.payload['details'][label]['calls'] -= 1

    def test_missing_construction_field_is_rejected(self):
        del self.payload['object_encode']
        with self.assertRaisesRegex(AssertionError, 'malformed'):
            self.validate()

    def test_missing_details_or_required_detail_is_rejected(self):
        del self.payload['details']['payload_adapter_consume']
        with self.assertRaisesRegex(AssertionError, 'lacks detail'):
            self.validate()
        del self.payload['details']
        with self.assertRaisesRegex(AssertionError, 'lacks details'):
            self.validate()

    def test_failed_construction_or_detail_is_rejected(self):
        for data in (self.payload['sdk_insert'], self.payload['details']['payload_source_read']):
            data['failures'] = 1
            with self.assertRaisesRegex(AssertionError, 'failed'):
                self.validate()
            data['failures'] = 0

    def test_negative_float_or_boolean_metric_is_rejected(self):
        for value in (-1, 0.5, True):
            with self.subTest(value=value):
                self.payload['tablet_build']['nanoseconds'] = value
                with self.assertRaisesRegex(AssertionError, 'malformed'):
                    self.validate()

    def test_sdk_timer_must_fit_inside_outer_tablet_timer(self):
        self.payload['sdk_insert']['nanoseconds'] = 101
        with self.assertRaisesRegex(AssertionError, 'exceeds containing'):
            self.validate()

    def test_session_acquisition_excludes_close_and_fetch(self):
        self.payload['query_close'] = metric(1, 5)
        self.payload['query_fetch'] = metric(2, 5)
        self.validate()
        self.payload['session_acquire']['calls'] += 3
        with self.assertRaisesRegex(AssertionError, 'expected'):
            self.validate()

    def test_unknown_detail_is_validated_too(self):
        self.payload['details']['future_diagnostic'] = metric(1, 5)
        self.validate()
        self.payload['details']['future_diagnostic']['failures'] = 1
        with self.assertRaisesRegex(AssertionError, 'failed'):
            self.validate()

    def test_invalid_checkpoint_count_is_rejected(self):
        for count in (-1, 9, True):
            with self.subTest(count=count):
                with self.assertRaisesRegex(AssertionError, 'invalid chunk/checkpoint'):
                    validator.validate_construction_profile(self.payload, 8, count)

    def test_new_and_historical_protocol_modes_are_separate(self):
        old = [{'protocol': 'iotdbfs-publication-profile/2'}]
        new = [{'protocol': 'iotdbfs-publication-profile/3'}]
        validator.validate_profile_protocol(old, False)
        validator.validate_profile_protocol(new, True)
        retired = [{'protocol': 'iotdbfs-publication-profile/1'}]
        for records, mode in ((old, True), (new, False), (old + new, True),
                              (retired, False), ({}, False)):
            with self.subTest(records=records, mode=mode):
                with self.assertRaisesRegex(AssertionError, 'protocol'):
                    validator.validate_profile_protocol(records if isinstance(records, list)
                                                        else [records], mode)

    def test_construction_mode_requires_explicit_batch_configuration(self):
        with self.assertRaisesRegex(AssertionError, 'requires explicit'):
            validator.main(argparse.Namespace(construction_profile=True,
                                               manifest_batch_bytes=None))

    def synthetic_trace_result(self, transfer_bytes):
        # Only JSON declarations are generated: no dataset, Arrow or server.
        chunks_by_source = [427] * 4 + [426] * 12
        deficit = 6820 * 1048576 - 7142773610
        sources, observations, records = [], [], []
        for index, original_chunks in enumerate(chunks_by_source):
            size = original_chunks * 1048576 - deficit // 16 - (index < deficit % 16)
            chunks = (size + transfer_bytes - 1) // transfer_bytes
            source = {'id': str(index), 'raw': '/synthetic/' + str(index),
                      'logical_path': '/published/' + str(index), 'bytes': size,
                      'published_bytes': size, 'sha256': 'fake-source-digest',
                      'version': index + 1, 'publish_ns': 1000}
            sources.append(source)
            for case in ('point', 'roi', 'multichunk', 'levels'):
                observations.append({'source': str(index), 'case': case,
                    'native': {'rows': 1, 'digest': 'fake-values'},
                    'iotdb': {'rows': 1, 'digest': 'fake-values'}})
            checkpoints = (size + 8388607) // 8388608
            payload = {op: metric() for op in validator.OPERATIONS}
            payload.update(wall_nanoseconds=1000,
                query_submit=metric(checkpoints, 20), query_close=metric(checkpoints, 5),
                query_fetch=metric(checkpoints * 2, 5), tablet=metric(chunks, 100),
                nonquery=metric(checkpoints, 20), tablet_build=metric(chunks, 5),
                object_encode=metric(chunks, 10), sdk_insert=metric(chunks, 80),
                session_acquire=metric(chunks + checkpoints * 2, 5), details={
                    'payload_source_read': metric(chunks, 5),
                    'payload_adapter_consume': metric(chunks, 5),
                    'mutation_lock_wait': metric(checkpoints, 5)})
            records.append({'protocol': 'iotdbfs-publication-profile/3', 'ok': True,
                'path': source['logical_path'], 'version': source['version'],
                'source_bytes': size, 'profile': {'stages': {'payload': payload}}})
            empty = {op: metric() for op in validator.OPERATIONS +
                     validator.CONSTRUCTION_OPERATIONS}
            empty.update(wall_nanoseconds=0, details={})
            records.append({'protocol': 'iotdbfs-publication-profile/3', 'ok': True,
                'profile': {'stages': {'begin': empty}}})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pilot = {'complete': True, 'sources': sources, 'observations': observations}
            (root / 'pilot.json').write_text(json.dumps(pilot))
            (root / 'trace.jsonl').write_text('\n'.join(json.dumps(r) for r in records))
            args = argparse.Namespace(construction_profile=True, manifest_batch_bytes=8388608,
                baseline_pilot=root / 'pilot.json', pilot=root / 'pilot.json',
                trace=root / 'trace.jsonl', payload_stage='payload', identity_field='path',
                chunk_bytes=transfer_bytes)
            captured = io.StringIO()
            with contextlib.redirect_stdout(captured):
                validator.main(args)
            result = json.loads(captured.getvalue())
        return result

    def test_full_synthetic_trace_aggregates_diagnostics_without_adding_api_time(self):
        result = self.synthetic_trace_result(1048576)
        self.assertEqual(result['protocol'], 'iotdbfs-geos-publication-profile-validation/3')
        self.assertEqual(result['payload_chunks'], 6820)
        aggregate = result['stage_aggregates']['payload']
        self.assertEqual(aggregate['tablet_build']['calls'], 6820)
        self.assertEqual(aggregate['details']['payload_source_read']['calls'], 6820)
        self.assertEqual(aggregate['tablet']['nanoseconds'], 1600)
        self.assertEqual(aggregate['sdk_insert']['nanoseconds'], 1280)
        self.assertIn('do not add', ' '.join(result['semantics']))

    def test_larger_transfers_change_chunk_costs_and_preserve_manifest_costs(self):
        for transfer_bytes, total in ((2097152, 3412), (4194304, 1712), (8388608, 864)):
            with self.subTest(transfer_bytes=transfer_bytes):
                result = self.synthetic_trace_result(transfer_bytes)
                self.assertEqual(result['transfer_bytes'], transfer_bytes)
                self.assertEqual(result['payload_chunks'], total)
                self.assertEqual(result['manifest_checkpoints'], 864)
                payload = result['stage_aggregates']['payload']
                for op in ('tablet', 'tablet_build', 'object_encode', 'sdk_insert'):
                    self.assertEqual(payload[op]['calls'], total)
                for op in ('query_submit', 'nonquery', 'query_close'):
                    self.assertEqual(payload[op]['calls'], 864)
                self.assertEqual(payload['query_fetch']['calls'], 1728)
                self.assertEqual(payload['session_acquire']['calls'], total + 1728)
                for label in ('payload_source_read', 'payload_adapter_consume'):
                    self.assertEqual(payload['details'][label]['calls'], total)
                self.assertEqual(payload['details']['mutation_lock_wait']['calls'], 864)

    def test_tail_transfer_and_checkpoint_counts_use_each_file_size(self):
        for transfer_bytes, expected_chunks in ((2097152, 5), (4194304, 3), (8388608, 2)):
            with self.subTest(transfer_bytes=transfer_bytes):
                chunks, checkpoints, counts = validator.expected_payload_counts(
                    8388608 + 17, transfer_bytes, 8388608)
                self.assertEqual((chunks, checkpoints), (expected_chunks, 2))
                self.assertEqual(counts, {'tablet': expected_chunks, 'nonquery': 2,
                    'query_submit': 2, 'query_close': 2, 'query_fetch': 4})
        chunks, checkpoints, counts = validator.expected_payload_counts(0, 8388608, 8388608)
        self.assertEqual((chunks, checkpoints), (0, 0))
        self.assertTrue(all(value == 0 for value in counts.values()))

    def test_invalid_transfer_budget_is_rejected_before_reading_evidence(self):
        for value in (0, -1, True, 0.5, 3145728, 16777216):
            with self.subTest(value=value):
                with self.assertRaisesRegex(AssertionError, 'transfer size'):
                    validator.main(argparse.Namespace(construction_profile=True,
                        manifest_batch_bytes=8388608, chunk_bytes=value))
        for chunk, checkpoint in ((2097152, 1048576), (8388608, 4194304),
                                  (1048576, True), (1048576, 16777216)):
            with self.subTest(chunk=chunk, checkpoint=checkpoint):
                with self.assertRaisesRegex(AssertionError, 'checkpoint/transfer'):
                    validator.validate_transfer_configuration(chunk, checkpoint)

    def test_historical_default_still_counts_two_manifest_reads_per_transfer(self):
        chunks, checkpoints, counts = validator.expected_payload_counts(1048577, 1048576, None)
        self.assertEqual((chunks, checkpoints), (2, 2))
        self.assertEqual(counts, {'tablet': 2, 'nonquery': 2, 'query_submit': 4,
                                  'query_close': 4, 'query_fetch': 8})


if __name__ == '__main__':
    unittest.main()
