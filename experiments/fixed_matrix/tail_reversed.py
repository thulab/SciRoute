"""Diagnostic endpoint reversal: BA/AB/AB/BA with actual accepted A and summary B."""
from . import hdf5_native_ab, run
from .fee_stage_summary import SPEC as PRODUCT_SPEC, runner as product_runner
from .fee_stage_summary import FeeStageSummaryRunner
from .hdf5_native_ab import NativeABRunner, ProtocolSpec, CONFIGS
from .tail_controls import TailControlRunner, ARCHIVED_N8_REPORT_SHA, ARCHIVED_N8_QUERIES_SHA
from experiments.identity.registry import file_sha

PROTOCOL = 'iotdbfs-fixed-tail-reversed-diagnostic/1'
SCHEDULE = 'BA/AB/AB/BA; paired alternating arms; parity repeat+campaign+configuration'
SPEC = ProtocolSpec(protocol=PROTOCOL, module='experiments.fixed_matrix.tail_reversed',
    description=__doc__, baseline_sha=PRODUCT_SPEC.baseline_sha,
    product_diff=PRODUCT_SPEC.product_diff,
    required_regression_tests=PRODUCT_SPEC.required_regression_tests,
    extra_closure=('experiments/fixed_matrix/tests/test_tail_controls.py',
        'experiments/fixed_matrix/tests/test_tail_reversed.py',
        'experiments/cross_version/work/fs-index-tail-controls-20261023/run_reversed.py',
        'experiments/cross_version/work/fs-index-tail-controls-20261023/instrumentation/pid_cpu_sampler.py'))


class ReversedTailRunner(TailControlRunner, FeeStageSummaryRunner):
    def __init__(self):
        NativeABRunner.__init__(self, SPEC)

    def diagnostic_metadata(self):
        return dict(kind='actual-AB-reversed-endpoint',
            configuration_labels='A=accepted baseline; B=stage-summary candidate',
            schedule=SCHEDULE, optimization_claim=False)

    def configuration_order(self, campaign):
        return tuple(reversed(super().configuration_order(campaign)))

    def verify_candidate(self, args):
        return NativeABRunner.verify_candidate(self, args)

    def validate_n8_gate(self, gate, baseline_sha, candidate_sha):
        # Same actual binaries as the archived, fully restored N8 qualification.
        product_runner.validate_n8_gate(gate, baseline_sha, candidate_sha)

    def source_proof(self, args):
        run.need(args.tier == 'n128' and args.gate_report is not None and
                 file_sha(args.gate_report) == ARCHIVED_N8_REPORT_SHA and
                 file_sha(args.gate_report.parent/'queries.jsonl') == ARCHIVED_N8_QUERIES_SHA,
                 'pinned actual A/B qualification differs')
        proof = NativeABRunner.source_proof(self, args)
        proof['diagnostic_control'] = dict(self.diagnostic_metadata(),
            client_lifetime='one worker for all eight blocks',
            qualification='archived N8 of the exact same actual A/B binaries')
        return proof

    def collect(self, *args):
        original = hdf5_native_ab.build_run

        def build_run(*pos, **kw):
            kw['replication'] = dict(kw['replication'], schedule=SCHEDULE)
            return original(*pos, **kw)

        # Correct the inherited descriptive schedule in run identities. This
        # changes metadata only, outside all query clocks, with finally restore.
        hdf5_native_ab.build_run = build_run
        try:
            return super().collect(*args)
        finally:
            hdf5_native_ab.build_run = original

    def analyze(self, *args):
        result = NativeABRunner.analyze(self, *args)
        result.update(diagnostic_only=True, optimization_claim=False,
                      schedule=SCHEDULE)
        return result


runner = ReversedTailRunner()
if __name__ == '__main__':
    runner.main()
