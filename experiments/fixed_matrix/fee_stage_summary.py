"""Borrowed immutable fee batches and stage-summary fixed HDF5 A/B."""
from .hdf5_native_ab import NativeABRunner, ProtocolSpec
from . import run
PROTOCOL='iotdbfs-fixed-fee-stage-summary/1'
BASELINE_SHA='f1b896244f2a7cbb1abc43b7f08c1386178e2cd9f59755b4ee06c11bbd843087'
PRODUCT_DIFF=frozenset({'include/iotdbfs/query/core/QueryMetadataBudget.h','src/IoTDBStructuredBlobBackend.cpp'})
REQUIRED_REGRESSION_TESTS=(('QueryMetadataStageSummary', 'KnownStagesBorrowBatchesAndCorrectInterleavedMaximums'), ('QueryMetadataStageSummary', 'BudgetPreflightFallsBackToExactSortedFailurePrefix'), ('QueryMetadataStageSummary', 'MixedDomainsChooseFirstFailureAndPreserveZeroAndBinaryIdentities'), ('QueryMetadataStageSummary', 'CollectorsPresentAtActivationAndInstalledLaterKeepAllEvidence'), ('QueryMetadataStageSummary', 'RequiredCollectorFailureAfterBorrowPreservesChargeAndCapturePrefix'), ('QueryMetadataStageSummary', 'OptionalCollectorExitStillForwardsToOuterAndDoesNotPoisonRequest'), ('QueryMetadataStageSummary', 'PreexistingCollectorAndTemporaryCollectorBothPermanentlyExitAggregation'), ('QueryMetadataStageSummary', 'PrincipalPlanRearmRequiresIdenticalAlreadyBorrowedPrefix'), ('QueryMetadataStageSummary', 'UnknownOrderCrossPlanAndLargeOverlayExitWithoutLosingBorrowedMaximums'), ('QueryMetadataStageSummary', 'QuotaDeclinesAndPlanBoundsDoNotChangeOrdinaryReplay'), ('QueryMetadataStageSummary', 'BorrowedPinsAndQuotaSurviveCacheRetirementUntilLastRequest'), ('QueryMetadataStageSummary', 'SummaryRetainsNoIdentityLengthDependentStorageAndReleasesLastPin'), ('QueryMetadataStageSummary', 'MemoCapacityAndConcurrentLedgersDoNotExemptUnknownBatches'), ('QueryMetadataStageSummary', 'SeededDifferentialReplayMatchesUnarmedLedgerAfterEveryAction'))
SPEC=ProtocolSpec(protocol=PROTOCOL,module='experiments.fixed_matrix.fee_stage_summary',description=__doc__,
    baseline_sha=BASELINE_SHA,product_diff=PRODUCT_DIFF,required_regression_tests=REQUIRED_REGRESSION_TESTS,
    extra_closure=('experiments/fixed_matrix/tests/test_fee_stage_summary.py',
        'experiments/cross_version/fee_stage_summary/run.sh','experiments/cross_version/fee_stage_summary/README.md'))

class FeeStageSummaryRunner(NativeABRunner):
    def validate_row(self,row,group,configuration):
        super().validate_row(row,group,configuration)
        if configuration=='B' and row.get('phase')=='measurement':
            c=row['discovery_call']['server_profile']['counters']
            wanted={'hot_query.fee_summary_stages':4,'hot_query.fee_replay_entries':0,
                    'hot_query.fee_summary_fallbacks':0,'hot_query.fee_summary_collector_exits':0}
            run.need(all(type(c.get(k)) is int and c[k]==v for k,v in wanted.items()),
                     'formal candidate must use four borrowed summary stages without fallback or collector')
            run.need(type(c.get('hot_query.fee_summary_entries_skipped')) is int and
                     c['hot_query.fee_summary_entries_skipped']>0,
                     'formal candidate lacks explicit borrowed fee evidence')
        return row
runner=FeeStageSummaryRunner(SPEC)
if __name__=='__main__':runner.main()
