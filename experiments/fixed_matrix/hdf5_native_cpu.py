"""Fixed native-HDF5 CPU A/B; independent N8/N128, 480 records total."""
from .hdf5_native_ab import NativeABRunner, ProtocolSpec

PROTOCOL='iotdbfs-fixed-hdf5-native-cpu/1'
BASELINE_SHA='f1b896244f2a7cbb1abc43b7f08c1386178e2cd9f59755b4ee06c11bbd843087'
PRODUCT_DIFF=frozenset({
    'src/formats/hdf5/Hdf5SourceDriver.cpp',
    'src/formats/hdf5/Hdf5FormatQueryAdapter.cpp',
})
REQUIRED_REGRESSION_TESTS=(
    ('Hdf5SourceDriverTest','NativeSessionWarmHitRetainsHandlesAndChunkWithoutSourceReads'),
    ('Hdf5SourceDriverTest','NativeSessionRetainsNewLogicalDemandAcrossWarmReturns'),
    ('Hdf5SourceDriverTest','NativeSessionPreleaseCancellationLeavesCachedEntryReusable'),
    ('Hdf5SourceDriverTest','NativeSessionMutableCancellationPreservesColdAndWarmPollSequence'),
)
SPEC=ProtocolSpec(
    protocol=PROTOCOL,module=__name__ if __name__!='__main__' else 'experiments.fixed_matrix.hdf5_native_cpu',
    description=__doc__,baseline_sha=BASELINE_SHA,product_diff=PRODUCT_DIFF,
    required_regression_tests=REQUIRED_REGRESSION_TESTS,
    extra_closure=(
        'experiments/fixed_matrix/tests/test_hdf5_native_cpu.py',
        'experiments/cross_version/hdf5_native_cpu/run.sh',
        'experiments/cross_version/hdf5_native_cpu/README.md',
    ),
)
runner=NativeABRunner(SPEC)

if __name__=='__main__':runner.main()
