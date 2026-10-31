# Route-3 HDF5 native reader

`route3_hdf5_native.cpp` is an experiment-only C ABI used by `../route3_native.py`.
It implements generic version preselection, an immutable SQLite metadata index,
bounded HDF5 file/dataset handles, and H5Dread into client-owned Float32 and uint8
buffers. It does not call IoTDB or control services.

From the remote repository root, build into a **new** output directory with the
same pinned HDF5 prefix as the existing single-file environment. The server also
provides the SQLite development headers/library and `nlohmann/json.hpp`.

```sh
TASK_HDF5_PREFIX=/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/single_file/work/session-20261012/hdf5-prefix
TASK_NATIVE_OUT=/absolute/new-native-build
mkdir "$TASK_NATIVE_OUT"
g++ -std=c++17 -O3 -DNDEBUG -fPIC -shared \
  experiments/cross_version/hdf5_route3/native/route3_hdf5_native.cpp \
  -I"$TASK_HDF5_PREFIX/include" -L"$TASK_HDF5_PREFIX/lib" \
  -Wl,-rpath,"$TASK_HDF5_PREFIX/lib" -lhdf5 -lsqlite3 \
  -o "$TASK_NATIVE_OUT/libroute3_hdf5_native.so"
HDF5_PLUGIN_PRELOAD=:: ROUTE3_NATIVE_LIBRARY="$TASK_NATIVE_OUT/libroute3_hdf5_native.so" \
  experiments/single_file/work/session-20261012/venv/bin/python \
  -m unittest experiments.cross_version.hdf5_route3.tests.test_route3_native
sha256sum "$TASK_NATIVE_OUT/libroute3_hdf5_native.so"
```

Freeze the actual source, compiler, linked-library, test-output and binary hashes
before using that build in a new benchmark. Pass its digest explicitly to the
runner's `--native-sha`. Do not overwrite a build used by an existing result.

The 2026-10-18 R2 experiment uses HDF5 1.14.4 and a shared-library SHA of
`d834c4c7a2a0a57f99eae39fcfbdd1d21b95990fbd6ad4f5a789d6f94c15c50a`.
Its exact compiler command and eight passing native tests are retained in
`experiments/cross_version/work/route3-hdf5-20261018/native-build-r2/verification.json`. A different build
is a new experimental identity even if its functional tests pass.

Index construction and context setup are outside the complete-query timer and
must be reported separately. The two scans are not equivalent: FS-scan inspects
all dataset attributes in the caller-selected sources; FS-index uses a general
persistent predicate/version index and opens only matched values. Neither uses
known-answer filenames or oracle values to discover matches.
