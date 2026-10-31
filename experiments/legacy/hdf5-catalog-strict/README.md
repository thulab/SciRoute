# Historical HDF5 strict catalog probe

2026-10-24: the retired test/probe source copies are archived and ignored; they are not current build inputs. Original bytes remain locally and in the SHA-verified PR #7 archive at `/data_01/zzy/iotdb-fs-exp/document-archives/20261024-pr7-content-curation-6e63794/files.tar.gz` (manifest alongside). Existing frozen source bundles below remain the full reproduction inputs.

`dataset_catalog_probe.cpp` is preserved byte for byte from the previous formal
benchmark. It compares the removed strict SQL catalog discovery policy with the
sealed substructure catalog policy and therefore is not part of the current
formal build. Its old policy enum and constructor calls intentionally remain
historical source; do not compile it against the current backend API.

The complete matching implementation is already preserved in
[`source-r3.tar.gz`](../../cross_version/work/route3-global-hot-cache-20261018/source-r3.tar.gz)
(SHA-256 `3662b48ce6f01599a1868c8174476520ded674a4d2055f729fac92daaba484cb`).
The matching
[`source-r3-manifest.json`](../../cross_version/work/route3-global-hot-cache-20261018/source-r3-manifest.json)
has SHA-256
`b12c0f20aefffad996e628aa6d49feb9550224be1aba38087fae38b645462265`.
Reproduction requires that frozen implementation and its matching build and
experiment configuration. Existing experiment evidence under `work/` remains
unchanged; this directory does not introduce a compatibility alias or alter the
published storage format.

The current implementation shares one sealed catalog discovery path between
its native HDF5 and IoTDB-FS indexed execution modes. Publication verification,
namespace admission, authorization, request budgets, and the owner/legacy
filesystem storage configuration remain separate requirements.
