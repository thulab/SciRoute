# perf(query): reduce per-query discovery and value processing costs

## Summary

Repeated GRIB and HDF5 queries currently spend substantial time rebuilding request state, selecting candidates, replaying metadata charges, preparing value requests, and delivering results. Increasing the number of files does not automatically give IoTDB-FS an advantage over an indexed filesystem baseline.

This PR focuses on **reducing the cost of each query** by reusing immutable state and removing repeated work. The implementation preserves authorization, version freshness, request budgets, collector observations, and result ownership. Current evidence supports specific reductions in work and limited warm-query improvements; broader scalability and end-to-end advantages remain to be established.

## Scope

This PR includes the complete 23-commit series originally starting at `1479ce15412960ffbaf6e03204e8a20ee5c9b446` and ending at `ee06e44a5cfd86cb86bdec29a3541594b0ee037d`, rebased onto `master` (`74f37159104d98e6db6b0f495bc5805c13b61097`). It covers the benchmark and correctness foundations as well as the later discovery, fee-accounting, and request-template optimizations.

## Completed work

- **Version and content reuse foundations.** Introduce a persistent copy-on-write version directory, immutable content identities, and content sharing. These provide the identity and freshness checks required for safe reuse across requests and versions.
- **HDF5 discovery foundations.** Refactor dataset discovery around sealed substructure artifacts, compile typed attribute predicates once, and combine bounded manifest revalidation with marker verification. The final discovery path uses sealed artifacts; the historical secondary-index mode switch is no longer exposed. Pin the dependency lock to thread-safe HDF5 1.14.4.
- **Shared discovery reuse.** Reuse frozen candidate domains, principal-specific permission proofs, and immutable attribute indexes across GRIB and HDF5. Add compiled selector handles to avoid repeatedly transmitting, normalizing, and compiling the same path scope or rebuilding long cache keys. Share immutable candidate manifest vectors and use binary search for exact membership in immutable path scopes. Add public `format_catalog` discovery for GRIB that returns exact unit locators without building an Arrow result stream or publishing a FUSE result route.
- **Metadata fee summaries.** Borrow existing immutable fee batches and reuse stage summaries instead of replaying every metadata identity on each eligible warm discovery. Preserve logical charges, request-local accounting, budget failure prefixes, collector fallback, and actual owner admission. Reservations remain charged while borrowed state is retained.
- **Format-specific reuse.** Extend GRIB reuse across parsed layouts, encoded messages, and decoded fields, including content sharing between eligible versions. Correct BinaryView encoded-message cache handling, repeated layout reads, and cross-version identity handling. Add the `native-hdf5` execution option with verified filesystem bindings, session/file/dataset reuse, and batched value requests. Batch processing retains item order, cumulative limits, and rollback behavior.
- **Public query request templates — Stage A.** Add `query_template_prepare`, `query_template_execute`, and `query_template_release` for immutable single and batch declarations. Reuse frontend selector state and predicate ASTs while retaining the full planner and fresh authorization/admission checks. Enforce UID isolation, TTLs, shared quotas, conditional release, and lifetime accounting through the last borrowing route or request.
- **Reproducible validation.** Add single-file HDF5 experiments, a native ecCodes benchmark reader, multi-file corpus management, cross-version publication/acceptance tools, and semantic regressions. Add fixed-corpus GRIB/HDF5 comparisons with separate discovery (`d`), value (`q`), and complete query (`T=d+q`) clocks. Retain warmups, slow blocks, preparation/release costs, and resource observations. Balance CPU/resource allocation and add independent-client, process-lifetime, and reversed-order controls for investigating slow final blocks.

## Validation and observed results

Before this rebase, the final Stage A candidate passed a clean Linux Release build, module dependency checks, **1,913 unit tests**, and **301 focused tests**. All 129 required semantic regressions ran in both suites. Coverage includes permission changes, Latest/exact versions, budget failures, collectors, batch rollback, shared quotas, and retained request lifetimes.

Real-backend regression probes and an independent N8 qualification passed before the main experiment. The main experiment used the existing sealed GRIB/HDF5 N8 and N128 groups: **960 records, including 640 formal observations**, across 16 mounts with independent client processes. Results, charges, profiles, source identities, and service restoration were independently audited; no slow observations were excluded.

The following results isolate **ordinary requests versus Stage A templates within the same candidate binary**. They are not a measurement of the entire PR against the target branch.

| Workload | Mean q reduction | Mean complete T reduction |
| --- | ---: | ---: |
| GRIB N8 | 7.12% | 5.82% |
| GRIB N128 | 6.02% | 5.04% |
| HDF5 N8 | 4.41% | 3.95% |
| HDF5 N128 | 5.22% | 4.42% |

The mechanism checks confirm that Stage A removes the repeated **frontend** selector compilation counted by the socket profiler. Internal adapter checks and construction remain. Discovery summaries also eliminate the measured large prepared-fee replay loops while retaining the same logical charges.

These gains have limits: FS-index-normalized block comparisons are not uniformly better, and the measured 15-query GRIB N128 lifecycle, including template preparation and release, is approximately unchanged. An earlier fee-summary comparison improved discovery while complete T regressed in that measurement window. The experiments therefore support individual mechanisms rather than a cumulative end-to-end speedup claim. FS-index remains faster for complete queries in the latest measured workloads.

## Integration checks

All 23 original commits were retained during the rebase onto `master`. The only conflict was in repository guidance (`AGENTS.md`); no product or build-code conflict occurred. All 528 files in the frozen candidate build/source manifest still match byte for byte after the rebase. The module dependency check and the offline request-template/control regressions were rerun successfully. The recorded Linux build and 1,913/301 test results above remain the frozen-candidate evidence; a new full post-rebase CI build has not been run.

## Remaining validation

- Run full CI on the rebased branch; source equality with the frozen candidate does not substitute for a fresh CI run.
- Separate namespace size N from explicit scope P and candidate count C. Existing scale groups vary all three together and cannot establish a pure N complexity trend.
- Validate larger working sets and different version counts, message/dataset counts, selectivities, output sizes, principals, and cache pressure.
- Measure cold construction, invalidation, preparation/release, concurrency, and longer reuse lifetimes alongside warm-query latency. Retain raw and paired results and quantify observer overhead.
- Compare complete update cycles: ingestion/publication, the first fresh query after an update, and subsequent queries. Include an incrementally maintained FS-index baseline before claiming a write-maintenance advantage.

## Follow-up optimization work

- Design and validate Stage B planning reuse or trusted unit hints. Stage A does not reuse complete value plans; GRIB value requests still reselect messages within each matched source.
- Profile and reduce repeated materialization, encoding, control, and owned-result delivery costs.
- Evaluate incremental domain/index maintenance across head changes. Current persistent directory updates are local, but new heads can still require candidate-domain and global-index reconstruction.

The original frozen prepared-plan experiment was removed from the production path. GRIB rendering/geometry reuse, the first batch-parsing reuse attempt, and HDF5 CPU micro-optimizations were also reverted; their experiment records are retained, and they are not included as completed product optimizations.

The existing reports document these boundaries and the unsuccessful or reverted experiments, including `docs/reports/public-q-template-stage-a-20261024.md`, `docs/reports/grib-fee-summary-20261024.md`, and `docs/reports/n-complexity-and-scenario-advantages-20261024.md`.

## Repository content and archived evidence

Keep current product code, regression tests, benchmark drivers/specifications, handwritten conclusions, and small acceptance receipts in this PR. Remove generated historical run output, two retired test/probe source copies, and five superseded notes from tracking; preserve their original bytes in a SHA-256-verified archive and the pre-cleanup Git commit. See `docs/reports/pr7-content-curation-20261024.md` for the line-count breakdown, archive locations, and verification status. These changes do not alter product behavior or remove the active semantic test suites.
