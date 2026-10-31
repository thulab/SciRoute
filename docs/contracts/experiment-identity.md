# Experiment identity contract

An experiment compares a frozen query over a complete, independently published
corpus. A query subset never becomes a smaller corpus. The identity tools use
the Python standard library and operate on files only: they do not ingest data,
build an index, mount a filesystem, query IoTDB, change a service, or create an
account, database or license.

## Three identities

| Document | Frozen content |
|---|---|
| Corpus | Complete logical-file/history inventory, source sizes/hashes, generation parameters, native unit metadata/oracles and explicit publication evidence |
| Case | Corpus, exact candidate scope, version selection, predicate, operation, ROI, output contract and native expected matches |
| Run | Case, arm, implementation hashes/engine, clock, cache, environment and replication protocol |

Each document has its own versioned protocol and content-derived ID. Registry
registration checks its identity and references. A filename, directory nickname,
old result label, or equal K cannot substitute for those documents.

Use one authoritative registry for a comparison series. Within it, identical
complete corpus variables cannot be rebound to another publication, and
identical case variables cannot silently select another corpus. Registrations
are immutable and idempotent. A separate scratch registry is not evidence that
two equally labelled runs share a corpus; their content-derived IDs must agree.

N counts all independently published logical files in the corpus. V describes
their stored scientific histories; a selected version window is recorded in the
case. S is derived from the full raw-byte inventory, with its distribution;
D is derived from each source's native units. They are never copied from a
convenient output label. A four-bucket query selecting 128 paths from a published
512-file corpus is **N512/B4/C128**. It is not an independent N128 experiment.
The case separately records selected paths and selected file versions.

| Variable | Meaning and source |
|---|---|
| `N` | Full independently published logical-file population; invariant under query subsetting |
| `V_stored`, `version_counts` | Stored scientific ordinals per logical file; irregular histories retain per-path counts |
| `C_paths` | Distinct logical paths selected by the case, before the unit predicate |
| `C_versions` | Exact file versions in that candidate domain after the version selector, before the unit predicate |
| `B` | Number of explicitly named complete buckets; not a replacement for N or C |
| `K_files` | Distinct logical files with at least one matching unit |
| `K_file_versions` | Distinct logical-file/ordinal pairs with a matching unit |
| `K_units` | Matching GRIB messages or HDF5 datasets; multiple matches in one file are allowed |
| `D`, `D_histogram` | Native units per file version, including the distribution when heterogeneous |
| `S` | Encoded source bytes per file version: count, min/max, optional constant size, histogram and summed logical-version bytes |

S is neither decoded array size nor ROI/output bytes, and its sum is not a
measurement of deduplicated database storage. Histogram counts cover the whole
inventory. A full N512 query has C_paths=512; its display can be `N512` or
`N512/C512` without implying a different population. For stored V8 with a
two-version window, a complete N512 candidate domain has C_versions=1024 even
though C_paths remains 512.

## Import existing evidence

Run the CLI from the `IoTDB-FS-C` repository root:

```sh
python3 -m experiments.identity import-corpus \
  --registry experiments/cross_version/work/identity-registry \
  --format multi-file --manifest PATH/manifest.json \
  --expected-sha256 ACCEPTED_MANIFEST_BYTE_SHA256 --publication publication.json
```

Use `--format route3` for `iotdbfs-route3-hdf5-data/1` manifests. These importers
retain every file and generation ordinal. They reject partial histories,
inconsistent counts, changed fingerprints, unsafe identities and contradictory
bindings. The multi-file importer checks both the accepted document-byte hash
and its original internal canonical fingerprint. Route3 imports retain the
declared complete backing N and all recorded ROI oracles.

`publication.json` is explicit evidence from an accepted publication receipt:

```json
{
  "receipt_sha256": "<SHA-256 of the accepted receipt>",
  "namespace": "<actual recorded owner namespace or prefix>",
  "bindings": [
    {
      "logical_file": "raw/b00/file.grib",
      "ordinal": 1,
      "path": "/actual/published/path/file.grib",
      "version": 1790000000123,
      "bytes": 33484,
      "sha256": "<raw source SHA-256>"
    }
  ]
}
```

The example is a schema illustration, not publication evidence. Every complete
inventory member requires exactly one matching binding. Ordinal 1 means the
first raw generation, **not** database version 1. The importer never invents a
head, incarnation, database version, source hash or missing native mask hash.
Actual recorded head information may be retained in the publication evidence.

Missing publication, source hashes or critical generation evidence produces an
unbound registry record rather than a corpus eligible for strict comparison.
Contradictory evidence fails. Importing a manifest checks document evidence; it
does not claim a fresh read or hash of the raw files. `source_evidence.raw_files`
contains portable relative locations. The raw root is supplied separately:

```sh
python3 -m experiments.identity verify-inputs --registry REGISTRY \
  --corpus-id CORPUS_ID --raw-root EXISTING_RAW_ROOT
```

This command reads existing raw files and hashes their bytes. Its verification
receipt is separate from query timing and warms file-system pages.

## Freeze a case and run

```sh
python3 -m experiments.identity case --registry REGISTRY \
  --corpus-id CORPUS_ID --request request.json
python3 -m experiments.identity run --registry REGISTRY --document run.json
python3 -m experiments.identity compare --registry REGISTRY \
  --left BASELINE_RUN_ID --right CANDIDATE_RUN_ID
python3 -m experiments.identity show --registry REGISTRY --kind case --id CASE_ID
```

Case requests explicitly contain `scope`, `selection`, `predicate`, `operation`,
`roi`, `output` and `expected_K`. Scope lists sorted logical-file identities;
bucket scope also lists complete buckets and its selection basis. An `all`
scope must retain the entire corpus. Version selection is `latest`, `all`,
`exact`, or a contiguous `between` ordinal window. Actual database bindings
remain distinct from these scientific ordinals.

For example, this request describes an illustrative two-file 2×2 GRIB corpus
with exactly one matching message. Replace the file identities and declared K
with evidence from the actual registered corpus; the CLI rejects disagreement:

```json
{
  "scope": {
    "kind": "all",
    "logical_files": ["raw/f0.grib", "raw/f1.grib"],
    "buckets": [],
    "selection_basis": "complete independently published population"
  },
  "selection": {"kind": "latest"},
  "predicate": {"level": 850},
  "operation": "discover-values",
  "roi": {"kind": "whole", "origin": [0, 0], "shape": [2, 2]},
  "output": {"projection": ["value"], "dtype": "float32"},
  "expected_K": {"K_files": 1, "K_file_versions": 1, "K_units": 1}
}
```

Version selector forms are `{"kind":"latest"}`, `{"kind":"all"}`,
`{"kind":"exact","ordinals":[2]}` and
`{"kind":"between","ordinals":[2,3]}`. The latter explicitly lists a
contiguous scientific window; every selected path must contain those ordinals.
It does not turn 2 or 3 into a database timestamp, and cannot be applied to a
stored-V1 corpus.

ROI has `kind` (`whole` or `window`), `origin` and `shape`. `whole` must equal
the native complete geometry; a whole-unit oracle cannot certify a smaller ROI.
Output has an explicit projection and dtype. `discover` returns
`{"projection":["locators"],"dtype":"none"}` and does not require a value
oracle. `discover-values` requires a value projection and Float32/Float64 output;
requesting a mask additionally requires a recorded native mask hash. Thus old
N128 HDF5 manifests without mask hashes cannot certify a value-plus-mask case.
The tool preserves that limitation instead of generating an all-valid mask.

An unsealed run document contains exactly `case_id`, `execution`, `arm`,
`implementation`, `clock`, `cache`, `environment` and `replication`.
Implementation records binary SHA-256, runner SHA-256 and engine. The CLI also
accepts an already sealed run document. Registration creates identity evidence;
it does not execute a query or prove a performance result.

Strict A/B requires identical corpus, case, arm, clock, cache, environment and
replication protocol. Different implementation binaries are expected. Comparing
different operations, ROI/output contracts, source populations, history windows
or timer boundaries fails. Native worker-internal timing and client IPC-to-owned
array timing must remain explicit rather than being silently treated as one
clock convention.

## Historical evidence and execution gates

```sh
python3 -m experiments.identity map-history --registry REGISTRY --document mapping.json
python3 -m experiments.identity unbound --registry REGISTRY --document unbound.json
```

A historical mapping supplies `artifact: {path, sha256}`, a nonempty
`row_selector`, `case_id`, an `interpretation` and optional evidence artifact
references. Registry registration verifies local artifact hashes. Mapping adds
an external interpretation; it never edits original rows, relabels their N, or
turns an old result into a fresh execution. Insufficient evidence belongs in an
unbound record with a reason and provenance.

Future runs through the maintained multi-file runner must pass its strict
identity gate before publication, startup or timed work. Supply
`--identity-registry REGISTRY --identity-cases CASE_BUNDLE` together with the
existing `--reuse-preparation` and `--reuse-work` arguments. The bundle contains
exactly `grib` and `hdf5`, each with `discover` and `discover-values` case IDs.
This runner requires complete independent stored-V1/Latest corpora and full-unit
reads; its bundle cannot silently accept route3 C128 subsets or a value-only
HDF case in place of its value-plus-mask output. Missing mask evidence therefore
blocks that slot before a connection or mount. `--prepare-only` retains the
existing data-generation and database-publication workflow; it does not collect
query measurements and is not an offline-only command.

Frozen historical
replay scripts keep their original request and clock behavior and require their
frozen implementations. Their legacy rows cannot enter strict A/B comparison
merely because a new registry exists. They require an independently justified
mapping and measurement evidence; an identity registration alone supplies no
new timing, oracle verification or service-restoration guarantee.

The [2026-10-20 migration summary](../../experiments/cross_version/work/experiment-identity-20261020/migration-summary.json)
records three existing corpora, seven query cases, five historical mappings and
two unbound records. They distinguish independent GRIB/HDF5 N128 from route3
HDF5 N512 and its N512/B4/C128 query scope. This was a file-only evidence
migration: original historical artifacts were not rewritten, and no scientific
data, performance queries or database operations were performed. Historical
run identities were not reconstructed from those mappings.

Offline protocol and runner-gate tests (synthetic JSON/byte fixtures only):

```sh
python3 -B -m unittest discover -s experiments/identity/tests -t . -p 'test_*.py'
```
