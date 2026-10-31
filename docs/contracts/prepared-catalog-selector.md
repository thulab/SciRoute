# Prepared catalog selector

The public query frontend can register an immutable selector template in the
daemon's shared hot-query pool. Owner-backed HDF5 dataset discovery and GRIB
format-unit discovery use this mechanism. A handle conveys no authorization
and stores no query result.

## Control protocol

For HDF5, `catalog_selector_prepare` accepts `request.sources`, optional
`format_id` (`hdf5`), optional `dataset_path`, and optional `max_sources` (default
1024). The sources schema is the ordinary dataset catalog selector schema,
excluding `file_predicate`. Preparation validates and compiles the static
declaration without discovering datasets or saving an authorization decision.

Success returns `kind: prepared_catalog_selector`, a random 48-character
lowercase hexadecimal `selector_id`, and `idle_ttl_ms: 300000`.

`dataset_catalog_by_selector` accepts `selector_id`, `predicate`, optional
`max_datasets` (default 1024), and optional `max_catalog_bytes` (the ordinary
catalog default). Its response is the existing `dataset_catalog` response.
The predicate must satisfy the existing complete, correctly typed, non-null
six-attribute HDF5 equality-AND grammar. Each call has its own result limits,
metadata ledger, actual socket principal, authorization, and final admission.
Static selector overrides are rejected; changing static selection requires a
new handle. Unsupported syntax can be submitted explicitly through the ordinary
dataset catalog endpoint.

For GRIB, preparation accepts `request.sources`, `format_id: grib`, a static
`subset` containing only `projection`, and optional `limits`. Sources must list
exactly the declared adapter and exclude byte passthrough and file predicates.
The Adapter validates metadata-only unit-catalog support. All seven QueryLimits
are fixed at preparation, using ordinary defaults for omitted fields. The
initial GRIB client uses the existing offset projection. Preparation does not
select candidates, prepare a query plan, or acquire an authorization grant.

`format_catalog_by_selector` accepts only `selector_id` and `predicate`. It
creates a fresh request from the static declaration and returns the ordinary
`format_catalog` response. The Adapter interprets the current predicate and
creates request-local posting hints using the frozen limits. The common
execution chain still selects at the current head, authorizes the full frozen
candidate set, obtains an execution lease, prepares matching sources, validates
locators, and performs final admission. HDF5 and format-unit handles reject the
other execution endpoint before accessing their explicitly typed payload.

`catalog_selector_release` accepts `selector_id` and returns
`kind: released_catalog_selector` with `released: true` or `false`.
Unknown, expired, and other-UID handles have the same execute error.

## Freshness and ownership

The registry belongs to one daemon. Each handle is UID-bound, and its backend
binding is tied to the original hot cache and namespace incarnation. Each
execution takes a fresh owner view. A cached internal reference is usable only
after the original cache atomically validates full head, cache identity,
payload kind, slot, generation, and resident membership, then takes a strong
execution pin and updates LRU.

Head changes or eviction cause a miss and use the original discovery path with
the compiled selector. Latest remains dynamic. A namespace/backend change
invalidates the binding. Old executions cannot replace a newer binding. Large
domains remain owned and charged by the original cache; a handle retains only
the template and a small reference.

## Resource lifecycle

The existing shared pool remains 64 MiB, with 16 domain and 64 proof limits.
Handles have a separate 64-entry limit, 16 entries per UID, 1 MiB per retained
template, and an 8 MiB template sublimit within that pool. Control metadata is
also reserved from the pool. Conservative template reservation occurs before
compilation. Pending builders and retired, leased templates continue to count
against their quotas until actual destruction.

The monotonic idle TTL is five minutes. Release, expiry, or stop retires the
registration; in-flight leases remain valid and charged. Failed preparation,
failed preparation response delivery, and allocation failure refund ownership.
The server stops and joins workers before clearing its selector registry.
Registry locks do not cover backend execution or payload destruction.

## Measurement boundary

Warm executions can avoid repeated path parsing/compilation and long domain
keys. Preparation, first execution, and invalidation work are separate costs.
Existing authorization, fee summaries/fallbacks, result validation, and owned
value delivery remain part of execution. Client scope validation may still
depend linearly on the supplied path count.

GRIB owner-backed candidate domains can borrow the already retained selection,
actual-principal proof, and successfully published Adapter-domain fee batches
into a staged summary. The format-neutral read-context bridge binds only an
immutable batch, after publication; it retains no prepared query or request
ledger. Summary compilation waits until the domain's batches are complete,
reserves wrapper and temporary union storage from the existing shared pool,
and refunds temporary storage. Quota refusal leaves ordinary replay available.
A principal proof and its summary remain charged until their last pin exits.

Each request still checks the current owner head and performs all three GRIB
admissions. The first principal proof is charged in the summary; later identical
proof replays use the existing request-local batch memo. A normal fully warmed
GRIB discovery therefore advances three distinct summary stages. Changing the
principal can reuse only a pointer-identical already charged prefix. Head
changes, cache retirement, unsupported index requests and failed publication
cannot supply a replacement authorization or waive a fee.

Summary preflight cannot combine or reorder budget failures: insufficient budget
falls back to the original sorted batch, preserving the successful charge prefix
and first error. An active collector defers optional compilation and receives
every original identity. Installing a collector after a borrowed prefix exits
aggregation for that ledger, retaining its charged maxima and ordinary collector
replay. HDF5 dataset discovery retains its existing four-stage behavior.

Profiling separates logical evidence size, actual prepared-batch ledger visits
and summary skips. Collector replay is explicitly marked when it bypasses those
counters; a missing optional counter must not be interpreted as a measured zero.
