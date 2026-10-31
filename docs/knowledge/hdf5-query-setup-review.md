# HDF5 query setup review (2026-10-07)

The review finding was reproduced. Before this change, one Adapter query validated its selected chunk layout in planning, then again in cursor creation and provider creation. Each validation compiled the chunk decoder and checked selected chunk origins. The provider compiled the decoder once more. A warm layout-page cache hit avoided storage I/O but copied encoded page bytes and repeated SHA-256 and record parsing.

The Adapter planning state now retains the validated working-byte reservation and immutable compiled decoder. The cursor and provider consume that prepared selection. The execution path still checks exact source identity, current transport capabilities and payload bounds; a plan cannot be executed with different decoder limits. Direct cursor callers prepare their own selection once.

The bounded layout cache now stores validated, parsed chunk records. A warm hit charges the same logical layout budget and filters records for the requested selection, without copying encoded page bytes or recalculating its digest. A miss still reads, checksums, parses, validates and then publishes the page. Cache keys retain namespace, exact path/version, revision, payload/layout sizes and page offset/length. Validated pages remain immutable and safe to retain in active plans after LRU eviction.

On the public GEOS `/T` direct-path probe, 24 before/after queries produced identical row counts, masks, value bytes and FNV hashes. Each after query recorded exactly one selection validation and one decoder compilation. On the 20 warm queries, page parsing fell from 2.319 ms accumulated to zero, and median execution setup fell from 0.015 to 0.010 ms. Median end-to-end latency was 4.884 ms before and 4.598 ms after. One before/after run is insufficient to attribute that full difference to this change; storage/catalog work still dominates these short queries. Raw profiles are in [the setup results](../hdf5-query-setup-results).

Verification: 70 focused HDF5 tests passed before the final safety assertions were added. After adding those assertions, all 1,338 unit tests passed. `git diff --check` passed.
