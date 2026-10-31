# Borrowed fee stage summary A/B

Run only the existing sealed independent FM1 HDF5 N8/C8 and N128/C128 V1/S0/latest cases. No new corpus, publication, native binding or index rebuild.

A is accepted f1b896244f2a7cbb1abc43b7f08c1386178e2cd9f59755b4ee06c11bbd843087. B overlays only QueryMetadataBudget.h and IoTDBStructuredBlobBackend.cpp with borrowed immutable batches and stage summaries; prior unaccepted selector changes are excluded.

Use the unchanged NativeABRunner resource/service/oracle/owned-output and statistical protocol: four AB/BA/BA/AB campaigns, both IoTDB-FS and FS-index, five warmups and ten measured queries per arm per mount. Each tier has 240 rows; 480 total, including 320 measurements. N8 must complete and restore before N128. T=d+q includes client-owned values and masks. Ratios compare block medians, paired within campaign; no slow-row exclusions or historical substitutions.

Formal B discovery profiles must explicitly report four summary stages, positive skipped entries, zero prepared-loop visits, zero fallback and zero collector exits. Missing A counters remain unobserved. Collector dispatch has independent per-item forwarding and is tested semantically; its loop is outside the prepared-replay visit counter.

Compile/bind source and both XML test gates before prepare-only/verify-prepared. Invoke run.sh with the same path arguments as NativeABRunner. Save all rows and resource telemetry on the server. This is a sequential warm-query experiment; it makes no cold-disk or concurrency performance claim.
