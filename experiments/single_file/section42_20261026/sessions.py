#!/usr/bin/env python3
"""Directly timed section 4.2 independent Q sessions.

The caller owns the private mount, resource envelope, immutable source identity,
and format-specific readers. No daemon, database, source or configuration is
changed here. A read callback must allocate a new independently owned result;
return (observation, audit_payload). Hash/oracle/profile work belongs in audit,
which runs after the actual session closes and its wall clock stops.
"""
from __future__ import annotations

import time
from typing import Any, Callable

PROTOCOL = "iotdbfs-section42-independent-session/1"
Q_VALUES = (1, 10, 100)


def require(ok: bool, message: str) -> None:
    if not ok:
        raise ValueError(message)


def run_session(*, q: int, prepare_reuse: int,
                prepare: Callable[[], Any],
                read_owned: Callable[[Any, int], tuple[dict, Any]],
                release: Callable[[Any], None],
                audit: Callable[[dict, Any], None] | None = None,
                deferred_audits: list | None = None,
                clock: Callable[[], int] = time.perf_counter_ns) -> dict:
    """Execute exactly Q new reads, never a prefix of another Q session.

    prepare_reuse=0: prepare and release around every read; query T includes
    that per-query preparation and release. prepare_reuse=1: one preparation
    and one release for this independent session; query T includes only read.
    Session wall always includes preparation, all Q reads, and final release.
    On read failure release still runs and the exception propagates; no partial
    observations can be mistaken for a complete scientific session.
    """
    require(type(q) is int and q in Q_VALUES, "Q must be one of 1/10/100")
    require(type(prepare_reuse) is int and prepare_reuse in (0, 1),
            "prepare_reuse must be 0/1")
    rows, payloads = [], []
    prepare_ns = release_ns = 0
    begin = clock()
    state = None
    if prepare_reuse:
        prepared = clock()
        state = prepare()
        prepare_ns = clock() - prepared
    try:
        for index in range(q):
            query_begin = clock()
            current = state
            per_prepare_ns = per_release_ns = 0
            if not prepare_reuse:
                opened = clock()
                current = prepare()
                per_prepare_ns = clock() - opened
                prepare_ns += per_prepare_ns
            try:
                read_begin = clock()
                observation, payload = read_owned(current, index)
                read_end = clock()
            finally:
                if not prepare_reuse:
                    closed = clock()
                    release(current)
                    per_release_ns = clock() - closed
                    release_ns += per_release_ns
            query_end = clock()
            reader_discovery = observation.get("discovery_ns", 0)
            require(type(reader_discovery) is int and reader_discovery >= 0,
                    "invalid reader discovery clock")
            discovery_ns = per_prepare_ns + reader_discovery
            rows.append(dict(observation, index=index,
                             query_begin_ns=query_begin, query_end_ns=query_end,
                             reader_total_ns=observation.get("total_ns"),
                             total_ns=query_end-query_begin,
                             discovery_ns=discovery_ns,
                             value_and_delivery_ns=query_end-query_begin-discovery_ns,
                             read_owned_wall_ns=read_end-read_begin,
                             per_query_prepare_ns=per_prepare_ns,
                             per_query_release_ns=per_release_ns))
            payloads.append(payload)
    finally:
        if prepare_reuse:
            closed = clock()
            release(state)
            release_ns = clock() - closed
    end = clock()
    # All observations are retained until the measured session closes. This
    # makes release/session timing independent of oracle and profile-tail waits.
    for row, payload in zip(rows, payloads):
        if deferred_audits is not None:
            deferred_audits.append((row, payload))
        elif audit:
            audit(row, payload)
        require(row["total_ns"] == row["discovery_ns"] + row["value_and_delivery_ns"],
                "T != d + q")
    require(len(rows) == q, "session omitted a query")
    result = {"protocol": PROTOCOL, "complete": True, "q": q,
              "prepare_reuse": prepare_reuse,
              "session_begin_ns": begin, "session_end_ns": end,
              "session_wall_ns": end-begin, "prepare_ns": prepare_ns,
              "release_ns": release_ns, "query_sum_ns": sum(r["total_ns"] for r in rows),
              "observations": rows,
              "session_wall_measured_directly": True,
              "audit_outside_session_wall": True,
              "audit_deferred_to_caller": deferred_audits is not None,
              "unbound_prepare_and_release_in_query_T": not bool(prepare_reuse)}
    require(result["session_wall_ns"] >= result["query_sum_ns"], "invalid session wall")
    return result


def iotdb_callbacks(*, control: Callable[[dict], tuple[dict, int]],
                    request: dict, read_owned: Callable[[dict, int], tuple[dict, Any]]):
    """Reject the historical experimental interface withdrawn in this build."""
    raise ValueError("Current production does not support experiment_typed_bind/read/release; "
                     "use independent normal catalog+typed requests with prepare_reuse=0")


def prepared_query_reuse_support() -> dict:
    return {"protocol": "iotdbfs-section42-prepared-query-capability/1",
            "supported": False,
            "reason": "The old private experiment_typed_bind/read/release interface was removed; "
                      "IOTDBFS_EXPERIMENT_FROZEN_TYPED is rejected by current production.",
            "evidence": ["src/main.cpp:716", "src/query/frontend/QueryRegistrationSocket.cpp"],
            "historical_binary_substitution_allowed": False,
            "normal_catalog_plus_fresh_typed_query_prepare_reuse": 0}


def run_native_session(*, reader, spec: dict, q: int, prepare_reuse: int,
                       audit: Callable[[dict, Any], None] | None = None,
                       deferred_audits: list | None = None) -> dict:
    """Use real same-library file handles, with no value/message cache."""
    if prepare_reuse:
        def prepare():
            session, _ = reader.open_session(spec)
            return session
        def release(session):
            reader.close_session(session)
    else:
        prepare = lambda: None
        release = lambda session: None
    result = run_session(q=q, prepare_reuse=prepare_reuse, prepare=prepare,
        read_owned=lambda session, index: reader.read(spec, session=session),
        release=release, audit=audit, deferred_audits=deferred_audits)
    result.update(method="native FS", format_id=spec["format_id"], case_id=spec["case_id"],
                  application_decoded_cache=False, application_encoded_cache=False,
                  native_library_cache_policy="format library defaults; HDF5 chunk cache can be reused by open handles",
                  metadata_reuse=bool(prepare_reuse),
                  each_query_performs_native_value_read=True)
    return result


def run_iotdb_session(*, context: dict, spec: dict, q: int,
                      read_iotdb: Callable,
                      audit: Callable[[dict, Any], None] | None = None,
                      deferred_audits: list | None = None) -> dict:
    """Exactly Q catalog+typed independent queries on the current product."""
    routes = set()
    def read(state, index):
        row, owned = read_iotdb(context, spec, discover=True)
        route = row.get("result_path")
        require(isinstance(route, str) and route not in routes,
                "IoTDB query did not materialize a new result route")
        routes.add(route)
        return row, owned
    result = run_session(q=q, prepare_reuse=0, prepare=lambda: None,
        read_owned=read, release=lambda state: None, audit=audit,
        deferred_audits=deferred_audits)
    result.update(method="IoTDB-FS", format_id=spec["format_id"], case_id=spec["case_id"],
                  fresh_catalog_and_typed_each_query=True,
                  prepared_reuse_support=prepared_query_reuse_support(),
                  unique_result_routes=len(routes))
    return result


def encoded_cache_ablation_support() -> dict:
    """Fail closed: current production has no independent encoded toggle."""
    return {"protocol": "iotdbfs-section42-encoded-cache-capability/1",
            "supported": False,
            "reason": "GRIB encoded messages unconditionally use the shared runtime cache; "
                      "no independent encoded_cache=0/1 option exists in this build.",
            "evidence": ["src/formats/grib/GribFormatQueryAdapter.cpp:839",
                         "src/query/core/DecodedFragmentCache.cpp:369"],
            "historical_old_new_binary_comparison_is_same_build_ablation": False,
            "changing_shared_cache_capacity_is_independent_encoded_toggle": False}
